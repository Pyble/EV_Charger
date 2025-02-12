from datetime import datetime, timedelta
from copy import deepcopy
import requests
import json

import logging

logger = logging.getLogger(__name__)

# returns list type
def parse_master_file(url: str, token: str, reg_no: str, save_dir: str):
	# Protocol Type, Signal, CANID, PID, LID, HL/LH, Expression, Value type(optional)]
	column_indices = [4, 5, 11, 12, 13, 15, 16]
	sheet_no = 0
	header_row = 5
	target_url = url + reg_no
	tkn = "Token {}".format(token)
	file_name = save_dir if save_dir.endswith(".xlsx") else "{}temp_master.xlsx".format(save_dir) if save_dir.endswith("/") else "{}/temp_master.xlsx".format(save_dir)
	target_url = url + reg_no
	try:
		r = requests.get(target_url, headers={'Authorization': tkn})
		with open(file_name, "wb") as file:
			file.write(r.content)
	except Exception as e:
		logger.error("Error occurred on getting excel file: {}".format(e))
		return None

	try:
		return Reader.read_excel_file(file_name, column_indices, sheet_no, header_row)
	except CloudException as e:
		logger.error("Error occurred on reading excel file: {}".format(e))
		return None


# returns json type 
def parse_master_file_as_json(url: str, token: str, reg_no: str, save_dir: str):
	parse = parse_master_file(url, token, reg_no, save_dir)
	if not parse:
		return None

	r_dict = {"data": parse}
	return json.dumps(r_dict)


def divide_long_short_msg(can_msgs: list, parse_info: list):
	group = ExprGroup.generate_from_list(None, parse_info)
	short_msgs = []
	long_msgs = []
	junk_msgs = []
	for msg in can_msgs:
		src_bytes = bytes.fromhex(msg["PAYLOAD"])
		can_id = hex(int.from_bytes(src_bytes[8:12], byteorder="little"))[2:]
		if group.is_broadcast_can_id(can_id):
			short_msgs.append(msg)
			continue

		long_idx = 0
		if group.is_extended_can_id(can_id):
			long_idx = 13
		elif group.is_normal_can_id(can_id):
			long_idx = 12
		else:
			junk_msgs.append(msg)
			continue

		if long_idx > 0 and src_bytes[long_idx] >= 0x10:
			long_msgs.append(msg)
		else:
			short_msgs.append(msg)

	return [short_msgs, long_msgs, junk_msgs]


def divide_long_short_msg_from_json(can_msgs: list, json_str: str):
	parse_dict = json.loads(json_str)
	parse_info = parse_dict["data"]
	return divide_long_short_msg(can_msgs, parse_info)


def stack_to_json(json_points: dict, col_data: dict):
	ts = col_data["time_stamp"]
	key = datetime.strftime(col_data["time_stamp"], "%Y%m%d%H%M%S")

	if key not in json_points:
		json_points[key] = {"time_stamp": datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second), "values": col_data["values"]}
	else:
		for k in col_data["values"]:
			if "fault" in k.lower() and "code" in k.lower() and k in json_points[key]["values"]:
				json_points[key]["values"][k] += ("/" + col_data["values"][k])
			else:
				json_points[key]["values"][k] = col_data["values"][k]


def get_send_body(json_points: dict):
	send_body = []
	for key in json_points:
		ts_kst = json_points[key]["time_stamp"]
		ts_uct = ts_kst + timedelta(hours=-9)
		new_point = {}
		new_point["time"] = ts_uct
		new_point["fields"] = json_points[key]["values"]
		new_point["fields"]["Date"] = datetime.strftime(ts_kst, "%Y-%m-%d")
		new_point["fields"]["SystemTime"] = datetime.strftime(ts_kst, "%H%M%S.%f")
		send_body.append(new_point)

	return send_body


# msg_type: 1 for short, 2 for long, 3 for all
def convert_msg(can_msgs: list, msg_type: int, parse_info: list, topic_name: str = "TempTopic"):
	group = ExprGroup.generate_from_list(None, parse_info)
	topic_unit = TopicUnit(topic_name, group)

	cpy_msgs = deepcopy(can_msgs)
	json_points = {}
	del_list = []
	idx_count = 0
	for msg in cpy_msgs:
		if "PAYLOAD" not in msg or not msg["PAYLOAD"]:
			idx_count += 1
			continue

		ret_dict = None
		try:
			src_bytes = bytes.fromhex(msg["PAYLOAD"])
			if src_bytes is None:
				idx_count += 1
				continue
			can_id = hex(int.from_bytes(src_bytes[8:12], byteorder="little"))[2:]
			if group.is_broadcast_can_id(can_id):
				if msg_type != 2:
					ret_dict = topic_unit.add_byte_stream(src_bytes, idx_count)
			elif group.is_extended_can_id(can_id):
				is_long = src_bytes[13] >= 0x10 or src_bytes[13] >= 0x20
				if (is_long and msg_type >= 2) or (not is_long and msg_type != 2):
					ret_dict = topic_unit.add_byte_stream(src_bytes, idx_count)
			elif group.is_normal_can_id(can_id):
				is_long = src_bytes[12] >= 0x10 or src_bytes[12] >= 0x20
				if (is_long and msg_type >= 2) or (not is_long and msg_type != 2):
					ret_dict = topic_unit.add_byte_stream(src_bytes, idx_count)
		except Exception as e:
			# print("Error occurred on byte stream {}: {}".format(msg, e))
			idx_count += 1
			continue

		if ret_dict:
			try:
				if "ret_val" in ret_dict and ret_dict["ret_val"] is not None:
					stack_to_json(json_points, ret_dict["ret_val"])
					del_list.extend(ret_dict["del_idx"])
			except Exception as e:
				# print("Error occurred on stacking {}: {}".format(ret_dict["ret_val"], e))
				pass
		idx_count += 1

	converted_msgs = get_send_body(json_points)
	remained_msgs = cpy_msgs
	del_list.sort(reverse=True)
	for idx in del_list:
		del remained_msgs[idx]

	return [converted_msgs, remained_msgs]


def convert_msg_from_json(can_msgs: list, msg_type: int, json_str: str, topic_name: str = "TempTopic"):
	parse_dict = json.loads(json_str)
	parse_info = parse_dict["data"]
	return convert_msg(can_msgs, msg_type, parse_info)

