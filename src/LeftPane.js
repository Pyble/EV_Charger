//publishing page export
import React from 'react';
import SlidingPane from 'react-sliding-pane';
import PublishingPage from './PublishingPage';

const LeftPane = ({ isOpen, handleClosePane, 
  clickedChargerId, showChart, clickedChargerName, clickedMnfacrName, clickedModelName}) => {

  return (
    <div
        onClick={handleClosePane}
        style={{width: '100%', height: '100%'}}
        >
    <SlidingPane
      isOpen={isOpen}
      from="bottom"
      width="334px"
      onRequestClose={handleClosePane}
      className="left2-pannel"
    >
        <PublishingPage  showChart={showChart} charger_name={clickedChargerName} 
        charger_id={clickedChargerId} mnfacr_name={clickedMnfacrName} model_name={clickedModelName} />
    </SlidingPane>
    </div>
  );
}

export default LeftPane;
