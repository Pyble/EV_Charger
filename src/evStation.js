import React, { useState, useEffect } from 'react';
import MapboxGL from 'mapbox-gl';
import DeckGL from '@deck.gl/react';
import { InteractiveMap } from 'react-map-gl';
import { ColumnLayer, IconLayer, PathLayer } from '@deck.gl/layers';
import 'react-sliding-pane/dist/react-sliding-pane.css';
import { LinearInterpolator } from '@deck.gl/core';
import './evStation.css';
import searchTerms from './searchTerms';
import rightPane from './rightPane';
import leftPane from './leftPane';
import searchFilterPane from './searchFilterPane';
import tooltip from './toolTip';
import buttonGroup from './buttonGroup';

MapboxGL.accessToken = '';

const DATA_URL = '';

const INITIAL_VIEW_STATE = {
  longitude: 127.7,
  latitude: 36.1,
  zoom: 7,
  maxZoom: 16,
  pitch: 57,
  bearing: -15
};

export default function Map() {
  const [paneIsOpen, setPaneIsOpen] = useState(false);
  const [clickedChargerId, setClickedChargerId] = useState(null);
  const [clickedChargerName, setClickedChargerName] = useState(null);
  const [clickedModelName, setClickedModelName] = useState(null);
  const [clickedMnfacrName, setClickedMnfacrName] = useState(null);
  const [leftPaneIsOpen, setLeftPaneIsOpen] = useState(false);
  const [rightPaneIsOpen, setRightPaneIsOpen] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [manufacturer, setManufacturer] = useState("");
  const [manufacturers, setManufacturers] = useState([]);
  const [filteredResults, setFilteredResults] = useState([]);
  const [data, setData] = useState([]);
  // eslint-disable-next-line
  // const [radiusData, setRadiusData] = useState(Array.from({ length: data.length }, () => Math.random() * 5 + 2));
  // eslint-disable-next-line
  const [isClicked, setIsClicked] = useState(false);
  const [showSearch, setShowSearch] = useState(false);
  const [selectedAddress, setSelectedAddress] = useState(null);
  const [mapViewState, setMapViewState] = useState(INITIAL_VIEW_STATE);
  const [zoomButtonDisabled, setZoomButtonDisabled] = useState(false);
  const [voltType, setVoltType] = useState('');
  const [voltTypes, setVoltTypes] = useState([]);
  const [efficiencyValue, setEfficiencyValue] = useState('');
  const [efficiencyValues, setEfficiencyValues] = useState([]);
  const [filterStep, setFilterStep] = useState(0);
  const [selectedFilters, setSelectedFilters] = useState({region:'', manufacturer:'', voltType:'', efficiencyValue:''});
  const [hasZoomedIn, setHasZoomedIn] = useState(false);
  const [sortOrder, setSortOrder] = useState('asc');
  const [tooltipInfo, setTooltipInfo] = useState(null);
  const [mapStyle] = useState('mapbox://styles/djangbogo/cli8sh8dd010d01po64z6hkyl');
  // eslint-disable-next-line

  const [theme] = useState('');
  const [color] = useState('');
  // eslint-disable-next-line
  const [chargerNameSearchTerm, setChargerNameSearchTerm] = useState('');
  const [showAllData, setShowAllData] = useState(false);
  const [elevationFactor, setElevationFactor] = useState(0);

  // 클릭 시 지도 스타일 밝기 변경
  // const toggleMapStyle = () => {
  //   if (mapStyle === 'mapbox://styles/djangbogo/cli8sh8dd010d01po64z6hkyl') {
  //     setMapStyle('mapbox://styles/djangbogo/clguo7jun002c01r8eny34b5q');
  //     setTheme('dark');
  //     setColor('cyan');
  //   } else {
  //     setMapStyle('mapbox://styles/djangbogo/cli8sh8dd010d01po64z6hkyl');
  //     setTheme('light');
  //     setColor('#212163');
  //   }
  // }

  // 낮과 밤 시간대에 따른 화면 밝기 변경
  // const updateTheme = () => {
  //   const currentHour = new Date().getHours();
  //   // if (currentHour >= 6 && currentHour < 18) {
  //     if (currentHour > 0) {
  //     setMapStyle('mapbox://styles/djangbogo/cli8sh8dd010d01po64z6hkyl');
  //     setTheme('light');
  //     setColor('#212163');
  //   } else {
  //     setMapStyle('mapbox://styles/djangbogo/clguo7jun002c01r8eny34b5q');
  //     setTheme('dark');
  //     setColor('cyan');
  //   }
  // }

  const validData = data.filter(d => {
    return d.geometry.coordinates[0] !== "NULL" &&
    d.geometry.coordinates[1] !== "NULL" &&
    parseFloat(d.geometry.coordinates[0]) !== 0 &&
    parseFloat(d.geometry.coordinates[1]) !== 0
  });

  let lastDataPoint;
  if (validData.length > 0) {
    lastDataPoint = validData.reduce((prev, curr) => 
      prev.properties.systemtime > curr.properties.systemtime ? prev : curr
    );
  } else {
    lastDataPoint = null;
    // console.error('No data available');
  }
  
  const paths = validData.reduce((acc, curr) => {
    if (acc.length === 0 || acc[acc.length - 1][0] !== curr.geometry.coordinates[0] || acc[acc.length - 1][1] !== curr.geometry.coordinates[1]) {
      acc.push(curr.geometry.coordinates);
    }
    return acc;
  }, []);

  const layers = [
    showAllData ?
    new ColumnLayer({
      id: 'column-layer',
      data: validData,
      pickable: true,
      extruded: true,
      radius: 200,
      diskResolution: 12,
      elevationScale: 2,
      getPosition: d => d.geometry.coordinates,
      getFillColor: d => {
        return d.properties.speed < 0 ? [0, 0, 255] : [255, 0, 0];
      },
      getElevation: d => Math.abs(d.properties.speed) * elevationFactor,
      onClick: () => setShowAllData(!showAllData),
      onHover: ({object, x, y}) => {
        if (object) {
            setTooltipInfo({
                text: ("curr_speed - avg_speed: " + object.properties.speed + "km/h"),
                x: x,
                y: y
            });
        } else {
            setTooltipInfo(null);
        }
      }
    }) : null,

    new IconLayer({
      id: 'icon-layer',
      data: [lastDataPoint],
      pickable: true,
      iconAtlas: 'http://127.0.0.1:5500/public/car.png', 
      iconMevstationing: {
        marker: {x: 10, y: 150, width: 512, height: 512, mask: false}
      },
      getIcon: d => 'marker',
      getSize: d => 55,
      getPosition: d => [d.geometry.coordinates[0] - 0.019, d.geometry.coordinates[1]],
      onClick: () => setShowAllData(!showAllData), 
    }),

    showAllData ?
    new PathLayer({
      id: 'path-layer',
      data: [{path: paths}],
      getPath: d => d.path,
      getColor: [8, 128, 121, 255],
      widthMinPixels: 15,
      rounded: true,
      pickable: true,
    }) : null
  ];

  useEffect(() => {
    let intervalId;

    if (showAllData) {
      intervalId = setInterval(() => {
        setElevationFactor(prevFactor => {
          // updateTheme();
          if (prevFactor < 14) {
            return prevFactor + 0.5;
          } else {
            clearInterval(intervalId);
            return prevFactor;
          }
        });
      }, 23);
    }

    return () => {
      if (intervalId) {
        clearInterval(intervalId);
      }
      setElevationFactor(0);
    }
  }, [showAllData]);

  // const handleClick = () => {
  //   setShowAllData(prevState => !prevState);
  // };

  const objectToCSVRow = (dataObject) => {
    let dataArray = [];
    for (let o in dataObject) {
      let innerValue = dataObject[o] === null ? '' : '"' + dataObject[o].toString() + '"';
      dataArray.push(innerValue);
    }
    return dataArray.join(',') + '\r\n';
  }

  // const objectToRow = (dataObject) => {
  //   let ArrayD = [];
  //   for (let i in dataObject) {
  //     let innerValue = dataObject[i] === null ? '' : '"' + dataObject[i].toString() + '"';
  //     ArrayD.push(innerValue);
  //   }
  //   return ArrayD.join(',') + '\r\n';
  // }
  const propertiesData = data.map(d => d.properties);
  const selectedPropertiesData = propertiesData.filter(item => {
    const regionMatch = selectedFilters.region 
    ? (selectedFilters.region.includes('/') 
        ? selectedFilters.region.split('/').some(r => item.address.includes(r)) 
        : item.address.includes(selectedFilters.region)
      ) 
    : true;
    const manufacturerMatch = selectedFilters.manufacturer ? item.mnfacr_name === selectedFilters.manufacturer : true;
    const voltTypeMatch = selectedFilters.voltType ? item.volt_type === selectedFilters.voltType : true;
    const efficiencyValueMatch = Number(selectedFilters.efficiencyValue) ? Number(item.charging_efficiency) === Number(selectedFilters.efficiencyValue) : true;
    return regionMatch && manufacturerMatch && voltTypeMatch && efficiencyValueMatch
  }); 
  
  const convertToCSV = (selectedPropertiesData) => {
    let csvContent = '\uFEFF';
    // to header (key value) column name
    csvContent += objectToCSVRow(Object.keys(selectedPropertiesData[0])); 
  
    for (let i = 0; i < propertiesData.length; i++) {
      csvContent += objectToCSVRow(selectedPropertiesData[i]);
    }
    return csvContent;
  }
  
  const downloadCSV = (csvContent, fileName) => {
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
  
    const link = document.createElement('a');
    link.href = url;
    link.download = fileName;
  
    const clickEvent = new MouseEvent('click', {
      view: window,
      bubbles: true,
      cancelable: false
    });
    link.dispatchEvent(clickEvent);
  
    URL.revokeObjectURL(url);
  }

  const toggleSortOrder = () => {
    setSortOrder(sortOrder === 'desc' ? 'asc' : 'desc');
  };

  const sortResults = (results) => {
    return results.sort((a, b) => {
      if (sortOrder === 'asc') {
        return a.properties.charging_efficiency - b.properties.charging_efficiency;
      } else {
        return b.properties.charging_efficiency - a.properties.charging_efficiency;
      }
    });
  };

  const efficiencyArray = filteredResults.map(result => parseFloat(result.properties.charging_efficiency));
  const avgEfficiency = efficiencyArray.reduce((acc, val) => acc + val, 0) / efficiencyArray.length;
  const minEfficiency = Math.min(...efficiencyArray);
  const maxEfficiency = Math.max(...efficiencyArray);

  
  const handleZoomIn = () => {
    setZoomButtonDisabled(false);
    const interpolator = new LinearInterpolator({
      transitionProps: ['longitude', 'latitude', 'zoom'],
    });
    setMapViewState((mapViewState) => ({
      ...mapViewState,
      longitude: 127.0053101,
      latitude: 37.4199248,
      zoom: 10.3,
      transitionDuration: 800,  
      transitionInterpolator: interpolator,
    }));
  };

  const handleZoomInJeju = () => {
    setZoomButtonDisabled(true);
    const interpolator = new LinearInterpolator({
      transitionProps: ['longitude', 'latitude', 'zoom'],
    });
    setMapViewState((viewState) => ({
      ...viewState,
      longitude: 126.5253101,
      latitude: 33.3999248,
      zoom: 10.2,
      transitionDuration: 800,  
      transitionInterpolator: interpolator,
    }));
  };

  const handleZoomOut = () => {
    const interpolator = new LinearInterpolator({
      transitionProps: ['longitude', 'latitude', 'zoom'],
    });
  
    setMapViewState({
      ...INITIAL_VIEW_STATE,
      transitionDuration:800,
      transitionInterpolator: interpolator,
    });
  };
  // isClicked ?  left2pane(setPaneIsOpen) info link
  // const handleClick = ({ x, y, object }) => {
  //   if (object && object.Longitude !== "NULL" && object.Latitude !== "NULL") {
  //   setIsClicked(true);
  //   setPaneIsOpen(false);
  //   setRightPaneIsOpen(false);
  //   setClickedChargerName(object.properties.charger_name);
  //   setChargingEfficiency(1);
  //   }
  // };
  
  const handleClosePane = () => {
    setIsClicked(false);
    setPaneIsOpen(false);
    setRightPaneIsOpen(false);
  };

  const handleHomeClick = () => {
    setPaneIsOpen(false);
    setIsClicked(false);
    setSelectedAddress(null);
    setSearchTerm('');
    setShowSearch(false);
    setLeftPaneIsOpen(false);
    handleZoomOut();
    setZoomButtonDisabled(false);
    setChargerNameSearchTerm('');
    setHasZoomedIn(false);
    setManufacturer('');
    setVoltType('');
    // setSpeed('');
    setSelectedFilters([]);
    setFilterStep(0);
  };


  const handleAddressClick = (charger_id) => {
    if (selectedAddress === charger_id) {
      setSelectedAddress(null);
      setPaneIsOpen(false);
    } else {
      setSelectedAddress(charger_id);
      setPaneIsOpen(true);
      const clickedPoint = validData.find((d) => d.properties.charger_id === charger_id);
      if (clickedPoint) {
        setMapViewState({
          latitude: clickedPoint.geometry.coordinates[1],
          longitude: clickedPoint.geometry.coordinates[0],
          zoom: 10.3,
          pitch: 60,
        });
      }
    }
  };

  

  // if soc under => scatter change
  useEffect(() => {
    if (manufacturer !== "" && !Object.values(selectedFilters).includes(manufacturer)) {
      setSelectedFilters(prevFilters => ({ ...prevFilters, manufacturer: manufacturer }));
    }
  }, [manufacturer, selectedFilters]);
  
  useEffect(() => {
    if (voltType !== "" && !Object.values(selectedFilters).includes(voltType)) {
      setSelectedFilters(prevFilters => ({ ...prevFilters, voltType: voltType }));
    }
  }, [voltType, selectedFilters]);

  // useEffect  efficiency
  useEffect(() => {
    if (efficiencyValue !== "" && !Object.values(selectedFilters).includes(efficiencyValue)) {
      setSelectedFilters(prevFilters => ({ ...prevFilters, efficiencyValue: efficiencyValue }));
    }
  }, [efficiencyValue, selectedFilters])

  // if curr_speed - avg_speed
  // const [speed, setSpeed] = useState('');
  // useEffect(() => {
  //   if (speed !== "" && !Object.values(selectedFilters).includes(speed)) {
  //     setSelectedFilters(prevFilters => ({ ...prevFilters, speed: speed}));
  //   }
  // }, [speed, selectedFilters])


  useEffect(() => {
  const handleKeyDown = (event) => {
    if (event.key === 'Escape') {
      if (selectedAddress) {
        setSelectedAddress(null);
        setPaneIsOpen(false);
        setLeftPaneIsOpen(false);
      } else if (!leftPaneIsOpen) {
        handleZoomOut();
      }
    }
  };

  if (selectedAddress || !leftPaneIsOpen) {
    window.addEventListener('keydown', handleKeyDown);
  }

  return () => {
    window.removeEventListener('keydown', handleKeyDown);
  };
}, [selectedAddress, leftPaneIsOpen]);
  
useEffect(() => {
  fetch(DATA_URL)
    .then(response => response.json())
    .then(json => {
      console.log(json); 
      setData(json.features);
      // setRadiusData(Array.from({ length: json.features.length }, () => Math.random() * 5 + 3));
    })
    .catch(error => console.log(error));
}, []);

// useEffect(() => {
//   const interval = setInterval(() => {
//     setRadiusData(Array.from({ length: data.length }, () => Math.random() * 5 + 5));
//     if (!isClicked && !selectedAddress) {
//       setRadiusData(Array.from({ length: data.length }, () => Math.random() * 5 + 3));
//     }
//   }, 100); 

//   return () => {
//     clearInterval(interval);
//   };
// }, [isClicked, selectedAddress, data.length]);

useEffect(() => {
  const selectedData = validData.find(d => d.properties.charger_id === selectedAddress);
  if (selectedData) {
    setClickedChargerId(selectedData.properties.charger_id);
    setClickedChargerName(selectedData.properties.charger_name);
    setClickedMnfacrName(selectedData.properties.mnfacr_name);
    setClickedModelName(selectedData.properties.model_name);
  } 
}, [selectedAddress, validData]); 

  useEffect(() => {
    const fetchData = async () => {
      const response = await fetch('http://v2cloud.iptime.org:5000/charger');
      const data = await response.json();
  
      const uniqueManufacturers = Array.from(new Set(data.feature.map(feature => feature.properties.mnfacr_name))).filter(Boolean);
      setManufacturers(uniqueManufacturers);

      const uniqueVoltTypes = Array.from(new Set(data.features.map(feature => feature.properties.volt_type))).filter(Boolean);
      setVoltTypes(uniqueVoltTypes);

      const uniqueEfficiencys = Array.from(new Set(data.features.map(feature => parseFloat(feature.properties.charging_efficiency)))).filter(Boolean);
      setEfficiencyValues(uniqueEfficiencys);

      let filterFunction;
  
      if (searchTerm === "") {
        filterFunction = (feature) => true;
        
      
      } else if (searchTerm === "") {
        filterFunction = (feature) => {
          const address = feature.properties.speed;
          return !address.includes('') && !address.includes('') 
            && (manufacturer === '' || feature.properties.mnfacr_name === manufacturer) 
            && (voltType === '' || feature.properties.volt_type === voltType)
            && (efficiencyValue === '' || Math.abs(feature.properties.charging_efficiency - Number(efficiencyValue)) <= 0.0001)

        };
      } else if (["", ""].includes(searchTerm)) {
        filterFunction = (feature) => {
          const address = feature.properties.speed;
          return searchTerms.some((term) => address.includes(term) && searchTerm === term) 
            && (manufacturer === '' || feature.properties.mnfacr_name === manufacturer) 
            && (voltType === '' || feature.properties.volt_type === voltType)
            && (efficiencyValue === '' || Math.abs(feature.properties.charging_efficiency - Number(efficiencyValue)) <= 0.0001)

        };
      } else {
        filterFunction = (feature) => {
          return (feature.properties.speed.includes("") || feature.properties.speed.includes("")) 
            && (manufacturer === '' || feature.properties.mnfacr_name === manufacturer) 
            && (voltType === '' || feature.properties.volt_type === voltType)
            && (efficiencyValue === '' || Math.abs(feature.properties.charging_efficiency - Number(efficiencyValue)) <= 0.0001)

        };
      }
      
      const results = data.features.filter(filterFunction);
      setFilteredResults(results);
      
    };
  
    fetchData();
  }, [searchTerm, manufacturer, voltType, efficiencyValue]); 


  return (
    <div>
      <DeckGL
        layers={layers}
        viewState={mapViewState}
        onViewStateChange={({ viewState }) => setMapViewState(viewState)}
        controller={{ doubleClickZoom: false, scrollZoom: true, dragRotate: true, dragPan: true }}
        getCursor={({ isHovering }) => 'default'}
      >
  <InteractiveMap reuseMap mapStyle={mapStyle}  preventStyleDiffing={true} mapboxApiAccessToken={MapboxGL.accessToken} >
  {tooltipInfo && <Tooltip tooltipInfo={tooltipInfo} theme={theme} />}
        <RightPane isOpen={rightPaneIsOpen} handleClosePane={handleClosePane} />
        <LeftPane isOpen={paneIsOpen} handleClosePane={handleClosePane} showChart={true} 
        clickedChargerName={clickedChargerName} clickedChargerId={clickedChargerId} 
        clickedMnfacrName={clickedMnfacrName} clickedModelName={clickedModelName} />
        <SearchFilterPane 
          leftPaneIsOpen={leftPaneIsOpen}
          setLeftPaneIsOpen={setLeftPaneIsOpen}
          searchTerm={searchTerm}
          setSearchTerm={setSearchTerm}
          setSelectedFilters={setSelectedFilters}
          handleZoomOut={handleZoomOut}
          setHasZoomedIn={setHasZoomedIn}
          handleZoomIn={handleZoomIn}
          setFilterStep={setFilterStep}
          searchTerms={searchTerms}
          handleZoomInJeju={handleZoomInJeju}
          filterStep={filterStep}
          manufacturer={manufacturer}
          setManufacturer={setManufacturer}
          manufacturers={manufacturers}
          voltType={voltType}
          setVoltType={setVoltType}
          voltTypes={voltTypes}
          efficiencyValue={efficiencyValue}
          efficiencyValues={efficiencyValues}
          setEfficiencyValue={setEfficiencyValue}
          setEfficiencyValues={setEfficiencyValues}
          selectedFilters={selectedFilters}
          setRightPaneIsOpen={setRightPaneIsOpen}
          filteredResults={filteredResults}
          toggleSortOrder={toggleSortOrder}
          sortResults={sortResults}
          handleAddressClick={handleAddressClick}
          selectedAddress={selectedAddress}
          // eslint-disable-next-line no-unused-vars
          hasZoomedIn={hasZoomedIn}
          convertToCSV={convertToCSV}
          downloadCSV={downloadCSV}
          propertiesData={propertiesData}
          selectedPropertiesData={selectedPropertiesData}
          avgEfficiency={avgEfficiency}
          minEfficiency={minEfficiency}
          maxEfficiency={maxEfficiency}
          />
  </InteractiveMap>
  <ButtonGroup
    handleZoomIn={handleZoomIn}
    zoomButtonDisabled={zoomButtonDisabled}
    handleZoomInJeju={handleZoomInJeju}
    handleHomeClick={handleHomeClick}
    showSearch={showSearch}
    setShowSearch={setShowSearch}
    setSearchTerm={setSearchTerm}
    searchTerm={searchTerm}
    setLeftPaneIsOpen={setLeftPaneIsOpen}
    chargerNameSearchTerm={chargerNameSearchTerm}
    setChargerNameSearchTerm={setChargerNameSearchTerm}    
    />
      <div className="title-container">
        <h1 className="title-main" style={{color: color}}>TEST PathLayer</h1>
        {/* <h1 className="title-sub" style={{color: color}} onClick={toggleMapStyle}>서울, 경기/인천, 제주 테스트</h1> */}
      </div>
    </DeckGL>
     </div>
  );
}
