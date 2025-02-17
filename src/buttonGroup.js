import React from 'react';
import { NavigationControl } from 'react-map-gl';

function ButtonGroup({
  handleZoomIn,
  zoomButtonDisabled,
  handleZoomInJeju,
  handleHomeClick,
  showSearch,
  setShowSearch,
  setSearchTerm,
  setLeftPaneIsOpen,
  leftPaneIsOpen,
  chargerNameSearchTerm,
  setChargerNameSearchTerm
}) {

    return (
    <div>
        <div id='zoom-button-container' className="zoom-button-container">
          <NavigationControl showCompass={false} showZoom={false} />
          <button className="zoom-in-button" onClick={handleZoomIn} disabled={zoomButtonDisabled}>확대</button>
        </div>  

        <div className="jeju-button-container">
        <button className="jeju-button" onClick={handleZoomInJeju} disabled={zoomButtonDisabled}>
          Jeju
        </button>
      </div>
          <button
        onClick={() => handleHomeClick()}
        className="home-button"
      >
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M12 1.5l10 9v11h-7v-6h-6v6h-7v-11l10-9z" />
        </svg>
      </button>

      <button
        onClick={() => {
          setShowSearch(!showSearch);
          if (showSearch) {
            setSearchTerm('');
          }
        }}
        className="search-button"
      >
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 24 24"
          width="24"
          height="24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2.2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <circle cx="11" cy="11" r="6" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
      </button>
      {showSearch && (
        <div className="search-input-container">
          <input
          type="text"
          value={chargerNameSearchTerm}
          onChange={(e) => setChargerNameSearchTerm(e.target.value)}
          onRequestClose={(e) => setChargerNameSearchTerm('')}
          placeholder="검색할 충전소 입력"
          className="search-input"
        />
      </div>
      )}
      <div className="title-container">
        <h1 className="title-main">배터리 충전소 데이터</h1>
        <h1 className="title-sub">서울, 경기/인천, 제주 테스트</h1>
      </div>

      <button onClick={() => setLeftPaneIsOpen(!leftPaneIsOpen)} className="info-button">
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 24 24"
          width="24"
          height="24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3" />
        </svg>
      </button>
    </div>
    );
} 

export default ButtonGroup;
