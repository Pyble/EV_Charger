import React from 'react';

function Tooltip({ tooltipInfo, theme }) {
    return (
    <div className='hole-tooltip'>    
      <div style={{
          position: 'absolute',
          pointerEvents: 'none',
          left: tooltipInfo.x + 5,
          top: tooltipInfo.y - 60,
          zIndex: 0,
      }}>
          <div style={{
              position: 'relative',
              display: 'flex',
              flexDirection: 'row',
              alignItems: 'center',
              color: '#063561',
              fontFamily: 'Arial, sans-serif',
              fontSize: '15px',
              lineHeight: '18px',
              fontWeight: 'bold',
              letterSpacing: '0.03em',
          }}>
              <div style={{
                  borderLeft: theme === 'light' ? '2px solid #063561' : '2px solid #00a5a5',
                  height: '30px',
                  position: 'absolute',
                  left: '0',
                  bottom: '-5px',
                  transform: 'rotate(40deg)',
                  transformOrigin: 'bottom left',
              }} />
              <div style={{marginLeft: '20px', marginBottom: '25px'}}>
                  <span style={{borderBottom: theme === 'light' ? '2px solid #063561' : '2px solid #00a5a5', paddingBottom: '8px'}}>
                      {tooltipInfo.text}
                  </span>
              </div>
          </div>
      </div>
    </div>
    );
  }

  export default Tooltip;
  
 
  