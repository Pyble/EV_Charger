// leftPane 으로 export
import React from 'react';

const PublishingPage = ({ showChart, charger_name, charger_id, mnfacr_name, model_name }) => {

  return(
  <div>
    <h2 style={{ color:'white' }}>Rendering Panel</h2>
    {showChart && (
      <>
        <p style={{ color: 'white' }}>{charger_name}</p>
        <p style={{ color: 'white' }}>{charger_id}</p>
        <p style={{ color: 'white' }}>{mnfacr_name}/{model_name}</p>
      </>
    )}
  </div>
);
};
export default PublishingPage;
