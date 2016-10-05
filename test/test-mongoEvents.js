'use strict';


const url = 'mongodb://localhost:27017/test'; // local
// remote mlab?
// mongodb://<user>:<password>@X.mlab.com:49556,X-a1.mlab.com:49556/<db-name>?replicaSet=rs-X


const MongoEvents = require('../lib/mongoEvents');

const sAction        = 'test-mongoEvents';
const options        = {
  sUrl          :  url,
  sCollection   : 'Events'  
}
MongoEvents.mdbSetConnection(options)
.then( (mdb) => {
  console.log({ action: sAction + '.worked' });

  // generate test data
  const nGet      = 10; // for gets to show intermediate state just get the first 10
  const N         = 100000;
  const center    = [-73.993549, 40.727248];
  const lowerLeft = [-74.009180, 40.716425];
  const deltaLon  = Math.abs(lowerLeft[0] - (-73.97725));
  const deltaLat  = Math.abs(lowerLeft[1] - (40.7518692));
  let   tPrevious = 1475431264754;

  let oBatch = {};
  for (let i = 0; i < N; i++) {
    const incidentLon = lowerLeft[0] + Math.random() * deltaLon;
    const incidentLat = lowerLeft[1] + Math.random() * deltaLat;
    tPrevious         += Math.random() * 60 * 1000; // random time after previous
    const oIncident = { key: '-k'+i, ll: [incidentLat,incidentLon], ts: tPrevious };
    oBatch[oIncident.key] = oIncident;
  }    

  // upsert
  mdb.upsert({ id  : '-k0', val :  oBatch['-k0'] })
  .catch( (err) => {
    console.error({action: sAction + '.upsert.err', err:err, stack: err.stack });
  })
  .then( () => {
    console.log({action: sAction + '.upsert.success'});    
    // batchUpsert
    return mdb.batchUpsert(oBatch);
  })
  .catch( (err) => {
    console.error({action: sAction + '.batchUpsert.err', err:err, stack: err.stack });
  })
  .then( () => {
    console.log({action: sAction + '.batchUpsert.success'});
    
    // nearby
    const options = {
      N               :   50,
      lowerLatitude   :   40.719543,
      lowerLongitude  :  -74.003881, 
      upperLatitude   :   40.735792,
      upperLongitude  :  -73.982166
    }
    return mdb.nearby(options);
  })
  .catch( (err) => {
    console.error({action: sAction + '.nearby.err', err:err, stack: err.stack });
  })
  .then( (aIncidentIds) => {
    console.log({action: sAction + '.nearby.success', aIncidentIds: aIncidentIds });
    
    // timeSpaceQuery
    const options = {
      N                   : 50,
      tStart              : 1475431264754,
      lonlat              : [ -73.993549, 40.727248 ],
      maxDistanceMeters   : 1000 
    }
    return mdb.timeSpaceQuery(options);
  })
  .catch( (err) => {
    console.error({action: sAction + '.timeSpaceQuery.err', err:err, stack: err.stack });
  })
  .then( (aIncidentIds) => {
    console.log({action: sAction + '.timeSpaceQuery.success', aIncidentIds: aIncidentIds });
    return mdb.get({},nGet);
  })
  .then( (oRes) => {
    console.log({ action: sAction + '.get.'+nGet, oRes: oRes });
    // remove one
    const id = '-k0';
    delete oBatch[id];
    return mdb.remove(id);
  })
  .catch( (err) => {
    console.error({action: sAction + '.remove.err', err:err, stack: err.stack });
  })
  .then( () => {
    console.log({action: sAction + '.remove.success' });
    return mdb.get({},nGet);
  })
  .then( (oRes) => {
    console.log({ action: sAction + '.get.'+nGet, oRes: oRes });
    // batch remove, clean up
    let aIds = Object.keys(oBatch);
    return mdb.batchRemove(aIds);    
  })
  .then( () => {
    console.log({action: sAction + '.remove.all.success' });
    return mdb.get({},nGet);
  })
  .then( (oRes) => {
    console.log({ action: sAction + '.get.'+nGet, oRes: oRes });
    console.log({action: sAction + '.allTests.success' });
    process.exit(0);
  })
  .catch( (err) => {
    console.error({action: sAction + '.err', err:err, stack: err.stack });
    process.exit(1);
  })
})
.catch( (err) => {
  console.error({action: sAction + '.mdbSetConnection.err', err:err });
  process.exit(1);
})