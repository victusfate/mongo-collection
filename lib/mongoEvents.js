'use strict';

const util            = require('./util');
const MongoCollection = require('./mongoCollection').MongoCollection;

const sClassModule = 'MongoEvents';

// an example of a MongoCollection...
class MongoEvents extends MongoCollection {
  
  connect() {
    return super.connect().then( (mdb) => {
      mdb.setIndices();
      return mdb;
    })
  }

  setIndices() {
    const sAction = sClassModule + '.setIndices';

    if (!this.bConnected) {
      console.error({ action: `${sAction}.not.connected.err` });
      return;
    }

    const tIndices = Date.now();
    
    // some indices for querying goodness
    this.collection.createIndex({ ts: 1, loc: "2dsphere" });
    this.collection.createIndex({ ts: 1 });
    this.collection.createIndex({ ts: -1 });
    this.collection.createIndex({ loc: "2dsphere" });
    this.collection.createIndex({ loc: "2d" });
    console.log({action: sAction, time: Date.now()-tIndices + 'ms'});
  }

  swapLatLon(ll) {
    const sAction = sClassModule + '.swapLatLon';
    if (Array.isArray(ll) && ll.length === 2 && typeof ll[0] === 'number' && typeof ll[1] === 'number') {
      return [ll[1],ll[0]];
    }
    else {
      return undefined;
    }
  }

  // add loc which is [lon,lat] from incident property ll which is [lat,lon]
  upsert(oKV) {
    const sAction = sClassModule + '.upsert';
    if (util.exists(oKV.val)) {
      // add loc property which is [longitude,latitude] if insert/update has an ll [latitude,longitude] property
      const loc = this.swapLatLon(oKV.val.ll);
      if (util.exists(loc)) {
        oKV.val.loc = loc;
      }
    }
    return super.upsert({ id: oKV.id, val: oKV.val });
  }

  // add loc which is [lon,lat] from incident property ll which is [lat,lon]
  batchUpsert(oBatch) {
    const sAction = sClassModule + '.batchUpsert';
    
    for (let id in oBatch) {
      const loc = this.swapLatLon(oBatch[id].ll);
      if (util.exists(loc)) {
        oBatch[id].loc = loc;
      }
    }
    return super.batchUpsert(oBatch)
  }

  
  // N most recent without boundaries
  nearby(options) {
    const sAction             = sClassModule + '.nearby';

    // set in MongoCollection parent class during connection()
    if (!this.bConnected) {
      return Promise.reject(Error(`${sAction}.not.connected.err`));
    }

    const N                   = options.N;
    const lowerLatitude       = options.lowerLatitude;
    const lowerLongitude      = options.lowerLongitude;
    const upperLatitude       = options.upperLatitude;
    const upperLongitude      = options.upperLongitude;

    if (typeof N !== 'number'             || Math.floor(N) !== N                ||
        typeof lowerLatitude !== 'number' || typeof lowerLongitude !== 'number' ||
        typeof upperLatitude !== 'number' || typeof upperLongitude !== 'number')         
    {
      return Promise.reject(Error(`${sAction}.invalid.inputs.err options ${JSON.stringify(options)}`));
    }

    const t0 = Date.now();
    // manual box
    // return collection.find({ 
    //   "loc.0": { "$gte": lowerLongitude, "$lte": upperLongitude },
    //   "loc.1": { "$gte": lowerLatitude , "$lte": upperLatitude  }
    //   box
    return this.collection.find({ 
      loc: { 
        $geoWithin: {
          $box: [
            [lowerLongitude,lowerLatitude],
            [upperLongitude,upperLatitude]
          ]              
        }
      }
    }).limit(N).sort({"ts":-1}).toArray().then( (result) => {
      console.info({ action: sAction + '.time', time: Date.now() - t0 + 'ms' });
      return result;
    });
  }

 timeSpaceQuery(options) {
    const sAction             = 'timeSpaceQuery';
    const N                   = options.N;
    const tStart              = options.tStart;
    const tEnd                = options.tEnd || Date.now();
    const lonlat              = options.lonlat;
    const maxDistanceMeters   = options.maxDistanceMeters;

    if (typeof N !== 'number'       || Math.floor(N) !== N           || typeof tStart !== 'number'    || 
       !util.isLonLat(lonlat)       || typeof maxDistanceMeters !== 'number')
    {
      return Promise.reject(Error(`${sAction}.invalid.inputs.err options ${JSON.stringify(options)}`));
    }

    const t0 = Date.now();
    // http://stackoverflow.com/questions/22393790/mongodb-geospatial-query-with-time-field
    return this.collection.find({ 
      ts  : { 
        $gte: tStart, $lte: tEnd
      },
      loc : { 
        $near : { 
          $geometry : { 
            type : "Point",
            coordinates : options.lonlat
          },
          $maxDistance : options.maxDistanceMeters
        }
      }
    }).limit(N).sort({"ts":-1}).toArray().then( (result) => {
      console.log(sAction + ' time',Date.now() - t0 + 'ms');
      return result;
    });
  }

}

// async generator
const mdbSetConnection = (options) => {
  let mdb = new MongoEvents(options);
  return mdb.connect();
}


module.exports = {
  MongoEvents: MongoEvents,
  mdbSetConnection: mdbSetConnection
}
