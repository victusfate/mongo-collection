'use strict';

const util        = require('./util');
const mongodb     = require('mongodb');
const MongoClient = mongodb.MongoClient;

const sClassModule = 'MongoCollection';
class MongoCollection {
  constructor(options) {
    
    const sAction = sClassModule + '.constructor';
    if (!util.validStringId(options.sUrl) || !util.validStringId(options.sCollection)) {
      return Promise.reject(Error(`${sAction}.invalid.inputs.error options ${JSON.stringify(options)}`));
    }
    this.sUrl        = options.sUrl;
    this.sCollection = options.sCollection;
    this.bConnected  = false;
  }

  connect() {
    const sAction = sClassModule + '.connect';
    return new Promise( (resolve,reject) => {
      MongoClient.connect(this.sUrl, (err, db) => {
        if (err) {
          console.error({ action: sAction + '.failed', message: 'Unable to connect to the mongoDB server.', err:err });
          reject(Error(`${sAction}.failed error ${err}`));
        } 
        else {
          console.log({ action: sAction + '.MongoDB.Connection.established' });

          // Get the documents collection
          this.db           = db;
          this.collection   = this.db.collection(this.sCollection);
          this.bConnected   = true;
          resolve(this);
        }
      })      
    })    
  }

  // upsert a single { id : val }
  upsert(oKV) {
    const sAction = sClassModule + '.upsert';
    const id      = util.exists(oKV) ? oKV.id : undefined;
    const val     = util.exists(oKV) ? oKV.val : undefined;

    if (!this.bConnected) {
      return Promise.reject(Error(`${sAction}.not.connected.err`));
    }

    if (!util.exists(oKV) || !util.exists(id) || !util.exists(val)) {
      return Promise.reject(Error(`${sAction}.invalid.inputs.error options ${JSON.stringify(options)}`));
    }

    if (util.exists(val)) {
      return this.collection.update({_id: id},{$set: val},{upsert: true });
    }
    else {
      return this.remove(id);
    }
  }

  // handle an object collection of { id:val }
  batchUpsert(oBatch) {
    const sAction = sClassModule + '.batchUpsert';

    if (!this.bConnected) {
      return Promise.reject(Error(`${sAction}.not.connected.err`));
    }
    const t0 = Date.now();
    let batch = this.collection.initializeUnorderedBulkOp();
    
    for (let id in oBatch) {
      const val = oBatch[id];
      if (util.exists(val)) {
        batch.find({_id: id}).upsert().updateOne({$set: val});
      }
      else { // if val null or undefined remove from db
        batch.find({_id: id}).remove();
      }
    }

    // Execute the operations
    return batch.execute()
    .then( (result) => {
      console.log( {action: sAction, message:`Upserted ${result.nUpserted}, Modified ${result.nModified}, Removed ${result.nRemoved} documents into the "${this.sCollection}" collection.` });
      console.info({action: sAction + '.time', time: Date.now() - t0 + 'ms' });
    })
  }

  // remove an object
  remove(id) {
    const sAction = sClassModule + '.remove';

    if (!this.bConnected) {
      return Promise.reject(Error(`${sAction}.not.connected.err`));
    }

    if (!util.exists(id)) {
      return Promise.reject(Error(`${sAction}.invalid.inputs.error id ${id}`));
    }
    return this.collection.deleteOne({ _id: id});
  }

  batchRemove(aIds) {
    const sAction = sClassModule + '.batchRemove';

    if (!this.bConnected) {
      return Promise.reject(Error(`${sAction}.not.connected.err`));
    }

    const t0 = Date.now();
    let batch = this.collection.initializeUnorderedBulkOp();
  
    let bAnyRemoved = false;  

    for (let i in aIds) {
      const id  = aIds[i];
      if (util.exists(id)) {
        bAnyRemoved = true;
        batch.find({_id: id}).remove();
      }
    }

    if (!bAnyRemoved) {
      console.log({ action: sAction + '.none.removed'});
      return Promise.resolve();
    }

    // Execute the operations
    return batch.execute()
    .then( (result) => {
      console.log({action: sAction, message:`Removed ${result.nRemoved} documents from the "${this.sCollection}" collection.` });
      console.info({action: sAction + '.time', time: Date.now() - t0 + 'ms' });      
    })

  }

  get(oQuery,nLimit) {
    if (!this.bConnected) {
      return Promise.reject(Error(`${sAction}.not.connected.err`));
    }
    return this.collection.find(oQuery).limit(nLimit).toArray();
  }

}


module.exports = {
  MongoCollection : MongoCollection
}

