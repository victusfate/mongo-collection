'use strict';

const validStringId = (sid) => {
  return (typeof sid === 'string' && sid.length > 0);
}

const isInteger = (val) => {
  return typeof val === 'number' && Math.floor(val) !== val;
}

const exists = (obj) => {
  return (obj !== undefined && obj !== null);
}

const existsVal = (obj) => {
  return exists(obj) && exists(obj.val);
}

const isLonLat = (lonlat) => {
  return Array.isArray(lonlat)         && lonlat.length    === 2        &&
         typeof lonlat[0] === 'number' && typeof lonlat[1] === 'number'; 
}



module.exports = {
  validStringId   : validStringId,
  isInteger       : isInteger,
  exists          : exists,
  existsVal       : existsVal,
  isLonLat        : isLonLat
}