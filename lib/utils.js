'use strict';

const clone = function(o) {
  var obj = {};
  for (var name in o) obj[name] = o[name];
  return obj;
};

const delay = function(ms) {
  return new Promise(resolve => setTimeout(() => resolve(), ms));
};

module.exports = {
  clone: clone,
  delay: delay
};
