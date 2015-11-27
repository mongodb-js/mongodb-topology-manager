// Polyfill Promise if none exists
if(!global.Promise) {
  require('es6-promise').polyfill();
}

// Check if we support es6 generators
try {
  eval("(function *(){})");

  // Expose all the managers
  var ConfigServers = require('./lib/config_servers');
  var Mongos = require('./lib/mongos');
  var ReplSet = require('./lib/replset');
  var Server = require('./lib/server');
  var Sharded = require('./lib/sharded');
  var Logger = require('./lib/logger');
} catch(err) {
  // Load the ES6 polyfills
  require("babel-polyfill");

  // Load ES5 versions of our managers
  var ConfigServers = require('./es5/config_servers');
  var Mongos = require('./es5/mongos');
  var ReplSet = require('./es5/replset');
  var Server = require('./es5/server');
  var Sharded = require('./es5/sharded');
  var Logger = require('./es5/logger');
}

// Export all the modules
module.exports = {
  ConfigServers: ConfigServers,
  Mongos: Mongos,
  ReplSet: ReplSet,
  Server: Server,
  Sharded: Sharded,
  Logger: Logger
}
