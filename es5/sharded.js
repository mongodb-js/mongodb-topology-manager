"use strict";

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var co = require('co'),
    f = require('util').format,
    mkdirp = require('mkdirp'),
    rimraf = require('rimraf'),
    Server = require('./server'),
    Logger = require('./logger'),
    ReplSet = require('./replset'),
    ConfigServers = require('./config_servers'),
    Mongos = require('./mongos'),
    CoreServer = require('mongodb-core').Server,
    spawn = require('child_process').spawn;

var Promise = require("bluebird");

var clone = function clone(o) {
  var obj = {};for (var name in o) {
    obj[name] = o[name];
  }return obj;
};

var waitMS = function waitMS(ms) {
  return new Promise(function (resolve, reject) {
    setTimeout(function () {
      resolve();
    }, ms);
  });
};

var reportError = function reportError(self, reject) {
  return function (err) {
    self.logger.error(f('%s at %s', err.message, err.stack));
    reject(err);
  };
};

var Sharded = function () {
  function Sharded(options) {
    _classCallCheck(this, Sharded);

    options = options || {};
    // Unpack default runtime information
    this.mongod = options.mongod || 'mongod';
    this.mongos = options.mongos || 'mongos';

    // Create logger instance
    this.logger = Logger('Sharded', options);

    // All pieces of the topology
    this.shards = [];
    this.configurationServers = null;
    this.proxies = [];
  }

  _createClass(Sharded, [{
    key: 'discover',
    value: function discover() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee() {
          var proc, stdout, stderr;
          return regeneratorRuntime.wrap(function _callee$(_context) {
            while (1) {
              switch (_context.prev = _context.next) {
                case 0:
                  proc = spawn(self.mongod, ['--version']);
                  // Variables receiving data

                  stdout = '';
                  stderr = '';
                  // Get the stdout

                  proc.stdout.on('data', function (data) {
                    stdout += data;
                  });
                  // Get the stderr
                  proc.stderr.on('data', function (data) {
                    stderr += data;
                  });
                  // Got an error
                  proc.on('error', function (err) {
                    reject(err);
                  });
                  // Process terminated
                  proc.on('close', function (code) {
                    // Perform version match
                    var versionMatch = stdout.match(/[0-9]+\.[0-9]+\.[0-9]+/);

                    // Check if we have ssl
                    var sslMatch = stdout.match(/ssl/i);

                    // Resolve the server version
                    resolve({
                      version: versionMatch.toString().split('.').map(function (x) {
                        return parseInt(x, 10);
                      }),
                      ssl: sslMatch != null
                    });
                  });

                case 7:
                case 'end':
                  return _context.stop();
              }
            }
          }, _callee, this);
        })).catch(reportError(self, reject));
      });
    }
  }, {
    key: 'addShard',
    value: function addShard(nodes, options) {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee2() {
          var shard;
          return regeneratorRuntime.wrap(function _callee2$(_context2) {
            while (1) {
              switch (_context2.prev = _context2.next) {
                case 0:
                  options = options || {};
                  // Create a shard
                  shard = new ReplSet(self.mongod, nodes, options);
                  // Add shard to list of shards

                  self.shards.push(shard);
                  resolve();

                case 4:
                case 'end':
                  return _context2.stop();
              }
            }
          }, _callee2, this);
        })).catch(reportError(self, reject));
      });
    }
  }, {
    key: 'addConfigurationServers',
    value: function addConfigurationServers(nodes, options) {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee3() {
          var result, version;
          return regeneratorRuntime.wrap(function _callee3$(_context3) {
            while (1) {
              switch (_context3.prev = _context3.next) {
                case 0:
                  options = options || {};
                  // Establish the version of the mongod process
                  _context3.next = 3;
                  return self.discover();

                case 3:
                  result = _context3.sent;
                  version = result.version;

                  // If configuration server has not been set up

                  options = clone(options);
                  // Clone the nodes
                  nodes = JSON.parse(JSON.stringify(nodes));
                  // Add config server to each of the nodes
                  nodes = nodes.map(function (x) {
                    if (x.arbiter) {
                      delete x['arbiter'];
                    }

                    if (!x.arbiter) {
                      x.options.configsvr = null;
                    }

                    return x;
                  });

                  // Check if we have 3.2.0 or higher where we need to boot up a replicaset
                  // not a set of configuration server
                  if (version[0] >= 4 || version[0] == 3 && version[1] >= 2) {
                    self.configurationServers = new ReplSet(self.mongod, nodes, options);
                  } else {
                    self.configurationServers = new ConfigServers(self.mongod, nodes.map(function (x) {
                      return x.options;
                    }), options);
                  }

                  resolve();

                case 10:
                case 'end':
                  return _context3.stop();
              }
            }
          }, _callee3, this);
        })).catch(reportError(self, reject));
      });
    }
  }, {
    key: 'addProxies',
    value: function addProxies(nodes, options) {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee4() {
          var i, proxy;
          return regeneratorRuntime.wrap(function _callee4$(_context4) {
            while (1) {
              switch (_context4.prev = _context4.next) {
                case 0:
                  options = options || {};

                  // Clone the options
                  options = clone(options);

                  // For each node create a proxy
                  for (i = 0; i < nodes.length; i++) {
                    proxy = new Mongos(self.mongos, nodes[i], options);

                    self.proxies.push(proxy);
                  }

                  resolve();

                case 4:
                case 'end':
                  return _context4.stop();
              }
            }
          }, _callee4, this);
        })).catch(reportError(self, reject));
      });
    }
  }, {
    key: 'enableSharding',
    value: function enableSharding(db, credentials) {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee5() {
          var proxy, result;
          return regeneratorRuntime.wrap(function _callee5$(_context5) {
            while (1) {
              switch (_context5.prev = _context5.next) {
                case 0:
                  // Get a proxy
                  proxy = self.proxies[0];


                  if (self.logger.isInfo()) {
                    self.logger.info(f('enable sharding for db %s', db));
                  }

                  // Execute the enable sharding command
                  _context5.next = 4;
                  return proxy.executeCommand('admin.$cmd', {
                    enableSharding: db
                  }, credentials);

                case 4:
                  result = _context5.sent;


                  if (self.logger.isInfo()) {
                    self.logger.info(f('successfully enabled sharding for db %s with result [%s]', db, JSON.stringify(result)));
                  }

                  // Resolve
                  resolve();

                case 7:
                case 'end':
                  return _context5.stop();
              }
            }
          }, _callee5, this);
        })).catch(reportError(self, reject));
      });
    }
  }, {
    key: 'shardCollection',
    value: function shardCollection(db, collection, shardKey, options, credentials) {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee6() {
          var proxy, command, result;
          return regeneratorRuntime.wrap(function _callee6$(_context6) {
            while (1) {
              switch (_context6.prev = _context6.next) {
                case 0:
                  options = options || {};
                  options = clone(options);
                  // Get a proxy
                  proxy = self.proxies[0];

                  // Create shard collection command

                  command = {
                    shardCollection: f('%s.%s', db, collection), key: shardKey
                  };

                  // Unique shard key

                  if (options.unique) {
                    command.unique = true;
                  }

                  if (self.logger.isInfo()) {
                    self.logger.info(f('shard collection for %s.%s with command [%s]', db, collection, JSON.stringify(command)));
                  }

                  // Execute the enable sharding command
                  _context6.next = 8;
                  return proxy.executeCommand('admin.$cmd', command, credentials);

                case 8:
                  result = _context6.sent;


                  if (self.logger.isInfo()) {
                    self.logger.info(f('successfully sharded collection for %s.%s with command [%s] and result [%s]', db, collection, JSON.stringify(command), JSON.stringify(result)));
                  }

                  // Resolve
                  resolve();

                case 11:
                case 'end':
                  return _context6.stop();
              }
            }
          }, _callee6, this);
        })).catch(reportError(self, reject));
      });
    }
  }, {
    key: 'start',
    value: function start() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee7() {
          var i, proxy, result;
          return regeneratorRuntime.wrap(function _callee7$(_context7) {
            while (1) {
              switch (_context7.prev = _context7.next) {
                case 0:
                  i = 0;

                case 1:
                  if (!(i < self.shards.length)) {
                    _context7.next = 10;
                    break;
                  }

                  if (self.logger.isInfo()) {
                    self.logger.info(f('start shard %s', self.shards[i].shardUrl()));
                  }

                  // Purge directories
                  _context7.next = 5;
                  return self.shards[i].purge();

                case 5:
                  _context7.next = 7;
                  return self.shards[i].start();

                case 7:
                  i++;
                  _context7.next = 1;
                  break;

                case 10:

                  if (self.logger.isInfo()) {
                    self.logger.info(f('start configuration server %s', self.configurationServers.url()));
                  }

                  // Purge directories
                  _context7.next = 13;
                  return self.configurationServers.purge();

                case 13:
                  _context7.next = 15;
                  return self.configurationServers.start();

                case 15:
                  i = 0;

                case 16:
                  if (!(i < self.proxies.length)) {
                    _context7.next = 25;
                    break;
                  }

                  if (self.logger.isInfo()) {
                    self.logger.info(f('start proxy at %s', self.proxies[i].name));
                  }

                  // Purge directories
                  _context7.next = 20;
                  return self.proxies[i].purge();

                case 20:
                  _context7.next = 22;
                  return self.proxies[i].start();

                case 22:
                  i++;
                  _context7.next = 16;
                  break;

                case 25:

                  // Connect and add the shards
                  proxy = self.proxies[0];

                  if (proxy) {
                    _context7.next = 28;
                    break;
                  }

                  return _context7.abrupt('return', reject('no mongos process found'));

                case 28:
                  i = 0;

                case 29:
                  if (!(i < self.shards.length)) {
                    _context7.next = 38;
                    break;
                  }

                  if (self.logger.isInfo()) {
                    self.logger.info(f('add shard at %s', self.shards[i].shardUrl()));
                  }

                  // Add the shard
                  _context7.next = 33;
                  return proxy.executeCommand('admin.$cmd', {
                    addShard: self.shards[i].shardUrl()
                  }, null, {
                    reExecuteOnError: true
                  });

                case 33:
                  result = _context7.sent;


                  if (self.logger.isInfo()) {
                    self.logger.info(f('add shard at %s with result [%s]', self.shards[i].shardUrl(), JSON.stringify(result)));
                  }

                case 35:
                  i++;
                  _context7.next = 29;
                  break;

                case 38:

                  if (self.logger.isInfo()) {
                    self.logger.info(f('sharded topology is up'));
                  }

                  resolve();

                case 40:
                case 'end':
                  return _context7.stop();
              }
            }
          }, _callee7, this);
        })).catch(reportError(self, reject));
      });
    }
  }, {
    key: 'purge',
    value: function purge() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee8() {
          var i;
          return regeneratorRuntime.wrap(function _callee8$(_context8) {
            while (1) {
              switch (_context8.prev = _context8.next) {
                case 0:
                  if (!(self.state == 'running')) {
                    _context8.next = 2;
                    break;
                  }

                  return _context8.abrupt('return', resolve());

                case 2:

                  if (self.logger.isInfo()) {
                    self.logger.info(f('purging mongo proxy directories'));
                  }

                  // Shutdown all the proxies
                  i = 0;

                case 4:
                  if (!(i < self.proxies.length)) {
                    _context8.next = 10;
                    break;
                  }

                  _context8.next = 7;
                  return self.proxies[i].purge();

                case 7:
                  i++;
                  _context8.next = 4;
                  break;

                case 10:

                  if (self.logger.isInfo()) {
                    self.logger.info(f('purging configuration server directories'));
                  }

                  // Shutdown configuration server

                  if (!self.configurationServers) {
                    _context8.next = 14;
                    break;
                  }

                  _context8.next = 14;
                  return self.configurationServers.purge();

                case 14:

                  if (self.logger.isInfo()) {
                    self.logger.info(f('puring shard directories'));
                  }

                  // Shutdown all the shards
                  i = 0;

                case 16:
                  if (!(i < self.shards.length)) {
                    _context8.next = 22;
                    break;
                  }

                  _context8.next = 19;
                  return self.shards[i].purge();

                case 19:
                  i++;
                  _context8.next = 16;
                  break;

                case 22:

                  if (self.logger.isInfo()) {
                    self.logger.info(f('done purging directories for topology'));
                  }

                  // Set the state to running
                  self.state == 'running';

                  // Resolve
                  resolve();

                case 25:
                case 'end':
                  return _context8.stop();
              }
            }
          }, _callee8, this);
        })).catch(reportError(self, reject));
      });
    }
  }, {
    key: 'stop',
    value: function stop() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee9() {
          var i;
          return regeneratorRuntime.wrap(function _callee9$(_context9) {
            while (1) {
              switch (_context9.prev = _context9.next) {
                case 0:
                  if (!(self.state == 'running')) {
                    _context9.next = 2;
                    break;
                  }

                  return _context9.abrupt('return', resolve());

                case 2:

                  if (self.logger.isInfo()) {
                    self.logger.info(f('Shutting down mongos proxies'));
                  }

                  // Shutdown all the proxies
                  i = 0;

                case 4:
                  if (!(i < self.proxies.length)) {
                    _context9.next = 10;
                    break;
                  }

                  _context9.next = 7;
                  return self.proxies[i].stop();

                case 7:
                  i++;
                  _context9.next = 4;
                  break;

                case 10:

                  if (self.logger.isInfo()) {
                    self.logger.info(f('Shutting down configuration servers'));
                  }

                  // Shutdown configuration server
                  _context9.next = 13;
                  return self.configurationServers.stop();

                case 13:

                  if (self.logger.isInfo()) {
                    self.logger.info(f('Shutting down shards'));
                  }

                  // Shutdown all the shards
                  i = 0;

                case 15:
                  if (!(i < self.shards.length)) {
                    _context9.next = 21;
                    break;
                  }

                  _context9.next = 18;
                  return self.shards[i].stop();

                case 18:
                  i++;
                  _context9.next = 15;
                  break;

                case 21:

                  if (self.logger.isInfo()) {
                    self.logger.info(f('done shutting down sharding topology'));
                  }

                  // Set the state to running
                  self.state == 'running';

                  // Resolve
                  resolve();

                case 24:
                case 'end':
                  return _context9.stop();
              }
            }
          }, _callee9, this);
        })).catch(reportError(self, reject));
      });
    }
  }, {
    key: 'restart',
    value: function restart() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee10() {
          return regeneratorRuntime.wrap(function _callee10$(_context10) {
            while (1) {
              switch (_context10.prev = _context10.next) {
                case 0:
                case 'end':
                  return _context10.stop();
              }
            }
          }, _callee10, this);
        })).catch(reportError(self, reject));
      });
    }
  }]);

  return Sharded;
}();

module.exports = Sharded;
