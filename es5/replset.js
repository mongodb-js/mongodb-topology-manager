"use strict";

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var co = require('co'),
    f = require('util').format,
    mkdirp = require('mkdirp'),
    rimraf = require('rimraf'),
    Logger = require('./logger'),
    Server = require('./server'),
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

var ReplSet = function () {
  function ReplSet(binary, nodes, options) {
    _classCallCheck(this, ReplSet);

    options = options || {};
    // Save the default passed in parameters
    this.nodes = nodes;
    this.options = clone(options);

    // Create logger instance
    this.logger = Logger('ReplSet', options);

    // Did we specify special settings for the configuration JSON used
    // to set up the replicaset (delete it from the internal options after
    // transferring it to a new variable)
    if (this.options.configSettings) {
      this.configSettings = this.options.configSettings;
      delete this.options['configSettings'];
    }

    // Ensure we have a list of nodes
    if (!Array.isArray(this.nodes) || this.nodes.length == 0) {
      throw new Error('a list of nodes must be passed in');
    }

    // Ensure we have set basic options
    if (!options.replSet) throw new Error('replSet must be set');

    // Server state
    this.state = 'stopped';

    // Unpack default runtime information
    this.binary = binary || 'mongod';

    // Wait times
    this.electionCycleWaitMS = typeof this.options.electionCycleWaitMS == 'number' ? this.options.electionCycleWaitMS : 31000;
    this.retryWaitMS = typeof this.options.retryWaitMS == 'number' ? this.options.retryWaitMS : 5000;

    // Remove the values from the options
    delete this.options['electionCycleWaitMS'];
    delete this.options['retryWaitMS'];

    // Self reference
    var self = this;

    // Basic config settings for replicaset
    this.version = 1;
    this.replSet = options.replSet;

    // Contains all the configurations
    this.configurations = [];

    // Get the current electionId
    this.electionId = null;

    // Create server managers for each node
    this.managers = this.nodes.map(function (x) {
      var opts = clone(x.options);
      delete opts['logpath'];

      // Add the needed replicaset options
      opts.replSet = options.replSet;

      // Set server instance
      var server = new Server(self.binary, opts, options);

      // Create manager
      return server;
    });
  }

  _createClass(ReplSet, [{
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
                  proc = spawn(self.binary, ['--version']);
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
                    // Final result
                    var result = {
                      version: versionMatch.toString().split('.').map(function (x) {
                        return parseInt(x, 10);
                      }),
                      ssl: sslMatch != null
                    };

                    if (self.logger.isInfo()) {
                      self.logger.info(f('mongod discovery returned %s', JSON.stringify(result)));
                    }

                    // Resolve the server version
                    resolve(result);
                  });

                case 7:
                case 'end':
                  return _context.stop();
              }
            }
          }, _callee, this);
        })).catch(reject);
      });
    }
  }, {
    key: 'start',
    value: function start() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee2() {
          var result, i, config, ignoreError, numberOfArbiters, state, member, ismaster;
          return regeneratorRuntime.wrap(function _callee2$(_context2) {
            while (1) {
              switch (_context2.prev = _context2.next) {
                case 0:
                  if (!(self.state == 'running')) {
                    _context2.next = 2;
                    break;
                  }

                  return _context2.abrupt('return', resolve());

                case 2:
                  _context2.next = 4;
                  return self.discover();

                case 4:
                  result = _context2.sent;
                  i = 0;

                case 6:
                  if (!(i < self.managers.length)) {
                    _context2.next = 12;
                    break;
                  }

                  _context2.next = 9;
                  return self.managers[i].start();

                case 9:
                  i++;
                  _context2.next = 6;
                  break;

                case 12:

                  // Time to configure the servers by generating the
                  config = generateConfiguration(self.replSet, self.version, self.nodes, self.configSettings);


                  if (self.logger.isInfo()) {
                    self.logger.info(f('initialize replicaset with config %s', JSON.stringify(config)));
                  }

                  // Ignore Error
                  ignoreError = result.version[0] == 2 && result.version[1] <= 6 ? true : false;

                  // Pick the first manager and execute replicaset configuration

                  _context2.next = 17;
                  return self.managers[0].executeCommand('admin.$cmd', {
                    replSetInitiate: config
                  }, null, { ignoreError: ignoreError });

                case 17:
                  result = _context2.sent;

                  if (!(result.ok == 0)) {
                    _context2.next = 20;
                    break;
                  }

                  return _context2.abrupt('return', reject(new Error(f('failed to initialize replicaset with config %s', JSON.stringify(config)))));

                case 20:

                  // Push configuration to the history
                  self.configurations.push(config);

                  // Waiting
                  numberOfArbiters = 0;
                  // Count the number of expected arbiters

                  self.nodes.forEach(function (x) {
                    if (x.arbiter) numberOfArbiters = numberOfArbiters + 1;
                  });

                  // Now monitor until we have all the servers in a healthy state

                case 23:
                  if (!true) {
                    _context2.next = 42;
                    break;
                  }

                  _context2.next = 26;
                  return waitMS(1000);

                case 26:

                  // Monitoring state
                  state = {
                    primaries: 0,
                    secondaries: 0,
                    arbiters: 0
                  };

                  // Get the replicaset status

                  _context2.prev = 27;
                  _context2.next = 30;
                  return self.managers[0].executeCommand('admin.$cmd', {
                    replSetGetStatus: true
                  });

                case 30:
                  result = _context2.sent;
                  _context2.next = 36;
                  break;

                case 33:
                  _context2.prev = 33;
                  _context2.t0 = _context2['catch'](27);
                  return _context2.abrupt('continue', 23);

                case 36:

                  // Sum up expected servers
                  for (i = 0; i < result.members.length; i++) {
                    member = result.members[i];


                    if (member.health == 1) {
                      if (member.state == 2) {
                        state.secondaries = state.secondaries + 1;
                      }

                      if (member.state == 1) {
                        state.primaries = state.primaries + 1;
                      }

                      if (member.state == 7) {
                        state.arbiters = state.arbiters + 1;
                      }
                    }
                  }

                  if (self.logger.isInfo()) {
                    self.logger.info(f('replicaset current state %s', JSON.stringify(state)));
                  }

                  // Validate the state

                  if (!(state.primaries == 1 && state.arbiters == numberOfArbiters && state.secondaries == self.nodes.length - numberOfArbiters - 1)) {
                    _context2.next = 40;
                    break;
                  }

                  return _context2.abrupt('break', 42);

                case 40:
                  _context2.next = 23;
                  break;

                case 42:
                  _context2.next = 44;
                  return self.waitForPrimary();

                case 44:
                  _context2.next = 46;
                  return self.managers[0].ismaster();

                case 46:
                  ismaster = _context2.sent;

                  // Save the current election Id if it exists
                  self.electionId = ismaster.electionId;
                  self.lastKnownPrimary = ismaster.me;

                  // We have a stable replicaset
                  resolve();

                case 50:
                case 'end':
                  return _context2.stop();
              }
            }
          }, _callee2, this, [[27, 33]]);
        })).catch(reject);
      });
    }

    /**
     * Locate the primary server manager
     * @method
     * @returns {Promise}
     */

  }, {
    key: 'primary',
    value: function primary() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee3() {
          var i, ismaster;
          return regeneratorRuntime.wrap(function _callee3$(_context3) {
            while (1) {
              switch (_context3.prev = _context3.next) {
                case 0:
                  i = 0;

                case 1:
                  if (!(i < self.managers.length)) {
                    _context3.next = 10;
                    break;
                  }

                  _context3.next = 4;
                  return self.managers[i].ismaster();

                case 4:
                  ismaster = _context3.sent;

                  if (!ismaster.ismaster) {
                    _context3.next = 7;
                    break;
                  }

                  return _context3.abrupt('return', resolve(self.managers[i]));

                case 7:
                  i++;
                  _context3.next = 1;
                  break;

                case 10:

                  reject(new Error('no primary server found in set'));

                case 11:
                case 'end':
                  return _context3.stop();
              }
            }
          }, _callee3, this);
        })).catch(reject);
      });
    }

    /**
     * Return add shard url
     * @method
     * return {String}
     */

  }, {
    key: 'shardUrl',
    value: function shardUrl() {
      var members = this.nodes.map(function (x) {
        return f('%s:%s', x.options.bind_ip || 'localhost', x.options.port);
      });

      // Generate the url
      return f('%s/%s', this.replSet, members.join(','));
    }

    /**
     * Return members url
     * @method
     * return {String}
     */

  }, {
    key: 'url',
    value: function url() {
      var members = this.nodes.map(function (x) {
        return f('%s:%s', x.options.bind_ip || 'localhost', x.options.port);
      });

      // Generate the url
      return f('%s', members.join(','));
    }

    /**
     * Locate all the arbiters
     * @method
     * @returns {Promise}
     */

  }, {
    key: 'arbiters',
    value: function arbiters() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee4() {
          var arbiters, i, ismaster;
          return regeneratorRuntime.wrap(function _callee4$(_context4) {
            while (1) {
              switch (_context4.prev = _context4.next) {
                case 0:
                  arbiters = [];

                  // Go over all the managers

                  i = 0;

                case 2:
                  if (!(i < self.managers.length)) {
                    _context4.next = 10;
                    break;
                  }

                  _context4.next = 5;
                  return self.managers[i].ismaster();

                case 5:
                  ismaster = _context4.sent;

                  if (ismaster.arbiterOnly) arbiters.push(self.managers[i]);

                case 7:
                  i++;
                  _context4.next = 2;
                  break;

                case 10:

                  resolve(arbiters);

                case 11:
                case 'end':
                  return _context4.stop();
              }
            }
          }, _callee4, this);
        })).catch(reject);
      });
    }

    /**
     * Locate all the secondaries
     * @method
     * @returns {Promise}
     */

  }, {
    key: 'secondaries',
    value: function secondaries() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee5() {
          var secondaries, i, ismaster;
          return regeneratorRuntime.wrap(function _callee5$(_context5) {
            while (1) {
              switch (_context5.prev = _context5.next) {
                case 0:
                  secondaries = [];

                  // Go over all the managers

                  i = 0;

                case 2:
                  if (!(i < self.managers.length)) {
                    _context5.next = 10;
                    break;
                  }

                  _context5.next = 5;
                  return self.managers[i].ismaster();

                case 5:
                  ismaster = _context5.sent;

                  // Check if we have a secondary but might be a passive
                  if (ismaster.secondary && ismaster.passives && ismaster.passives.indexOf(ismaster.me) == -1) {
                    secondaries.push(self.managers[i]);
                  } else if (ismaster.secondary && !ismaster.passives) {
                    secondaries.push(self.managers[i]);
                  }

                case 7:
                  i++;
                  _context5.next = 2;
                  break;

                case 10:

                  resolve(secondaries);

                case 11:
                case 'end':
                  return _context5.stop();
              }
            }
          }, _callee5, this);
        })).catch(reject);
      });
    }

    /**
     * Locate all the passives
     * @method
     * @returns {Promise}
     */

  }, {
    key: 'passives',
    value: function passives() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee6() {
          var secondaries, i, ismaster;
          return regeneratorRuntime.wrap(function _callee6$(_context6) {
            while (1) {
              switch (_context6.prev = _context6.next) {
                case 0:
                  secondaries = [];

                  // Go over all the managers

                  i = 0;

                case 2:
                  if (!(i < self.managers.length)) {
                    _context6.next = 10;
                    break;
                  }

                  _context6.next = 5;
                  return self.managers[i].ismaster();

                case 5:
                  ismaster = _context6.sent;

                  // Check if we have a secondary but might be a passive
                  if (ismaster.secondary && ismaster.passives && ismaster.passives.indexOf(ismaster.me) != -1) {
                    secondaries.push(self.managers[i]);
                  }

                case 7:
                  i++;
                  _context6.next = 2;
                  break;

                case 10:

                  resolve(secondaries);

                case 11:
                case 'end':
                  return _context6.stop();
              }
            }
          }, _callee6, this);
        })).catch(reject);
      });
    }

    /**
     * Block until we have a new primary available
     * @method
     * @returns {Promise}
     */

  }, {
    key: 'waitForPrimary',
    value: function waitForPrimary() {
      var self = this;
      var waitedForElectionCycle = false;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee7() {
          var i, ismaster;
          return regeneratorRuntime.wrap(function _callee7$(_context7) {
            while (1) {
              switch (_context7.prev = _context7.next) {
                case 0:
                  if (!true) {
                    _context7.next = 34;
                    break;
                  }

                  i = 0;

                case 2:
                  if (!(i < self.managers.length)) {
                    _context7.next = 30;
                    break;
                  }

                  _context7.prev = 3;
                  _context7.next = 6;
                  return self.managers[i].ismaster();

                case 6:
                  ismaster = _context7.sent;

                  if (!(ismaster.electionId && ismaster.ismaster && !ismaster.electionId.equals(self.electionId))) {
                    _context7.next = 13;
                    break;
                  }

                  // We have a new primary
                  self.electionId = ismaster.electionId;
                  self.lastKnownPrimary = ismaster.me;
                  // Return the manager
                  return _context7.abrupt('return', resolve(self.managers[i]));

                case 13:
                  if (!(ismaster.ismaster && !waitedForElectionCycle)) {
                    _context7.next = 19;
                    break;
                  }

                  _context7.next = 16;
                  return waitMS(self.electionCycleWaitMS);

                case 16:
                  // Set waitedForElectionCycle
                  waitedForElectionCycle = true;
                  _context7.next = 21;
                  break;

                case 19:
                  if (!(ismaster.ismaster && waitedForElectionCycle)) {
                    _context7.next = 21;
                    break;
                  }

                  return _context7.abrupt('return', resolve());

                case 21:
                  _context7.next = 27;
                  break;

                case 23:
                  _context7.prev = 23;
                  _context7.t0 = _context7['catch'](3);
                  _context7.next = 27;
                  return waitMS(self.retryWaitMS);

                case 27:
                  i++;
                  _context7.next = 2;
                  break;

                case 30:
                  _context7.next = 32;
                  return waitMS(1000);

                case 32:
                  _context7.next = 0;
                  break;

                case 34:
                case 'end':
                  return _context7.stop();
              }
            }
          }, _callee7, this, [[3, 23]]);
        })).catch(reject);
      });
    }

    /**
     * Step down the primary server
     * @method
     * @param {boolean} [returnImmediately=false] Return immediately after executing stepdown, otherwise block until new primary is available.
     * @param {number} [options.stepDownSecs=60] The number of seconds to wait before stepping down primary.
     * @param {number} [options.secondaryCatchUpPeriodSecs=null] The number of seconds that the mongod will wait for an electable secondary to catch up to the primary.
     * @param {boolean} [options.force=false] A boolean that determines whether the primary steps down if no electable and up-to-date secondary exists within the wait period.
     * @returns {Promise}
     */

  }, {
    key: 'stepDownPrimary',
    value: function stepDownPrimary(returnImmediately, options, credentials) {
      var self = this;
      options = options || {};

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee8() {
          var command, name, manager, result, r;
          return regeneratorRuntime.wrap(function _callee8$(_context8) {
            while (1) {
              switch (_context8.prev = _context8.next) {
                case 0:
                  options = clone(options);

                  // Step down command
                  command = {
                    replSetStepDown: typeof options.stepDownSecs == 'number' ? options.stepDownSecs : 60
                  };

                  // Remove stepDownSecs

                  delete options['stepDownSecs'];
                  // Mix in any other options
                  for (name in options) {
                    command[name] = options[name];
                  }

                  // Locate the current primary
                  _context8.next = 6;
                  return self.primary();

                case 6:
                  manager = _context8.sent;

                  if (!(manager == null)) {
                    _context8.next = 9;
                    break;
                  }

                  return _context8.abrupt('return', reject(new Error('no primary found in the replicaset')));

                case 9:
                  _context8.prev = 9;
                  _context8.next = 12;
                  return manager.executeCommand('admin.$cmd', command, credentials);

                case 12:
                  result = _context8.sent;
                  _context8.next = 19;
                  break;

                case 15:
                  _context8.prev = 15;
                  _context8.t0 = _context8['catch'](9);

                  if (!(_context8.t0.ok == 0)) {
                    _context8.next = 19;
                    break;
                  }

                  return _context8.abrupt('return', reject(_context8.t0));

                case 19:
                  _context8.next = 21;
                  return self.discover();

                case 21:
                  r = _context8.sent;

                  if (!(r.version[0] >= 3)) {
                    _context8.next = 25;
                    break;
                  }

                  if (!(result && result.ok == 0)) {
                    _context8.next = 25;
                    break;
                  }

                  return _context8.abrupt('return', reject(result));

                case 25:
                  if (!returnImmediately) {
                    _context8.next = 27;
                    break;
                  }

                  return _context8.abrupt('return', resolve());

                case 27:
                  _context8.next = 29;
                  return self.waitForPrimary();

                case 29:

                  // Finish up
                  resolve();

                case 30:
                case 'end':
                  return _context8.stop();
              }
            }
          }, _callee8, this, [[9, 15]]);
        })).catch(reject);
      });
    }

    /**
     * Get the current replicaset configuration
     * @method
     * @param {object} manager The server manager that we wish to use to get the current configuration.
     * @param {object} [credentials] Credentials needed to perform an admin authenticated command.
     * @returns {Promise}
     */

  }, {
    key: 'configuration',
    value: function configuration(manager, credentials) {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee9() {
          var result, server, cursor;
          return regeneratorRuntime.wrap(function _callee9$(_context9) {
            while (1) {
              switch (_context9.prev = _context9.next) {
                case 0:
                  _context9.next = 2;
                  return self.discover();

                case 2:
                  result = _context9.sent;

                  if (!(result[0] >= 3)) {
                    _context9.next = 12;
                    break;
                  }

                  _context9.next = 6;
                  return manager.executeCommand('admin.$cmd', {
                    replSetGetConfig: true
                  }, credentials);

                case 6:
                  result = _context9.sent;

                  if (!(result && result.ok == 0)) {
                    _context9.next = 9;
                    break;
                  }

                  return _context9.abrupt('return', reject(new Error(f('failed to execute replSetGetConfig against server [%s]', node.name))));

                case 9:

                  resolve(result.config);
                  _context9.next = 17;
                  break;

                case 12:
                  _context9.next = 14;
                  return manager.instance(credentials);

                case 14:
                  server = _context9.sent;

                  // Get the configuration document
                  cursor = server.cursor('local.system.replset', {
                    find: 'local.system.replset',
                    query: {},
                    limit: 1
                  });

                  // Execute next

                  cursor.next(function (err, d) {
                    if (err) return reject(err);
                    if (!d) return reject(new Error('no replicaset configuration found'));
                    resolve(d);
                  });

                case 17:
                case 'end':
                  return _context9.stop();
              }
            }
          }, _callee9, this);
        })).catch(reject);
      });
    }

    /**
     * Set a new configuration
     * @method
     * @param {object} configuration The configuration JSON object
     * @param {object} [options] Any options for the operation.
     * @param {boolean} [options.returnImmediately=false] Return immediately after executing stepdown, otherwise block until new primary is available.
     * @param {boolean} [options.force=false] Force the server reconfiguration
     * @param {object} [credentials] Credentials needed to perform an admin authenticated command.
     * @returns {Promise}
     */

  }, {
    key: 'reconfigure',
    value: function reconfigure(config, options, credentials) {
      options = options || { returnImmediately: false };
      var self = this;

      // Default returnImmediately to false
      var returnImmediately = typeof options.returnImmediately == 'boolean' ? options.returnImmediately : false;
      // Default force to false
      var force = typeof options.force == 'boolean' ? options.force : false;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee10() {
          var lastConfig, primary, result, waitedForElectionCycle, ismaster;
          return regeneratorRuntime.wrap(function _callee10$(_context10) {
            while (1) {
              switch (_context10.prev = _context10.next) {
                case 0:
                  // Last known config
                  lastConfig = self.configurations[self.configurations.length - 1];
                  // Grab the current configuration and clone it (including member object)

                  config = clone(config);
                  config.members = config.members.map(function (x) {
                    return clone(x);
                  });

                  // Update the version to the latest + 1
                  config.version = lastConfig.version + 1;

                  // Reconfigure the replicaset
                  _context10.next = 6;
                  return self.primary();

                case 6:
                  primary = _context10.sent;

                  if (primary) {
                    _context10.next = 9;
                    break;
                  }

                  return _context10.abrupt('return', reject(new Error('no primary available')));

                case 9:
                  _context10.next = 11;
                  return primary.executeCommand('admin.$cmd', {
                    replSetReconfig: config, force: force
                  }, credentials, { ignoreError: true });

                case 11:
                  result = _context10.sent;

                  if (!(result && result.ok == 0)) {
                    _context10.next = 14;
                    break;
                  }

                  return _context10.abrupt('return', reject(new Error(f('failed to execute replSetReconfig with configuration [%s]', JSON.stringify(config)))));

                case 14:

                  // Push new configuration to list
                  self.configurations.push(config);

                  // If we want to return immediately do so now

                  if (!returnImmediately) {
                    _context10.next = 17;
                    break;
                  }

                  return _context10.abrupt('return', resolve(server));

                case 17:

                  // Found a valid state
                  waitedForElectionCycle = false;

                  // Wait for the server to get in a stable state

                case 18:
                  if (!true) {
                    _context10.next = 60;
                    break;
                  }

                  _context10.prev = 19;
                  _context10.next = 22;
                  return self.primary();

                case 22:
                  primary = _context10.sent;

                  if (primary) {
                    _context10.next = 27;
                    break;
                  }

                  _context10.next = 26;
                  return waitMS(self.retryWaitMS);

                case 26:
                  return _context10.abrupt('continue', 18);

                case 27:
                  _context10.next = 29;
                  return primary.ismaster();

                case 29:
                  ismaster = _context10.sent;

                  if (!(ismaster.ismaster && ismaster.electionId && !self.electionId.equals(ismaster.electionId))) {
                    _context10.next = 36;
                    break;
                  }

                  _context10.next = 33;
                  return self.waitForPrimary();

                case 33:
                  return _context10.abrupt('return', resolve());

                case 36:
                  if (!((ismaster.secondary || ismaster.arbiterOnly) && ismaster.electionId && self.electionId.equals(ismaster.electionId))) {
                    _context10.next = 40;
                    break;
                  }

                  return _context10.abrupt('return', resolve());

                case 40:
                  if (!((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly) && !waitedForElectionCycle)) {
                    _context10.next = 46;
                    break;
                  }

                  // Wait for an election cycle to have passed
                  waitedForElectionCycle = true;
                  _context10.next = 44;
                  return waitMS(self.electionCycleWaitMS);

                case 44:
                  _context10.next = 52;
                  break;

                case 46:
                  if (!((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly) && waitedForElectionCycle)) {
                    _context10.next = 50;
                    break;
                  }

                  return _context10.abrupt('return', resolve());

                case 50:
                  _context10.next = 52;
                  return waitMS(self.retryWaitMS);

                case 52:
                  _context10.next = 58;
                  break;

                case 54:
                  _context10.prev = 54;
                  _context10.t0 = _context10['catch'](19);
                  _context10.next = 58;
                  return waitMS(self.retryWaitMS);

                case 58:
                  _context10.next = 18;
                  break;

                case 60:

                  // Should not reach here
                  reject(new Error(f('failed to successfully set a configuration [%s]', JSON.stringify(config))));

                case 61:
                case 'end':
                  return _context10.stop();
              }
            }
          }, _callee10, this, [[19, 54]]);
        })).catch(reject);
      });
    }

    /**
     * Get seed list node configuration
     * @method
     * @param {object} node server manager we want node configuration from
     * @returns {Promise}
     */

  }, {
    key: 'serverConfiguration',
    value: function serverConfiguration(n) {
      var node = null;

      // Is the node an existing server manager, get the info from the node
      if (n instanceof Server) {
        // Locate the known node for this server
        for (var i = 0; i < this.nodes.length; i++) {
          var _n = this.nodes[i];
          if (_n.options.bind_ip == n.host && _n.options.port == n.port) {
            node = _n;
            break;
          }
        }
      }

      return node;
    }

    /**
     * Adds a new member to the replicaset
     * @method
     * @param {object} node All the settings used to boot the mongod process.
     * @param {object} [options] Any options for the operation.
     * @param {boolean} [options.returnImmediately=false] Return immediately after executing stepdown, otherwise block until new primary is available.
     * @param {boolean} [options.force=false] Force the server reconfiguration
     * @param {object} [credentials] Credentials needed to perform an admin authenticated command.
     * @returns {Promise}
     */

  }, {
    key: 'addMember',
    value: function addMember(node, options, credentials) {
      options = options || { returnImmediately: false };
      var self = this;

      // Default returnImmediately to false
      var returnImmediately = typeof options.returnImmediately == 'boolean' ? options.returnImmediately : false;
      // Default force to false
      var force = typeof options.force == 'boolean' ? options.force : false;

      // Is the node an existing server manager, get the info from the node
      if (node instanceof Server) {
        // Locate the known node for this server
        for (var i = 0; i < this.nodes.length; i++) {
          var n = this.nodes[i];
          if (n.options.bind_ip == node.host && n.options.port == node.port) {
            node = n;
            break;
          }
        }
      }

      // Return the promise
      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee11() {
          var opts, server, newManagers, needWaitForPrimary, i, max, config, member, primary, result, waitedForElectionCycle, ismaster;
          return regeneratorRuntime.wrap(function _callee11$(_context11) {
            while (1) {
              switch (_context11.prev = _context11.next) {
                case 0:
                  // Clone the top level settings
                  node = clone(node);
                  // Clone the settings and remove the logpath
                  opts = clone(node.options);

                  delete opts['logpath'];

                  // Add the needed replicaset options
                  opts.replSet = self.options.replSet;

                  // Create a new server instance
                  server = new Server(self.binary, opts, self.options);

                  // If we have an existing manager remove it

                  newManagers = [];

                  // Need to wait for Primary

                  needWaitForPrimary = false;
                  // Do we already have a manager then stop it, purge it and remove it

                  i = 0;

                case 8:
                  if (!(i < self.managers.length)) {
                    _context11.next = 21;
                    break;
                  }

                  if (!(f('%s:%s', self.managers[i].host, self.managers[i].port) == f('%s:%s', server.host, server.port))) {
                    _context11.next = 17;
                    break;
                  }

                  _context11.next = 12;
                  return self.managers[i].stop();

                case 12:
                  _context11.next = 14;
                  return self.managers[i].purge();

                case 14:
                  needWaitForPrimary = true;
                  _context11.next = 18;
                  break;

                case 17:
                  newManagers.push(self.managers[i]);

                case 18:
                  i++;
                  _context11.next = 8;
                  break;

                case 21:

                  // Set up the managers
                  self.managers = newManagers;

                  // Purge the directory
                  _context11.next = 24;
                  return server.purge();

                case 24:
                  _context11.next = 26;
                  return server.start();

                case 26:
                  if (!needWaitForPrimary) {
                    _context11.next = 29;
                    break;
                  }

                  _context11.next = 29;
                  return self.waitForPrimary();

                case 29:
                  if (!(self.configurations.length == 0)) {
                    _context11.next = 31;
                    break;
                  }

                  return _context11.abrupt('return', reject(new Error('no configurations exist yet, did you start the replicaset?')));

                case 31:

                  // Locate max id
                  max = 0;

                  // Grab the current configuration and clone it (including member object)

                  config = clone(self.configurations[self.configurations.length - 1]);

                  config.members = config.members.map(function (x) {
                    max = x._id > max ? x._id : max;
                    return clone(x);
                  });

                  // Let's add our new server to the configuration
                  delete node['options'];
                  // Create the member
                  member = {
                    _id: max + 1,
                    host: f('%s:%s', opts.bind_ip, opts.port)
                  };

                  // Did we specify any special options

                  if (node.arbiter) member.arbiterOnly = true;
                  if (node.builIndexes) member.buildIndexes = true;
                  if (node.hidden) member.hidden = true;
                  if (typeof node.priority == 'number') member.priority = node.priority;
                  if (node.tags) member.tags = node.tags;
                  if (node.slaveDelay) member.slaveDelay = node.slaveDelay;
                  if (node.votes) member.votes = node.votes;

                  // Add to the list of members
                  config.members.push(member);
                  // Update the configuration version
                  config.version = config.version + 1;

                  // Reconfigure the replicaset
                  _context11.next = 47;
                  return self.primary();

                case 47:
                  primary = _context11.sent;

                  if (primary) {
                    _context11.next = 50;
                    break;
                  }

                  return _context11.abrupt('return', reject(new Error('no primary available')));

                case 50:
                  _context11.next = 52;
                  return primary.executeCommand('admin.$cmd', {
                    replSetReconfig: config, force: force
                  }, credentials);

                case 52:
                  result = _context11.sent;

                  if (!(result && result.ok == 0)) {
                    _context11.next = 55;
                    break;
                  }

                  return _context11.abrupt('return', reject(new Error(f('failed to execute replSetReconfig with configuration [%s]', JSON.stringify(config)))));

                case 55:

                  // Push new configuration to list
                  self.configurations.push(config);

                  // Add manager to list of managers
                  self.managers.push(server);

                  // If we want to return immediately do so now

                  if (!returnImmediately) {
                    _context11.next = 59;
                    break;
                  }

                  return _context11.abrupt('return', resolve(server));

                case 59:

                  // Found a valid state
                  waitedForElectionCycle = false;

                  // Wait for the server to get in a stable state

                case 60:
                  if (!true) {
                    _context11.next = 97;
                    break;
                  }

                  _context11.prev = 61;
                  _context11.next = 64;
                  return server.ismaster();

                case 64:
                  ismaster = _context11.sent;

                  if (!(ismaster.ismaster && ismaster.electionId && !self.electionId.equals(ismaster.electionId))) {
                    _context11.next = 71;
                    break;
                  }

                  _context11.next = 68;
                  return self.waitForPrimary();

                case 68:
                  return _context11.abrupt('return', resolve(server));

                case 71:
                  if (!((ismaster.secondary || ismaster.arbiterOnly) && ismaster.electionId && self.electionId.equals(ismaster.electionId))) {
                    _context11.next = 75;
                    break;
                  }

                  return _context11.abrupt('return', resolve(server));

                case 75:
                  if (!((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly) && !waitedForElectionCycle)) {
                    _context11.next = 81;
                    break;
                  }

                  // Wait for an election cycle to have passed
                  waitedForElectionCycle = true;
                  _context11.next = 79;
                  return waitMS(self.electionCycleWaitMS);

                case 79:
                  _context11.next = 89;
                  break;

                case 81:
                  if (!((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly) && waitedForElectionCycle)) {
                    _context11.next = 87;
                    break;
                  }

                  _context11.next = 84;
                  return self.waitForPrimary();

                case 84:
                  return _context11.abrupt('return', resolve(server));

                case 87:
                  _context11.next = 89;
                  return waitMS(self.retryWaitMS);

                case 89:
                  _context11.next = 95;
                  break;

                case 91:
                  _context11.prev = 91;
                  _context11.t0 = _context11['catch'](61);
                  _context11.next = 95;
                  return waitMS(self.retryWaitMS);

                case 95:
                  _context11.next = 60;
                  break;

                case 97:

                  // Should not reach here
                  reject(new Error(f('failed to successfully add a new member with options [%s]', JSON.stringify(node))));

                case 98:
                case 'end':
                  return _context11.stop();
              }
            }
          }, _callee11, this, [[61, 91]]);
        })).catch(reject);
      });
    }

    /**
     * Remove a member from the set
     * @method
     * @param {object} manager The server manager that we wish to remove from the set.
     * @param {object} [options] Any options for the operation.
     * @param {boolean} [options.returnImmediately=false] Return immediately after executing stepdown, otherwise block until new primary is available.
     * @param {boolean} [options.force=false] Force the server reconfiguration
     * @param {boolean} [options.skipWait=false] Skip waiting for the feedback
     * @param {object} [credentials] Credentials needed to perform an admin authenticated command.
     * @returns {Promise}
     */

  }, {
    key: 'removeMember',
    value: function removeMember(node, options, credentials) {
      options = options || { returnImmediately: false };
      var self = this;

      // Default returnImmediately to false
      var returnImmediately = typeof options.returnImmediately == 'boolean' ? options.returnImmediately : false;
      // Default force to false
      var force = typeof options.force == 'boolean' ? options.force : false;
      // Default skipWait
      var skipWait = typeof options.skipWait == 'boolean' ? options.skipWait : false;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee12() {
          var config, primary, result;
          return regeneratorRuntime.wrap(function _callee12$(_context12) {
            while (1) {
              switch (_context12.prev = _context12.next) {
                case 0:
                  // Grab the current configuration and clone it (including member object)
                  config = clone(self.configurations[self.configurations.length - 1]);

                  config.members = config.members.map(function (x) {
                    return clone(x);
                  });

                  // Locate the member and remove it
                  config.members = config.members.filter(function (x) {
                    return x.host != node.name;
                  });

                  // Update the configuration version
                  config.version = config.version + 1;

                  // Reconfigure the replicaset
                  _context12.next = 6;
                  return self.primary();

                case 6:
                  primary = _context12.sent;

                  if (primary) {
                    _context12.next = 9;
                    break;
                  }

                  return _context12.abrupt('return', reject(new Error('no primary available')));

                case 9:
                  _context12.next = 11;
                  return primary.executeCommand('admin.$cmd', {
                    replSetReconfig: config, force: force
                  }, credentials, { ignoreError: true });

                case 11:
                  result = _context12.sent;


                  // Push new configuration to list
                  self.configurations.push(config);

                  // If we want to return immediately do so now

                  if (!returnImmediately) {
                    _context12.next = 17;
                    break;
                  }

                  _context12.next = 16;
                  return node.stop();

                case 16:
                  return _context12.abrupt('return', resolve());

                case 17:
                  _context12.next = 19;
                  return node.stop();

                case 19:
                  _context12.next = 21;
                  return self.waitForPrimary();

                case 21:
                  return _context12.abrupt('return', resolve());

                case 22:
                case 'end':
                  return _context12.stop();
              }
            }
          }, _callee12, this);
        })).catch(reject);
      });
    }

    /**
     * Remove a member from the set
     * @method
     * @param {object} node The server manager that we wish to remove from the set.
     * @param {object} [options] Any options for the operation.
     * @param {boolean} [options.returnImmediately=false] Return immediately after executing stepdown, otherwise block until new primary is available.
     * @param {boolean} [options.maxRetries=30] Number of retries before giving up for the server to come back as secondary.
     * @param {object} [credentials] Credentials needed to perform an admin authenticated command.
     * @returns {Promise}
     */

  }, {
    key: 'maintenance',
    value: function maintenance(value, node, options, credentials) {
      options = options || { returnImmediately: false };
      var self = this;

      // Default returnImmediately to false
      var returnImmediately = typeof options.returnImmediately == 'boolean' ? options.returnImmediately : false;
      var maxRetries = typeof options.maxRetries == 'number' ? options.maxRetries : 30;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee13() {
          var ismaster, result, currentTries;
          return regeneratorRuntime.wrap(function _callee13$(_context13) {
            while (1) {
              switch (_context13.prev = _context13.next) {
                case 0:
                  _context13.next = 2;
                  return node.ismaster();

                case 2:
                  ismaster = _context13.sent;

                  if (!(value == true && !ismaster.secondary)) {
                    _context13.next = 7;
                    break;
                  }

                  return _context13.abrupt('return', reject(new Error(f('the server at %s is not a secondary', node.name))));

                case 7:
                  if (!(value == false && (ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly))) {
                    _context13.next = 9;
                    break;
                  }

                  return _context13.abrupt('return', reject(new Error(f('the server at %s is not in maintenance mode', node.name))));

                case 9:
                  _context13.next = 11;
                  return node.executeCommand('admin.$cmd', {
                    replSetMaintenance: value
                  }, credentials);

                case 11:
                  result = _context13.sent;

                  if (!(result && result.ok == 0)) {
                    _context13.next = 14;
                    break;
                  }

                  return _context13.abrupt('return', reject(new Error(f('failed to execute replSetMaintenance for server [%s]', node.name))));

                case 14:
                  if (!(value == false && returnImmediately || value == true)) {
                    _context13.next = 16;
                    break;
                  }

                  return _context13.abrupt('return', resolve());

                case 16:

                  // Max waitTime
                  currentTries = maxRetries;

                  // Did we pull the server back from maintenance mode

                case 17:
                  if (!true) {
                    _context13.next = 30;
                    break;
                  }

                  if (!(currentTries == 0)) {
                    _context13.next = 20;
                    break;
                  }

                  return _context13.abrupt('return', reject(new Error(f('server %s failed to come back as a secondary after %s milliseconds waiting', node.name, maxRetries * 1000))));

                case 20:
                  _context13.next = 22;
                  return waitMS(1000);

                case 22:
                  _context13.next = 24;
                  return node.ismaster();

                case 24:
                  ismaster = _context13.sent;

                  if (!ismaster.secondary) {
                    _context13.next = 27;
                    break;
                  }

                  return _context13.abrupt('return', resolve());

                case 27:

                  currentTries = currentTries - 1;
                  _context13.next = 17;
                  break;

                case 30:

                  resolve();

                case 31:
                case 'end':
                  return _context13.stop();
              }
            }
          }, _callee13, this);
        })).catch(reject);
      });
    }
  }, {
    key: 'stop',
    value: function stop(signal) {
      var self = this;
      signal = typeof signal == 'number' ? signals[signal] : signals[9];

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee14() {
          var i;
          return regeneratorRuntime.wrap(function _callee14$(_context14) {
            while (1) {
              switch (_context14.prev = _context14.next) {
                case 0:
                  i = 0;

                case 1:
                  if (!(i < self.managers.length)) {
                    _context14.next = 7;
                    break;
                  }

                  _context14.next = 4;
                  return self.managers[i].stop(signal);

                case 4:
                  i++;
                  _context14.next = 1;
                  break;

                case 7:

                  resolve();

                case 8:
                case 'end':
                  return _context14.stop();
              }
            }
          }, _callee14, this);
        })).catch(reject);
      });
    }
  }, {
    key: 'restart',
    value: function restart(signal, options) {
      var self = this;
      signal = typeof signal == 'number' ? signals[signal] : signals[9];
      options = options || {};

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee15() {
          return regeneratorRuntime.wrap(function _callee15$(_context15) {
            while (1) {
              switch (_context15.prev = _context15.next) {
                case 0:
                  _context15.next = 2;
                  return self.stop(signal);

                case 2:
                  if (!(typeof options.waitMS == 'number')) {
                    _context15.next = 5;
                    break;
                  }

                  _context15.next = 5;
                  return waitMS(options.waitMS);

                case 5:
                  _context15.next = 7;
                  return self.purge();

                case 7:

                  // Clean out the configuration
                  self.configurations = [];

                  // Restart the servers
                  _context15.next = 10;
                  return self.start();

                case 10:
                  resolve();

                case 11:
                case 'end':
                  return _context15.stop();
              }
            }
          }, _callee15, this);
        })).catch(function (e) {
          console.log(e.stack);
          reject(e);
        });
      });
    }
  }, {
    key: 'purge',
    value: function purge() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee16() {
          var i;
          return regeneratorRuntime.wrap(function _callee16$(_context16) {
            while (1) {
              switch (_context16.prev = _context16.next) {
                case 0:
                  i = 0;

                case 1:
                  if (!(i < self.managers.length)) {
                    _context16.next = 7;
                    break;
                  }

                  _context16.next = 4;
                  return self.managers[i].purge();

                case 4:
                  i++;
                  _context16.next = 1;
                  break;

                case 7:

                  resolve();

                case 8:
                case 'end':
                  return _context16.stop();
              }
            }
          }, _callee16, this);
        })).catch(reject);
      });
    }
  }]);

  return ReplSet;
}();

/*
 * Generate the replicaset configuration file
 */


var generateConfiguration = function generateConfiguration(_id, version, nodes, settings) {
  var members = [];

  // Generate members
  for (var i = 0; i < nodes.length; i++) {
    var node = nodes[i];
    var member = {
      _id: i + 1,
      host: f('%s:%s', node.options.bind_ip, node.options.port)
    };

    // Did we specify any special options
    if (node.arbiter) member.arbiterOnly = true;
    if (node.builIndexes) member.buildIndexes = true;
    if (node.hidden) member.hidden = true;
    if (typeof node.priority == 'number') member.priority = node.priority;
    if (node.tags) member.tags = node.tags;
    if (node.slaveDelay) member.slaveDelay = node.slaveDelay;
    if (node.votes) member.votes = node.votes;

    // Add to members list
    members.push(member);
  }

  // Configuration passed back
  var configuration = {
    _id: _id, version: version, members: members
  };

  if (settings) {
    configuration.settings = settings;
  }

  return configuration;
};

module.exports = ReplSet;

// SIGHUP      1       Term    Hangup detected on controlling terminal
//                             or death of controlling process
// SIGINT      2       Term    Interrupt from keyboard
// SIGQUIT       3       Core    Quit from keyboard
// SIGILL      4       Core    Illegal Instruction
// SIGABRT       6       Core    Abort signal from abort(3)
// SIGFPE      8       Core    Floating point exception
// SIGKILL       9       Term    Kill signal
// SIGSEGV      11       Core    Invalid memory reference
// SIGPIPE      13       Term    Broken pipe: write to pipe with no readers
// SIGALRM      14       Term    Timer signal from alarm(2)
// SIGTERM      15       Term    Termination signal
// Signal map
var signals = {
  1: 'SIGHUP',
  2: 'SIGINT',
  3: 'SIGQUIT',
  4: 'SIGABRT',
  6: 'SIGABRT',
  8: 'SIGFPE',
  9: 'SIGKILL',
  11: 'SIGSEGV',
  13: 'SIGPIPE',
  14: 'SIGALRM',
  15: 'SIGTERM',
  17: 'SIGSTOP',
  19: 'SIGSTOP',
  23: 'SIGSTOP'
};
