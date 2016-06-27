"use strict";

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var co = require('co'),
    f = require('util').format,
    mkdirp = require('mkdirp'),
    rimraf = require('rimraf'),
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

var ConfigServers = function () {
  function ConfigServers(binary, nodes, options) {
    _classCallCheck(this, ConfigServers);

    options = options || {};
    // Save the default passed in parameters
    this.nodes = nodes;
    this.options = clone(options);

    // Ensure we have a list of nodes
    if (!Array.isArray(this.nodes) || this.nodes.length == 0) {
      throw new Error('a list of nodes must be passed in');
    }

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

    // Create server managers for each node
    this.managers = this.nodes.map(function (x) {
      var opts = clone(x);
      delete opts['logpath'];
      delete opts['replSet'];

      // Add the needed config server options
      if (!opts.configsvr) opts.configsvr = null;

      // Set server instance
      var server = new Server(self.binary, opts, options);

      // Create manager
      return server;
    });
  }

  _createClass(ConfigServers, [{
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
        })).catch(reject);
      });
    }
  }, {
    key: 'start',
    value: function start() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee2() {
          var i;
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
                  i = 0;

                case 3:
                  if (!(i < self.managers.length)) {
                    _context2.next = 9;
                    break;
                  }

                  _context2.next = 6;
                  return self.managers[i].start();

                case 6:
                  i++;
                  _context2.next = 3;
                  break;

                case 9:

                  // Set the state to running
                  self.state == 'running';

                  // We have a stable replicaset
                  resolve();

                case 11:
                case 'end':
                  return _context2.stop();
              }
            }
          }, _callee2, this);
        })).catch(reject);
      });
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
        return f('%s:%s', x.bind_ip || 'localhost', x.port);
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
    key: 'secondaries',
    value: function secondaries() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee3() {
          var arbiters, i, ismaster;
          return regeneratorRuntime.wrap(function _callee3$(_context3) {
            while (1) {
              switch (_context3.prev = _context3.next) {
                case 0:
                  arbiters = [];

                  // Go over all the managers

                  i = 0;

                case 2:
                  if (!(i < self.managers.length)) {
                    _context3.next = 10;
                    break;
                  }

                  _context3.next = 5;
                  return self.managers[i].ismaster();

                case 5:
                  ismaster = _context3.sent;

                  if (ismaster.arbiterOnly) arbiters.push(self.managers[i]);

                case 7:
                  i++;
                  _context3.next = 2;
                  break;

                case 10:

                  resolve(arbiters);

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
     * Locate all the secondaries
     * @method
     * @returns {Promise}
     */

  }, {
    key: 'secondaries',
    value: function secondaries() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee4() {
          var secondaries, i, ismaster;
          return regeneratorRuntime.wrap(function _callee4$(_context4) {
            while (1) {
              switch (_context4.prev = _context4.next) {
                case 0:
                  secondaries = [];

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

                  if (ismaster.secondary) secondaries.push(self.managers[i]);

                case 7:
                  i++;
                  _context4.next = 2;
                  break;

                case 10:

                  resolve(secondaries);

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
        co(regeneratorRuntime.mark(function _callee5() {
          var i, ismaster;
          return regeneratorRuntime.wrap(function _callee5$(_context5) {
            while (1) {
              switch (_context5.prev = _context5.next) {
                case 0:
                  if (!true) {
                    _context5.next = 34;
                    break;
                  }

                  i = 0;

                case 2:
                  if (!(i < self.managers.length)) {
                    _context5.next = 30;
                    break;
                  }

                  _context5.prev = 3;
                  _context5.next = 6;
                  return self.managers[i].ismaster();

                case 6:
                  ismaster = _context5.sent;

                  if (!(ismaster.electionId && ismaster.ismaster && !ismaster.electionId.equals(self.electionId))) {
                    _context5.next = 13;
                    break;
                  }

                  // We have a new primary
                  self.electionId = ismaster.electionId;
                  self.lastKnownPrimary = ismaster.me;
                  // Return the manager
                  return _context5.abrupt('return', resolve(self.managers[i]));

                case 13:
                  if (!(ismaster.ismaster && !waitedForElectionCycle)) {
                    _context5.next = 19;
                    break;
                  }

                  _context5.next = 16;
                  return waitMS(self.electionCycleWaitMS);

                case 16:
                  // Set waitedForElectionCycle
                  waitedForElectionCycle = true;
                  _context5.next = 21;
                  break;

                case 19:
                  if (!(ismaster.ismaster && waitedForElectionCycle)) {
                    _context5.next = 21;
                    break;
                  }

                  return _context5.abrupt('return', resolve());

                case 21:
                  _context5.next = 27;
                  break;

                case 23:
                  _context5.prev = 23;
                  _context5.t0 = _context5['catch'](3);
                  _context5.next = 27;
                  return waitMS(self.retryWaitMS);

                case 27:
                  i++;
                  _context5.next = 2;
                  break;

                case 30:
                  _context5.next = 32;
                  return waitMS(1000);

                case 32:
                  _context5.next = 0;
                  break;

                case 34:
                case 'end':
                  return _context5.stop();
              }
            }
          }, _callee5, this, [[3, 23]]);
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
        co(regeneratorRuntime.mark(function _callee6() {
          var command, name, manager, result;
          return regeneratorRuntime.wrap(function _callee6$(_context6) {
            while (1) {
              switch (_context6.prev = _context6.next) {
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
                  _context6.next = 6;
                  return self.primary();

                case 6:
                  manager = _context6.sent;

                  if (!(manager == null)) {
                    _context6.next = 9;
                    break;
                  }

                  return _context6.abrupt('return', reject(new Error('no primary found in the replicaset')));

                case 9:
                  _context6.prev = 9;
                  _context6.next = 12;
                  return manager.executeCommand('admin.$cmd', command, credentials);

                case 12:
                  result = _context6.sent;
                  _context6.next = 19;
                  break;

                case 15:
                  _context6.prev = 15;
                  _context6.t0 = _context6['catch'](9);

                  if (!(_context6.t0.ok == 0)) {
                    _context6.next = 19;
                    break;
                  }

                  return _context6.abrupt('return', reject(new Error('failed to step down primary')));

                case 19:
                  if (!returnImmediately) {
                    _context6.next = 21;
                    break;
                  }

                  return _context6.abrupt('return', resolve());

                case 21:
                  _context6.next = 23;
                  return self.waitForPrimary();

                case 23:

                  // Finish up
                  resolve();

                case 24:
                case 'end':
                  return _context6.stop();
              }
            }
          }, _callee6, this, [[9, 15]]);
        })).catch(reject);
      });
    }

    /**
     * Get the current replicaset configuration
     * @method
     * @param {object} manager The server manager that we wish to remove from the set.
     * @param {object} [credentials] Credentials needed to perform an admin authenticated command.
     * @returns {Promise}
     */

  }, {
    key: 'configuration',
    value: function configuration(manager, credentials) {
      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee7() {
          var result;
          return regeneratorRuntime.wrap(function _callee7$(_context7) {
            while (1) {
              switch (_context7.prev = _context7.next) {
                case 0:
                  _context7.next = 2;
                  return manager.executeCommand('admin.$cmd', {
                    replSetGetConfig: true
                  }, credentials);

                case 2:
                  result = _context7.sent;

                  if (!(result && result.ok == 0)) {
                    _context7.next = 5;
                    break;
                  }

                  return _context7.abrupt('return', reject(new Error(f('failed to execute replSetGetConfig against server [%s]', node.name))));

                case 5:

                  resolve(result.config);

                case 6:
                case 'end':
                  return _context7.stop();
              }
            }
          }, _callee7, this);
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
        co(regeneratorRuntime.mark(function _callee8() {
          var lastConfig, primary, result, waitedForElectionCycle, ismaster;
          return regeneratorRuntime.wrap(function _callee8$(_context8) {
            while (1) {
              switch (_context8.prev = _context8.next) {
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
                  _context8.next = 6;
                  return self.primary();

                case 6:
                  primary = _context8.sent;

                  if (primary) {
                    _context8.next = 9;
                    break;
                  }

                  return _context8.abrupt('return', reject(new Error('no primary available')));

                case 9:
                  _context8.next = 11;
                  return primary.executeCommand('admin.$cmd', {
                    replSetReconfig: config, force: force
                  }, credentials);

                case 11:
                  result = _context8.sent;

                  if (!(result && result.ok == 0)) {
                    _context8.next = 14;
                    break;
                  }

                  return _context8.abrupt('return', reject(new Error(f('failed to execute replSetReconfig with configuration [%s]', JSON.stringify(config)))));

                case 14:

                  // Push new configuration to list
                  self.configurations.push(config);

                  // If we want to return immediately do so now

                  if (!returnImmediately) {
                    _context8.next = 17;
                    break;
                  }

                  return _context8.abrupt('return', resolve(server));

                case 17:

                  // Found a valid state
                  waitedForElectionCycle = false;

                  // Wait for the server to get in a stable state

                case 18:
                  if (!true) {
                    _context8.next = 60;
                    break;
                  }

                  _context8.prev = 19;
                  _context8.next = 22;
                  return self.primary();

                case 22:
                  primary = _context8.sent;

                  if (primary) {
                    _context8.next = 27;
                    break;
                  }

                  _context8.next = 26;
                  return waitMS(self.retryWaitMS);

                case 26:
                  return _context8.abrupt('continue', 18);

                case 27:
                  _context8.next = 29;
                  return primary.ismaster();

                case 29:
                  ismaster = _context8.sent;

                  if (!(ismaster.ismaster && ismaster.electionId && !self.electionId.equals(ismaster.electionId))) {
                    _context8.next = 36;
                    break;
                  }

                  _context8.next = 33;
                  return self.waitForPrimary();

                case 33:
                  return _context8.abrupt('return', resolve());

                case 36:
                  if (!((ismaster.secondary || ismaster.arbiterOnly) && ismaster.electionId && self.electionId.equals(ismaster.electionId))) {
                    _context8.next = 40;
                    break;
                  }

                  return _context8.abrupt('return', resolve());

                case 40:
                  if (!((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly) && !waitedForElectionCycle)) {
                    _context8.next = 46;
                    break;
                  }

                  // Wait for an election cycle to have passed
                  waitedForElectionCycle = true;
                  _context8.next = 44;
                  return waitMS(self.electionCycleWaitMS);

                case 44:
                  _context8.next = 52;
                  break;

                case 46:
                  if (!((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly) && waitedForElectionCycle)) {
                    _context8.next = 50;
                    break;
                  }

                  return _context8.abrupt('return', resolve());

                case 50:
                  _context8.next = 52;
                  return waitMS(self.retryWaitMS);

                case 52:
                  _context8.next = 58;
                  break;

                case 54:
                  _context8.prev = 54;
                  _context8.t0 = _context8['catch'](19);
                  _context8.next = 58;
                  return waitMS(self.retryWaitMS);

                case 58:
                  _context8.next = 18;
                  break;

                case 60:

                  // Should not reach here
                  reject(new Error(f('failed to successfully set a configuration [%s]', JSON.stringify(config))));

                case 61:
                case 'end':
                  return _context8.stop();
              }
            }
          }, _callee8, this, [[19, 54]]);
        })).catch(reject);
      });
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

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee9() {
          var opts, server, max, config, member, primary, result, waitedForElectionCycle, ismaster;
          return regeneratorRuntime.wrap(function _callee9$(_context9) {
            while (1) {
              switch (_context9.prev = _context9.next) {
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

                  // Purge the directory

                  _context9.next = 7;
                  return server.purge();

                case 7:
                  _context9.next = 9;
                  return server.start();

                case 9:
                  if (!(self.configurations.length == 0)) {
                    _context9.next = 11;
                    break;
                  }

                  return _context9.abrupt('return', reject(new Error('no configurations exist yet, did you start the replicaset?')));

                case 11:

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
                  _context9.next = 27;
                  return self.primary();

                case 27:
                  primary = _context9.sent;

                  if (primary) {
                    _context9.next = 30;
                    break;
                  }

                  return _context9.abrupt('return', reject(new Error('no primary available')));

                case 30:
                  _context9.next = 32;
                  return primary.executeCommand('admin.$cmd', {
                    replSetReconfig: config, force: force
                  }, credentials);

                case 32:
                  result = _context9.sent;

                  if (!(result && result.ok == 0)) {
                    _context9.next = 35;
                    break;
                  }

                  return _context9.abrupt('return', reject(new Error(f('failed to execute replSetReconfig with configuration [%s]', JSON.stringify(config)))));

                case 35:

                  // Push new configuration to list
                  self.configurations.push(config);

                  // Add manager to list of managers
                  self.managers.push(server);

                  // If we want to return immediately do so now

                  if (!returnImmediately) {
                    _context9.next = 39;
                    break;
                  }

                  return _context9.abrupt('return', resolve(server));

                case 39:

                  // Found a valid state
                  waitedForElectionCycle = false;

                  // Wait for the server to get in a stable state

                case 40:
                  if (!true) {
                    _context9.next = 75;
                    break;
                  }

                  _context9.prev = 41;
                  _context9.next = 44;
                  return server.ismaster();

                case 44:
                  ismaster = _context9.sent;

                  if (!(ismaster.ismaster && ismaster.electionId && !self.electionId.equals(ismaster.electionId))) {
                    _context9.next = 51;
                    break;
                  }

                  _context9.next = 48;
                  return self.waitForPrimary();

                case 48:
                  return _context9.abrupt('return', resolve(server));

                case 51:
                  if (!((ismaster.secondary || ismaster.arbiterOnly) && ismaster.electionId && self.electionId.equals(ismaster.electionId))) {
                    _context9.next = 55;
                    break;
                  }

                  return _context9.abrupt('return', resolve(server));

                case 55:
                  if (!((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly) && !waitedForElectionCycle)) {
                    _context9.next = 61;
                    break;
                  }

                  // Wait for an election cycle to have passed
                  waitedForElectionCycle = true;
                  _context9.next = 59;
                  return waitMS(self.electionCycleWaitMS);

                case 59:
                  _context9.next = 67;
                  break;

                case 61:
                  if (!((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly) && waitedForElectionCycle)) {
                    _context9.next = 65;
                    break;
                  }

                  return _context9.abrupt('return', resolve(server));

                case 65:
                  _context9.next = 67;
                  return waitMS(self.retryWaitMS);

                case 67:
                  _context9.next = 73;
                  break;

                case 69:
                  _context9.prev = 69;
                  _context9.t0 = _context9['catch'](41);
                  _context9.next = 73;
                  return waitMS(self.retryWaitMS);

                case 73:
                  _context9.next = 40;
                  break;

                case 75:

                  // Should not reach here
                  reject(new Error(f('failed to successfully add a new member with options [%s]', JSON.stringify(node))));

                case 76:
                case 'end':
                  return _context9.stop();
              }
            }
          }, _callee9, this, [[41, 69]]);
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

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee10() {
          var config, primary, result, waitedForElectionCycle, ismaster;
          return regeneratorRuntime.wrap(function _callee10$(_context10) {
            while (1) {
              switch (_context10.prev = _context10.next) {
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
                  }, credentials);

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

                  // Remove from the list of managers
                  self.managers = self.managers.filter(function (x) {
                    return x.name != node.name;
                  });

                  // If we want to return immediately do so now

                  if (!returnImmediately) {
                    _context10.next = 20;
                    break;
                  }

                  _context10.next = 19;
                  return node.stop();

                case 19:
                  return _context10.abrupt('return', resolve());

                case 20:

                  // Found a valid state
                  waitedForElectionCycle = false;

                  // Wait for the server to get in a stable state

                case 21:
                  if (!true) {
                    _context10.next = 67;
                    break;
                  }

                  _context10.prev = 22;
                  _context10.next = 25;
                  return self.primary();

                case 25:
                  primary = _context10.sent;

                  if (primary) {
                    _context10.next = 30;
                    break;
                  }

                  _context10.next = 29;
                  return waitMS(self.retryWaitMS);

                case 29:
                  return _context10.abrupt('continue', 21);

                case 30:
                  _context10.next = 32;
                  return primary.ismaster();

                case 32:
                  ismaster = _context10.sent;

                  if (!(ismaster.ismaster && ismaster.electionId && !self.electionId.equals(ismaster.electionId))) {
                    _context10.next = 41;
                    break;
                  }

                  _context10.next = 36;
                  return self.waitForPrimary();

                case 36:
                  _context10.next = 38;
                  return node.stop();

                case 38:
                  return _context10.abrupt('return', resolve());

                case 41:
                  if (!((ismaster.secondary || ismaster.arbiterOnly) && ismaster.electionId && self.electionId.equals(ismaster.electionId))) {
                    _context10.next = 45;
                    break;
                  }

                  return _context10.abrupt('return', resolve());

                case 45:
                  if (!((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly) && !waitedForElectionCycle)) {
                    _context10.next = 51;
                    break;
                  }

                  // Wait for an election cycle to have passed
                  waitedForElectionCycle = true;
                  _context10.next = 49;
                  return waitMS(self.electionCycleWaitMS);

                case 49:
                  _context10.next = 59;
                  break;

                case 51:
                  if (!((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly) && waitedForElectionCycle)) {
                    _context10.next = 57;
                    break;
                  }

                  _context10.next = 54;
                  return node.stop();

                case 54:
                  return _context10.abrupt('return', resolve());

                case 57:
                  _context10.next = 59;
                  return waitMS(self.retryWaitMS);

                case 59:
                  _context10.next = 65;
                  break;

                case 61:
                  _context10.prev = 61;
                  _context10.t0 = _context10['catch'](22);
                  _context10.next = 65;
                  return waitMS(self.retryWaitMS);

                case 65:
                  _context10.next = 21;
                  break;

                case 67:

                  // Should not reach here
                  reject(new Error(f('failed to successfully remove member [%s]', JSON.stringify(node.name))));

                case 68:
                case 'end':
                  return _context10.stop();
              }
            }
          }, _callee10, this, [[22, 61]]);
        }));
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
        co(regeneratorRuntime.mark(function _callee11() {
          var ismaster, result, currentTries;
          return regeneratorRuntime.wrap(function _callee11$(_context11) {
            while (1) {
              switch (_context11.prev = _context11.next) {
                case 0:
                  _context11.next = 2;
                  return node.ismaster();

                case 2:
                  ismaster = _context11.sent;

                  if (!(value == true && !ismaster.secondary)) {
                    _context11.next = 7;
                    break;
                  }

                  return _context11.abrupt('return', reject(new Error(f('the server at %s is not a secondary', node.name))));

                case 7:
                  if (!(value == false && (ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly))) {
                    _context11.next = 9;
                    break;
                  }

                  return _context11.abrupt('return', reject(new Error(f('the server at %s is not in maintenance mode', node.name))));

                case 9:
                  _context11.next = 11;
                  return node.executeCommand('admin.$cmd', {
                    replSetMaintenance: value
                  }, credentials);

                case 11:
                  result = _context11.sent;

                  if (!(result && result.ok == 0)) {
                    _context11.next = 14;
                    break;
                  }

                  return _context11.abrupt('return', reject(new Error(f('failed to execute replSetMaintenance for server [%s]', node.name))));

                case 14:
                  if (!(value == false && returnImmediately || value == true)) {
                    _context11.next = 16;
                    break;
                  }

                  return _context11.abrupt('return', resolve());

                case 16:

                  // Max waitTime
                  currentTries = maxRetries;

                  // Did we pull the server back from maintenance mode

                case 17:
                  if (!true) {
                    _context11.next = 30;
                    break;
                  }

                  if (!(currentTries == 0)) {
                    _context11.next = 20;
                    break;
                  }

                  return _context11.abrupt('return', reject(new Error(f('server %s failed to come back as a secondary after %s milliseconds waiting', node.name, maxRetries * 1000))));

                case 20:
                  _context11.next = 22;
                  return waitMS(1000);

                case 22:
                  _context11.next = 24;
                  return node.ismaster();

                case 24:
                  ismaster = _context11.sent;

                  if (!ismaster.secondary) {
                    _context11.next = 27;
                    break;
                  }

                  return _context11.abrupt('return', resolve());

                case 27:

                  currentTries = currentTries - 1;
                  _context11.next = 17;
                  break;

                case 30:

                  resolve();

                case 31:
                case 'end':
                  return _context11.stop();
              }
            }
          }, _callee11, this);
        })).catch(reject);
      });
    }
  }, {
    key: 'stop',
    value: function stop() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee12() {
          var i;
          return regeneratorRuntime.wrap(function _callee12$(_context12) {
            while (1) {
              switch (_context12.prev = _context12.next) {
                case 0:
                  i = 0;

                case 1:
                  if (!(i < self.managers.length)) {
                    _context12.next = 7;
                    break;
                  }

                  _context12.next = 4;
                  return self.managers[i].stop();

                case 4:
                  i++;
                  _context12.next = 1;
                  break;

                case 7:

                  resolve();

                case 8:
                case 'end':
                  return _context12.stop();
              }
            }
          }, _callee12, this);
        })).catch(reject);
      });
    }
  }, {
    key: 'restart',
    value: function restart() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee13() {
          return regeneratorRuntime.wrap(function _callee13$(_context13) {
            while (1) {
              switch (_context13.prev = _context13.next) {
                case 0:
                case 'end':
                  return _context13.stop();
              }
            }
          }, _callee13, this);
        })).catch(reject);
      });
    }
  }, {
    key: 'purge',
    value: function purge() {
      var self = this;

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
                  return self.managers[i].purge();

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
  }]);

  return ConfigServers;
}();

module.exports = ConfigServers;
