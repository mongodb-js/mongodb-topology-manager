"use strict"

var co = require('co'),
  f = require('util').format,
  mkdirp = require('mkdirp'),
  rimraf = require('rimraf'),
  Logger = require('./logger'),
  Server = require('./server'),
  EventEmitter = require('events'),
  CoreServer = require('mongodb-core').Server,
  spawn = require('child_process').spawn;

var Promise = require("bluebird");

var clone = function(o) {
  var obj = {}; for(var name in o) obj[name] = o[name]; return obj;
}

var waitMS = function(ms) {
  return new Promise(function(resolve, reject) {
    setTimeout(function() {
      resolve();
    }, ms);
  });
}

class ReplSet extends EventEmitter {
  constructor(binary, nodes, options) {
    super();
    options = options || {};
    // Save the default passed in parameters
    this.nodes = nodes;
    this.options = clone(options);

    // Create logger instance
    this.logger = Logger('ReplSet', options);

    // Did we specify special settings for the configuration JSON used
    // to set up the replicaset (delete it from the internal options after
    // transferring it to a new variable)
    if(this.options.configSettings) {
      this.configSettings = this.options.configSettings;
      delete this.options['configSettings'];
    }

    // Ensure we have a list of nodes
    if(!Array.isArray(this.nodes) || this.nodes.length == 0) {
      throw new Error('a list of nodes must be passed in');
    }

    // Ensure we have set basic options
    if(!options.replSet) throw new Error('replSet must be set');

    // Server state
    this.state = 'stopped';

    // Unpack default runtime information
    this.binary = binary || 'mongod';

    // Wait times
    this.electionCycleWaitMS = typeof this.options.electionCycleWaitMS == 'number'
      ? this.options.electionCycleWaitMS : 31000;
    this.retryWaitMS = typeof this.options.retryWaitMS == 'number'
      ? this.options.retryWaitMS : 5000;

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
    this.managers = this.nodes.map(function(x) {
      var opts = clone(x.options);
      delete opts['logpath'];

      // Add the needed replicaset options
      opts.replSet = options.replSet;

      // Set server instance
      var server = new Server(self.binary, opts, options);
      server.on('state', function(state) {
        self.emit('state', state);
      });

      // Create manager
      return server;
    });
  }

  discover() {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        var proc = spawn(self.binary, ['--version']);
        // Variables receiving data
        var stdout = '';
        var stderr = '';
        // Get the stdout
        proc.stdout.on('data', function(data) { stdout += data; });
        // Get the stderr
        proc.stderr.on('data', function(data) { stderr += data; });
        // Got an error
        proc.on('error', function(err) { reject(err); });
        // Process terminated
        proc.on('close', function(code) {
          // Perform version match
          var versionMatch = stdout.match(/[0-9]+\.[0-9]+\.[0-9]+/)

          // Check if we have ssl
          var sslMatch = stdout.match(/ssl/i)
          // Final result
          var result = {
            version: versionMatch.toString().split('.').map(function(x) {
              return parseInt(x, 10);
            }),
            ssl: sslMatch != null
          }

          if(self.logger.isInfo()) {
            self.logger.info(f('mongod discovery returned %s', JSON.stringify(result)));
          }

          // Resolve the server version
          resolve(result);
        });
      }).catch(reject);
    });
  }

  start() {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        // We are already running, just return
        if(self.state == 'running') return resolve();

        // Emit start event
        self.emit('state', {
          event: 'start', topology: 'replSet', nodes: self.nodes, options: self.options,
        });

        // Get the version information
        var result = yield self.discover();

        // Boot all the servers
        for(var i = 0; i < self.managers.length; i++) {
          yield self.managers[i].start();
        }

        // Time to configure the servers by generating the
        var config = generateConfiguration(self.replSet, self.version, self.nodes, self.configSettings);

        if(self.logger.isInfo()) {
          self.logger.info(f('initialize replicaset with config %s', JSON.stringify(config)));
        }

        // Ignore Error
        var ignoreError = result.version[0] == 2
          && result.version[1] <= 6 ? true : false;

        // Pick the first manager and execute replicaset configuration
        var result = yield self.managers[0].executeCommand('admin.$cmd', {
          replSetInitiate: config
        }, null, { ignoreError: ignoreError });

        // Did the command fail, error out
        if(result.ok == 0) {
          return reject(new Error(f('failed to initialize replicaset with config %s', JSON.stringify(config))));
        }

        // Push configuration to the history
        self.configurations.push(config);

        // Waiting
        var numberOfArbiters = 0;
        // Count the number of expected arbiters
        self.nodes.forEach(function(x) {
          if(x.arbiter) numberOfArbiters = numberOfArbiters + 1;
        })

        // Now monitor until we have all the servers in a healthy state
        while(true) {
          // Wait for 200 ms before trying again
          yield waitMS(1000);
          // console.log("================= start 6:1")

          // Monitoring state
          var state = {
            primaries: 0,
            secondaries: 0,
            arbiters: 0
          }

          // Get the replicaset status
          try {
            var result = yield self.managers[0].executeCommand('admin.$cmd', {
              replSetGetStatus: true
            });
          } catch(err) {
            console.log(err)
            continue;
          }

          // Sum up expected servers
          for(var i = 0; i < result.members.length; i++) {
            var member = result.members[i];

            if(member.health == 1) {
              if(member.state == 2) {
                state.secondaries = state.secondaries + 1;
              }

              if(member.state == 1) {
                state.primaries = state.primaries + 1;
              }

              if(member.state == 7) {
                state.arbiters = state.arbiters + 1;
              }
            }
          }

          if(self.logger.isInfo()) {
            self.logger.info(f('replicaset current state %s', JSON.stringify(state)));
          }

          // Validate the state
          if(state.primaries == 1
            && state.arbiters == numberOfArbiters
            && state.secondaries == (self.nodes.length - numberOfArbiters - 1)) {
            break;
          }
        }

        // Wait for the primary to appear in ismaster result
        yield self.waitForPrimary();
        // Get the last seen election Id
        var ismaster = yield self.managers[0].ismaster();
        // Save the current election Id if it exists
        self.electionId = ismaster.electionId;
        self.lastKnownPrimary = ismaster.me;

        // Emit start event
        self.emit('state', {
          event: 'running', topology: 'replSet', nodes: self.nodes, options: self.options,
        });

        // We have a stable replicaset
        resolve();
      }).catch(reject);
    });
  }

  /**
   * Locate the primary server manager
   * @method
   * @returns {Promise}
   */
  primary() {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        // Go over all the managers
        for(var i = 0; i < self.managers.length; i++) {
          var ismaster = yield self.managers[i].ismaster();

          if(ismaster.ismaster) {
            return resolve(self.managers[i]);
          }
        }

        reject(new Error('no primary server found in set'));
      }).catch(reject);
    });
  }

  /**
   * Return add shard url
   * @method
   * return {String}
   */
  shardUrl() {
    var members = this.nodes.map(function(x) {
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
  url() {
    var members = this.nodes.map(function(x) {
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
  arbiters() {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        var arbiters = [];

        // Go over all the managers
        for(var i = 0; i < self.managers.length; i++) {
          var ismaster = yield self.managers[i].ismaster();
          if(ismaster.arbiterOnly) arbiters.push(self.managers[i]);
        }

        resolve(arbiters);
      }).catch(reject);
    });
  }

  /**
   * Locate all the secondaries
   * @method
   * @returns {Promise}
   */
  secondaries() {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        var secondaries = [];

        // Go over all the managers
        for(var i = 0; i < self.managers.length; i++) {
          var ismaster = yield self.managers[i].ismaster();
          // Check if we have a secondary but might be a passive
          if(ismaster.secondary
            && ismaster.passives
            && ismaster.passives.indexOf(ismaster.me) == -1) {
              secondaries.push(self.managers[i]);
          } else if(ismaster.secondary
            && !ismaster.passives) {
              secondaries.push(self.managers[i]);
          }
        }

        resolve(secondaries);
      }).catch(reject);
    });
  }

  /**
   * Locate all the passives
   * @method
   * @returns {Promise}
   */
  passives() {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        var secondaries = [];

        // Go over all the managers
        for(var i = 0; i < self.managers.length; i++) {
          var ismaster = yield self.managers[i].ismaster();
          // Check if we have a secondary but might be a passive
          if(ismaster.secondary
            && ismaster.passives
            && ismaster.passives.indexOf(ismaster.me) != -1) {
              secondaries.push(self.managers[i]);
          }
        }

        resolve(secondaries);
      }).catch(reject);
    });
  }

  /**
   * Block until we have a new primary available
   * @method
   * @returns {Promise}
   */
  waitForPrimary() {
    var self = this;
    var waitedForElectionCycle = false;

    return new Promise(function(resolve, reject) {
      co(function*() {
        // Keep going until we have a new primary
        while(true) {
          for(var i = 0; i < self.managers.length; i++) {
            try {
              var ismaster = yield self.managers[i].ismaster();

              // Do we have an electionId and ismaster
              if(ismaster.electionId
                && ismaster.ismaster
                && !ismaster.electionId.equals(self.electionId)) {
                // We have a new primary
                self.electionId = ismaster.electionId;
                self.lastKnownPrimary = ismaster.me;
                // Return the manager
                return resolve(self.managers[i]);
              } else if(ismaster.ismaster
                && !waitedForElectionCycle) {
                // Wait for 31 seconds to allow a full election cycle to pass
                yield waitMS(self.electionCycleWaitMS);
                // Set waitedForElectionCycle
                waitedForElectionCycle = true;
              } else if(ismaster.ismaster
                && waitedForElectionCycle) {
                return resolve();
              }
            } catch(err) {
              yield waitMS(self.retryWaitMS);
            }
          }

          // Wait for second and retry detection
          yield waitMS(1000);
        }
      }).catch(reject);
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
  stepDownPrimary(returnImmediately, options, credentials) {
    var self = this;
    options = options || {};

    return new Promise(function(resolve, reject) {
      co(function*() {
        options = clone(options);

        // Step down command
        var command = {
          replSetStepDown: typeof options.stepDownSecs == 'number'
            ? options.stepDownSecs
            : 60
        }

        // Remove stepDownSecs
        delete options['stepDownSecs'];
        // Mix in any other options
        for(var name in options) {
          command[name] = options[name];
        }

        // Locate the current primary
        var manager = yield self.primary();
        if(manager == null) {
          return reject(new Error('no primary found in the replicaset'));
        }

        // Pick the first manager and execute replicaset configuration
        try {
          var result = yield manager.executeCommand('admin.$cmd', command, credentials);
        } catch(err) {
          // We got an error back from the command, if successful the socket is closed
          if(err.ok == 0) {
            return reject(err);
          }
        }

        // Get the result
        var r = yield self.discover();
        // We have an error and the server is > 3.0
        if(r.version[0] >= 3) {
          if(result && result.ok == 0) {
            return reject(result);
          }
        }

        // Do we need to return immediately
        if(returnImmediately) {
          return resolve();
        }

        // We want to wait for a new primary to appear
        yield self.waitForPrimary();

        // Finish up
        resolve();
      }).catch(reject);
    });
  }

  /**
   * Get the current replicaset configuration
   * @method
   * @param {object} manager The server manager that we wish to use to get the current configuration.
   * @param {object} [credentials] Credentials needed to perform an admin authenticated command.
   * @returns {Promise}
   */
  configuration(manager, credentials) {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {

        // Get the current mongod version
        var result = yield self.discover();

        // Do we have a mongod version 3.0.0 or higher
        if(result[0] >= 3) {
          // Execute the reconfigure command
          var result = yield manager.executeCommand('admin.$cmd', {
            replSetGetConfig: true
          }, credentials);

          if(result && result.ok == 0) {
            return reject(new Error(f('failed to execute replSetGetConfig against server [%s]', node.name)));
          }

          resolve(result.config);
        } else {

          // Get a server instance
          var server = yield manager.instance(credentials);
          // Get the configuration document
          var cursor = server.cursor('local.system.replset', {
              find: 'local.system.replset'
            , query: {}
            , limit: 1
          });

          // Execute next
          cursor.next(function(err, d) {
            if(err) return reject(err);
            if(!d) return reject(new Error('no replicaset configuration found'));
            resolve(d);
          });
        }
      }).catch(reject);
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
  reconfigure(config, options, credentials) {
    options = options || {returnImmediately:false};
    var self = this;

    // Default returnImmediately to false
    var returnImmediately = typeof options.returnImmediately == 'boolean' ? options.returnImmediately : false;
    // Default force to false
    var force = typeof options.force == 'boolean' ? options.force : false;

    return new Promise(function(resolve, reject) {
      co(function*() {
        // Last known config
        var lastConfig = self.configurations[self.configurations.length - 1];
        // Grab the current configuration and clone it (including member object)
        config = clone(config);
        config.members = config.members.map(function(x) {
          return clone(x);
        });

        // Update the version to the latest + 1
        config.version = lastConfig.version + 1;

        // Reconfigure the replicaset
        var primary = yield self.primary();
        if(!primary) return reject(new Error('no primary available'));
        // Execute the reconfigure command
        var result = yield primary.executeCommand('admin.$cmd', {
          replSetReconfig: config, force: force
        }, credentials, {ignoreError:true});

        if(result && result.ok == 0) {
          return reject(new Error(f('failed to execute replSetReconfig with configuration [%s]', JSON.stringify(config))))
        }

        // Push new configuration to list
        self.configurations.push(config);

        // If we want to return immediately do so now
        if(returnImmediately) return resolve(server);

        // Found a valid state
        var waitedForElectionCycle = false;

        // Wait for the server to get in a stable state
        while(true) {
          try {
            var primary = yield self.primary();
            if(!primary) {
              yield waitMS(self.retryWaitMS);
              continue;
            }

            // Get the current ismaster
            var ismaster = yield primary.ismaster();

            // Did we cause a new election
            if(ismaster.ismaster
              && ismaster.electionId
              && !self.electionId.equals(ismaster.electionId)) {
              yield self.waitForPrimary();
              return resolve();
            } else if((ismaster.secondary || ismaster.arbiterOnly)
              && ismaster.electionId
              && self.electionId.equals(ismaster.electionId)) {
              return resolve();
            } else if((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly)
              && !waitedForElectionCycle) {
              // Wait for an election cycle to have passed
              waitedForElectionCycle = true;
              yield waitMS(self.electionCycleWaitMS);
            } else if((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly)
              && waitedForElectionCycle) {
              return resolve();
            } else {
              yield waitMS(self.retryWaitMS);
            }
          } catch(err) {
            yield waitMS(self.retryWaitMS);
          }
        }

        // Should not reach here
        reject(new Error(f('failed to successfully set a configuration [%s]', JSON.stringify(config))));
      }).catch(reject);
    });
  }

  /**
   * Get seed list node configuration
   * @method
   * @param {object} node server manager we want node configuration from
   * @returns {Promise}
   */
  serverConfiguration(n) {
    var node = null;

    // Is the node an existing server manager, get the info from the node
    if(n instanceof Server) {
      // Locate the known node for this server
      for(var i = 0; i < this.nodes.length; i++) {
        var _n = this.nodes[i];
        if(_n.options.bind_ip == n.host
          && _n.options.port == n.port) {
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
  addMember(node, options, credentials) {
    options = options || {returnImmediately:false};
    var self = this;

    // Default returnImmediately to false
    var returnImmediately = typeof options.returnImmediately == 'boolean' ? options.returnImmediately : false;
    // Default force to false
    var force = typeof options.force == 'boolean' ? options.force : false;

    // Is the node an existing server manager, get the info from the node
    if(node instanceof Server) {
      // Locate the known node for this server
      for(var i = 0; i < this.nodes.length; i++) {
        var n = this.nodes[i];
        if(n.options.bind_ip == node.host
          && n.options.port == node.port) {
            node = n;
            break;
          }
      }
    }

    // Return the promise
    return new Promise(function(resolve, reject) {
      co(function*() {
        // Clone the top level settings
        node = clone(node);
        // Clone the settings and remove the logpath
        var opts = clone(node.options);
        delete opts['logpath'];

        // Add the needed replicaset options
        opts.replSet = self.options.replSet;

        // Create a new server instance
        var server = new Server(self.binary, opts, self.options);
        server.on('state', function(state) {
          self.emit('state', state);
        });

        // If we have an existing manager remove it
        var newManagers = [];

        // Need to wait for Primary
        var needWaitForPrimary = false;
        // Do we already have a manager then stop it, purge it and remove it
        for(var i = 0; i < self.managers.length; i++) {
          if(f('%s:%s', self.managers[i].host, self.managers[i].port) == f('%s:%s', server.host, server.port)) {
            yield self.managers[i].stop();
            yield self.managers[i].purge();
            needWaitForPrimary = true;
          } else {
            newManagers.push(self.managers[i]);
          }
        }

        // Set up the managers
        self.managers = newManagers;

        // Purge the directory
        yield server.purge();

        // Boot the instance
        yield server.start();

        // Wait primary
        if(needWaitForPrimary) {
          yield self.waitForPrimary();
        }

        // No configurations available
        if(self.configurations.length == 0) {
          return reject(new Error('no configurations exist yet, did you start the replicaset?'));
        }

        // Locate max id
        var max = 0;

        // Grab the current configuration and clone it (including member object)
        var config = clone(self.configurations[self.configurations.length - 1]);
        config.members = config.members.map(function(x) {
          max = x._id > max ? x._id : max;
          return clone(x);
        });

        // Let's add our new server to the configuration
        delete node['options'];
        // Create the member
        var member = {
          _id: max + 1,
          host: f('%s:%s', opts.bind_ip, opts.port),
        };

        // Did we specify any special options
        if(node.arbiter) member.arbiterOnly = true;
        if(node.builIndexes) member.buildIndexes = true;
        if(node.hidden) member.hidden = true;
        if(typeof node.priority == 'number') member.priority = node.priority;
        if(node.tags) member.tags = node.tags;
        if(node.slaveDelay) member.slaveDelay = node.slaveDelay;
        if(node.votes) member.votes = node.votes;

        // Add to the list of members
        config.members.push(member);
        // Update the configuration version
        config.version = config.version + 1;

        // Reconfigure the replicaset
        var primary = yield self.primary();
        if(!primary) return reject(new Error('no primary available'));

        // Execute the reconfigure command
        var result = yield primary.executeCommand('admin.$cmd', {
          replSetReconfig: config, force: force
        }, credentials);

        if(result && result.ok == 0) {
          return reject(new Error(f('failed to execute replSetReconfig with configuration [%s]', JSON.stringify(config))))
        }

        // Push new configuration to list
        self.configurations.push(config);

        // Add manager to list of managers
        self.managers.push(server);

        // If we want to return immediately do so now
        if(returnImmediately) return resolve(server);

        // Found a valid state
        var waitedForElectionCycle = false;

        // Wait for the server to get in a stable state
        while(true) {
          try {
            // Get the ismaster for this server
            var ismaster = yield server.ismaster();
            // Did we cause a new election
            if(ismaster.ismaster
              && ismaster.electionId
              && !self.electionId.equals(ismaster.electionId)) {
              yield self.waitForPrimary();
              return resolve(server);
            } else if((ismaster.secondary || ismaster.arbiterOnly)
              && ismaster.electionId
              && self.electionId.equals(ismaster.electionId)) {
              return resolve(server);
            } else if((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly)
              && !waitedForElectionCycle) {
              // Wait for an election cycle to have passed
              waitedForElectionCycle = true;
              yield waitMS(self.electionCycleWaitMS);
            } else if((ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly)
              && waitedForElectionCycle) {
              // Wait for a primary to appear
              yield self.waitForPrimary();
              // Return
              return resolve(server);
            } else {
              yield waitMS(self.retryWaitMS);
            }
          } catch(err) {
            yield waitMS(self.retryWaitMS);
          }
        }

        // Should not reach here
        reject(new Error(f('failed to successfully add a new member with options [%s]', JSON.stringify(node))));
      }).catch(reject);
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
  removeMember(node, options, credentials) {
    options = options || {returnImmediately:false};
    var self = this;

    // Default returnImmediately to false
    var returnImmediately = typeof options.returnImmediately == 'boolean' ? options.returnImmediately : false;
    // Default force to false
    var force = typeof options.force == 'boolean' ? options.force : false;
    // Default skipWait
    var skipWait = typeof options.skipWait == 'boolean' ? options.skipWait : false;

    return new Promise(function(resolve, reject) {
      co(function*() {
        // Grab the current configuration and clone it (including member object)
        var config = clone(self.configurations[self.configurations.length - 1]);
        config.members = config.members.map(function(x) {
          return clone(x);
        });

        // Locate the member and remove it
        config.members = config.members.filter(function(x) {
          return x.host != node.name;
        });

        // Update the configuration version
        config.version = config.version + 1;

        // Reconfigure the replicaset
        var primary = yield self.primary();
        if(!primary) return reject(new Error('no primary available'));

        // Execute the reconfigure command
        var result = yield primary.executeCommand('admin.$cmd', {
          replSetReconfig: config, force: force
        }, credentials, {ignoreError:true});

        // Push new configuration to list
        self.configurations.push(config);

        // If we want to return immediately do so now
        if(returnImmediately) {
          // Shut down node
          yield node.stop();
          // Finished
          return resolve();
        }

        // Shut down node
        yield node.stop();
        // Wait for a primary to appear
        yield self.waitForPrimary();
        // Resolve
        return resolve();
      }).catch(reject);
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
  maintenance(value, node, options, credentials) {
    options = options || {returnImmediately:false};
    var self = this;

    // Default returnImmediately to false
    var returnImmediately = typeof options.returnImmediately == 'boolean' ? options.returnImmediately : false;
    var maxRetries = typeof options.maxRetries == 'number' ? options.maxRetries : 30;

    return new Promise(function(resolve, reject) {
      co(function*() {
        // Establish if the node is a secondary
        var ismaster = yield node.ismaster();

        // Ensure we only call the operation on a server in the right mode
        if(value == true && !ismaster.secondary) {
          return reject(new Error(f('the server at %s is not a secondary', node.name)));
        } else if(value == false && (ismaster.ismaster || ismaster.secondary || ismaster.arbiterOnly)) {
          return reject(new Error(f('the server at %s is not in maintenance mode', node.name)));
        }

        // We have a secondary, execute the command against it
        var result = yield node.executeCommand('admin.$cmd', {
          replSetMaintenance: value
        }, credentials);

        // Return the error
        if(result && result.ok == 0) {
          return reject(new Error(f('failed to execute replSetMaintenance for server [%s]', node.name)));
        }

        // Bring back the node from maintenance but don't wait around
        if((value == false && returnImmediately) || value == true) {
          return resolve();
        }

        // Max waitTime
        var currentTries = maxRetries;

        // Did we pull the server back from maintenance mode
        while(true) {
          if(currentTries == 0) {
            return reject(new Error(f('server %s failed to come back as a secondary after %s milliseconds waiting', node.name, (maxRetries*1000))));
          }

          // Wait for 1000 ms before figuring out if the node is back
          yield waitMS(1000);

          // Get the result
          var ismaster = yield node.ismaster();

          // Is it back to secondary state
          if(ismaster.secondary) {
            return resolve();
          }

          currentTries = currentTries - 1;
        }

        resolve();
      }).catch(reject);
    });
  }

  stop(signal) {
    var self = this;
    signal = typeof signal == 'number' ? signals[signal] : signals[9];

    return new Promise(function(resolve, reject) {
      co(function*() {
        for(var i = 0; i < self.managers.length; i++) {
          yield self.managers[i].stop(signal);
        }

        resolve();
      }).catch(reject);
    });
  }

  restart(signal, options) {
    var self = this;
    signal = typeof signal == 'number' ? signals[signal] : signals[9];
    options = options || {};

    return new Promise(function(resolve, reject) {
      co(function*() {
        // console.log("=================================== restart 0")
        // Stop the servers
        yield self.stop(signal);
        // console.log("=================================== restart 1")

        // Do we wait after stop
        if(typeof options.waitMS == 'number') {
          yield waitMS(options.waitMS);
        }
        // console.log("=================================== restart 2")

        // Purge directories
        yield self.purge();
        // console.log("=================================== restart 3")

        // Clean out the configuration
        self.configurations = [];
        // console.log("=================================== restart 4")

        // Restart the servers
        yield self.start();
        // console.log("=================================== restart 5")
        resolve();
      }).catch(function(e) {
        console.log(e.stack)
        reject(e);
      });
    });
  }

  purge() {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        // Purge all directories
        for(var i = 0; i < self.managers.length; i++) {
          yield self.managers[i].purge();
        }

        resolve();
      }).catch(reject);
    });
  }
}

/*
 * Generate the replicaset configuration file
 */
var generateConfiguration = function(_id, version, nodes, settings) {
  var members = [];

  // Generate members
  for(var i = 0; i < nodes.length; i++) {
    var node = nodes[i];
    var member = {
      _id: i+1,
      host: f('%s:%s', node.options.bind_ip, node.options.port),
    };

    // Did we specify any special options
    if(node.arbiter) member.arbiterOnly = true;
    if(node.builIndexes) member.buildIndexes = true;
    if(node.hidden) member.hidden = true;
    if(typeof node.priority == 'number') member.priority = node.priority;
    if(node.tags) member.tags = node.tags;
    if(node.slaveDelay) member.slaveDelay = node.slaveDelay;
    if(node.votes) member.votes = node.votes;

    // Add to members list
    members.push(member);
  }

  // Configuration passed back
  var configuration = {
    _id: _id, version:version, members: members
  }

  if(settings) {
    configuration.settings = settings;
  }

  return configuration;
}

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
