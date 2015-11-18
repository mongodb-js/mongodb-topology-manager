"use strict"

var co = require('co'),
  f = require('util').format,
  mkdirp = require('mkdirp'),
  rimraf = require('rimraf'),
  Server = require('./server'),
  CoreServer = require('mongodb-core').Server,
  spawn = require('child_process').spawn;

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

class ReplSet {
  constructor(binary, nodes, options) {
    options = options || {};
    // Save the default passed in parameters
    this.nodes = nodes;
    this.options = clone(options);

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

          // Resolve the server version
          resolve({
            version: versionMatch.toString().split('.').map(function(x) {
              return parseInt(x, 10);
            })
          })          
        });
      }).catch(reject);
    });    
  }

  start() {    
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        // console.log("---------------------------------- replicaset start")
        // We are already running, just return
        if(self.state == 'running') return resolve();
        
        // Boot all the servers
        for(var i = 0; i < self.managers.length; i++) {
          yield self.managers[i].start();
        }
        // console.log("---------------------------------- replicaset nodes started")

        // Time to configure the servers by generating the 
        var config = generateConfiguration(self.replSet, self.version, self.nodes, self.configSettings);

        // console.log("---------------------------------- replicaset generated config")

        // console.log("-------------------------------------- configuration")
        // console.dir(config)

        // Pick the first manager and execute replicaset configuration
        var result = yield self.managers[0].executeCommand('admin.$cmd', {
          replSetInitiate: config
        });

        // console.log("---------------------------------- replicaset generated config set")

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
          yield waitMS(200);

          // Monitoring state
          var state = {
            primaries: 0,
            secondaries: 0,
            arbiters: 0
          }

          // Get the replicaset status
          var result = yield self.managers[0].executeCommand('admin.$cmd', {
            replSetGetStatus: true
          });

          // Failed to execute monitoring
          if(result.ok == 0) {
            return reject(new Error(f('failed to execute replSetGetStatus against replicaset with config %s', JSON.stringify(config))));
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

          // Validate the state
          if(state.primaries == 1
            && state.arbiters == numberOfArbiters
            && state.secondaries == (self.nodes.length - numberOfArbiters - 1)) {
            break;
          }
        }

        // console.log("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% START")
        // console.dir(state)

        // Wait for the primary to appear in ismaster result
        yield self.waitForPrimary();
        // console.log("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% START 1")

        // Get the last seen election Id
        var ismaster = yield self.managers[0].ismaster();
        // Save the current election Id if it exists
        self.electionId = ismaster.electionId;
        self.lastKnownPrimary = ismaster.me;
        // console.log("------------------------------------------------------")
        // console.dir(ismaster)

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
          if(ismaster.ismaster) return resolve(self.managers[i]);
        }

        resolve();
      }).catch(reject);
    });
  }

  /**
   * Locate all the arbiters
   * @method
   * @returns {Promise}
   */
  secondaries() {
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
          if(ismaster.secondary) secondaries.push(self.managers[i]);
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
                yield waitMS(31000);
                // Set waitedForElectionCycle
                waitedForElectionCycle = true;
              } else if(ismaster.ismaster
                && waitedForElectionCycle) {
                return resolve();
              }
            } catch(err) {              
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

        // console.log("------------------------------------------- stepDownPrimary 0")

        // Locate the current primary
        var manager = yield self.primary();
        if(manager == null) {
          return reject(new Error('no primary found in the replicaset'));
        }
        // console.log("------------------------------------------- stepDownPrimary 1")
        // console.dir(command)

        // Pick the first manager and execute replicaset configuration
        try {
          var result = yield manager.executeCommand('admin.$cmd', command, credentials);
        } catch(err) {
          // console.log("------------------------------------------- stepDownPrimary 1:1")
          // console.dir(err)
          // We got an error back from the command, if successful the socket is closed
          if(err.ok == 0) {
            return reject(new Error('failed to step down primary'));
          }
        }
        // console.log("------------------------------------------- stepDownPrimary 2")

        // Do we need to return immediately
        if(returnImmediately) {
          return resolve();
        }
        // console.log("------------------------------------------- stepDownPrimary 3")

        // We want to wait for a new primary to appear
        yield self.waitForPrimary();
        // console.log("------------------------------------------- stepDownPrimary 4")
        // Finish up
        resolve();
      }).catch(reject);
    });
  }

  stop() {    
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        for(var i = 0; i < self.managers.length; i++) {
          yield self.managers[i].stop();
        }

        resolve();
      }).catch(reject);
    });    
  }

  restart() {    
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
      }).catch(reject);
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