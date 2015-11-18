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

        // We have a stable replicaset
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