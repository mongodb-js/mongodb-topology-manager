"use strict"

var co = require('co'),
  f = require('util').format,
  mkdirp = require('mkdirp'),
  rimraf = require('rimraf'),
  Server = require('./server'),
  Logger = require('./logger'),
  ReplSet = require('./replset'),
  Mongos = require('./mongos'),
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

class Sharded {
  constructor(options) {
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

  discover() {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        var proc = spawn(self.mongod, ['--version']);
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

  addShard(nodes, options) {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        options = options || {};
        // Create a shard
        var shard = new ReplSet(self.mongod, nodes, options);
        // Add shard to list of shards
        self.shards.push(shard);
        resolve();
      }).catch(reject);
    });
  }

  addConfigurationServers(nodes, options) {    
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        options = options || {};
        // Establish the version of the mongod process
        var result = yield self.discover();
        var version = result.version;

        // If configuration server has not been set up
        options = clone(options);
        // Clone the nodes
        nodes = JSON.parse(JSON.stringify(nodes));
        // Add config server to each of the nodes
        nodes = nodes.map(function(x) {
          if(x.arbiter) {
            delete x['arbiter'];
          }

          if(!x.arbiter) {
            x.options.configsvr = null;
          }

          return x;
        });

        // Check if we have 3.2.0 or higher where we need to boot up a replicaset
        // not a set of configuration server
        if(version[0] >= 4 || (version[0] == 3 && version[1] >= 2)) {
          self.configurationServers = new ReplSet(self.mongod, nodes, options);
        } else {
          // Add special configuration server issue here
        }

        resolve();
      }).catch(reject);
    });
  }

  addProxies(nodes, options) {    
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        options = options || {};

        // Clone the options
        options = clone(options);

        // For each node create a proxy
        for(var i = 0; i < nodes.length; i++) {
          var proxy = new Mongos(self.mongos, nodes[i], options);
          self.proxies.push(proxy);
        }

        resolve();
      }).catch(reject);
    });
  }

  enableSharding(db, credentials) {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        // Get a proxy
        var proxy = self.proxies[0];

        if(self.logger.isInfo()) {
          self.logger.info(f('enable sharding for db %s', db));
        }

        // Execute the enable sharding command
        var result = yield proxy.executeCommand('admin.$cmd', {
          enableSharding: db
        }, credentials);

        if(self.logger.isInfo()) {
          self.logger.info(f('successfully enabled sharding for db %s with result [%s]', db, JSON.stringify(result)));
        }

        // Resolve
        resolve();
      }).catch(reject);
    });
  }

  shardCollection(db, collection, shardKey, options, credentials) {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        options = options || {};
        options = clone(options);
        // Get a proxy
        var proxy = self.proxies[0];

        // Create shard collection command
        var command = {
          shardCollection: f('%s.%s', db, collection), key: shardKey
        }

        // Unique shard key
        if(options.unique) {
          command.unique = true;
        }

        if(self.logger.isInfo()) {
          self.logger.info(f('shard collection for %s.%s with command [%s]', db, collection, JSON.stringify(command)));
        }

        // Execute the enable sharding command
        var result = yield proxy.executeCommand('admin.$cmd', command, credentials);

        if(self.logger.isInfo()) {
          self.logger.info(f('successfully sharded collection for %s.%s with command [%s] and result [%s]', db, collection, JSON.stringify(command), JSON.stringify(result)));
        }

        // Resolve
        resolve();
      }).catch(reject);
    });
  }

  start() {    
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {       
        // Boot up the shards first
        for(var i = 0; i < self.shards.length; i++) {
          if(self.logger.isInfo()) {
            self.logger.info(f('start shard %s', self.shards[i].shardUrl()));
          }

          // Purge directories
          yield self.shards[i].purge();
          // Start shard
          yield self.shards[i].start();
        }

        if(self.logger.isInfo()) {
          self.logger.info(f('start configuration server %s', self.configurationServers.shardUrl()));
        }

        // Purge directories
        yield self.configurationServers.purge();
        // Boot up the configuration servers
        yield self.configurationServers.start();

        // Boot up the proxies
        for(var i = 0; i < self.proxies.length; i++) {
          if(self.logger.isInfo()) {
            self.logger.info(f('start proxy at %s', self.proxies[i].name));
          }

          // Purge directories
          yield self.proxies[i].purge();
          // Start proxy
          yield self.proxies[i].start();
        }

        // Connect and add the shards
        var proxy = self.proxies[0];

        // Add all the shards
        for(var i = 0; i < self.shards.length; i++) {
          if(self.logger.isInfo()) {
            self.logger.info(f('add shard at %s', self.shards[i].shardUrl()));
          }
          // Add the shard
          yield proxy.executeCommand('admin.$cmd', {
            addShard: self.shards[i].shardUrl()
          });
        }

        if(self.logger.isInfo()) {
          self.logger.info(f('sharded topology is up'));
        }

        resolve();
      }).catch(reject);
    });
  }

  stop() {    
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        if(self.logger.isInfo()) {
          self.logger.info(f('Shutting down mongos proxies'));
        }

        // Shutdown all the proxies
        for(var i = 0; i < self.proxies.length; i++) {
          yield self.proxies[i].stop();
        }

        if(self.logger.isInfo()) {
          self.logger.info(f('Shutting down configuration servers'));
        }

        // Shutdown configuration server
        yield self.configurationServers.stop();


        if(self.logger.isInfo()) {
          self.logger.info(f('Shutting down shards'));
        }

        // Shutdown all the shards
        for(var i = 0; i < self.shards.length; i++) {
          yield self.shards[i].stop();
        }

        if(self.logger.isInfo()) {
          self.logger.info(f('done shutting down sharding topology'));
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
}

module.exports = Sharded;