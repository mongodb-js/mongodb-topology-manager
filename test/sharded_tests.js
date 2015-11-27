"use strict"

var co = require('co'),
  f = require('util').format,
  Logger = require('../lib/logger'),
  assert = require('assert');

// Polyfill Promise if none exists
if(!global.Promise) {
  require('es6-promise').polyfill();
}

// Get babel polyfill
require("babel-polyfill");

describe('Sharded', function() {
  describe('manager', function() {
    it('establish server version for sharded system', function(done) {
      this.timeout(200000);

      co(function*() {
        var Sharded = require('../').Sharded;
        // Create new instance
        var topology = new Sharded('mongod');
        // Perform discovery
        var version = yield topology.discover();
        // Expect 3 integers
        assert.ok(typeof version.version[0] == 'number');
        assert.ok(typeof version.version[1] == 'number');
        assert.ok(typeof version.version[2] == 'number');
        done();
      }).catch(function(err) {
        console.log(err.stack);
      });
    });

    it('create a sharded system with 2 shards', function(done) {
      this.timeout(250000);

      co(function*() {
        var Sharded = require('../').Sharded;
        // Create new instance
        var topology = new Sharded({
          mongod: 'mongod',
          mongos: 'mongos'
        });

        // Add one shard
        yield topology.addShard([{
          options: {
            bind_ip: 'localhost', port: 31000, dbpath: f('%s/../db/31000', __dirname)
          }
        }, {
          options: {
            bind_ip: 'localhost', port: 31001, dbpath: f('%s/../db/31001', __dirname)
          }
        }, {
          // Type of node
          arbiter: true,
          // mongod process options
          options: {
            bind_ip: 'localhost', port: 31002, dbpath: f('%s/../db/31002', __dirname)
          }
        }], {
          replSet: 'rs1'
        });

        // Add one shard
        yield topology.addShard([{
          options: {
            bind_ip: 'localhost', port: 31010, dbpath: f('%s/../db/31010', __dirname)
          }
        }, {
          options: {
            bind_ip: 'localhost', port: 31011, dbpath: f('%s/../db/31011', __dirname)
          }
        }, {
          // Type of node
          arbiter: true,
          // mongod process options
          options: {
            bind_ip: 'localhost', port: 31012, dbpath: f('%s/../db/31012', __dirname)
          }
        }], {
          replSet: 'rs2'
        });

        // Add configuration servers
        yield topology.addConfigurationServers([{
          options: {
            bind_ip: 'localhost', port: 35000, dbpath: f('%s/../db/35000', __dirname)
          }
        }, {
          options: {
            bind_ip: 'localhost', port: 35001, dbpath: f('%s/../db/35001', __dirname)
          }
        }, {
          options: {
            bind_ip: 'localhost', port: 35002, dbpath: f('%s/../db/35002', __dirname)
          }
        }], {
          replSet: 'rs3'
        });

        // Add proxies
        yield topology.addProxies([{
          bind_ip: 'localhost', port: 51000, configdb: 'localhost:35000,localhost:35001,localhost:35002'
        }, {
          bind_ip: 'localhost', port: 51001, configdb: 'localhost:35000,localhost:35001,localhost:35002'
        }], {
          binary: 'mongos'
        });

        // // Set the info level
        // Logger.setLevel('info');

        // Start up topology
        yield topology.start();

        // Shard db
        yield topology.enableSharding('test');
        // Shard a collection
        yield topology.shardCollection('test', 'testcollection', {_id: 1});

        // Stop the topology
        yield topology.stop();

        // All done
        done();
      }).catch(function(err) {
        console.log(err.stack);
      });
    });

    it('create a sharded system with a single shard and take down mongos and bring it back', function(done) {
      this.timeout(250000);

      co(function*() {
        var Sharded = require('../').Sharded;
        // Create new instance
        var topology = new Sharded({
          mongod: 'mongod',
          mongos: 'mongos'
        });

        // Add one shard
        yield topology.addShard([{
          options: {
            bind_ip: 'localhost', port: 31000, dbpath: f('%s/../db/31000', __dirname)
          }
        }, {
          options: {
            bind_ip: 'localhost', port: 31001, dbpath: f('%s/../db/31001', __dirname)
          }
        }, {
          // Type of node
          arbiter: true,
          // mongod process options
          options: {
            bind_ip: 'localhost', port: 31002, dbpath: f('%s/../db/31002', __dirname)
          }
        }], {
          replSet: 'rs1'
        });

        // Add configuration servers
        yield topology.addConfigurationServers([{
          options: {
            bind_ip: 'localhost', port: 35000, dbpath: f('%s/../db/35000', __dirname)
          }
        }, {
          options: {
            bind_ip: 'localhost', port: 35001, dbpath: f('%s/../db/35001', __dirname)
          }
        }, {
          options: {
            bind_ip: 'localhost', port: 35002, dbpath: f('%s/../db/35002', __dirname)
          }
        }], {
          replSet: 'rs3'
        });

        // Add proxies
        yield topology.addProxies([{
          bind_ip: 'localhost', port: 51000, configdb: 'localhost:35000,localhost:35001,localhost:35002'
        }, {
          bind_ip: 'localhost', port: 51001, configdb: 'localhost:35000,localhost:35001,localhost:35002'
        }], {
          binary: 'mongos'
        });

        // // Set the info level
        // Logger.setLevel('info');

        // Start up topology
        yield topology.start();

        // Shard db
        yield topology.enableSharding('test');

        // Shard a collection
        yield topology.shardCollection('test', 'testcollection', {_id: 1});

        // Get first proxy
        var mongos = topology.proxies[0];
        // Stop the proxy
        yield mongos.stop();

        // Start the proxy again
        yield mongos.start();

        // Stop the topology
        yield topology.stop();

        // All done
        done();
      }).catch(function(err) {
        console.log(err.stack);
      });
    });
  });
});
