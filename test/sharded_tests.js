'use strict';

var co = require('co'),
  f = require('util').format,
  assert = require('assert'),
  Promise = require('bluebird');

describe('Sharded', function() {
  this.timeout(250000);

  // Context variable stores all managers to clean up after test is completed
  var managers = [];

  afterEach(function() {
    return Promise.map(managers, manager => manager.stop()).then(() => (managers = []));
  });

  describe('manager', function() {
    it('establish server version for sharded system', function() {
      this.timeout(200000);

      return co(function*() {
        var Sharded = require('../').Sharded;
        // Create new instance
        var topology = new Sharded('mongod');

        // Perform discovery
        var version = yield topology.discover();

        // Expect 3 integers
        assert.ok(typeof version.version[0] === 'number');
        assert.ok(typeof version.version[1] === 'number');
        assert.ok(typeof version.version[2] === 'number');
      });
    });

    it('create a sharded system with 2 shards', function() {
      return co(function*() {
        var Sharded = require('../').Sharded;
        // Create new instance
        var topology = new Sharded({
          mongod: 'mongod',
          mongos: 'mongos'
        });
        managers.push(topology);

        // Add one shard
        yield topology.addShard(
          [
            {
              options: {
                bind_ip: '127.0.0.1',
                port: 31000,
                dbpath: f('%s/../db/31000', __dirname),
                shardsvr: null
              }
            },
            {
              options: {
                bind_ip: '127.0.0.1',
                port: 31001,
                dbpath: f('%s/../db/31001', __dirname),
                shardsvr: null
              }
            },
            {
              // Type of node
              arbiter: true,
              // mongod process options
              options: {
                bind_ip: '127.0.0.1',
                port: 31002,
                dbpath: f('%s/../db/31002', __dirname),
                shardsvr: null
              }
            }
          ],
          {
            replSet: 'rs1'
          }
        );

        // Add one shard
        yield topology.addShard(
          [
            {
              options: {
                bind_ip: '127.0.0.1',
                port: 31010,
                dbpath: f('%s/../db/31010', __dirname),
                shardsvr: null
              }
            },
            {
              options: {
                bind_ip: '127.0.0.1',
                port: 31011,
                dbpath: f('%s/../db/31011', __dirname),
                shardsvr: null
              }
            },
            {
              // Type of node
              arbiter: true,
              // mongod process options
              options: {
                bind_ip: '127.0.0.1',
                port: 31012,
                dbpath: f('%s/../db/31012', __dirname),
                shardsvr: null
              }
            }
          ],
          {
            replSet: 'rs2'
          }
        );

        // Add configuration servers
        yield topology.addConfigurationServers(
          [
            {
              options: {
                bind_ip: '127.0.0.1',
                port: 35000,
                dbpath: f('%s/../db/35000', __dirname)
              }
            },
            {
              options: {
                bind_ip: '127.0.0.1',
                port: 35001,
                dbpath: f('%s/../db/35001', __dirname)
              }
            },
            {
              options: {
                bind_ip: '127.0.0.1',
                port: 35002,
                dbpath: f('%s/../db/35002', __dirname)
              }
            }
          ],
          {
            replSet: 'rs3'
          }
        );

        // Add proxies
        yield topology.addProxies(
          [
            {
              bind_ip: '127.0.0.1',
              port: 51000,
              configdb: '127.0.0.1:35000,127.0.0.1:35001,127.0.0.1:35002'
            },
            {
              bind_ip: '127.0.0.1',
              port: 51001,
              configdb: '127.0.0.1:35000,127.0.0.1:35001,127.0.0.1:35002'
            }
          ],
          {
            binary: 'mongos'
          }
        );

        // // Set the info level
        // Logger.setLevel('info');

        // Start up topology
        yield topology.start();

        // Shard db
        yield topology.enableSharding('test');
        // Shard a collection
        yield topology.shardCollection('test', 'testcollection', { _id: 1 });

        // Stop the topology
        yield topology.stop();
      });
    });

    it('create a sharded system with a single shard and take down mongos and bring it back', function() {
      return co(function*() {
        var Sharded = require('../').Sharded;
        // Create new instance
        var topology = new Sharded({
          mongod: 'mongod',
          mongos: 'mongos'
        });
        managers.push(topology);

        // Add one shard
        yield topology.addShard(
          [
            {
              options: {
                bind_ip: '127.0.0.1',
                port: 31000,
                dbpath: f('%s/../db/31000', __dirname),
                shardsvr: null
              }
            },
            {
              options: {
                bind_ip: '127.0.0.1',
                port: 31001,
                dbpath: f('%s/../db/31001', __dirname),
                shardsvr: null
              }
            },
            {
              // Type of node
              arbiter: true,
              // mongod process options
              options: {
                bind_ip: '127.0.0.1',
                port: 31002,
                dbpath: f('%s/../db/31002', __dirname),
                shardsvr: null
              }
            }
          ],
          {
            replSet: 'rs1'
          }
        );

        // Add configuration servers
        yield topology.addConfigurationServers(
          [
            {
              options: {
                bind_ip: '127.0.0.1',
                port: 35000,
                dbpath: f('%s/../db/35000', __dirname)
              }
            },
            {
              options: {
                bind_ip: '127.0.0.1',
                port: 35001,
                dbpath: f('%s/../db/35001', __dirname)
              }
            },
            {
              options: {
                bind_ip: '127.0.0.1',
                port: 35002,
                dbpath: f('%s/../db/35002', __dirname)
              }
            }
          ],
          {
            replSet: 'rs3'
          }
        );

        // Add proxies
        yield topology.addProxies(
          [
            {
              bind_ip: '127.0.0.1',
              port: 51000,
              configdb: '127.0.0.1:35000,127.0.0.1:35001,127.0.0.1:35002'
            },
            {
              bind_ip: '127.0.0.1',
              port: 51001,
              configdb: '127.0.0.1:35000,127.0.0.1:35001,127.0.0.1:35002'
            }
          ],
          {
            binary: 'mongos'
          }
        );

        // // Set the info level
        // Logger.setLevel('info');

        // Start up topology
        yield topology.start();

        // Shard db
        yield topology.enableSharding('test');

        // Shard a collection
        yield topology.shardCollection('test', 'testcollection', { _id: 1 });

        // Get first proxy
        var mongos = topology.proxies[0];
        // Stop the proxy
        yield mongos.stop();

        // Start the proxy again
        yield mongos.start();

        // Stop the topology
        yield topology.stop();
      });
    });

    it('properly tears down a sharded system', function() {
      const Sharded = require('../').Sharded;
      const topology = new Sharded({
        mongod: 'mongod',
        mongos: 'mongos'
      });
      managers.push(topology);

      return Promise.resolve()
        .then(() => {
          return topology.addShard(
            [
              {
                options: {
                  bind_ip: '127.0.0.1',
                  port: 31000,
                  dbpath: f('%s/../db/31000', __dirname),
                  shardsvr: null
                }
              },
              {
                options: {
                  bind_ip: '127.0.0.1',
                  port: 31001,
                  dbpath: f('%s/../db/31001', __dirname),
                  shardsvr: null
                }
              },
              {
                // Type of node
                arbiter: true,
                // mongod process options
                options: {
                  bind_ip: '127.0.0.1',
                  port: 31002,
                  dbpath: f('%s/../db/31002', __dirname),
                  shardsvr: null
                }
              }
            ],
            {
              replSet: 'rs1'
            }
          );
        })
        .then(() => {
          return topology.addConfigurationServers(
            [
              {
                options: {
                  bind_ip: '127.0.0.1',
                  port: 35000,
                  dbpath: f('%s/../db/35000', __dirname)
                }
              },
              {
                options: {
                  bind_ip: '127.0.0.1',
                  port: 35001,
                  dbpath: f('%s/../db/35001', __dirname)
                }
              },
              {
                options: {
                  bind_ip: '127.0.0.1',
                  port: 35002,
                  dbpath: f('%s/../db/35002', __dirname)
                }
              }
            ],
            {
              replSet: 'rs3'
            }
          );
        })
        .then(() => {
          topology.addProxies(
            [
              {
                bind_ip: '127.0.0.1',
                port: 51000,
                configdb: '127.0.0.1:35000,127.0.0.1:35001,127.0.0.1:35002'
              },
              {
                bind_ip: '127.0.0.1',
                port: 51001,
                configdb: '127.0.0.1:35000,127.0.0.1:35001,127.0.0.1:35002'
              }
            ],
            {
              binary: 'mongos'
            }
          );
        })
        .then(() => topology.purge())
        .then(() => topology.start())
        .then(() => topology.enableSharding('test'))
        .then(() => topology.shardCollection('test', 'testcollection', { _id: 1 }))
        .then(() => topology.stop())
        .then(() => assert.strictEqual(topology.state, 'stopped'));
    });
  });
});
