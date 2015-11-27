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

describe('ReplSet', function() {
  describe('manager', function() {
    it('establish server version', function(done) {
      this.timeout(1000000);

      co(function*() {
        var ReplSet = require('../').ReplSet;

        // Create new instance
        var topology = new ReplSet('mongod', [{
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31000,
            dbpath: f('%s/../db/31000', __dirname)
          }
        }, {
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31001,
            dbpath: f('%s/../db/31001', __dirname)
          }
        }, {
          // Type of node
          arbiter: true,
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31002,
            dbpath: f('%s/../db/31002', __dirname)
          }
        }], {
          replSet: 'rs'
        });

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

    it('start simple replicaset with 1 primary, 1 secondary and one arbiter', function(done) {
      this.timeout(1000000);

      co(function*() {
        var ReplSet = require('../').ReplSet;

        // Create new instance
        var topology = new ReplSet('mongod', [{
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31000,
            dbpath: f('%s/../db/31000', __dirname)
          }
        }, {
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31001,
            dbpath: f('%s/../db/31001', __dirname)
          }
        }, {
          // Type of node
          arbiter: true,
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31002,
            dbpath: f('%s/../db/31002', __dirname)
          }
        }], {
          replSet: 'rs'
        });

        // Purge any directories
        yield topology.purge();

        // Start set
        yield topology.start();

        // Stop the set
        yield topology.stop();

        // Finish up
        done();
      }).catch(function(err) {
        console.log(err.stack);
      });
    });

    it('start simple ssl replicaset with 1 primary, 1 secondary and one arbiter', function(done) {
      this.timeout(200000);

      co(function*() {
        var ReplSet = require('../').ReplSet;

        // Create new instance
        var topology = new ReplSet('mongod', [{
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31000,
            dbpath: f('%s/../db/31000', __dirname),

            // SSL server instance options
            sslOnNormalPorts:null,
            sslPEMKeyFile: f('%s/ssl/server.pem', __dirname),
            sslAllowInvalidCertificates:null
          }
        }, {
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31001,
            dbpath: f('%s/../db/31001', __dirname),

            // SSL server instance options
            sslOnNormalPorts:null,
            sslPEMKeyFile: f('%s/ssl/server.pem', __dirname),
            sslAllowInvalidCertificates:null
          }
        }, {
          // Type of node
          arbiter: true,
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31002,
            dbpath: f('%s/../db/31002', __dirname),

            // SSL server instance options
            sslOnNormalPorts:null,
            sslPEMKeyFile: f('%s/ssl/server.pem', __dirname),
            sslAllowInvalidCertificates:null
          }
        }], {
          // SSL client instance options
          replSet: 'rs',
          ssl:true,
          rejectUnauthorized:false
        });

        // Perform discovery
        var result = yield topology.discover();
        // Skip ssl test
        if(!result.ssl) return done();

        // Purge any directories
        yield topology.purge();

        // Start set
        yield topology.start();

        // Stop the set
        yield topology.stop();

        // Finish up
        done();
      }).catch(function(err) {
        console.log(err.stack);
      });
    });

    it('stepdown primary', function(done) {
      this.timeout(200000);

      co(function*() {
        var ReplSet = require('../').ReplSet;

        // Create new instance
        var topology = new ReplSet('mongod', [{
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31000,
            dbpath: f('%s/../db/31000', __dirname)
          }
        }, {
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31001,
            dbpath: f('%s/../db/31001', __dirname)
          }
        }, {
          // Type of node
          arbiter: true,
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31002,
            dbpath: f('%s/../db/31002', __dirname)
          }
        }], {
          replSet: 'rs',
          electionCycleWaitMS: 5000,
          retryWaitMS: 1000
        });

        // Purge any directories
        yield topology.purge();

        // Start set
        yield topology.start();

        // Step down primary and block until we have a new primary
        yield topology.stepDownPrimary(false, {stepDownSecs: 0, force:true});

        // Step down primary and immediately return
        yield topology.stepDownPrimary(true, {stepDownSecs: 0, force:true});

        // Block waiting for a new primary to be elected
        yield topology.waitForPrimary();

        // Stop the set
        yield topology.stop();

        // Finish up
        done();
      }).catch(function(err) {
        console.log(err.stack);
      });
    });

    it('add new member to set', function(done) {
      this.timeout(200000);

      co(function*() {
        var ReplSet = require('../').ReplSet;

        // Create new instance
        var topology = new ReplSet('mongod', [{
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31000,
            dbpath: f('%s/../db/31000', __dirname)
          }
        }, {
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31001,
            dbpath: f('%s/../db/31001', __dirname)
          }
        }, {
          // Type of node
          arbiter: true,
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31002,
            dbpath: f('%s/../db/31002', __dirname)
          }
        }], {
          replSet: 'rs',
          electionCycleWaitMS: 5000,
          retryWaitMS: 1000
        });

        // Purge any directories
        yield topology.purge();

        // Start set
        yield topology.start();

        // Add a new member to the set
        var manager = yield topology.addMember({
          options: {
            bind_ip: 'localhost',
            port: 31003,
            dbpath: f('%s/../db/31003', __dirname)
          }
        }, {
          returnImmediately: false, force:false
        });

        // Assert we have the expected number of instances
        var primary = yield topology.primary();
        var ismaster = yield primary.ismaster();
        assert.equal(1, ismaster.arbiters.length);
        assert.equal(3, ismaster.hosts.length);

        // Stop the set
        yield topology.stop();

        // Finish up
        done();
      }).catch(function(err) {
        console.log(err.stack);
      });
    });

    it('add new member to set with high priority', function(done) {
      this.timeout(200000);

      // // Set the info level
      // Logger.setLevel('info');

      co(function*() {
        var ReplSet = require('../').ReplSet;

        // Create new instance
        var topology = new ReplSet('mongod', [{
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31000,
            dbpath: f('%s/../db/31000', __dirname)
          }
        }, {
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31001,
            dbpath: f('%s/../db/31001', __dirname)
          }
        }, {
          // Type of node
          arbiter: true,
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31002,
            dbpath: f('%s/../db/31002', __dirname)
          }
        }], {
          replSet: 'rs',
          electionCycleWaitMS: 5000,
          retryWaitMS: 1000
        });

        // Purge any directories
        yield topology.purge();

        // Start set
        yield topology.start();

        // Add a new member to the set
        var manager = yield topology.addMember({
          priority: 20,
          options: {
            bind_ip: 'localhost',
            port: 31003,
            dbpath: f('%s/../db/31003', __dirname),
          }
        }, {
          returnImmediately: false, force:false
        });

        // Assert we have the expected number of instances
        var primary = yield topology.primary();
        var ismaster = yield primary.ismaster();
        assert.equal(1, ismaster.arbiters.length);
        assert.equal(3, ismaster.hosts.length);

        // Stop the set
        yield topology.stop();

        // Finish up
        done();
      }).catch(function(err) {
        console.log(err.stack);
      });
    });

    it('remove member from set', function(done) {
      this.timeout(200000);

      co(function*() {
        var ReplSet = require('../').ReplSet;

        // Create new instance
        var topology = new ReplSet('mongod', [{
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31000,
            dbpath: f('%s/../db/31000', __dirname)
          }
        }, {
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31001,
            dbpath: f('%s/../db/31001', __dirname)
          }
        }, {
          // Type of node
          arbiter: true,
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31002,
            dbpath: f('%s/../db/31002', __dirname)
          }
        }, {
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31003,
            dbpath: f('%s/../db/31003', __dirname)
          }
        }], {
          replSet: 'rs',
          electionCycleWaitMS: 5000,
          retryWaitMS: 1000
        });

        // Purge any directories
        yield topology.purge();

        // Start set
        yield topology.start();

        // Get all the secondaries
        var secondaries = yield topology.secondaries();

        // Remove a member from the set
        yield topology.removeMember(secondaries[0], {
          returnImmediately: false, force: false
        });

        // Assert we have the expected number of instances
        var primary = yield topology.primary();
        var ismaster = yield primary.ismaster();
        assert.equal(1, ismaster.arbiters.length);
        assert.equal(2, ismaster.hosts.length);

        // Stop the set
        yield topology.stop();

        // Finish up
        done();
      }).catch(function(err) {
        console.log(err.stack);
      });
    });

    it('put secondary in maintenance mode', function(done) {
      this.timeout(200000);

      co(function*() {
        var ReplSet = require('../').ReplSet;

        // Create new instance
        var topology = new ReplSet('mongod', [{
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31000,
            dbpath: f('%s/../db/31000', __dirname)
          }
        }, {
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31001,
            dbpath: f('%s/../db/31001', __dirname)
          }
        }, {
          // Type of node
          arbiter: true,
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31002,
            dbpath: f('%s/../db/31002', __dirname)
          }
        }], {
          replSet: 'rs',
          electionCycleWaitMS: 5000,
          retryWaitMS: 1000
        });

        // Purge any directories
        yield topology.purge();

        // Start set
        yield topology.start();

        // Get all the secondaries
        var secondaries = yield topology.secondaries();

        // Put secondary in maintenance mode
        yield topology.maintenance(true, secondaries[0], {
          returnImmediately: false
        });

        // Assert we have the expected number of instances
        var ismaster = yield secondaries[0].ismaster();
        assert.equal(false, ismaster.secondary);
        assert.equal(false, ismaster.ismaster);

        // Wait for server to come back
        yield topology.maintenance(false, secondaries[0], {
          returnImmediately: false
        });

        var ismaster = yield secondaries[0].ismaster();
        assert.equal(true, ismaster.secondary);

        // Stop the set
        yield topology.stop();

        // Finish up
        done();
      }).catch(function(err) {
        console.log(err.stack);
      });
    });

    it('reconfigure using existing configuration', function(done) {
      this.timeout(200000);

      co(function*() {
        var ReplSet = require('../').ReplSet;

        // Create new instance
        var topology = new ReplSet('mongod', [{
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31000,
            dbpath: f('%s/../db/31000', __dirname)
          }
        }, {
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31001,
            dbpath: f('%s/../db/31001', __dirname)
          }
        }, {
          // mongod process options
          options: {
            bind_ip: 'localhost',
            port: 31002,
            dbpath: f('%s/../db/31002', __dirname)
          }
        }], {
          replSet: 'rs',
          electionCycleWaitMS: 5000,
          retryWaitMS: 1000
        });

        // Purge any directories
        yield topology.purge();

        // Start set
        yield topology.start();

        // Get the configuration
        var config = JSON.parse(JSON.stringify(topology.configurations[0]));
        config.members[2].priority = 10;

        // Force the reconfiguration
        yield topology.reconfigure(config, {
          returnImmediately:false, force:false
        })

        // Get the current configuration
        var primary = yield topology.primary();
        var currentConfig = yield topology.configuration(primary);
        assert.equal(10, currentConfig.members[2].priority);

        // Stop the set
        yield topology.stop();

        // Finish up
        done();
      }).catch(function(err) {
        console.log(err.stack);
      });
    });
  });
});
