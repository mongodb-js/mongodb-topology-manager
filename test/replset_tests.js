"use strict"

var co = require('co'),
  f = require('util').format,
  assert = require('assert');

describe('ReplSet', function() {
  describe('manager', function() {
    it('establish server version', function(done) {
      this.timeout(50000);

      co(function*() {
        var ReplSet = require('../lib/replset');
        
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
      this.timeout(50000);

      co(function*() {
        var ReplSet = require('../lib/replset');
        
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
  });
});
