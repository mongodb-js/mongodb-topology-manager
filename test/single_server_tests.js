"use strict"

var co = require('co'),
  f = require('util').format,
  assert = require('assert');

// Polyfill Promise if none exists
if(!global.Promise) {
  require('es6-promise').polyfill();
}

// Get babel polyfill
require("babel-polyfill");

describe('Server', function() {
  describe('manager', function() {
    it('establish server version', function(done) {
      this.timeout(50000);

      co(function*() {
        var Server = require('../').Server;
        // Create new instance
        var server = new Server();
        // Perform discovery
        var version = yield server.discover();
        // Expect 3 integers
        assert.ok(typeof version.version[0] == 'number');
        assert.ok(typeof version.version[1] == 'number');
        assert.ok(typeof version.version[2] == 'number');
        done();
      }).catch(function(err) {
        console.log(err.stack);
      });
    });

    it('start server instance', function(done) {
      this.timeout(50000);

      co(function*() {
        var Server = require('../').Server;

        // Create dbpath
        var dbpath = f('%s/../db', __dirname);

        // Create new instance
        var server = new Server('mongod', {
          dbpath: dbpath
        });

        // Purge the directory
        yield server.purge();

        // Start process
        yield server.start();

        // Stop the process
        yield server.stop();

        // Finish test
        done();
      }).catch(function(err) {
        console.log(err);
      });
    });

    it('restart server instance', function(done) {
      this.timeout(50000);

      co(function*() {
        var Server = require('../').Server;

        // Create dbpath
        var dbpath = f('%s/../db', __dirname);

        // Create new instance
        var server = new Server('mongod', {
          dbpath: dbpath
        });

        // Start process
        yield server.start();

        // Get current pid
        var pid1 = server.process.pid;

        // Restart
        yield server.restart();

        // Get new pid
        var pid2 = server.process.pid;

        // Stop the process
        yield server.stop();

        // Assert we had different processes
        assert.ok(pid1 != pid2);

        // Finish test
        done();
      }).catch(function(err) {
        console.log(err);
      });
    });

    it('call ismaster on server instance', function(done) {
      this.timeout(50000);

      co(function*() {
        var Server = require('../').Server;

        // Create dbpath
        var dbpath = f('%s/../db', __dirname);

        // Create new instance
        var server = new Server('mongod', {
          dbpath: dbpath
        });

        // Start process
        yield server.start();

        // Call ismaster
        var ismaster = yield server.ismaster();
        assert.equal(true, ismaster.ismaster);

        // Stop the process
        yield server.stop();

        // Finish test
        done();
      }).catch(function(err) {
        console.log(err);
      });
    });

    it('start up authenticated server', function(done) {
      this.timeout(50000);

      co(function*() {
        var Server = require('../').Server;

        // Create dbpath
        var dbpath = f('%s/../db', __dirname);

        // Create new instance
        var server = new Server('mongod', {
          dbpath: dbpath, auth:null
        });

        // Start process
        yield server.start();

        // Call ismaster
        var ismaster = yield server.ismaster();
        assert.equal(true, ismaster.ismaster);

        // Stop the process
        yield server.stop();

        // Finish test
        done();
      }).catch(function(err) {
        console.log(err);
      });
    });

    it('start up ssl server server', function(done) {
      this.timeout(50000);

      co(function*() {
        var Server = require('../').Server;

        // Create dbpath
        var dbpath = f('%s/../db', __dirname);

        // Create new instance
        var server = new Server('mongod', {
          dbpath: dbpath,
          sslOnNormalPorts:null,
          sslPEMKeyFile: f('%s/ssl/server.pem', __dirname),
          sslAllowInvalidCertificates:null
        }, {
          ssl:true,
          rejectUnauthorized:false
        });

        // Perform discovery
        var result = yield server.discover();
        // Skip ssl test
        if(!result.ssl) return done();

        // Start process
        yield server.start();

        // Call ismaster
        var ismaster = yield server.ismaster();
        assert.equal(true, ismaster.ismaster);

        // Stop the process
        yield server.stop();

        // Finish test
        done();
      }).catch(function(err) {
        console.log(err);
      });
    });
  });
});
