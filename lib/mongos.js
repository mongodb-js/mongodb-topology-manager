"use strict"

var co = require('co'),
  f = require('util').format,
  mkdirp = require('mkdirp'),
  rimraf = require('rimraf'),
  Logger = require('./logger'),
  EventEmitter = require('events'),
  CoreServer = require('mongodb-core').Server,
  spawn = require('child_process').spawn;

var Promise = require("bluebird");

var clone = function(o) {
  var obj = {}; for(var name in o) obj[name] = o[name]; return obj;
}

class Mongos extends EventEmitter {
  constructor(binary, options, clientOptions) {
    super();
    // Ensure options are correct
    options = options || {};
    this.options = clone(options);

    // Server state
    this.state = 'stopped';

    // Create logger instance
    this.logger = Logger('Mongos', options);

    // Unpack default runtime information
    this.binary = binary || 'mongos';
    // Current process
    this.process = null;
    // Current command
    this.command = null;
    // Credentials store
    this.credentials = [];
    // Additional internal client options
    this.clientOptions = clientOptions || {};

    // Default values for host and port if not set
    this.options.bind_ip = typeof this.options.bind_ip == 'string' ? this.options.bind_ip : 'localhost';
    this.options.port = typeof this.options.port == 'number' ? this.options.port : 27017;
  }

  get name() {
    return f('%s:%s', this.options.bind_ip, this.options.port);
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

          // Resolve the server version
          resolve({
            version: versionMatch.toString().split('.').map(function(x) {
              return parseInt(x, 10);
            }),
            ssl: sslMatch != null
          });
        });
      }).catch(reject);
    });
  }

  executeCommand(ns, command, credentials, options) {
    var self = this;
    options = options || {};
    options = clone(options);

    return new Promise(function(resolve, reject) {
      co(function*() {
        // Copy the basic options
        var opt = clone(self.clientOptions);
        opt.host = self.options.bind_ip;
        opt.port = self.options.port;
        opt.connectionTimeout = 5000;
        opt.socketTimeout = 5000;
        opt.pool = 1;

        // Ensure we only connect once and emit any error caught
        opt.reconnect = false;
        opt.emitError = true;

        // Execute command
        var executeCommand = function(_ns, _command, _credentials, _server) {
          // Do we have credentials
          var authenticate = function(_server, _credentials, _callback) {
            if(!_credentials) return _callback();
            // Perform authentication using provided
            // credentials for this command
            _server.auth(
              _credentials.provider,
              _credentials.db,
              _credentials.user,
              _credentials.password, _callback);
          }

          authenticate(_server, _credentials, function(err) {
            if(err && options.ignoreError) return resolve({ok:1});
            // If we had an error return
            if(err) {
              _server.destroy();
              return reject(err);
            }
            // Execute command
            _server.command(_ns, _command, function(err, r) {
              // Destroy the connection
              _server.destroy();
              // Return an error
              if(err && options.ignoreError) return resolve({ok:1});
              if(err) return reject(err);
              // Return the ismaster command
              resolve(r.result);
            });
          });
        }

        // Create an instance
        var s = new CoreServer(opt);

        s.on('error', function(err) {
          if(options.ignoreError) return resolve({ok:1});
          if(options.reExecuteOnError) {
            options.reExecuteOnError = false;
            return executeCommand(ns, command, credentials, s);
          }

          reject(err);
        });

        s.on('close', function(err) {
          if(options.ignoreError) return resolve({ok:1});
          if(options.reExecuteOnError) {
            options.reExecuteOnError = false;
            return executeCommand(ns, command, credentials, s);
          }

          reject(err);
        });

        s.on('timeout', function(err) {
          if(options.ignoreError) return resolve({ok:1});
          reject(err);
        });

        s.on('connect', function(_server) {
          executeCommand(ns, command, credentials, _server);
        });

        // Connect
        s.connect();
      }).catch(reject);
    });
  }

  start() {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        // Get the version numbers
        var result = yield self.discover();
        var version = result.version;

        // All errors found during validation
        var errors = [];

        // Do we have any errors
        if(errors.length > 0) return reject(errors);

        // Figure out what special options we need to pass into the boot script
        // Removing any non-compatible parameters etc.
        if(version[0] == 3 && version[1] >= 0 && version[1] <= 2) {
        } else if(version[0] == 3 && version[1] >= 2) {
        } else if(version[0] == 2 && version[1] <= 6) {
        }

        // Merge in all the options
        var options = clone(self.options);

        // Build command options list
        var commandOptions = [];

        // Go over all the options
        for(var name in options) {
          if(options[name] == null) {
            commandOptions.push(f('--%s', name));
          } else {
            commandOptions.push(f('--%s=%s', name, options[name]));
          }
        }

        // Command line
        var commandLine = f('%s %s', self.binary, commandOptions.join(' '));

        if(self.logger.isInfo()) {
          self.logger.info(f('start mongos server [%s]', commandLine));
        }

        // Emit start event
        self.emit('state', {
          event: 'start', topology: 'mongos', cmd: commandLine, options: self.options
        });

        // Spawn a mongos process
        self.process = spawn(self.binary, commandOptions);

        // Variables receiving data
        var stdout = '';
        var stderr = '';

        // Get the stdout
        self.process.stdout.on('data', function(data) {
          stdout += data.toString();
          self.emit('state', {
            event: 'stdout', topology: 'mongos', stdout: data.toString(), options: self.options
          });

          //
          // Only emit event at start
          if(self.state == 'stopped') {
            if(stdout.indexOf('waiting for connections') != -1) {
              if(self.logger.isInfo()) {
                self.logger.info(f('successfully started mongos proxy [%s]', commandLine));
              }

              // Emit running state
              self.emit('state', {
                event: 'running', topology: 'mongos', cmd: commandLine, options: self.options
              });

              // Mark state as running
              self.state = 'running';
              // Resolve
              resolve();
            }
          }
        });

        // Get the stderr
        self.process.stderr.on('data', function(data) { stderr += data; });

        // Got an error
        self.process.on('error', function(err) {
          self.emit('state', {
            event: 'sterr', topology: 'mongos', sterr: sterr.toString(), options: self.options
          });
          self.logger.error(f('failed to start mongos instance [%s]', commandLine));
          reject(new Error({error:error, stdout: stdout, stderr: stderr}));
        });

        // Process terminated
        self.process.on('close', function(code) {
          if(self.state == 'stopped' && stdout == '' || (code != 0)) {
            self.logger.error(f('failed to start mongos instance [%s]', commandLine));
            return reject(new Error(f('failed to start mongos with options %s', commandOptions)))
          }

          self.state = 'stopped';
        });
      }).catch(reject);
    });
  }

  /*
   * Retrieve the ismaster for this server
   */
  ismaster() {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        // Copy the basic options
        var opt = clone(self.clientOptions);
        opt.host = self.options.bind_ip;
        opt.port = self.options.port;
        opt.connectionTimeout = 5000;
        opt.socketTimeout = 5000;
        opt.pool = 1;

        // Ensure we only connect once and emit any error caught
        opt.reconnect = false;
        opt.emitError = true;

        // Create an instance
        var s = new CoreServer(opt);
        // Add listeners
        s.on('error', function(err) {
          reject(err);
        });

        s.on('timeout', function(err) {
          reject(err);
        });

        s.on('connect', function(_server) {
          _server.command('system.$cmd', {
            ismaster: true
          }, function(err, r) {
            // Destroy the connection
            _server.destroy();
            // Return an error
            if(err) return callback(err);
            // Return the ismaster command
            resolve(r.result);
          });
        });

        // Connect
        s.connect();
      }).catch(reject);
    });
  }

  /*
   * Purge the db directory
   */
  purge() {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        try {
          // Delete the dbpath
          rimraf.sync(self.options.dbpath);
        } catch(err) {}

        try {
          // Re-Create the directory
          mkdirp.sync(self.options.dbpath);
        } catch(err) {}

        // Return
        resolve();
      }).catch(reject);
    });
  }

  stop(signal) {
    var self = this;
    signal = typeof signal == 'number' ? signal : signals.SIGTERM;

    return new Promise(function(resolve, reject) {
      co(function*() {
        if(self.logger.isInfo()) {
          self.logger.info(f('stop mongos proxy process with pid [%s]', self.process.pid));
        }

        // No process, just resolve
        if(!self.process) return resolve();

        // Wait for service to stop
        self.process.on('close', function() {
          if(self.logger.isInfo()) {
            self.logger.info(f('stopped mongos proxy process with pid [%s]', self.process.pid));
          }

          // Set process to stopped
          self.state = 'stopped';
          // Return
          resolve();
        });

        // Terminate the process
        self.process.kill(signal);
      }).catch(reject);
    });
  }

  restart(purge) {
    var self = this;

    return new Promise(function(resolve, reject) {
      co(function*() {
        // Attempt to stop the server
        yield self.stop();
        // Do we wish to purge the directory ?
        if(purge) yield self.purge();
        // Start process
        yield self.start();
        resolve();
      }).catch(reject);
    });
  }
}

module.exports = Mongos;

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
