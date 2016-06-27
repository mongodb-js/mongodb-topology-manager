"use strict";

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var co = require('co'),
    f = require('util').format,
    mkdirp = require('mkdirp'),
    rimraf = require('rimraf'),
    Logger = require('./logger'),
    CoreServer = require('mongodb-core').Server,
    spawn = require('child_process').spawn;

var Promise = require("bluebird");

var clone = function clone(o) {
  var obj = {};for (var name in o) {
    obj[name] = o[name];
  }return obj;
};

var Mongos = function () {
  function Mongos(binary, options, clientOptions) {
    _classCallCheck(this, Mongos);

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

  _createClass(Mongos, [{
    key: 'discover',
    value: function discover() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee() {
          var proc, stdout, stderr;
          return regeneratorRuntime.wrap(function _callee$(_context) {
            while (1) {
              switch (_context.prev = _context.next) {
                case 0:
                  proc = spawn(self.binary, ['--version']);
                  // Variables receiving data

                  stdout = '';
                  stderr = '';
                  // Get the stdout

                  proc.stdout.on('data', function (data) {
                    stdout += data;
                  });
                  // Get the stderr
                  proc.stderr.on('data', function (data) {
                    stderr += data;
                  });
                  // Got an error
                  proc.on('error', function (err) {
                    reject(err);
                  });
                  // Process terminated
                  proc.on('close', function (code) {
                    // Perform version match
                    var versionMatch = stdout.match(/[0-9]+\.[0-9]+\.[0-9]+/);

                    // Check if we have ssl
                    var sslMatch = stdout.match(/ssl/i);

                    // Resolve the server version
                    resolve({
                      version: versionMatch.toString().split('.').map(function (x) {
                        return parseInt(x, 10);
                      }),
                      ssl: sslMatch != null
                    });
                  });

                case 7:
                case 'end':
                  return _context.stop();
              }
            }
          }, _callee, this);
        })).catch(reject);
      });
    }
  }, {
    key: 'executeCommand',
    value: function executeCommand(ns, command, credentials, options) {
      var self = this;
      options = options || {};
      options = clone(options);

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee2() {
          var opt, executeCommand, s;
          return regeneratorRuntime.wrap(function _callee2$(_context2) {
            while (1) {
              switch (_context2.prev = _context2.next) {
                case 0:
                  // Copy the basic options
                  opt = clone(self.clientOptions);

                  opt.host = self.options.bind_ip;
                  opt.port = self.options.port;
                  opt.connectionTimeout = 5000;
                  opt.socketTimeout = 5000;
                  opt.pool = 1;

                  // Ensure we only connect once and emit any error caught
                  opt.reconnect = false;
                  opt.emitError = true;

                  // Execute command

                  executeCommand = function executeCommand(_ns, _command, _credentials, _server) {
                    // Do we have credentials
                    var authenticate = function authenticate(_server, _credentials, _callback) {
                      if (!_credentials) return _callback();
                      // Perform authentication using provided
                      // credentials for this command
                      _server.auth(_credentials.provider, _credentials.db, _credentials.user, _credentials.password, _callback);
                    };

                    authenticate(_server, _credentials, function (err) {
                      if (err && options.ignoreError) return resolve({ ok: 1 });
                      // If we had an error return
                      if (err) {
                        _server.destroy();
                        return reject(err);
                      }
                      // Execute command
                      _server.command(_ns, _command, function (err, r) {
                        // Destroy the connection
                        _server.destroy();
                        // Return an error
                        if (err && options.ignoreError) return resolve({ ok: 1 });
                        if (err) return reject(err);
                        // Return the ismaster command
                        resolve(r.result);
                      });
                    });
                  };

                  // Create an instance


                  s = new CoreServer(opt);


                  s.on('error', function (err) {
                    if (options.ignoreError) return resolve({ ok: 1 });
                    if (options.reExecuteOnError) {
                      options.reExecuteOnError = false;
                      return executeCommand(ns, command, credentials, s);
                    }

                    reject(err);
                  });

                  s.on('close', function (err) {
                    if (options.ignoreError) return resolve({ ok: 1 });
                    if (options.reExecuteOnError) {
                      options.reExecuteOnError = false;
                      return executeCommand(ns, command, credentials, s);
                    }

                    reject(err);
                  });

                  s.on('timeout', function (err) {
                    if (options.ignoreError) return resolve({ ok: 1 });
                    reject(err);
                  });

                  s.on('connect', function (_server) {
                    executeCommand(ns, command, credentials, _server);
                  });

                  // Connect
                  s.connect();

                case 15:
                case 'end':
                  return _context2.stop();
              }
            }
          }, _callee2, this);
        })).catch(reject);
      });
    }
  }, {
    key: 'start',
    value: function start() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee3() {
          var result, version, errors, options, commandOptions, name, commandLine, stdout, stderr;
          return regeneratorRuntime.wrap(function _callee3$(_context3) {
            while (1) {
              switch (_context3.prev = _context3.next) {
                case 0:
                  _context3.next = 2;
                  return self.discover();

                case 2:
                  result = _context3.sent;
                  version = result.version;

                  // All errors found during validation

                  errors = [];

                  // Do we have any errors

                  if (!(errors.length > 0)) {
                    _context3.next = 7;
                    break;
                  }

                  return _context3.abrupt('return', reject(errors));

                case 7:

                  // Figure out what special options we need to pass into the boot script
                  // Removing any non-compatible parameters etc.
                  if (version[0] == 3 && version[1] >= 0 && version[1] <= 2) {} else if (version[0] == 3 && version[1] >= 2) {} else if (version[0] == 2 && version[1] <= 6) {}

                  // Merge in all the options
                  options = clone(self.options);

                  // Build command options list

                  commandOptions = [];

                  // Go over all the options

                  for (name in options) {
                    if (options[name] == null) {
                      commandOptions.push(f('--%s', name));
                    } else {
                      commandOptions.push(f('--%s=%s', name, options[name]));
                    }
                  }

                  // Command line
                  commandLine = f('%s %s', self.binary, commandOptions.join(' '));


                  if (self.logger.isInfo()) {
                    self.logger.info(f('start mongos server [%s]', commandLine));
                  }

                  // console.log("---------------- start mongos")
                  // console.dir(commandLine)

                  // Spawn a mongos process
                  self.process = spawn(self.binary, commandOptions);

                  // Variables receiving data
                  stdout = '';
                  stderr = '';

                  // Get the stdout

                  self.process.stdout.on('data', function (data) {
                    stdout += data.toString();
                    // console.log(stdout.toString())
                    //
                    // Only emit event at start
                    if (self.state == 'stopped') {
                      if (stdout.indexOf('waiting for connections') != -1) {
                        if (self.logger.isInfo()) {
                          self.logger.info(f('successfully started mongos proxy [%s]', commandLine));
                        }

                        // Mark state as running
                        self.state = 'running';
                        // Resolve
                        resolve();
                      }
                    }
                  });

                  // Get the stderr
                  self.process.stderr.on('data', function (data) {
                    stderr += data;
                  });

                  // Got an error
                  self.process.on('error', function (err) {
                    self.logger.error(f('failed to start mongos instance [%s]', commandLine));
                    reject(new Error({ error: error, stdout: stdout, stderr: stderr }));
                  });

                  // Process terminated
                  self.process.on('close', function (code) {
                    if (self.state == 'stopped' && stdout == '' || code != 0) {
                      self.logger.error(f('failed to start mongos instance [%s]', commandLine));
                      return reject(new Error(f('failed to start mongos with options %s', commandOptions)));
                    }

                    self.state = 'stopped';
                  });

                case 20:
                case 'end':
                  return _context3.stop();
              }
            }
          }, _callee3, this);
        })).catch(reject);
      });
    }

    /*
     * Retrieve the ismaster for this server
     */

  }, {
    key: 'ismaster',
    value: function ismaster() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee4() {
          var opt, s;
          return regeneratorRuntime.wrap(function _callee4$(_context4) {
            while (1) {
              switch (_context4.prev = _context4.next) {
                case 0:
                  // Copy the basic options
                  opt = clone(self.clientOptions);

                  opt.host = self.options.bind_ip;
                  opt.port = self.options.port;
                  opt.connectionTimeout = 5000;
                  opt.socketTimeout = 5000;
                  opt.pool = 1;

                  // Ensure we only connect once and emit any error caught
                  opt.reconnect = false;
                  opt.emitError = true;

                  // Create an instance
                  s = new CoreServer(opt);
                  // Add listeners

                  s.on('error', function (err) {
                    reject(err);
                  });

                  s.on('timeout', function (err) {
                    reject(err);
                  });

                  s.on('connect', function (_server) {
                    _server.command('system.$cmd', {
                      ismaster: true
                    }, function (err, r) {
                      // Destroy the connection
                      _server.destroy();
                      // Return an error
                      if (err) return callback(err);
                      // Return the ismaster command
                      resolve(r.result);
                    });
                  });

                  // Connect
                  s.connect();

                case 13:
                case 'end':
                  return _context4.stop();
              }
            }
          }, _callee4, this);
        })).catch(reject);
      });
    }

    /*
     * Purge the db directory
     */

  }, {
    key: 'purge',
    value: function purge() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee5() {
          return regeneratorRuntime.wrap(function _callee5$(_context5) {
            while (1) {
              switch (_context5.prev = _context5.next) {
                case 0:
                  try {
                    // Delete the dbpath
                    rimraf.sync(self.options.dbpath);
                  } catch (err) {}

                  try {
                    // Re-Create the directory
                    mkdirp.sync(self.options.dbpath);
                  } catch (err) {}

                  // Return
                  resolve();

                case 3:
                case 'end':
                  return _context5.stop();
              }
            }
          }, _callee5, this);
        })).catch(reject);
      });
    }
  }, {
    key: 'stop',
    value: function stop(signal) {
      var self = this;
      signal = typeof signal == 'number' ? signal : signals.SIGTERM;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee6() {
          return regeneratorRuntime.wrap(function _callee6$(_context6) {
            while (1) {
              switch (_context6.prev = _context6.next) {
                case 0:
                  if (self.logger.isInfo()) {
                    self.logger.info(f('stop mongos proxy process with pid [%s]', self.process.pid));
                  }

                  // No process, just resolve

                  if (self.process) {
                    _context6.next = 3;
                    break;
                  }

                  return _context6.abrupt('return', resolve());

                case 3:

                  // Wait for service to stop
                  self.process.on('close', function () {
                    if (self.logger.isInfo()) {
                      self.logger.info(f('stopped mongos proxy process with pid [%s]', self.process.pid));
                    }

                    // Set process to stopped
                    self.state = 'stopped';
                    // Return
                    resolve();
                  });

                  // Terminate the process
                  self.process.kill(signal);

                case 5:
                case 'end':
                  return _context6.stop();
              }
            }
          }, _callee6, this);
        })).catch(reject);
      });
    }
  }, {
    key: 'restart',
    value: function restart(purge) {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee7() {
          return regeneratorRuntime.wrap(function _callee7$(_context7) {
            while (1) {
              switch (_context7.prev = _context7.next) {
                case 0:
                  _context7.next = 2;
                  return self.stop();

                case 2:
                  if (!purge) {
                    _context7.next = 5;
                    break;
                  }

                  _context7.next = 5;
                  return self.purge();

                case 5:
                  _context7.next = 7;
                  return self.start();

                case 7:
                  resolve();

                case 8:
                case 'end':
                  return _context7.stop();
              }
            }
          }, _callee7, this);
        })).catch(reject);
      });
    }
  }, {
    key: 'name',
    get: function get() {
      return f('%s:%s', this.options.bind_ip, this.options.port);
    }
  }]);

  return Mongos;
}();

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
