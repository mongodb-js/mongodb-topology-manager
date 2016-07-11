"use strict";

// var Promise = require('es6-promise').Promise;
// require('es6-promise').polyfill();

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var Promise = require("bluebird");

var co = require('co'),
    f = require('util').format,
    mkdirp = require('mkdirp'),
    rimraf = require('rimraf'),
    Logger = require('./logger'),
    CoreServer = require('mongodb-core').Server,
    spawn = require('child_process').spawn;

var clone = function clone(o) {
  var obj = {};for (var name in o) {
    obj[name] = o[name];
  }return obj;
};

var Server = function () {
  function Server(binary, options, clientOptions) {
    _classCallCheck(this, Server);

    options = options || {};
    this.options = clone(options);

    // Server state
    this.state = 'stopped';

    // Create logger instance
    this.logger = Logger('Server', options);

    // Unpack default runtime information
    this.binary = binary || 'mongod';
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

  _createClass(Server, [{
    key: "discover",
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

                  // Do we have a valid proc

                  if (!(!proc || !proc.stdout || !proc.stderr)) {
                    _context.next = 3;
                    break;
                  }

                  return _context.abrupt("return", reject(new Error(f('failed to start [%s --version]', self.binary))));

                case 3:

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

                case 9:
                case "end":
                  return _context.stop();
              }
            }
          }, _callee, this);
        })).catch(reject);
      });
    }
  }, {
    key: "instance",
    value: function instance(credentials, options) {
      var self = this;
      options = options || {};
      options = clone(options);

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee2() {
          var opt, s;
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

                  // Create an instance
                  s = new CoreServer(opt);

                  s.on('error', function (err) {
                    reject(err);
                  });

                  s.on('close', function (err) {
                    reject(err);
                  });

                  s.on('timeout', function (err) {
                    reject(err);
                  });

                  s.on('connect', function (_server) {
                    // Do we have credentials
                    var authenticate = function authenticate(_server, _credentials, _callback) {
                      if (!_credentials) return _callback();
                      // Perform authentication using provided
                      // credentials for this command
                      _server.auth(_credentials.provider, _credentials.db, _credentials.user, _credentials.password, _callback);
                    };

                    // Perform any necessary authentication
                    authenticate(_server, credentials, function (err) {
                      if (err) return reject(err);
                      resolve(_server);
                    });
                  });

                  // Connect
                  s.connect();

                case 14:
                case "end":
                  return _context2.stop();
              }
            }
          }, _callee2, this);
        }));
      });
    }
  }, {
    key: "executeCommand",
    value: function executeCommand(ns, command, credentials, options) {
      var self = this;
      options = options || {};
      options = clone(options);

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee3() {
          var opt, executeCommand, s;
          return regeneratorRuntime.wrap(function _callee3$(_context3) {
            while (1) {
              switch (_context3.prev = _context3.next) {
                case 0:
                  // Copy the basic options
                  opt = clone(self.clientOptions);

                  opt.host = self.options.bind_ip;
                  opt.port = self.options.port;
                  opt.connectionTimeout = 5000;
                  opt.socketTimeout = 0;
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
                case "end":
                  return _context3.stop();
              }
            }
          }, _callee3, this);
        })).catch(function (err) {
          if (options.ignoreError) return resolve({ ok: 1 });
          reject(err);
        });
      });
    }
  }, {
    key: "start",
    value: function start() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee4() {
          var result, version, errors, options, commandOptions, name, i, o, commandLine, stdout, stderr;
          return regeneratorRuntime.wrap(function _callee4$(_context4) {
            while (1) {
              switch (_context4.prev = _context4.next) {
                case 0:
                  _context4.next = 2;
                  return self.discover();

                case 2:
                  result = _context4.sent;
                  version = result.version;

                  // All errors found during validation

                  errors = [];

                  // Ensure basic parameters

                  if (!self.options.dbpath) {
                    errors.push(new Error('dbpath is required'));
                  }

                  // Do we have any errors

                  if (!(errors.length > 0)) {
                    _context4.next = 8;
                    break;
                  }

                  return _context4.abrupt("return", reject(errors));

                case 8:

                  // Figure out what special options we need to pass into the boot script
                  // Removing any non-compatible parameters etc.
                  if (version[0] == 3 && version[1] >= 0 && version[1] <= 2) {} else if (version[0] == 3 && version[1] >= 2) {} else if (version[0] == 2 && version[1] <= 6) {}

                  // Merge in all the options
                  options = clone(self.options);

                  // Build command options list

                  commandOptions = [];

                  // Do we have a 2.2 server, then we don't support setParameter

                  if (version[0] == 2 && version[1] == 2) {
                    delete options['setParameter'];
                  }

                  // Go over all the options
                  for (name in options) {
                    if (options[name] == null) {
                      commandOptions.push(f('--%s', name));
                    } else if (Array.isArray(options[name])) {
                      // We have an array of a specific option f.ex --setParameter
                      for (i = 0; i < options[name].length; i++) {
                        o = options[name][i];


                        if (o == null) {
                          commandOptions.push(f('--%s', name));
                        } else {
                          commandOptions.push(f('--%s=%s', name, options[name][i]));
                        }
                      }
                    } else {
                      commandOptions.push(f('--%s=%s', name, options[name]));
                    }
                  }

                  // Command line
                  commandLine = f('%s %s', self.binary, commandOptions.join(' '));
                  // console.log("----------------------------------------------------------------------------")
                  // console.log(commandLine)

                  if (self.logger.isInfo()) {
                    self.logger.info(f('started mongod with [%s]', commandLine));
                  }

                  // Spawn a mongod process
                  self.process = spawn(self.binary, commandOptions);

                  // Variables receiving data
                  stdout = '';
                  stderr = '';

                  // Get the stdout

                  self.process.stdout.on('data', function (data) {
                    stdout += data.toString();
                    // console.log(data.toString())

                    //
                    // Only emit event at start
                    if (self.state == 'stopped') {
                      if (stdout.indexOf('waiting for connections') != -1 || stdout.indexOf('connection accepted') != -1) {
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
                    reject(new Error({ error: error, stdout: stdout, stderr: stderr }));
                  });

                  // Process terminated
                  self.process.on('close', function (code) {
                    if (self.state == 'stopped' && stdout == '' || code != 0) {
                      return reject(new Error(f('failed to start mongod with options %s\n%s', commandOptions, stdout)));
                    }

                    self.state = 'stopped';
                  });

                case 22:
                case "end":
                  return _context4.stop();
              }
            }
          }, _callee4, this);
        })).catch(reject);
      });
    }

    /*
     * Retrieve the ismaster for this server
     */

  }, {
    key: "ismaster",
    value: function ismaster() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee5() {
          var opt, s;
          return regeneratorRuntime.wrap(function _callee5$(_context5) {
            while (1) {
              switch (_context5.prev = _context5.next) {
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

                  s.on('close', function (err) {
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
                      if (err) return reject(err);
                      // Return the ismaster command
                      resolve(r.result);
                    });
                  });

                  // Connect
                  s.connect();

                case 14:
                case "end":
                  return _context5.stop();
              }
            }
          }, _callee5, this);
        })).catch(reject);
      });
    }

    /*
     * Purge the db directory
     */

  }, {
    key: "purge",
    value: function purge() {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee6() {
          return regeneratorRuntime.wrap(function _callee6$(_context6) {
            while (1) {
              switch (_context6.prev = _context6.next) {
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
                case "end":
                  return _context6.stop();
              }
            }
          }, _callee6, this);
        })).catch(reject);
      });
    }
  }, {
    key: "stop",
    value: function stop(signal) {
      var self = this;
      signal = typeof signal == 'number' ? signals[signal] : signals['15'];
      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee7() {
          return regeneratorRuntime.wrap(function _callee7$(_context7) {
            while (1) {
              switch (_context7.prev = _context7.next) {
                case 0:
                  if (!(!self.process || self.state == 'stopped')) {
                    _context7.next = 3;
                    break;
                  }

                  // Set process to stopped
                  self.state = 'stopped';
                  // Return
                  return _context7.abrupt("return", resolve());

                case 3:

                  // Wait for service to stop
                  self.process.on('close', function () {
                    // Set process to stopped
                    self.state = 'stopped';
                    // Return
                    resolve();
                  });

                  // Terminate the process
                  self.process.kill(signal);

                case 5:
                case "end":
                  return _context7.stop();
              }
            }
          }, _callee7, this);
        })).catch(reject);
      });
    }
  }, {
    key: "restart",
    value: function restart(purge) {
      var self = this;

      return new Promise(function (resolve, reject) {
        co(regeneratorRuntime.mark(function _callee8() {
          return regeneratorRuntime.wrap(function _callee8$(_context8) {
            while (1) {
              switch (_context8.prev = _context8.next) {
                case 0:
                  _context8.next = 2;
                  return self.stop();

                case 2:
                  if (!purge) {
                    _context8.next = 5;
                    break;
                  }

                  _context8.next = 5;
                  return self.purge();

                case 5:
                  _context8.next = 7;
                  return self.start();

                case 7:
                  resolve();

                case 8:
                case "end":
                  return _context8.stop();
              }
            }
          }, _callee8, this);
        })).catch(reject);
      });
    }
  }, {
    key: "config",
    get: function get() {
      return clone(this.options);
    }
  }, {
    key: "host",
    get: function get() {
      return this.options.bind_ip;
    }
  }, {
    key: "port",
    get: function get() {
      return this.options.port;
    }
  }, {
    key: "name",
    get: function get() {
      return f('%s:%s', this.options.bind_ip, this.options.port);
    }
  }]);

  return Server;
}();

module.exports = Server;

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
