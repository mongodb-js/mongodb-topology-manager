'use strict';
const net = require('net');

const clone = function(o) {
  var obj = {};
  for (var name in o) obj[name] = o[name];
  return obj;
};

const delay = function(ms) {
  return new Promise(resolve => setTimeout(() => resolve(), ms));
};

function cleanupSocket(socket) {
  socket.removeAllListeners('connect');
  socket.removeAllListeners('error');
  socket.end();
  socket.destroy();
  socket.unref();
}

/**
 * Checks if a provided address is actively listening for incoming connections
 *
 * @param {string} host the host to connect to
 * @param {number} port the port to check for availability
 * @param {function} callback
 */
function checkAvailable(host, port, callback) {
  const socket = new net.Socket();

  socket.on('connect', () => {
    cleanupSocket(socket);
    callback(null, true);
  });

  socket.on('error', err => {
    cleanupSocket(socket);
    if (err.code !== 'ECONNREFUSED') {
      return callback(err);
    }

    callback(null, false);
  });

  socket.connect({ port: port, host: host });
}

/**
 * Waits for a provided address to actively listen for incoming connections on a given
 * port.
 *
 * @param {string} host the host to connect to
 * @param {number} port the port to check for availability
 * @param {object} [options] optional settings
 * @param {number} [options.retryMS] the amount of time to wait between retry attempts in ms
 * @param {number} [options.retryCount] the number of times to attempt retry
 * @param {function} callback
 */
function waitForAvailable(host, port, options, callback) {
  if (typeof options === 'function') (callback = options), (options = {});
  options = Object.assign(
    {},
    {
      retryMS: 100,
      retryCount: 10
    },
    options
  );

  let count = options.retryCount;

  function run() {
    checkAvailable(host, port, (err, available) => {
      if (err) return callback(err);

      count--;
      if (available) return callback();
      if (count === 0) return callback(new Error('Server is unavailable'));
      setTimeout(() => run(), options.retryMS);
    });
  }

  run();
}

function createCommandOptions(manager, localOptions) {
  const options = clone(manager.clientOptions);
  options.host = manager.options.bind_ip;
  options.port = manager.options.port;
  options.connectionTimeout =
    typeof localOptions.connectionTimeout === 'number' ? localOptions.connectionTimeoutMS : 5000;
  options.socketTimeout =
    typeof localOptions.socketTimeout === 'number' ? localOptions.socketTimeoutMS : 0;
  options.pool = 1;

  // Ensure we only connect once and emit any error caught
  options.reconnect = false;
  options.emitError = true;

  return options;
}

module.exports = {
  clone: clone,
  delay: delay,
  checkAvailable: checkAvailable,
  waitForAvailable: waitForAvailable,
  createCommandOptions: createCommandOptions
};
