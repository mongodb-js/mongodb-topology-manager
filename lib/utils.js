'use strict';
const net = require('net'),
  co = require('co');

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
 */
function checkAvailable(host, port) {
  return new Promise(function(resolve, reject) {
    const socket = new net.Socket();

    socket.on('connect', () => {
      cleanupSocket(socket);
      resolve(true);
    });

    socket.on('error', err => {
      cleanupSocket(socket);
      if (err.code !== 'ECONNREFUSED') {
        return reject(err);
      }

      resolve(false);
    });

    socket.connect({ port: port, host: host });
  });
}

/**
 * Waits for a provided address to actively listen for incoming connections on a given
 * port.
 *
 * @param {string} host the host to connect to
 * @param {number} port the port to check for availability
 * @param {object} [options] optional settings
 * @param {number} [options.retryMS] the amount of time to wait between retry attempts in ms
 * @param {number} [options.initialMS] the amount of time to wait before first attempting in ms
 * @param {number} [options.retryCount] the number of times to attempt retry
 */
function waitForAvailable(host, port, options) {
  return new Promise(function(resolve, reject) {
    co(function*() {
      options = Object.assign(
        {},
        {
          initialMS: 300,
          retryMS: 100,
          retryCount: 100
        },
        options
      );

      // Delay initial amount before attempting to connect
      yield delay(options.initialMs);

      // Try up to a certain number of times to connect
      for (var i = 0; i < options.retryCount; i++) {
        // Attempt to connect, returns true/false
        var available = yield checkAvailable(host, port);

        // If connected then return
        if (available) return resolve();

        // Otherwise delay the retry amount and try again
        yield delay(options.retryMs);
      }

      // If this is reached then unable to connect
      reject(new Error('Server is unavailable'));
    }).catch(reject);
  });
}

function createCommandOptions(manager, localOptions) {
  const options = Object.assign(
    {
      host: 'localhost',
      port: 27017,
      connectionTimeout: 5000,
      socketTimeout: 0,
      pool: 1,
      reconnect: false,
      emitError: true
    },
    manager.clientOptions,
    localOptions
  );

  if (manager.options.bind_ip) {
    options.host = manager.options.bind_ip;
  }

  if (manager.options.port) {
    options.port = manager.options.port;
  }

  return options;
}

module.exports = {
  clone,
  delay,
  checkAvailable,
  waitForAvailable,
  createCommandOptions
};
