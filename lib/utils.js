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
 * @param {number} [options.initialMs] the amount of time to wait before first attempting in ms
 * @param {number} [options.retryMs] the amount of time to wait between retry attempts in ms
 * @param {number} [options.retryCount] the number of times to attempt retry
 */
function waitForAvailable(host, port, options) {
  return new Promise(function(resolve, reject) {
    co(function*() {
      options = Object.assign(
        {},
        {
          initialMs: 300,
          retryMs: 100,
          retryCount: 100
        },
        options
      );

      yield delay(options.initialMs);

      for (var i = 0; i < options.retryCount; i++) {
        var available = yield checkAvailable(host, port);

        if (available) return resolve();

        yield delay(options.retryMs);
      }

      reject(new Error('Server is unavailable'));
    }).catch(reject);
  });
}

module.exports = {
  clone: clone,
  delay: delay,
  checkAvailable: checkAvailable,
  waitForAvailable: waitForAvailable
};
