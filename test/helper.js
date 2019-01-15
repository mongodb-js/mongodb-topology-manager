const mvm = require('mongodb-version-manager');
const Promise = require('bluebird');

before(function() {
  this.timeout(1000000);

  // Set default MONGODB_VERSION if not specified
  process.env.MONGODB_VERSION = process.env.MONGODB_VERSION.trim() || 'stable';

  console.log('Installing MongoDB server');

  // Skip installing a MongoDB version if the user sets the MONGODB_VERSION to be SKIP
  // Otherwise, install MongoDB server version
  let promise;
  if (process.env.MONGODB_VERSION.toUpperCase() === 'SKIP') {
    promise = Promise.resolve();
  } else {
    promise = Promise.promisify(mvm.use)({
      version: process.env.MONGODB_VERSION
    });
  }

  return promise.then(function() {
    console.log('Finished installing MongoDB server\n');
  });
});
