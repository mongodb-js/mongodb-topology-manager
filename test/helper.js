const mvm = require('mongodb-version-manager');
const Promise = require('bluebird');

before(function() {
  this.timeout(50000);

  // Set default MONGODB_VERSION to be checked against later
  process.env.MONGODB_VERSION = process.env.MONGODB_VERSION || '4.1.6';

  console.log('Installing MongoDB client');

  return Promise.promisify(mvm.use)({
    version: process.env.MONGODB_VERSION
  });
});
