var killMongodb = require('kill-mongodb');

// Kill all MongoDB instances after running each test
// Otherwise, if a test fails without properly shutting down its servers, all subsequent tests will also fail
afterEach(function(done) {
  killMongodb(done);
});
