# The MongoDB Topology Management API

The MongoDB Topology Management API is an API to allow to programatically spin up a MongoDB instance, Replicaset or Sharded cluster on your local machine.

## Setting up a single instance

It's very simple to create a single running MongoDB instance. All examples are using **ES6**.

```js
var Server = require('mongodb-topology-manager').Server;
// Create new instance
var server = new Server('binary', {
  dbpath: f('%s/db', __dirname)
});

// Perform discovery
var result = yield server.discover();
// Purge the directory
yield server.purge();
// Start process
yield server.start();
// Stop the process
yield server.stop();
```

## Setting up a replicaset

It's equally easy to create a new Replicaset instance.

```js
var ReplSet = require('mongodb-topology-manager').ReplSet;

// Create new instance
var topology = new ReplSet('mongod', [{
  // mongod process options
  options: {
    bind_ip: 'localhost', port: 31000, dbpath: f('%s/../db/31000', __dirname)
  }
}, {
  // mongod process options
  options: {
    bind_ip: 'localhost', port: 31001, dbpath: f('%s/../db/31001', __dirname)
  }
}, {
  // Type of node
  arbiterOnly: true,
  // mongod process options
  options: {
    bind_ip: 'localhost', port: 31002, dbpath: f('%s/../db/31002', __dirname)
  }
}], {
  replSet: 'rs'
});

// Perform discovery
var result = yield server.discover();
// Purge the directory
yield server.purge();
// Start process
yield server.start();
// Stop the process
yield server.stop();
```

Each of the node objects can take the following options at the top level.

Field | Description
----------------------------------|-------------------------
arbiter | node should become an arbiter.
builIndexes | should build indexes on the node.
hidden | node should be hidden.
builIndexes | should build indexes on the node.
priority | node should have the following priority.
tags | tags for the node.
slaveDelay | the node slaveDelay for replication.
votes | additional votes for the specific node.

The **object** contains the options that are used to start up the actual `mongod` instance.

The Replicaset manager has the following methods

Method | Description
----------------------------------|-------------------------
Replset.prototype.discover | Return the information from running mongod with --version flag.
Replset.prototype.start | Start the Replicaset.
Replset.prototype.primary | Return the current Primary server manager.
Replset.prototype.shardUrl | Return a add shard url string.
Replset.prototype.url | Return a connection url.
Replset.prototype.arbiters | Return a list of arbiter managers.
Replset.prototype.secondaries | Return a list of secondary managers.
Replset.prototype.passives | Return a list of secondary passive managers.
Replset.prototype.waitForPrimary | Wait for a new primary to be elected or for a specific timeout period.
Replset.prototype.stepDownPrimary | Stepdown the primary.
Replset.prototype.configuration | Return the replicaset configuration.
Replset.prototype.reconfigure | Perform a reconfiguration of the replicaset.
Replset.prototype.serverConfiguration | Get the initial node configuration for specific server manager.
Replset.prototype.addMember | Add a new member to the set.
Replset.prototype.removeMember | Remove a member to the set.
Replset.prototype.maintenance | Put a node into maintenance mode.
Replset.prototype.stop | Stop the replicaset.
Replset.prototype.restart | Restart the replicaset.
Replset.prototype.purge | Purge all the data directories for the replicaset.

## Setting up a sharded system

It's a little bit more complicated to set up a Sharded system but not much more.

```js
var Sharded = require('mongodb-topology-manager').Sharded;

// Create new instance
var topology = new Sharded({
  mongod: 'mongod', mongos: 'mongos'
});

// Add one shard
yield topology.addShard([{
  options: {
    bind_ip: 'localhost', port: 31000, dbpath: f('%s/../db/31000', __dirname)
  }
}, {
  options: {
    bind_ip: 'localhost', port: 31001, dbpath: f('%s/../db/31001', __dirname)
  }
}, {
  // Type of node
  arbiter: true,
  // mongod process options
  options: {
    bind_ip: 'localhost', port: 31002, dbpath: f('%s/../db/31002', __dirname)
  }
}], {
  replSet: 'rs1'
});

// Add one shard
yield topology.addShard([{
  options: {
    bind_ip: 'localhost', port: 31010, dbpath: f('%s/../db/31010', __dirname)
  }
}, {
  options: {
    bind_ip: 'localhost', port: 31011, dbpath: f('%s/../db/31011', __dirname)
  }
}, {
  // Type of node
  arbiter: true,
  // mongod process options
  options: {
    bind_ip: 'localhost', port: 31012, dbpath: f('%s/../db/31012', __dirname)
  }
}], {
  replSet: 'rs2'
});

// Add configuration servers
yield topology.addConfigurationServers([{
  options: {
    bind_ip: 'localhost', port: 35000, dbpath: f('%s/../db/35000', __dirname)
  }
}, {
  options: {
    bind_ip: 'localhost', port: 35001, dbpath: f('%s/../db/35001', __dirname)
  }
}, {
  options: {
    bind_ip: 'localhost', port: 35002, dbpath: f('%s/../db/35002', __dirname)
  }
}], {
  replSet: 'rs3'
});

// Add proxies
yield topology.addProxies([{
  bind_ip: 'localhost', port: 51000, configdb: 'localhost:35000,localhost:35001,localhost:35002'
}, {
  bind_ip: 'localhost', port: 51001, configdb: 'localhost:35000,localhost:35001,localhost:35002'
}], {
  binary: 'mongos'
});

// Start up topology
yield topology.start();

// Shard db
yield topology.enableSharding('test');

// Shard a collection
yield topology.shardCollection('test', 'testcollection', {_id: 1});

// Stop the topology
yield topology.stop();
```

The Sharded manager has the following methods

Method | Description
----------------------------------|-------------------------
Replset.prototype.discover | Return the information from running mongod with --version flag.
Replset.prototype.start | Start the Sharded cluster.
Replset.prototype.stop | Stop the replicaset.
Replset.prototype.restart | Restart the replicaset.
Replset.prototype.purge | Purge all the data directories for the replicaset.
Replset.prototype.addShard | Add a new shard to the cluster.
Replset.prototype.addConfigurationServers | Add a set of nodes to be configuration servers.
ReplSet.prototype.addProxies | Add a set of mongo proxies to the cluster.
ReplSet.prototype.enableSharding | Enable sharding on a specific db.
ReplSet.prototype.shardCollection | Shard a collection.
