# The MongoDB Topology Management API

The MongoDB Topology Management API is an API to allow to programatically spin up a MongoDB instance, Replicaset or Sharded cluster on your local machine.

## Setting up a single instance

It's very simple to create a single running MongoDB instance. All examples are using **ES6**.

```js
var Server = require('mongodb-topology-manager').Server;
// Create new instance
var server = new Server('binary', {
  dbpath: './db'
});

const init = async () => {
  // Perform discovery
  var result = await server.discover();
  // Purge the directory
  await server.purge();
  // Start process
  await server.start();
  // Stop the process
  await server.stop();
}

// start the server
init();
```

## Setting up a replicaset

It's equally easy to create a new Replicaset instance.

```js
var ReplSet = require('mongodb-topology-manager').ReplSet;

// Create new instance
var topology = new ReplSet('mongod', [{
  // mongod process options
  options: {
    bind_ip: 'localhost', port: 31000, dbpath: './db-1'
  }
}, {
  // mongod process options
  options: {
    bind_ip: 'localhost', port: 31001, dbpath: './db-2'
  }
}, {
  // Type of node
  arbiterOnly: true,
  // mongod process options
  options: {
    bind_ip: 'localhost', port: 31002, dbpath: './db-3'
  }
}], {
  replSet: 'rs'
});

const init = async () => {
  // Perform discovery
  var result = await server.discover();
  // Purge the directory
  await server.purge();
  // Start process
  await server.start();
  // Stop the process
  await server.stop();
}

// start the replica set
init();
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

const init = async () => {
  // Create new instance
  var topology = new Sharded({
    mongod: 'mongod', mongos: 'mongos'
  });

  // Add one shard
  await topology.addShard([{
    options: {
      bind_ip: 'localhost', port: 31000, dbpath: './db-1', shardsvr: null
    }
  }, {
    options: {
      bind_ip: 'localhost', port: 31001, dbpath: './db-2', shardsvr: null
  }, {
    // Type of node
    arbiter: true,
    // mongod process options
    options: {
      bind_ip: 'localhost', port: 31002, dbpath: './db-3', shardsvr: null
    }
  }], {
    replSet: 'rs1'
  });

  // Add one shard
  await topology.addShard([{
    options: {
      bind_ip: 'localhost', port: 31010, dbpath: './db-4', shardsvr: null
    }
  }, {
    options: {
      bind_ip: 'localhost', port: 31011, dbpath: './db-5', shardsvr: null
    }
  }, {
    // Type of node
    arbiter: true,
    // mongod process options
    options: {
      bind_ip: 'localhost', port: 31012, dbpath: './db-6', shardsvr: null
    }
  }], {
    replSet: 'rs2'
  });

  // Add configuration servers
  await topology.addConfigurationServers([{
    options: {
      bind_ip: 'localhost', port: 35000, dbpath: './db-7'
    }
  }, {
    options: {
      bind_ip: 'localhost', port: 35001, dbpath: './db-8'
    }
  }, {
    options: {
      bind_ip: 'localhost', port: 35002, dbpath: './db-9'
    }
  }], {
    replSet: 'rs3'
  });

  // Add proxies
  await topology.addProxies([{
    bind_ip: 'localhost', port: 51000, configdb: 'localhost:35000,localhost:35001,localhost:35002'
  }, {
    bind_ip: 'localhost', port: 51001, configdb: 'localhost:35000,localhost:35001,localhost:35002'
  }], {
    binary: 'mongos'
  });

  // Start up topology
  await topology.start();

  // Shard db
  await topology.enableSharding('test');

  // Shard a collection
  await topology.shardCollection('test', 'testcollection', {_id: 1});

  // Stop the topology
  await topology.stop();
}

// start the shards
init();
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
