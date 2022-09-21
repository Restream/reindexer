# Sharding

Reindexer supports sharding by index values and currently this is the only mode. The same as for replication setup each node has to have unique server id, set via #config namespace or web interface. Maximum number of nodes in any replication setup is limited by server id value (max value is 999).

# Table of contents:
- [Overall description](#overall-description)
- [Requests proxiying](#requests-proxying)
- [Local requests](#local-requests)
- [Known issues and constraints](#known-issues-and-constraints)
- [Usage](#usage)
  - [Configuration](#configuration)
  - [Example](#example)
  - [Combining sharding and synchronous replication](#combining-sharding-and-synchronous-replication)
  - [Dump, restore and resharding](#dump-restore-and-resharding)

## Overall description

Reindexer is able to distribute namespaces over few nodes (shards). Each namespace may have only one sharding key (index) and own nodes list. Currently the only supported sharding mode is 'by value', where each shard has explicitly specified values of sharding key. If sharding key for item is not specified in config, than this item will be sent to the default shard (default shard has to be specified for each sharded namespace in config).

It's recommended (hovewer, not required) to use composite primary key in sharded namespaces, where sharding key is part of this PK. Such approach allows to avoid PK conflicts during resharding. It's also recommended to create its own index for sharding key.

Some of the operations (namespaces creation and removing, indexes operations, meta access, etc) are executing on each shard and requires all of the shard being online. Reindexer awaits shards' startup for this reason and this also may lead to delays on such requests (shards usually will not startup at the exactly same time).

## Requests proxying

Any client's request may be sent to any shard and then it will be proxied to the proper shard automatically. However, proxying is not free and sometimes network delays may take much more time, than actual request's execution.

Each request and modified item has to have **single** explicit sharding key in it. So each operation may be executed either on the single node or over all of the nodes (execution on nodes' subsetes is not supported yet). For example, if sharding is configured by field 'location', correct request will be something like that:

```SQL
SELECT * FROM ns WHERE location='name1' and id > 100;
```

Distributed execution is also supported, however there are some constrains for aggregations. For example, this request will return items with id>100 from all the shards:

```SQL
SELECT * FROM ns WHERE id > 100;
```

Joined requests also require explicit sharding key and can be executed on the single shard only:

```SQL
SELECT * FROM sharded_ns1 JOIN (SELECT * FROM sharded_ns2 WHERE location='name1') ON sharded_ns1.id=sharded_ns2.id WHERE location='name1';
```

Also it is possible to join local namespace, which does not have sharding config:

```SQL
SELECT * FROM sharded_ns JOIN (SELECT * FROM not_sharded_ns) ON sharded_ns.id=not_sharded_ns.id WHERE location='name1';
```

Local namespace may also be used as left namespace in join-query. In this case request will be executed on proxied to the shard, set by joined query:

```SQL
SELECT * FROM not_sharded_ns JOIN (SELECT * FROM sharded WHERE location='name1') ON sharded_ns.id=not_sharded_ns.id;
```


## Local requests


For local execution of the query on the current shard use operator `LOCAL`

```SQL
LOCAL SELECT * FROM sharded_ns1 JOIN (SELECT * FROM sharded_ns2) ON sharded_ns1.id=sharded_ns2.id;
LOCAL SELECT * FROM sharded_ns JOIN (SELECT * FROM not_sharded_ns) ON sharded_ns.id=not_sharded_ns.id;
LOCAL SELECT * FROM not_sharded_ns JOIN (SELECT * FROM sharded_ns) ON sharded_ns.id=not_sharded_ns.id;
```

Operator `LOCAL` can be used only with `SELECT` queries.


## Known issues and constraints

- Sharding key value of the existing document should not be modified by update/upsert requests. Reindexer does not support automatic item's transfer from one shard to another, so this will lead to unexpected behaviour. If you need to modify this field and transfer item to another shard, the only way to do it is delete and reinsert.
- Meta is shared between all of the shards (except internal system meta records).
- Update/Delete requests can only be executed on the single shard with explicit sharding key in request.
- Distributed requests for fulltext index are not supported.
- The only allowed condition for sharding index in queries is '='. So each query must be executed either on the single node or on every node (when sharding key is not specified).
- `ORDER BY` in distributed requests requires json paths and index names beind the same for sorted fields.
- Distributed joins and aggregations AVG, Facet and Distinct are not supported.
- Fulltext, array or composite indexes may not be used as sharding key.
- `Explain` is unavailable for distributed queries.
- `Explain` does not show any information about proxy timings yet.
- Protobuf/MsgPack via GRPC are not supported for cluster/sharding reindexer setups yet.

## Usage

### Configuration

On startup reindexer_server (or builtin server) reads replication and sharding config from files `replication.conf`([sample](cpp_src/cluster/replication.conf)) and `sharding.conf`([sample](cpp_src/cluster/sharding/sharding.conf)), which have to be placed in database directory.

`replication.conf` sets general replication parameter and has to be unique for each node in cluster (those parameters also may be configured via `#config` namespace).

`sharding.conf` sets specific sharding parameters (description may be found in sample). This file has to have the same content on each node of the cluster (here 'cluster' means combination of all sharded nodes and synchorous clusters for each shard).


### Example

Simple example script for sharding preview may be found [here](cpp_src/cluster/examples/sharding_preview.sh). This script sets up 3 instances of reindexer_server, that are configured as 3 shard nodes (shard0 set as 'default').


### Combining sharding and synchronous replication

Sharding config supports any number of DSNs per shard, so each shard may be represented not only by single node, but by synchronous RAFT-cluster:

```
   shard0           shard1
   
   node0------------node3
   /    \      ___/ /   \
node1--node2__/  node4--node5

```

In this scenario each node has to have unique `server_id` and each RAFT-cluster may (but do not have to) have unique `cluster_id`. Nodes from different shards choose DSN from the list automatically, trying to connect to the RAFT-cluster's leader.


### Dump, restore and resharding

The only way to change data distribution between shards is dump/restore mechanism in reindexer_tool. Here is an example of dumping sharded namespace from first sharding cluster (via `cproto://127.0.0.1:6534/mydb`) and restoring of those dump to other sharding cluster (via `cproto://127.0.0.1:7231/mydb`):

```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/mydb --command '\dump' --dump-mode=sharded_only --output mydb.rxdump
reindexer_tool --dsn cproto://127.0.0.1:7231/mydb -f mydb.rxdump
```

In example above, if second cluster has different sharding configuration, than all of the dumped data will be redistributed between new shards.

More info about dump modes in reindexer_tool may be found [here](cpp_src/cmd/reindexer_tool/readme.md#dump-modes).

