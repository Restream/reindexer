# Sharding

Reindexer supports sharding by index values and currently this is the only mode. The same as for replication setup each node has to have unique server id, set via #config namespace or web interface. Maximum number of nodes in any replication setup is limited by server id value (max value is 999).

# Table of contents:
- [Overall description](#overall-description)
- [Proxy requests](#proxy-requests)
- [Local requests](#local-requests)
- [Known issues and constraints](#known-issues-and-constraints)
- [Usage](#usage)
  - [Configuration](#configuration)
  - [Runtime sharding configuration](#runtime-sharding-configuration)
  - [Example](#example)
  - [Combining sharding and synchronous replication](#combining-sharding-and-synchronous-replication)
  - [Dump, restore and resharding](#dump-restore-and-resharding)

## Overall description

Reindexer can distribute namespaces across multiple nodes (shards). Each namespace may have only one sharding key (index) and its own list of nodes. Currently, sharding keys' distribution may be configured either via explicit values sets (for numeric and string types) or via values ranges (for numeric types only). Explicit values and values ranges can be used in config simultaneously. During configuration, all intersections of values and ranges will be resolved and merged. If sharding key for item is not specified in config, then this item will be sent to the default shard (default shard has to be specified for each sharded namespace in config).

It's recommended (however, not required) to use composite primary key in sharded namespaces, where sharding key is part of this PK. Such approach allows to avoid PK conflicts during resharding. It's also recommended to create its own index for sharding key.

Some operations (creation and deleting namespaces, indexes operations, accessing metadata, etc.) are performed on each shard and requires all shards to be available. For this reason, the Reindexer waits for shards to start, which can also lead to delays in such queries (shards are usually not started at the same time).

## Proxy requests

Any client's request may be sent to any shard, and then it will be proxied to the proper shard automatically. However, proxying is not free and sometimes network delays may take much more time, than actual request's execution.

Each request and modified item has to have **single** explicit sharding key in it. So each operation may be executed either on the single node or over across all nodes (execution on nodes' subsetes is not supported yet). For example, if sharding is configured by field 'location', correct request will be something like that:

```SQL
SELECT * FROM ns WHERE location='name1' and id > 100;
```

Distributed execution is also supported, however there are some constrains for aggregations. For example, this request will return items with id>100 from all shards:

```SQL
SELECT * FROM ns WHERE id > 100;
```

Joined requests also require explicit sharding key and can be executed on the single shard only:

```SQL
SELECT * FROM sharded_ns1 JOIN (SELECT * FROM sharded_ns2 WHERE location='name1') ON sharded_ns1.id=sharded_ns2.id WHERE location='name1';
```

Also, it's possible to join local namespace, which does not have sharding config:

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

- The sharding key value of an existing document should not be changed by update/upsert requests. Reindexer does not support automatic item's transfer from one shard to another, so this may lead to unexpected behaviour. If you need to modify this field and transfer item to another shard, the only way to do this is to delete it and reinsert it.
- Meta is shared between all shards (except internal system meta records).
- Update/Delete requests can only be executed on the single shard with explicit sharding key in request.
- Distributed requests for fulltext index are not supported.
- The only allowed condition for sharding index in queries is '='. So each query must be executed either on the single node or on every node (when sharding key is not specified).
- Distributed joins and aggregations AVG, Facet and Distinct are not supported.
- Fulltext, array or composite indexes may not be used as sharding key.
- `Explain` is unavailable for distributed queries.
- `Explain` does not show any information about proxy timings yet.
- Protobuf/MsgPack via GRPC are not supported for cluster/sharding reindexer setups yet.

## Usage

### Configuration

On startup reindexer_server (or builtin server) reads replication and sharding config from files `replication.conf`([sample](cpp_src/cluster/replication.conf)) and `sharding.conf`([sample](cpp_src/cluster/sharding/sharding.conf)), which have to be placed in database directory.

`replication.conf` sets general replication parameter and has to be unique for each node in cluster (those parameters also may be configured via `#config` namespace).

`sharding.conf` sets specific sharding parameters (description may be found in sample). This file has to have the same content on each node of the cluster (here, 'cluster' means the combination of all sharded nodes and synchronous clusters for each shard).

### Runtime sharding configuration

You can change the configuration of the sharding in runtime using
`apply_sharding_config`-command through the mechanism of actions in `#config`-table via reindexer tool:
```SQL
Reindexer> \upsert #config  {"type":"action","action":{"command":"apply_sharding_config","config": __JSON_WITH_NEW_CONFIG__ }}
```
where `__JSON_WITH_NEW_CONFIG__` might look like this
```JSON

{
    "version": 1,
    "namespaces":
    [
        {
            "namespace": "ns",
            "default_shard": 0,
            "index": "id",
            "keys":
            [
                {
                    "shard_id": 1,
                    "values":
                    [
                        0,
                        {"range": [1,4]}
                    ]
                },
                {
                    "shard_id": 2,
                    "values":
                    [
                        10,
                        11,
                        {"range": [12,14]}
                    ]
                },
                {
                    "shard_id": 3,
                    "values":
                    [
                        20,
                        21,
                        22,
                        23,
                        24
                    ]
                }
            ]
        }
    ],
    "shards":
    [
        {
            "shard_id": 0,
            "dsns":
            [
                "cproto://127.0.0.1:7010/base_test"
            ]
        },
        {
            "shard_id": 1,
            "dsns":
            [
                "cproto://127.0.0.1:7011/base_test"
            ]
        },
        {
            "shard_id": 2,
            "dsns":
            [
                "cproto://127.0.0.1:7012/base_test"
            ]
        },
        {
            "shard_id": 3,
            "dsns":
            [
                "cproto://127.0.0.1:7013/base_test"
            ]
        }
    ],
    "this_shard_id": -1
}
```
OR
```JSON
{
    "version": 1,
    "namespaces":
    [
        {
            "namespace": "ns",
            "default_shard": 0,
            "index": "id",
            "keys":
            [
                {
                    "shard_id": 1,
                    "values":
                    [
                        0,
                        {"range": [1,4]}
                    ]
                },
                {
                    "shard_id": 2,
                    "values":
                    [
                        10,
                        11,
                        {"range": [12,14]}
                    ]
                },
                {
                    "shard_id": 3,
                    "values":
                    [
                        {"range": [20,24]}
                    ]
                }
            ]
        }
    ],
    "shards":
    [
        {
            "shard_id": 0,
            "dsns":
            [
                "cproto://127.0.0.1:7000/base_test",
                "cproto://127.0.0.1:7001/base_test",
                "cproto://127.0.0.1:7002/base_test"
            ]
        },
        {
            "shard_id": 1,
            "dsns":
            [
                "cproto://127.0.0.1:7010/base_test",
                "cproto://127.0.0.1:7011/base_test",
                "cproto://127.0.0.1:7012/base_test"
            ]
        },
        {
            "shard_id": 2,
            "dsns":
            [
                "cproto://127.0.0.1:7020/base_test",
                "cproto://127.0.0.1:7021/base_test",
                "cproto://127.0.0.1:7022/base_test"
            ]
        },
        {
            "shard_id": 3,
            "dsns":
            [
                "cproto://127.0.0.1:7030/base_test",
                "cproto://127.0.0.1:7031/base_test",
                "cproto://127.0.0.1:7032/base_test"
            ]
        }
    ],
    "this_shard_id": -1
}

```
for a synchronous cluster in the second case.

The value of the `"this_shard_id"`-parameter doesn't matter in the passed configuration. It will be calculated automatically in accordance with the `"shards"`-section in the config and correctly inserted into the config on the corresponding node.

The sharding config will be successfully applied only if:
- All nodes of the old and new sharding configs are available (to be more precise, consensus is needed for the cluster, and availability is needed for single nodes),
- Either all nodes listed in the `"shards"`-section for a specific `shard_id` must be nodes of the same synchronous cluster, and there should be no nodes in the cluster that are not part of the shard, or the shard is a separate node that is not part of the synchronous cluster,
- The namespaces of the new config either do not exist or do not contain data or contain data that does not conflict with the keys of the new config.

At the current stage of implementation, do not try to insert any data into namespaces participating in the new sharding config until it is applied to all nodes, otherwise you may get a non-consistent state and a non-working sharding.

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

In example above, if second cluster has different sharding configuration, than all the dumped data will be redistributed between new shards.

More info about dump modes in reindexer_tool may be found [here](cpp_src/cmd/reindexer_tool/readme.md#dump-modes).

