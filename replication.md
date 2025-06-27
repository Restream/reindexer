# Replication and cluster mode

Reindexer supports async logical leader-follower replication and sync RAFT-cluster. Each node has to have unique server id, set via #config namespace or web interface. Maximum number of nodes in any replication setup is limited by server id value (max value is 999).

# Table of contents:
- [Write ahead log (WAL)](#write-ahead-log-wal)
  - [Maximum WAL size configuration](#maximum-wal-size-configuration)
  - [Access namespace's WAL with reindexer_tool](#access-namespaces-wal-with-reindexer_tool)
- [Data integrity check](#data-integrity-check)
- [Async replication](#async-replication)
  - [Cascade replication setups](#cascade-replication-setups)
  - [Usage](#usage)
    - [Configuration](#configuration)
    - [Check replication status](#check-replication-status)
    - [Actions](#actions)
- [RAFT-cluster](#raft-cluster)
  - [Node roles](#node-roles)
    - [Base leader elections](#base-leader-elections)
    - [Operating as follower](#operating-as-follower)
    - [Operating as leader](#operating-as-leader)
  - [Guarantees](#guarantees)
  - [Configuration](#configuration)
    - [Configuration via YML-file](#configuration-via-yml-file)
    - [Configuration via system namespace](#configuration-via-system-namespace)
  - [Async replication of RAFT-cluster namespaces](#async-replication-of-raft-cluster-namespaces)
  - [Statistics](#statistics)
  - [Known issues and constraints](#known-issues-and-constraints)
- [Migration from Reindexer's v3.x.x replication config](#migration-from-reindexers-v3xx-replication-config)
  - [Examples](#examples)
    - [Single-level setup](#single-level-setup)
      - [Legacy replication config before migration](#legacy-replication-config-before-migration)
      - [New replication config after migration](#new-replication-config-after-migration)
    - [Cascade setup](#cascade-setup)
      - [Legacy cascade replication config before migration](#legacy-cascade-replication-config-before-migration)
      - [New cascade replication config after migration](#new-cascade-replication-config-after-migration)

## Write ahead log (WAL)

Write ahead log is combination of rows updates and statements execution. WAL is stored as part of namespace storage. Each WAL record has unique 64-bit log sequence number (LSN), which also contains server id. WAL overhead is 18 bytes of RAM per each row update record.
WAL is ring structure, therefore after N updates (4M by default) are recorded, the oldest WAL records will be automatically removed.
WAL in storage contains only records with statements (bulk updates, deletes and index structure changes). Rows updates are not stored as dedicated WAL records, but each document contains its own LSN - this is enough to restore complete WAL in RAM on namespace loading from disk.

Side effect of this mechanic is lack of exact sequence of indexes updates/data updates, and therefore in case of incompatible data migration (e.g. indexed field type changed) follower will fail to apply offline WAL, and will fallback to forced sync.

During `force sync` reindexer will try to transfer all the namespace's data into temporary in-memory table (it's name starts with `@tmp_`-prefix) and then perform atomic swap at the end of synchronization. This process requires an additional RAM for the full namespace copy.

When namespace is large (or network connection is not fast enough) `force sync` takes a long time. During the sync `leader`-node may still receive an updates, which will be placed into ring WAL buffer and internal online updates queue. So, it is possible to face situation, when right after `force sync` target namespace will have an outdated LSN (in cases, when both WAL buffer and online updates queue got overflow during synchronization process) and this will lead to another attempt of `force sync`. To avoid such situations, you may try to set larger WAL size (check the section below) and larger online updates buffer size (it may be set via `--updatessize` CLI flag or `net.maxupdatessize` config option on `reindexer_server` startup).

### Maximum WAL size configuration

WAL size (maximum number of WAL records) may be configured via `#config` namespace. For example to set `first_namespace`'s WAL size to 4000000 and `second_namespace`'s to 100000 this command may be used:

```SQL
Reindexer> \upsert #config { "type": "namespaces", "namespaces": [ { "namespace":"first_namespace", "wal_size": 4000000 }, { "namespace":"second_namespace", "wal_size": 100000 } ] }
```

Default WAL size is 4000000

### Access namespace's WAL with reindexer_tool

To view offline WAL contents from reindexer_tool `SELECT` statement with special condition to `#lsn` index is used:

```SQL
Reindexer> SELECT * FROM epg LIMIT 2 where #lsn > 1000000
```

## Data integrity check

Replication is complex mechanism and there are present potential possibilities to broke data consistence between master and slave. 
Reindexer is calculates lightweight incremental hash of all namespace data (DataHash). DataHash is used to quick check, that data of follower is really up-to-date with leader.

## Async replication

In async replication setup Leader may be standalone server, or golang application with builtin or builtinserver bindings and Follower must be standalone server or golang application with builtinserver reindexer binding.

Replication is using 3 different mechanics:

- Forced synchronization with namespace snapshot 
Used for initial synchronization for copy complete structure and data from leader namespace to follower or from one follower to another. Also used in case of error with WAL replication (e.g. WAL has been outdated, or incompatible changes in indexes structure). In this mode leader creates COW namespace snapshot sends all its indexes and data to follower.

- Offline write ahead log (WAL). Document updates are ROW based, index structure changes, deletes and bulk updates are STATEMENT based 
Used when leader established network connection to follower to sync data. In this mode leader queries all required records from it is WAL with log sequence number (LSN) greater than LSN of applied by follower's last WAL record.

- Online WAL updates live stream 
Used when connection is established. Leader pushes WAL updates stream to all connected followers. This mode is most lightweight and requires less leader's CPU and memory resources.


### Cascade replication setups

Cascade replication setups are also supported. In those setups only one leader exists, however, followers may have their own followers to:

```
             leader
           /        \
 follower1           follower2
     |                /     \
follower1.1   follower2.1 follower2.2
```

In example above follower1 and follower2 replicates data to other followers, but in comparison with leader, they don't permit write access for external clients.

### Usage

#### Configuration

Replication has to be configured on leader's side by using special documents of namespace `#config`.

First of all general replication parameters must be set via `replication` item (those parameters are common for both async and cluster replication):

```JSON
{
	"type":"replication",
	"replication":{
		"server_id": 0,
		"cluster_id": 2,
		"admissible_replication_tokens":[
			{
				"token":"<some_token_1>",
				"namespaces":["ns1"]
			},
			{
				"token":"<some_token_2>",
				"namespaces":["ns2", "ns3"]
			},
			{
				"token":"<some_token_3>",
				"namespaces":["ns4"]
			}
		]
	}
}
```

- `server_id` - Server ID, unique ID of the node (must be set by user)
- `cluster_id` - Cluster ID, must be same on each node in cluster
- `admissible_replication_tokens` - Optional object with lists of namespaces and their admissible tokens. In the example above:
	```json
	{
		"token":"<some_token_2>",
		"namespaces":["ns2", "ns3"]
	}
	```
	means that replication for the `ns2` and `ns3` namespaces will be approved if leader connects to current node using token `<some_token_2>`

	The following syntax is used to specify one admissible token for all namespaces:
	```json
	"admissible_replication_tokens":[
		{
			"token":"<some_common_token>",
			"namespaces":["*"]
		},
		{
			"token":"<some_token_1>",
			"namespaces":["ns1"]
		}
	]
	```
	This notation means that the token `<some_token_1>` is expected for `ns1` and `<some_common_token>` for the rest.

Then you are able to configure specific async replication via `async_replication` item:

```JSON
{
	"type":"async_replication",
	"async_replication":{
		"role":"none",
		"replication_mode":"default",
		"sync_threads": 4,
		"syncs_per_thread": 2,
		"online_updates_timeout_sec": 20,
		"online_updates_delay_msec": 100,
		"sync_timeout_sec": 60,
		"retry_sync_interval_msec": 30000,
		"enable_compression": true,
		"batching_routines_count": 100,
		"force_sync_on_logic_error": false,
		"force_sync_on_wrong_data_hash": false,
		"log_level":"info",
		"max_wal_depth_on_force_sync": 1000,
		"namespaces":[],
		"self_replication_token": "some_token",
		"nodes":
		[
			{
				"dsn": "cproto://192.168.1.5:6534/mydb",
				"namespaces": []
			}
		]
	}
}
```

- `role`  Replication role. May be on of
   - `none` - replication is disabled
   - `follower` - replication as follower
   - `leader` - replication as leader
- `replication_mode` - Replication mode. Allows to configure async replication from sync raft-cluster. May be one of
   - `default` - async replication from this node is always enabled, if there are any target nodes to replicate on;
   - `from_sync_leader` - async replication will be enabled only when current node is synchronous RAFT-cluster leader (or if this node does not have any sync cluster config)
- `sync_threads` - Number of replication thread
- `syncs_per_thread` - Max number of concurrent force/wal-syncs per each replication thread
- `online_updates_timeout_sec` - Network timeout for communication with followers (for online-replication mode), in seconds
- `sync_timeout_sec` - Network timeout for communication with followers (for force and wal synchronization), in seconds
- `retry_sync_interval_msec` - Synchronization retry delay in case of any errors during online replication
- `enable_compression` - Network traffic compression flag
- `batching_routines_count` - Number of concurrent routines, used to asynchronously send online updates for each follower. Larger values may reduce network trip-around, but also increase RAM consumption
- `force_sync_on_logic_error` - Force resync on logic error conditions
- `force_sync_on_wrong_data_hash` - Force resync if dataHash mismatch
- `log_level` - Replication log level on replicator's startup. Possible values: none, error, warning, info, trace
- `max_wal_depth_on_force_sync` - Maximum number of WAL records, which will be copied after force-sync
- `online_updates_delay_msec` - Delay between write operation and replication. Larger values here will leader to higher replication latency and buffering, but also will provide more effective network batching and CPU utilization
- `namespaces` - List of namespaces for replication. If empty, all namespaces. All replicated namespaces will become read only for follower. This parameter is used, when node doesn't have specific namespace list
- `self_replication_token` - Replication token of the current node that it sends to the follower for verification
- `nodes` - List of follower nodes

Follower settings:
- `ip_addr` - IP of the follower node
- `rpc_port` - Port of follower's RPC-server
- `database` - Follower database name
- `namespaces` - Optional list on namespaces to replicate to this specific node. If specific list is not set, value from general replication config will be used

As second option replication can be configured by YAML-config files, which has to be placed to database directory. Sample of async replication config file is [here](cpp_src/cluster/async_replication.conf) 
and sample for general replication config file is [here](cpp_src/cluster/replication.conf).

If config file is present, then it's overrides settings from `#config` namespace on reindexer startup.

#### Check replication status

Replication status is available in system namespace `#memstats`. e.g, execution of statement:

```SQL
Reindexer> SELECT name,replication FROM #memstats WHERE name='media_items'
```
will return JSON object with status of namespace `media_items `replication 

```JSON
{
	"last_lsn_v2":{
		"server_id":20,
		"counter":5
	},
	"ns_version":{
		"server_id":20,
		"counter":5
	},
	"clusterization_status":{
		"leader_id": 20,
		"role": "simple_replica"
	}
	"temporary":false,
	"incarnation_counter":0,
	"data_hash":6,
	"data_count":4,
	"updated_unix_nano":1594041127153561000,
	"status":"none",
	"wal_count":6,
	"wal_size":311
}
```

- `last_lsn_v2` - current lsn of this node (leader and follower will have the same LSN values for each record)
- `ns_version` - current namespace version. This value is set during namespace creation
- `temporary` - namespace is temporary
- `incarnation_counter` - number of switches between master slave 
- `data_hash` - hash of data
- `data_count` - number of records in the namespace
- `updated_unix_nano` - last operation time
- `status` - shows current synchronization status: idle, error, syncing, fatal
- `wal_count` - number of records in WAL
- `wal_size` - WAL size

#### Actions

There are also a few actions available to interact with async replication via reindexer tool or web interface.

- Restart replication (if sent to leader, restarts replication completely):

```SQL
Reindexer> \upsert #config { "type":"action","action":{ "command":"restart_replication" } }
```

- Reset replication role (useful to make follower's namespaces writable again, if you are going to use this follower outside of replication setup):

```SQL
Reindexer> \upsert #config { "type":"action","action":{ "command":"reset_replication_role" } }
```

- Reset replication role for specific namespace:

```SQL
Reindexer> \upsert #config { "type":"action","action":{ "command":"reset_replication_role", "namespace": "ns_name" } }
```

- Control replication's log levels:

```SQL
Reindexer> \upsert #config { "type":"action","action":{ "command":"set_log_level", "type": "async_replication", "level":"trace" } }
```

Possible `types`: `async_replication` (controls log level for asynchronous replication), `cluster` (controls log level for synchronous replication and RAFT-manager).

Possible `levels`: `none`, `error`, `warning`, `info`, `trace`.

## RAFT-cluster

Reindexer support RAFT-like algorithm for leader elections and synchronous replication with consensus awaiting. In cluster setup each node may be standalone server or golang application with builtinserver binding.

### Node roles

While running in cluster mode node may have one of 3 roles: Leader, Follower or Candidate. The role of each node is dynamic and defined by election algorithm.

#### Base leader elections

At startup each node begins an elections loop. Each elections' iteration (elections term) has the following steps:
- Switch role to Candidate;
- Increment elections term;
- Try to suggest itself as Leader for rest of the nodes;
- In case if this node reaches consensus, it becomes Leader and all other nodes become followers;
- Otherwise, await pings/leader suggestions for randomized period of time;
- If leader's ping was received, node becomes follower.

#### Operating as follower

Once node has become follower it starts checking if leader is available (via timestamp of the last received ping message). If leader becomes unavailable, follower initiate new election term.

In follower state node changes roles of each namespace from cluster config to "cluster_replica" and sets corresponding leader ID (i.e. server ID of current leader). From this moment all the follower namespaces are readonly for anyone except cluster leader.

Any request, which requires write access will be proxied to leader and executed on the leader's side.

Requests, which doesn't require write access (and request to system namespaces) will be executed locally.

#### Operating as leader

Once node has become leader it starts sending ping messages to all the followers. If follower did not respond to the ping message, it will be considered as dead.
If N/2 of the followers are dead (N is total number of nodes in cluster), leader has to initiate new elections term.

Right after role switch node begins initial leader sync. During initial sync leader collects the latest data from at least N/2 other nodes. It can not replicate any data while initial sync is not completed.

When initial sync is done, leader start to synchronize followers. Same as for asynchronous replication there are 3 different mechanics for data synchronization: force sync, WAL sync and online updates.
Leader continues to replicate data, while it won't receive request for manual re-elections or error in consensus. In both of this situations node will initiate new elections term.

### Guarantees

1. Consensus for each data write.

Reindexer is using consensus algorithm to provide data safety. Each data write on leader generates one or few online-updates and then each update awaits approves from N/2 followers. Follower can not approve updates, if WAL/force sync is not done yet.

2. Read after write is always safe.

If operation was proxied from follower to leader, then it won't be generally approved before it get approve from current node (emitter node), so if you wrote something in current thread on any node, then you'll be able to read this data from the same node.

3. Online updates are ordered.

For optimization purposes concurrent writes are available even if some the writing operations from other threads are awaiting consensus right now. Reindexer guarantees that all of this concurrent writes will be performed on followers with the same ordering.

### Configuration

#### Configuration via YML-file

On startup reindexer_server reads replication and cluster config from files `replication.conf`([sample](cpp_src/cluster/replication.conf)) and `cluster.conf`([sample](cpp_src/cluster/cluster.conf)), which have to be placed in database directory.

`replication.conf` sets general replication parameter and has to be unique for each node in cluster (those parameters also may be configured via `#config` namespace).

`cluster.conf` sets specific cluster parameters (description may be found in sample). This file has to have the same content on each node of the cluster.

#### Examples

1. [Example script](cpp_src/cmd/reindexer_server/clustertest/cluster_example.sh), which creates local cluster.

2. [Docker-compose config](cpp_src/cmd/reindexer_server/clustertest/docker-compose.yml), which create 3 docker-containers running RAFT-cluster. Usage:

```
# Run from docker-compose.yml directory
docker-compose up
```

In both examples default RPC ports are: 6534, 6535, 6536; and default HTTP ports are: 9088, 9089, 9090.

#### Configuration via system namespace
// work in progress

### Statistics

Cluster statistics may be received via select-request to `#replicationstats` namespace:

```SQL
Reindexer> SELECT * FROM #replicationstats where type="cluster"
```

This select has to have filter by `type` field with either `cluster` or `async` value. When `type="cluster"` is used, request will be proxied to leader.

Example of JSON-response:

```JSON
{
    "type": "cluster",
    "initial_sync": {
        "force_syncs": {
            "count": 0,
            "max_time_us": 0,
            "avg_time_us": 0
        },
        "wal_syncs": {
            "count": 0,
            "max_time_us": 0,
            "avg_time_us": 0
        },
        "total_time_us": 2806
    },
    "force_syncs": {
        "count": 0,
        "max_time_us": 0,
        "avg_time_us": 0
    },
    "wal_syncs": {
        "count": 0,
        "max_time_us": 0,
        "avg_time_us": 0
    },
    "update_drops": 0,
    "pending_updates_count": 275,
    "allocated_updates_count": 275,
    "allocated_updates_size": 43288,
    "nodes": [
        {
            "dsn": "cproto://127.0.0.1:14000/node0",
            "server_id": 0,
            "pending_updates_count": 60,
            "status": "offline",
            "sync_state": "awaiting_resync",
            "role": "none",
            "is_synchronized": false,
            "namespaces": []
        },
        {
            "dsn": "cproto://127.0.0.1:14002/node2",
            "server_id": 2,
            "pending_updates_count": 0,
            "status": "online",
            "sync_state": "online_replication",
            "role": "follower",
            "is_synchronized": true,
            "namespaces": []
        },
        {
            "dsn": "cproto://127.0.0.1:14003/node3",
            "server_id": 3,
            "pending_updates_count": 0,
            "status": "online",
            "sync_state": "online_replication",
            "role": "follower",
            "is_synchronized": true,
            "namespaces": []
        },
        {
            "dsn": "cproto://127.0.0.1:14004/node4",
            "server_id": 4,
            "pending_updates_count": 275,
            "status": "online",
            "sync_state": "online_replication",
            "role": "follower",
            "is_synchronized": true,
            "namespaces": []
        },
        {
            "dsn": "cproto://127.0.0.1:14001/node1",
            "server_id": 1,
            "pending_updates_count": 0,
            "status": "online",
            "sync_state": "online_replication",
            "role": "leader",
            "is_synchronized": true,
            "namespaces": []
        }
    ]
}

```

- `initial_sync` - statistics about leader's initial sync;
- `force_syncs` - statistics about follower's namespaces force syncs;
- `wal_syncs` - statistics about follower's namespaces wal syncs;
- `update_drops` - number of updates overflows, when updates were dropped;
- `pending_updates_count` - number of updates, awaiting replication in queue;
- `allocated_updates_count` - number of updates in queue (including those, which already were replicated, but was not deallocated yet);
- `allocated_updates_size` - total size of allocated updates in bytes;
- `nodes` - statistics about each node.

Node statistics fields:
- `dsn` - node's DSN;
- `server_id` - node's server ID;
- `pending_updates_count` - number of updates, which are awaiting replication to this specific follower;
- `status` - node's status: `none`, `online`, `offline` or `raft_error`;
- `sync_state` - node's replication sync state: `none`, `online_replication`, `awaiting_resync`, `syncing` or `initial_leader_sync`;
- `role` - node's role: `leader` or `follower`;
- `is_synchronized` - synchronization status. Shows if all the approved updates were replicated to this node;
- `namespaces` - namespaces which are configured for this node.

### Manual leader's selection

Leader of the cluster may be changed manually via `#config` namespace. For example, this request will transfer leadership to the node with `server ID` 2 (if it exists):

```SQL
Reindexer> \upsert #config { "type":"action","action":{ "command":"set_leader_node", "server_id": 2 } }
```

## Async replication of RAFT-cluster namespaces

### Async replication with "default" replication mode

It's possible to combine async replication and RAFT-cluster in setups like this:

```
        cluster1 (ns1, ns2)         cluster2 (ns1)
updates -> cl10 - cl11               cl20 - cl21
              \    /  async repl(ns2)  \    /
               cl12 -------------------> cl22
```

In setup above there are 2 independent RAFT-clusters: `cluster1`(over `ns1` and `ns2`) and `cluster2`(over `ns1`). Also, one of the nodes of the first cluster replicating its data (`ns2`) asynchronously to one of the nodes of the second cluster.

Take a notice:
- `ns2` can not taking part in second RAFT-cluster
- asynchronous replication here (with `default` replication mode) still works on node-to-node basis, i.e. it replicates all the data from `cl12` node to `cl22` node, but not to other nodes of the seconds cluster (i.e. if `cl12` is down, data will not be asynchronously replicated)

### Async replication with "from_sync_leader" replication mode

It's possible to combine async replication and RAFT-cluster in setups like this:

```
        cluster1 (ns1, ns2)               cluster2 (ns1)
updates -> cl10 - cl11   async repl(ns2)   cl20 - cl21
              \    /   ------------------->   \    /
               cl12                            cl22
```

In setup above there are 2 independent RAFT-clusters: `cluster1`(over `ns1` and `ns2`) and `cluster2`(over `ns1`). Also, *each* of the nodes of the first cluster has the same async replication config like this:
```JSON
{
	"type":"async_replication",
	"async_replication":{
		"role":"leader",
		"replication_mode":"from_sync_leader",
		"sync_threads": 4,
		"syncs_per_thread": 2,
		"online_updates_timeout_sec": 20,
		"sync_timeout_sec": 60,
		"retry_sync_interval_msec": 30000,
		"enable_compression": true,
		"batching_routines_count": 100,
		"force_sync_on_logic_error": false,
		"force_sync_on_wrong_data_hash": false,
		"max_wal_depth_on_force_sync": 1000,
		"nodes":
		[
			{
				"dsn": "cproto://192.168.1.5:6534/mydb",
				"namespaces": ["ns2"],
				"replication_mode":"from_sync_leader"
			},
			{
				"dsn": "cproto://192.168.1.6:6534/mydb",
				"namespaces": ["ns2"],
				"replication_mode":"from_sync_leader"
			},
			{
				"dsn": "cproto://192.168.1.7:6534/mydb",
				"namespaces": ["ns2"],
				"replication_mode":"from_sync_leader"
			}
		]
	}
}
```
 
With `replication_mode: "from_sync_leader"` option only the current leader of `cluster1` replicating its data (`ns2`) asynchronously to all the nodes of the second cluster (i.e. if one of the node from `cluster1` down, asynchronous replication will still work via new leader)

Take a notice:
- `ns2` can not taking part in second RAFT-cluster 

## Known issues and constraints

- Reindexer does not have global WAL and uses individual WAL for each namespace. Due to this peculiarity operations like `rename namespace` and `drop namespace` can not be replicated properly via force/WAL sync (only online replication is available).
This also may lead to situation, when new leader recovers some the dropped namespaces on initial sync.
- Reindexer does not remove 'extra' namespaces from replicas and does not download those namespaces to leader after it's initial sync. So this may lead to the situation, when some replicas will have namespaces, which does not exist anywhere in cluster
(in case if those replicas were offline during leader elections). For example, if you have cluster of 3 nodes, where two nodes are empty and one has actual data. In that case two empty nodes will startup faster and will be able to choose the leader, while third node is still reading its data (and stays offline).
- If server ID in cluster config does not correspond to actual server ID of the node, this may lead to undefined behaviour (currently reindexer does not validate those IDs by itself).
- Currently synchronous cluster config may not be changed in runtime via HTTP/RPC and rewriting YML-file is the only way to change it.

## Migration from Reindexer's v3.x.x replication config

Current asynchronous replication implementation is incompatible with configs from v3.x.x, so you will have to migrate manually.

1. Server ID's and cluster ID's doesn't require any changes (they are using the same object "replication" in #config namespace) 
2. Current scheme works as "push-replication" and legacy scheme was "pull-replication", so you have to move all information about namespaces and DSNs from legacy slaves to current leader (to "async_replication" in #config).
3. Any additional configs (i.e. timeouts or appnames) now should also be configured on leader's side.

### Examples

#### Single-level setup

For example, we have the following replication scheme:
```
             leader(192.168.10.1)
             /                  \
   follower1(192.168.10.2)   follower2(192.168.10.3)
```

##### Legacy replication config before migration

In this case `follower1` legacy replication config is:

```JSON
{
	"type":"replication",
	"replication":{
		"role":"slave",
		"master_dsn":"cproto://192.168.10.1/db",
		"cluster_id":2,
		"server_id":1,
		"force_sync_on_logic_error": false,
		"force_sync_on_wrong_data_hash": false,
		"namespaces":
		  - "ns1"
	}
}

```

`follower2` config is:

```JSON
{
	"type":"replication",
	"replication":{
		"role":"slave",
		"master_dsn":"cproto://192.168.10.1/db",
		"cluster_id":2,
		"server_id":2,
		"force_sync_on_logic_error": false,
		"force_sync_on_wrong_data_hash": false,
		"namespaces":
		  - "ns2"
		  - "ns3"
	}
}

```

And `leader` config is:

```JSON
{
	"type":"replication",
	"replication":{
		"role":"master",
		"master_dsn":"",
		"cluster_id":2,
		"server_id":0
	}
}

```

##### New replication config after migration

First of all deprecated parameters must be removed from old followers' configs.

New `follower1` config is:

```JSON
{
	"type":"replication",
	"replication":{
		"cluster_id":2,
		"server_id":1
	}
}

```

New `follower2` config:

```JSON
{
	"type":"replication",
	"replication":{
		"cluster_id":2,
		"server_id":2
	}
}

```

Also, follower-role should be set for both `follower1` and `follower2`:

```JSON
{
	"type":"async_replication",
	"async_replication":{
		"role":"follower"
	}
}

```

Then remove deprecated fields from `leader`'s replication config:

```JSON
{
	"type":"replication",
	"replication":{
		"cluster_id":2,
		"server_id":0
	}
}

```

And finally create new async replication config for `leader`:

```JSON
{
	"type":"async_replication",
	"async_replication":{
		"role":"leader",
		"sync_threads": 4,
		"syncs_per_thread": 2,
		"online_updates_timeout_sec": 60,
		"retry_sync_interval_msec": 30000,
		"enable_compression": true,
		"batching_routines_count": 100,
		"force_sync_on_logic_error": false,
		"force_sync_on_wrong_data_hash": false,
		"namespaces":[],
		"nodes":
		[
			{
				"dsn": "cproto://192.168.10.2:6534/mydb",
				"namespaces": ["ns1"]
			},
			{
				"dsn": "cproto://192.168.10.3:6534/mydb",
				"namespaces": ["ns2","ns3"]
			}
		]
	}
}
```

#### Cascade setup

Migration for cascade replication setup doesn't have many differences from migration for any other setups. Initial scheme is:
```
    leader(192.168.10.1)
             |
   follower1(192.168.10.2)
             |
   follower2(192.168.10.3)
```

##### Legacy cascade replication config before migration

In this case `follower1` legacy replication config is:

```JSON
{
	"type":"replication",
	"replication":{
		"role":"slave",
		"master_dsn":"cproto://192.168.10.1/db",
		"cluster_id":2,
		"server_id":1,
		"force_sync_on_logic_error": false,
		"force_sync_on_wrong_data_hash": false,
		"namespaces": []
	}
}

```

`follower2` config is:

```JSON
{
	"type":"replication",
	"replication":{
		"role":"slave",
		"master_dsn":"cproto://192.168.10.2/db",
		"cluster_id":2,
		"server_id":2,
		"force_sync_on_logic_error": false,
		"force_sync_on_wrong_data_hash": false,
		"namespaces": []
	}
}

```

And `leader` config is:

```JSON
{
	"type":"replication",
	"replication":{
		"role":"master",
		"master_dsn":"",
		"cluster_id":2,
		"server_id":0
	}
}

```

##### New cascade replication config after migration

Same as for simple single-level config, all deprecated parameters must be removed from old followers' configs.

New `follower1` config is:

```JSON
{
	"type":"replication",
	"replication":{
		"cluster_id":2,
		"server_id":1
	}
}

```

New `follower2` config:

```JSON
{
	"type":"replication",
	"replication":{
		"cluster_id":2,
		"server_id":2
	}
}

```

Then follower-role should be set for `follower2`:

```JSON
{
	"type":"async_replication",
	"async_replication":{
		"role":"follower"
	}
}

```

`follower1` in this case will still have follower-role, however it also gets `nodes` config to replicate data:

```JSON
{
	"type":"async_replication",
	"async_replication":{
		"role":"leader",
		"sync_threads": 4,
		"syncs_per_thread": 2,
		"online_updates_timeout_sec": 60,
		"retry_sync_interval_msec": 30000,
		"enable_compression": true,
		"batching_routines_count": 100,
		"force_sync_on_logic_error": false,
		"force_sync_on_wrong_data_hash": false,
		"namespaces":[],
		"nodes":
		[
			{
				"dsn": "cproto://192.168.10.3:6534/mydb",
			}
		]
	}
}
```

Note, that there were no specific namespaces' list for the followers, so there is no such field in `nodes` config (field `namespaces` from top level is used instead).

When followers configuration is done, remove deprecated fields from `leader`'s replication config:

```JSON
{
	"type":"replication",
	"replication":{
		"cluster_id":2,
		"server_id":0
	}
}

```

And finally create new async replication config for `leader` (it looks like similar to `follower1` config, however with different role and dsn):

```JSON
{
	"type":"async_replication",
	"async_replication":{
		"role":"leader",
		"sync_threads": 4,
		"syncs_per_thread": 2,
		"online_updates_timeout_sec": 60,
		"retry_sync_interval_msec": 30000,
		"enable_compression": true,
		"batching_routines_count": 100,
		"force_sync_on_logic_error": false,
		"force_sync_on_wrong_data_hash": false,
		"namespaces":[],
		"nodes":
		[
			{
				"dsn": "cproto://192.168.10.2:6534/mydb"
			}
		]
	}
}
```

