# Replication and cluster mode

Reindexer supports logical asynchronous leader-follower replication and synchronous replication within a RAFT cluster. A standalone node or a RAFT cluster can serve as the source or target of asynchronous replication. Set the node and cluster IDs in the [replication configuration](#configuration).

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
- [RAFT cluster](#raft-cluster)
  - [Node roles](#node-roles)
    - [Base leader elections](#base-leader-elections)
    - [Operating as follower](#operating-as-follower)
    - [Operating as leader](#operating-as-leader)
  - [Guarantees](#guarantees)
  - [Configuration](#configuration-1)
    - [Configuration via YML-file](#configuration-via-yml-file)
    - [Configuration via system namespace](#configuration-via-system-namespace)
  - [Statistics](#statistics)
  - [Manual leader's selection](#manual-leaders-selection)
- [Async replication and RAFT clusters](#async-replication-and-raft-clusters)
  - [Replication from the RAFT cluster](#replication-from-the-raft-cluster)
  - [Replication to the RAFT cluster](#replication-to-the-raft-cluster)
    - [Detaching a secondary](#detaching-a-secondary)
- [Known issues and constraints](#known-issues-and-constraints)
- [Migration from Reindexer's v3.x.x replication config](#migration-from-reindexers-v3xx-replication-config)
  - [Examples](#examples-1)
    - [Single-level setup](#single-level-setup)
      - [Legacy replication config before migration](#legacy-replication-config-before-migration)
      - [New replication config after migration](#new-replication-config-after-migration)
    - [Cascade setup](#cascade-setup)
      - [Legacy cascade replication config before migration](#legacy-cascade-replication-config-before-migration)
      - [New cascade replication config after migration](#new-cascade-replication-config-after-migration)

## Write ahead log (WAL)

The write-ahead log is a combination of row updates and statement execution. WAL is stored as part of namespace storage. Each WAL record has a unique 64-bit log sequence number (LSN), which also contains the server id. WAL overhead is 18 bytes of RAM per row update record.
WAL is ring structure, therefore after N updates (4M by default) are recorded, the oldest WAL records will be automatically removed.
WAL in storage contains only records with statements (bulk updates, deletes and index structure changes). Rows updates are not stored as dedicated WAL records, but each document contains its own LSN - this is enough to restore complete WAL in RAM on namespace loading from disk.

Side effect of this mechanic is lack of exact sequence of indexes updates/data updates, and therefore in case of incompatible data migration (e.g. indexed field type changed) follower will fail to apply offline WAL, and will fallback to forced sync.

During `force sync`, Reindexer will try to transfer all the namespace's data into a temporary in-memory table (its name starts with the `@tmp_` prefix) and then perform an atomic swap at the end of synchronization. This process requires additional RAM for a full copy of the namespace.

When a namespace is large (or the network connection is not fast enough), `force sync` takes a long time. During the sync, the `leader` node may still receive updates, which will be placed into the ring WAL buffer and internal online updates queue. So, it is possible to face situation, when right after `force sync` target namespace will have an outdated LSN (in cases, when both WAL buffer and online updates queue got overflow during synchronization process) and this will lead to another attempt of `force sync`. To avoid such situations, you may try to set larger WAL size (check the section below) and larger online updates buffer size (it may be set via `--updatessize` CLI flag or `net.max_updates_size` config option on `reindexer_server` startup).

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

Replication is a complex mechanism, and there is a potential risk of breaking data consistency between master and slave.
Reindexer calculates a lightweight incremental hash of all namespace data (Checksum). Checksum is used to quickly check that follower data is really up to date with the leader.

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

- `server_id` - Identifies the node in replication setup. Must be a value from 0 to 999. IDs must be unique within each RAFT cluster (prefer a distinct ID for each node in any replication setup.).
- `cluster_id` - Use the same value on all nodes connected by asynchronous or RAFT replication, including standalone nodes.
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
		"sync_threads": 4,
		"syncs_per_thread": 2,
		"online_updates_timeout_sec": 20,
		"online_updates_delay_msec": 100,
		"sync_timeout_sec": 60,
		"retry_sync_interval_msec": 20000,
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

- `role` - Local role for asynchronous replication.
  - `none` or `follower` - The async replicator preserves local namespace roles at startup.
  - `leader` - At startup, the async replicator clears the replica role of local namespaces outside the RAFT configuration.

  Any async role permits outgoing replication. In a multi-node RAFT cluster, only the elected leader can send updates, and it must first complete its initial RAFT sync. To stop sending updates, set `nodes` to `[]`.
- `sync_threads` - Number of replication thread
- `syncs_per_thread` - Max number of concurrent force/wal-syncs per each replication thread
- `online_updates_timeout_sec` - Network timeout for communication with followers (for online-replication mode), in seconds
- `sync_timeout_sec` - Network timeout for communication with followers (for force and wal synchronization), in seconds
- `retry_sync_interval_msec` - Delay in milliseconds before the next synchronization attempt after an error (default `20000`).
- `enable_compression` - Network traffic compression flag
- `batching_routines_count` - Number of concurrent routines, used to asynchronously send online updates for each follower. Larger values may reduce network trip-around, but also increase RAM consumption
- `force_sync_on_logic_error` - Force resync on logic error conditions
- `force_sync_on_wrong_data_hash` - Force resync if checksum mismatch
- `log_level` - Replication log level on replicator's startup. Possible values: none, error, warning, info, trace
- `max_wal_depth_on_force_sync` - Maximum number of WAL records, which will be copied after force-sync
- `online_updates_delay_msec` - Delay between write operation and replication. Larger values here will lead to higher replication latency and buffering, but will also provide more effective network batching and CPU utilization
- `namespaces` - Default list of namespaces to replicate. An empty list (`[]`) selects all user namespaces on the source. Replicated namespaces are read-only on the follower.
- `self_replication_token` - Replication token of the current node that it sends to the follower for verification
- `nodes` - Outgoing replication targets. All members of a RAFT cluster share this list. See [Replication from the RAFT cluster](#replication-from-the-raft-cluster).

Each entry in `nodes` must specify exactly one of the following fields. Both use database connection strings (DSNs), such as `cproto://db1:6534/mydb`. Use `cprotos://` for TLS.

- Use `dsn` for a single target node. It can be a standalone follower or a RAFT member if the replicated namespaces are outside that member's RAFT configuration.
- Use `dsns` for a target RAFT cluster. Supply one DSN for every member (at least three in total). The source discovers the current leader. See [Replication to the RAFT cluster](#replication-to-the-raft-cluster).

Omit a target's `namespaces` to inherit the default list. Set `"namespaces": []` on the target entry to select all user namespaces on the source. For a RAFT target, its RAFT configuration must cover every namespace the source tries to replicate.

As second option replication can be configured by YAML-config files, which has to be placed to database directory. Sample of async replication config file is [here](cpp_src/cluster/async_replication.conf) 
and sample for general replication config file is [here](cpp_src/cluster/replication.conf).

If a config file is present, it overrides settings from the `#config` namespace on Reindexer startup.

#### Check replication status

To check outgoing replication, query `#replicationstats` on the source. In a RAFT cluster, query the leader to see active asynchronous targets.

```SQL
Reindexer> SELECT * FROM #replicationstats WHERE type='async'
```

Each entry in `nodes` represents one target. Check `sync_state` for its progress and `last_error` for problems. See [Statistics](#statistics) for the field descriptions.

To inspect one namespace on a specific node, query `#memstats`.

```SQL
Reindexer> SELECT name,replication FROM #memstats WHERE name='user_items'
```

This excerpt shows the `replication` field on a follower in the secondary RAFT cluster. Primary node `10` created `user_items` and wrote its latest update. Node `20` leads the secondary cluster.

```JSON
{
	"last_lsn_v2":{
		"server_id":10,
		"counter":5
	},
	"ns_version":{
		"server_id":10,
		"counter":5
	},
	"clusterization_status":{
		"leader_id": 20,
		"role": "cluster_replica",
		"is_primary": false
	},
	"checksum":6,
	"data_count":4,
	"updated_unix_nano":1594041127153561000,
	"wal_count":6,
	"wal_size":311
}
```

- `last_lsn_v2` - LSN of the latest WAL record. Its `server_id` identifies the node where the record originated.
- `ns_version` - Version assigned when the namespace was created. Its `server_id` identifies the node that assigned the version.
- `clusterization_status` - Replication state of this namespace on this node. On a secondary RAFT cluster, `is_primary: false` identifies a namespace owned by an external source. It is read-only for clients even on the RAFT leader. See [Replication to the RAFT cluster](#replication-to-the-raft-cluster).
- `checksum` - Checksum of all records in namespace
- `data_count` - number of records in the namespace
- `updated_unix_nano` - last operation time
- `wal_count` - number of records in WAL
- `wal_size` - WAL size

#### Actions

There are also a few actions available to interact with async replication via reindexer tool or web interface.

- Restart replication (if sent to leader, restarts replication completely):

```SQL
Reindexer> \upsert #config { "type":"action","action":{ "command":"restart_replication" } }
```

- Reset replication role (useful to make follower's namespaces writable again, if you are going to use this follower outside of replication setup). On the leader of the secondary RAFT cluster it makes the replicated namespaces writable in the whole cluster (see [Replication to the RAFT cluster](#replication-to-the-raft-cluster)):

```SQL
Reindexer> \upsert #config { "type":"action","action":{ "command":"reset_replication_role" } }
```

- Reset replication role for specific namespace:

```SQL
Reindexer> \upsert #config { "type":"action","action":{ "command":"reset_replication_role", "namespace": "ns_name" } }
```

An asynchronous replication leader schedules role resets when a target is removed, its namespace list is narrowed, or the node's async role changes away from `leader`. Automatic resets apply only to namespaces marked `simple_replica` whose `leader_id` matches the async leader's `server_id`. This includes a namespace replicated to a single RAFT member outside its RAFT configuration. For a namespace replicated to the secondary cluster as a whole, follow [Detaching a secondary](#detaching-a-secondary).

Automatic resets use each follower's previous namespace list. If that list was empty (`[]`), the leader uses its own local user namespaces to decide which resets to send.

Automatic resets run in the background. A replication configuration update may return before the affected namespaces become writable on followers.

- Synchronize followers' replication roles after an automatic role reset failed (call on the async replication leader):

```SQL
Reindexer> \upsert #config { "type":"action","action":{ "command":"synchronize_replication_roles" } }
```

The leader checks each currently configured follower. It clears the `simple_replica` role from namespaces that still belong to this leader but are outside the follower's configured namespace list. An empty list (`[]`) stands for the leader's local user namespaces. Resets apply only to the follower being checked and do not propagate down a cascade.

The action waits for the checks to finish. Even if it succeeds, a reset on an individual follower may have failed. Check the server log for each follower's result. DSN credentials in the log are masked.

Repeating the action skips namespaces whose roles have already been reset. Nodes removed entirely from the configuration cannot be discovered by this action; use `reset_replication_role` directly on such a node.

- Apply the shared async replication configuration. Submit the following command in `reindexer_tool` on any node of the source cluster. A follower forwards it to the leader. On a standalone node it applies locally.

```SQL
Reindexer> \upsert #config {"type":"action","action":{"command":"apply_async_replication_config","config":{"nodes":[{"dsns":["cproto://secondary1:6534/mydb","cproto://secondary2:6534/mydb","cproto://secondary3:6534/mydb"],"namespaces":["orders"]}]}}}
```

  Supply the complete shared configuration each time. Omitted settings take their defaults and each node keeps its own `role`. To stop sending updates, submit the desired shared settings with `"nodes":[]`. Success requires a majority of the cluster to apply the configuration, including the node that forwarded the command. Offline followers receive it on reconnect. The action record in `#config` can contain unmasked credentials embedded in DSNs.

- Control replication's log levels:

```SQL
Reindexer> \upsert #config { "type":"action","action":{ "command":"set_log_level", "type": "async_replication", "level":"trace" } }
```

Possible `types`: `async_replication` (controls log level for asynchronous replication), `cluster` (controls log level for synchronous replication and RAFT manager).

Possible `levels`: `none`, `error`, `warning`, `info`, `trace`.

## RAFT cluster

Reindexer supports a RAFT-like algorithm for leader elections and synchronous replication with consensus awaiting. In a cluster setup, each node may be a standalone server or a Go application with the builtinserver binding.

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

Once a node has become a follower, it starts checking if the leader is available (via timestamp of the last received ping message). If the leader becomes unavailable, the follower initiates a new election term.

In follower state node changes roles of each namespace from cluster config to "cluster_replica" and sets corresponding leader ID (i.e. server ID of current leader). From this moment all the follower namespaces are readonly for anyone except cluster leader.

Any request, which requires write access will be proxied to leader and executed on the leader's side.

Requests that do not require write access (and requests to system namespaces) will be executed locally.

#### Operating as leader

Once node has become leader it starts sending ping messages to all the followers. If follower did not respond to the ping message, it will be considered as dead.
If N/2 of the followers are dead (N is total number of nodes in cluster), leader has to initiate new elections term.

Right after role switch node begins initial leader sync. During initial sync leader collects the latest data from at least N/2 other nodes. It can not replicate any data while initial sync is not completed.

When initial sync is done, the leader starts to synchronize followers. Same as for asynchronous replication there are 3 different mechanics for data synchronization: force sync, WAL sync and online updates.
The leader continues to replicate data until it receives a request for manual re-elections or an error in consensus. In both of these situations, the node will initiate a new election term.

### Guarantees

1. Consensus for each data write.

Reindexer is using consensus algorithm to provide data safety. Each data write on leader generates one or few online-updates and then each update awaits approves from N/2 followers. Follower can not approve updates, if WAL/force sync is not done yet.

2. Read after write is always safe.

If an operation was proxied from follower to leader, then it won't generally be approved before it gets approval from the current node (emitter node), so if you wrote something in the current thread on any node, you'll be able to read this data from the same node.

3. Online updates are ordered.

For optimization purposes, concurrent writes are available even if some write operations from other threads are awaiting consensus right now. Reindexer guarantees that all of these concurrent writes will be performed on followers in the same order.

### Configuration

#### Configuration via YML-file

On startup reindexer_server reads replication and cluster config from files `replication.conf`([sample](cpp_src/cluster/replication.conf)) and `cluster.conf`([sample](cpp_src/cluster/cluster.conf)), which have to be placed in database directory.

`replication.conf` sets general replication parameters and has to be unique for each node in the cluster (those parameters may also be configured via the `#config` namespace).

`cluster.conf` sets specific cluster parameters (description may be found in sample). This file has to have the same content on each node of the cluster.

#### Examples

1. [Example script](cpp_src/cmd/reindexer_server/clustertest/cluster_example.sh), which creates local cluster.

2. [Docker-compose config](cpp_src/cmd/reindexer_server/clustertest/docker-compose.yml), which creates 3 Docker containers running a RAFT cluster. Usage:

```
# Run from docker-compose.yml directory
docker-compose up
```

In both examples default RPC ports are: 6534, 6535, 6536; and default HTTP ports are: 9088, 9089, 9090.

#### Configuration via system namespace
// work in progress

### Statistics

Query `#replicationstats` for the status of RAFT members.

```SQL
Reindexer> SELECT * FROM #replicationstats WHERE type='cluster'
```

The `type` filter is required. Queries for `cluster` statistics go to the RAFT leader. Queries for `async` statistics show the outgoing targets of the node you contacted. The following example shows three online RAFT members.

```JSON
{
    "type": "cluster",
    "initial_sync": {
        "force_syncs": {"count": 0, "max_time_us": 0, "avg_time_us": 0},
        "wal_syncs": {"count": 0, "max_time_us": 0, "avg_time_us": 0},
        "total_time_us": 2806
    },
    "force_syncs": {"count": 0, "max_time_us": 0, "avg_time_us": 0},
    "wal_syncs": {"count": 0, "max_time_us": 0, "avg_time_us": 0},
    "update_drops": 0,
    "pending_updates_count": 0,
    "allocated_updates_count": 0,
    "allocated_updates_size": 0,
    "log_level": "info",
    "nodes": [
        {
            "dsn": "cproto://127.0.0.1:14000/node0",
            "server_id": 0,
            "pending_updates_count": 0,
            "status": "online",
            "sync_state": "online_replication",
            "role": "leader",
            "is_synchronized": true,
            "queued_namespace_syncs": 0,
            "last_error": {"code": 0, "message": ""},
            "namespaces": [],
            "async_replication_config": {
                "source_id": 1795000000000000,
                "apply_error": {"code": 0, "message": ""}
            }
        },
        {
            "dsn": "cproto://127.0.0.1:14001/node1",
            "server_id": 1,
            "pending_updates_count": 0,
            "status": "online",
            "sync_state": "online_replication",
            "role": "follower",
            "is_synchronized": true,
            "queued_namespace_syncs": 0,
            "last_error": {"code": 0, "message": ""},
            "namespaces": [],
            "async_replication_config": {
                "source_id": 1795000000000000,
                "apply_error": {"code": 0, "message": ""}
            }
        },
        {
            "dsn": "cproto://127.0.0.1:14002/node2",
            "server_id": 2,
            "pending_updates_count": 0,
            "status": "online",
            "sync_state": "online_replication",
            "role": "follower",
            "is_synchronized": true,
            "queued_namespace_syncs": 0,
            "last_error": {"code": 0, "message": ""},
            "namespaces": [],
            "async_replication_config": {
                "source_id": 1795000000000000,
                "apply_error": {"code": 0, "message": ""}
            }
        }
    ]
}
```

- `log_level` - Current log level for this replication type.
- `initial_sync` - statistics about leader's initial sync;
- `force_syncs` - statistics about follower's namespaces force syncs;
- `wal_syncs` - statistics about follower's namespaces wal syncs;
- `update_drops` - number of updates overflows, when updates were dropped;
- `pending_updates_count` - number of updates, awaiting replication in queue;
- `allocated_updates_count` - number of updates in queue (including those, which already were replicated, but was not deallocated yet);
- `allocated_updates_size` - total size of allocated updates in bytes;
- `nodes` - statistics about each node.

Both `last_error` and `apply_error` contain a `code` and a `message`. A code of `0` means no error is recorded.

Node statistics fields:

- `dsn` - The node's DSN. For a RAFT target in the source's `async` statistics, this field is empty until the source validates the secondary leader. It then contains that leader's DSN.
- `dsns` - Configured DSNs of all members of the target RAFT cluster. This field appears only for cluster targets in `async` statistics.
- `last_error` - The most recent replication error for this node or target.
- `server_id` - node's server ID;
- `pending_updates_count` - number of updates, which are awaiting replication to this specific follower;
- `queued_namespace_syncs` - Number of namespaces still waiting for synchronization to finish on this node.
- `status` - node's status: `none`, `online`, `offline` or `raft_error`;
- `sync_state` - node's replication sync state:
  - `none` - unknown;
  - `syncing` - namespaces are being synchronized (WAL or force sync);
  - `awaiting_resync` - the next synchronization attempt is awaited (before the first one or after an error);
  - `online_replication` - namespaces are synchronized, updates are replicated online;
  - `initial_leader_sync` - RAFT leader synchronizes its namespaces with the cluster after the elections;
  - `suspended` - the node of the RAFT cluster does not replicate to this target, e.g. it is the follower (see [Replication from the RAFT cluster](#replication-from-the-raft-cluster)). The node is not connected to the target (`status` is `none`) and does not keep the updates for it;
- `role` - node's role: `leader` or `follower`;
- `is_synchronized` - synchronization status. Shows if all the approved updates were replicated to this node;
- `namespaces` - namespaces which are configured for this node.
- `async_replication_config` - Status of the shared async configuration on this member, reported only in the `cluster` row. The leader updates follower entries when it sends the configuration.
  - `source_id` - A 64-bit ID assigned by `apply_async_replication_config`. `-1` means the member's configuration has no associated action. If the field is absent, the leader has no confirmed ID for that member.
  - `apply_error` - For a follower, reports errors sending or applying the configuration. For the leader, reports an error applying the configuration after an election.

### Manual leader's selection

Leader of the cluster may be changed manually via `#config` namespace. For example, this request will transfer leadership to the node with `server ID` 2 (if it exists):

```SQL
Reindexer> \upsert #config { "type":"action","action":{ "command":"set_leader_node", "server_id": 2 } }
```

## Async replication and RAFT clusters

Source and target of the async replication may be a standalone node or a RAFT cluster in any combination. The target node is set via `dsn`, the target RAFT cluster - via `dsns`, the list of DSNs of all its nodes:

```
       updates                                 updates (ns3)
          |                                          |
          v                                          v
+--------------------+                     +--------------------+
|  primary cluster   |  async repl (ns2)   | secondary cluster  |
|     (ns1, ns2)     |-------------------->|     (ns2, ns3)     |
|                    |        dsns         |                    |
|   cl10 -- cl11     |                     |   cl20 -- cl21     |
|      \    /        |                     |      \    /        |
|       cl12         |                     |       cl22         |
+--------------------+                     +--------------------+
          |
          | async repl (ns1)
          | dsn
          v
     +---------+
     |  node1  |
     +---------+
```

Upsert the following `#config` item on any primary member to send `ns2` to the secondary cluster and `ns1` to a single node. With the REST API, send the JSON as the body of `PATCH /api/v1/db/{database}/namespaces/%23config/items`.

```JSON
{
    "type": "action",
    "action": {
        "command": "apply_async_replication_config",
        "config": {
            "nodes": [
                {
                    "dsns": [
                        "cproto://192.168.1.5:6534/mydb",
                        "cproto://192.168.1.6:6534/mydb",
                        "cproto://192.168.1.7:6534/mydb"
                    ],
                    "namespaces": ["ns2"]
                },
                {
                    "dsn": "cproto://192.168.1.8:6534/mydb",
                    "namespaces": ["ns1"]
                }
            ]
        }
    }
}
```

The primary leader sends updates asynchronously to the secondary leader, which replicates them to its followers through RAFT. A successful write on the primary can take time to appear on the secondary. Both clusters must use the same `cluster_id`. The primary and secondary may reuse the same `server_id` values across clusters.

### Replication from the RAFT cluster

Only the elected leader of a multi-node RAFT cluster sends async updates. When leadership changes, the former leader stops sending updates and its targets show `sync_state: suspended` and `status: none`. The new leader can resume replication after its initial RAFT sync.

Use the [`apply_async_replication_config` action](#actions) to change the targets and other shared settings. The leader distributes them to the cluster, and each node keeps its local `role`.

After applying a configuration, query [`#replicationstats` with `type='cluster'`](#statistics). Check that each member reports the leader's new `async_replication_config.source_id` and no configuration error in `apply_error`. Matching IDs identify the same configuration source. Local edits to settings such as `log_level` can leave the ID unchanged.

If a new leader cannot apply the latest shared configuration, it suspends outgoing replication and configuration distribution until a configuration action succeeds or another election takes place. Its `apply_error` explains the failure.

Direct edits to shared settings in `#config` or `async_replication.conf` can be overwritten when a follower reconnects or leadership changes. To change the local log level at runtime, use `set_log_level`.

### Replication to the RAFT cluster

Before sending data, the source checks the target against the secondary leader's view of its cluster.

- The configured DSNs must match the host, port and database path of every member in the secondary's `cluster.conf`.
- The source and the secondary leader must agree on which members are reachable. Every reachable member must support async replication to a cluster.
- The secondary needs an online quorum and one reachable leader that has completed its initial sync.

When the secondary elects a new leader, the source discovers and validates it automatically. If validation fails, replication to that target pauses while other targets can continue. On the source leader, run `SELECT * FROM #replicationstats WHERE type='async'` and inspect the target's `last_error`, `dsns` and `dsn`.

Each replicated namespace must be included in the secondary's RAFT configuration on every member. A single namespace outside that configuration can stop replication to the entire target until the configurations agree. If the secondary already has a namespace with the same name, synchronization can overwrite its data.

Replicated namespaces are read-only for clients on every secondary member and keep `is_primary: false` after a restart. The secondary can accept writes to its own namespaces and act as a source in a cascade. Assign each namespace to one primary.

If several primaries feed one secondary, assign each namespace to one primary. You may also give each primary a distinct `self_replication_token` and set `admissible_replication_tokens` for its namespaces on every secondary member to control incoming async replication strictly. The secondary's internal RAFT replication runs independently of those tokens.

#### Detaching a secondary

1. Remove the target or namespace from the primary's replication configuration. Alternatively, set a new nonempty admissible token for each namespace you are detaching on every secondary member so the old primary cannot reclaim it.
2. Release the namespaces with [`reset_replication_role`](#actions).
   - For a namespace in the secondary's `cluster.conf`, run the action on the secondary leader. Specify `namespace` to release just that namespace. Omitting the name releases open RAFT namespaces replicated from any primary and also resets local async replicas on this node.
   - If the namespace has already been removed from the secondary's `cluster.conf`, run the action with its name on each secondary member.
3. Before routing writes to the secondary, check `#memstats` on every member. Verify that `replication.clusterization_status.is_primary` is `true` for each released namespace.

## Known issues and constraints

- Reindexer does not have global WAL and uses individual WAL for each namespace. Due to this peculiarity operations like `rename namespace` and `drop namespace` can not be replicated properly via force/WAL sync (only online replication is available).
This also may lead to situation, when new leader recovers some the dropped namespaces on initial sync.
- Reindexer does not remove 'extra' namespaces from replicas and does not download those namespaces to leader after it's initial sync. So this may lead to the situation, when some replicas will have namespaces, which does not exist anywhere in cluster
(in case if those replicas were offline during leader elections). For example, if you have cluster of 3 nodes, where two nodes are empty and one has actual data. In that case two empty nodes will startup faster and will be able to choose the leader, while third node is still reading its data (and stays offline).
- If server ID in cluster config does not correspond to actual server ID of the node, this may lead to undefined behaviour (currently reindexer does not validate those IDs by itself).
- Currently synchronous cluster config may not be changed in runtime via HTTP/RPC and rewriting YML-file is the only way to change it.

## Migration from Reindexer's v3.x.x replication config

Current asynchronous replication implementation is incompatible with configs from v3.x.x, so you will have to migrate manually.

1. Server IDs and cluster IDs don't require any changes (they use the same object "replication" in the #config namespace) 
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
		"retry_sync_interval_msec": 20000,
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
		"role":"follower",
		"sync_threads": 4,
		"syncs_per_thread": 2,
		"online_updates_timeout_sec": 60,
		"retry_sync_interval_msec": 20000,
		"enable_compression": true,
		"batching_routines_count": 100,
		"force_sync_on_logic_error": false,
		"force_sync_on_wrong_data_hash": false,
		"namespaces":[],
		"nodes":
		[
			{
				"dsn": "cproto://192.168.10.3:6534/mydb"
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
		"retry_sync_interval_msec": 20000,
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

