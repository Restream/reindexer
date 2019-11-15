# Replication & cluster mode

Reindexer supports async logical master-slave replication. Master must be standalone server or golang application with builtinserver reindexer binding.
Slave can be standalone server, or golang application with builtin or builtinserver bindings. Number of active slaves is not limited.

Replication is using 3 different mechanics:

- Forced synchronisation with namespace snapshot  
Used for initial synchonisation for copy complete structure and data from master namespace to slave. Also used in case of error with WAL replication (e.g. WAL has been outdated, or incompatible changes in indexes structure). In this mode slave queries all indexes and data from master, then master prepares COW namespace snapshot and sends it to slave.

- Offline write ahead log (WAL). Document updates are ROW based, index structure changes, deletes and bulk updates are STATEMENT based  
Used when slave established network connection to master to sync data. In this mode slave queries all records from master's WAL with log sequence number (LSN) greater than LSN of applied by slave last WAL record.

- Online WAL updates live stream  
Used when connection is established. Master pushes WAL updates stream to all connected slaves. This mode is most lightweight and requires few master CPU and memory resourcses. 

## Write ahead log (WAL)

Write ahead log is combination of rows updates and statements execution. WAL is stored as part of namespace storage. Each WAL record has unique 64-bit log sequence number (LSN).
WAL is ring structure, therefore after N updates (1M by default) are recorded, oldest WAL records will be automatically removed.
WAL in storage contatins only records with statements (bulk updates, deletes and index structure changes). Rows updates are not stored as dedicated WAL records, but each document contains it's own LSN - this is enought to restore complete WAL in RAM on namespace loading from disk.

Side effect of this mechanic is lack of exact sequence of indexes updates/data updates, and therefore in case of incompatible data migration (e.g. indexed field type changed) slave will fail to apply offline WAL, and wiil fallback to forced sync

WAL overhead is 16 byte of RAM per each ROW update record.

## Data integrity check

Replication is complex mechanism and there are present potential possiblities to broke data consitence beetwen master and slave. 
Reindexer is calculates lightweight incremental hash of all namespace data (DataHash). DataHash is used to quick check, that data of slave is really up to date with master.

## Usage

### Configuring replication

To configure replication special document of namespace `#config` is used:

```JSON
{
	"type":"replication",
	"replication":{
		"role":"none",
		"master_dsn":"cproto://127.0.0.1:6534/db",
		"cluster_id":2,
		"force_sync_on_logic_error": false,
		"force_sync_on_wrong_data_hash": false,
		"namespaces":[]
	}
}
```

- `role`  Replication role. May be on of
   - `none` - replication is disabled
   - `slave` - replication as slave
   - `master` - replication as master
- `master_dsn` DSN to master. Only cproto schema is supported
- `timeout_sec` Network timeout for communication with master, in seconds
- `cluster_id` Cluser ID - must be same for client and for master
- `force_sync_on_logic_error` - Force resync on logic error conditions
- `force_sync_on_wrong_data_hash` - Force resync if dataHash mismatch
- `namespaces` List of namespaces for replication. If emply, all namespaces. All replicated namespaces will become read only for slave

As second option replication can be configured by config file, which will be placed to database folder. Sample of replication config file is [here](cpp_src/replicator/replication.conf)

If config file is present, then it's overrides settings from `#config` namespace on reindexer startup

### Check replication status

Replication status is available in system namespace `#memstats`. e.g, execution of statament:

```SQL
Reindexer> SELECT name,replication FROM #memstats WHERE name='media_items'
```
will return JSON object with status of namespace `media_items `replication 

```JSON
{
	"last_lsn": 31863, 
	"cluster_id": 2, 
	"slave_mode": true, 
	"incarnation_counter": 1, 
	"data_hash": 2207033076418442500,
	"data_count": 20,
	"updated_unix_nano" : 1566579062277714700,
	"error_code": 0,
    "error_message": "",
    "status": "idle",
    "master_state":
    {
        "data_hash": 2207033076418442500,
        "last_lsn": 31863,
        "updated_unix_nano": 1566579062277000000,
        "data_count": 20
    }
}
```

### Access namespace's WAL with reindexer_tool

To view offline WAL contents from reindexer_tool `SELECT` statement with special condition to `#lsn` index is used:

```SQL
Reindexer> SELECT * FROM epg LIMIT 2 where #lsn > 1000000
```

To view online WAL updates stream command `\SUBSCRIBE` is used:

```SQL
Reindexer> \SUBSCRIBE ON
```
To test subscription for example execute some write statement:
```SQL
Reindexer> DELETE FROM media_items WHERE id>100
```

If subscribtion is working, then something like this should be displayed

```
#31864 media_items WalUpdateQuery DELETE FROM media_items WHERE id > 100
```

## Limitations and know issues

Replication is in beta stage, so there are some issues and limitations:

- Switch between master and slave mode on the same storage is not working properly. 
- After upgrading from previous version of reindexer, storage does not contain WAL, and therefore replication master will not start properly on old storage

Work around for this issues:  
Before enabling replication master make database backup with reindexer_tool, remove database, enable master mode, and then restore database backup. After this WAL will be initialized and master should work properly
