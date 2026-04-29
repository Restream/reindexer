# Reindexer REST API

<!-- toc -->

- [Overview](#overview)
- [Path Table](#path-table)
- [Reference Table](#reference-table)
- [Path Details](#path-details)
  * [List available databases](#list-available-databases)
  * [Create new database](#create-new-database)
  * [Drop database](#drop-database)
  * [List available namespaces](#list-available-namespaces)
  * [Create namespace](#create-namespace)
  * [Get namespace description](#get-namespace-description)
  * [Drop namespace](#drop-namespace)
  * [Truncate namespace](#truncate-namespace)
  * [Rename namespace](#rename-namespace)
  * [Get list of namespace's meta info](#get-list-of-namespaces-meta-info)
  * [Get namespace's meta info by key](#get-namespaces-meta-info-by-key)
  * [Remove namespace's meta info for key](#remove-namespaces-meta-info-for-key)
  * [Put namespace's meta info with specified key and value](#put-namespaces-meta-info-with-specified-key-and-value)
  * [Get documents from namespace](#get-documents-from-namespace)
  * [Update documents in namespace](#update-documents-in-namespace)
  * [Insert documents to namespace](#insert-documents-to-namespace)
  * [Delete documents from namespace](#delete-documents-from-namespace)
  * [Upsert documents in namespace](#upsert-documents-in-namespace)
  * [List available indexes](#list-available-indexes)
  * [Update index in namespace](#update-index-in-namespace)
  * [Add new index to namespace](#add-new-index-to-namespace)
  * [Drop index from namespace](#drop-index-from-namespace)
  * [Get namespace schema](#get-namespace-schema)
  * [Set namespace schema](#set-namespace-schema)
  * [Get protobuf communication parameters schema](#get-protobuf-communication-parameters-schema)
  * [Query documents from namespace](#query-documents-from-namespace)
  * [Update documents in namespace](#update-documents-in-namespace-1)
  * [Query documents from namespace](#query-documents-from-namespace-1)
  * [Delete documents from namespace](#delete-documents-from-namespace-1)
  * [Begin transaction to namespace](#begin-transaction-to-namespace)
  * [Commit transaction](#commit-transaction)
  * [Rollback transaction](#rollback-transaction)
  * [Update documents in namespace via transaction](#update-documents-in-namespace-via-transaction)
  * [Insert documents to namespace via transaction](#insert-documents-to-namespace-via-transaction)
  * [Delete documents from namespace via transaction](#delete-documents-from-namespace-via-transaction)
  * [Upsert documents in namespace via transaction](#upsert-documents-in-namespace-via-transaction)
  * [Delete/update queries for transactions](#deleteupdate-queries-for-transactions)
  * [Delete documents from namespace (transactions)](#delete-documents-from-namespace-transactions)
  * [Suggest for autocompletion of SQL query](#suggest-for-autocompletion-of-sql-query)
  * [Query documents from namespace](#query-documents-from-namespace-2)
  * [Get system information](#get-system-information)
  * [Try to release free memory back to the operating system for reuse by other applications.](#try-to-release-free-memory-back-to-the-operating-system-for-reuse-by-other-applications)
  * [Get memory usage information](#get-memory-usage-information)
  * [Get user role](#get-user-role)
  * [Get activity stats information](#get-activity-stats-information)
  * [Get client connection information](#get-client-connection-information)
  * [Get replication statistics](#get-replication-statistics)
  * [Get memory stats information](#get-memory-stats-information)
  * [Get performance stats information](#get-performance-stats-information)
  * [Get SELECT queries performance stats information](#get-select-queries-performance-stats-information)
  * [Get system configs](#get-system-configs)
  * [Update system config](#update-system-config)
  * [Get default system configs](#get-default-system-configs)
- [References](#references)
  * [SysInfo](#sysinfo)
  * [ActivityStats](#activitystats)
  * [ClientsStats](#clientsstats)
  * [ReplicationSyncStat](#replicationsyncstat)
  * [GlobalReplicationStats](#globalreplicationstats)
  * [Databases](#databases)
  * [Database](#database)
  * [Namespaces](#namespaces)
  * [Namespace](#namespace)
  * [Index](#index)
  * [JsonObjectDef](#jsonobjectdef)
  * [SchemaDef](#schemadef)
  * [UpdateField](#updatefield)
  * [Query](#query)
  * [SubQuery](#subquery)
  * [EqualPositionDef](#equalpositiondef)
  * [FilterDef](#filterdef)
  * [KnnSearchParamsDef](#knnsearchparamsdef)
  * [SortDef](#sortdef)
  * [JoinedDef](#joineddef)
  * [OnDef](#ondef)
  * [AggregationsDef](#aggregationsdef)
  * [SubQueryAggregationsDef](#subqueryaggregationsdef)
  * [AggregationsSortDef](#aggregationssortdef)
  * [FtStopWordObject](#ftstopwordobject)
  * [FloatVectorConfig](#floatvectorconfig)
  * [FulltextConfig](#fulltextconfig)
  * [FulltextFieldConfig](#fulltextfieldconfig)
  * [FulltextSynonym](#fulltextsynonym)
  * [FulltextTermsBoost](#fulltexttermsboost)
  * [MetaInfo](#metainfo)
  * [MetaListResponse](#metalistresponse)
  * [MetaByKeyResponse](#metabykeyresponse)
  * [Items](#items)
  * [SuggestItems](#suggestitems)
  * [QueryItems](#queryitems)
  * [Indexes](#indexes)
  * [ExplainDef](#explaindef)
  * [SingleQueryExplainDef](#singlequeryexplaindef)
  * [MergedQueryExplainDef](#mergedqueryexplaindef)
  * [AggregationResDef](#aggregationresdef)
  * [DistincOneItemDef](#distinconeitemdef)
  * [DistinctMultiItemDef](#distinctmultiitemdef)
  * [QueryColumnDef](#querycolumndef)
  * [StatusResponse](#statusresponse)
  * [ItemsUpdateResponse](#itemsupdateresponse)
  * [UpdateResponse](#updateresponse)
  * [DatabaseMemStats](#databasememstats)
  * [NamespaceMemStats](#namespacememstats)
  * [IndexMemStat](#indexmemstat)
  * [EmbedderStatus](#embedderstatus)
  * [EmbedderLastError](#embedderlasterror)
  * [EmbeddersCacheMemStat](#embedderscachememstat)
  * [JoinCacheMemStats](#joincachememstats)
  * [QueryCacheMemStats](#querycachememstats)
  * [IndexCacheMemStats](#indexcachememstats)
  * [CacheMemStats](#cachememstats)
  * [ReplicationStats](#replicationstats)
  * [DatabasePerfStats](#databaseperfstats)
  * [NamespacePerfStats](#namespaceperfstats)
  * [CommonPerfStats](#commonperfstats)
  * [UpdatePerfStats](#updateperfstats)
  * [SelectPerfStats](#selectperfstats)
  * [TransactionsPerfStats](#transactionsperfstats)
  * [QueriesPerfStats](#queriesperfstats)
  * [QueryPerfStats](#queryperfstats)
  * [LRUCachePerfStats](#lrucacheperfstats)
  * [EmbedderCachePerfStat](#embeddercacheperfstat)
  * [EmbedderPerfStat](#embedderperfstat)
  * [SystemConfigItems](#systemconfigitems)
  * [SystemConfigItem](#systemconfigitem)
  * [ProfilingConfig](#profilingconfig)
  * [LongQueriesLogging](#longquerieslogging)
  * [SelectLogging](#selectlogging)
  * [UpdateDeleteLogging](#updatedeletelogging)
  * [TransactionLogging](#transactionlogging)
  * [NamespacesConfig](#namespacesconfig)
  * [ReplicationConfig](#replicationconfig)
  * [AsyncReplicationConfig](#asyncreplicationconfig)
  * [EmbeddersConfig](#embeddersconfig)
  * [ActionCommand](#actioncommand)
  * [BeginTransactionResponse](#begintransactionresponse)
  * [UserRoleResponse](#userroleresponse)
  * [OK](#ok)
  * [BadRequest](#badrequest)
  * [RequestTimeout](#requesttimeout)
  * [Forbidden](#forbidden)
  * [NotFound](#notfound)
  * [UnexpectedError](#unexpectederror)

<!-- tocstop -->

> Version 5.13.0

## Overview

***

**Reindexer** is an embeddable, in-memory, document-oriented database with a high-level Query builder interface.
Reindexer's goal is to provide fast search with complex queries.
Reindexer is compact, fast and it does not have heavy dependencies.



## Path Table

| Method | Path | Description |
| --- | --- | --- |
| GET | [/db](#getdb) | List available databases |
| POST | [/db](#postdb) | Create new database |
| DELETE | [/db/{database}](#deletedbdatabase) | Drop database |
| GET | [/db/{database}/namespaces](#getdbdatabasenamespaces) | List available namespaces |
| POST | [/db/{database}/namespaces](#postdbdatabasenamespaces) | Create namespace |
| GET | [/db/{database}/namespaces/{name}](#getdbdatabasenamespacesname) | Get namespace description |
| DELETE | [/db/{database}/namespaces/{name}](#deletedbdatabasenamespacesname) | Drop namespace |
| DELETE | [/db/{database}/namespaces/{name}/truncate](#deletedbdatabasenamespacesnametruncate) | Truncate namespace |
| GET | [/db/{database}/namespaces/{name}/rename/{newname}](#getdbdatabasenamespacesnamerenamenewname) | Rename namespace |
| GET | [/db/{database}/namespaces/{name}/metalist](#getdbdatabasenamespacesnamemetalist) | Get list of namespace's meta info |
| GET | [/db/{database}/namespaces/{name}/metabykey/{key}](#getdbdatabasenamespacesnamemetabykeykey) | Get namespace's meta info by key |
| DELETE | [/db/{database}/namespaces/{name}/metabykey/{key}](#deletedbdatabasenamespacesnamemetabykeykey) | Remove namespace's meta info for key |
| PUT | [/db/{database}/namespaces/{name}/metabykey](#putdbdatabasenamespacesnamemetabykey) | Put namespace's meta info with specified key and value |
| GET | [/db/{database}/namespaces/{name}/items](#getdbdatabasenamespacesnameitems) | Get documents from namespace |
| PUT | [/db/{database}/namespaces/{name}/items](#putdbdatabasenamespacesnameitems) | Update documents in namespace |
| POST | [/db/{database}/namespaces/{name}/items](#postdbdatabasenamespacesnameitems) | Insert documents to namespace |
| DELETE | [/db/{database}/namespaces/{name}/items](#deletedbdatabasenamespacesnameitems) | Delete documents from namespace |
| PATCH | [/db/{database}/namespaces/{name}/items](#patchdbdatabasenamespacesnameitems) | Upsert documents in namespace |
| GET | [/db/{database}/namespaces/{name}/indexes](#getdbdatabasenamespacesnameindexes) | List available indexes |
| PUT | [/db/{database}/namespaces/{name}/indexes](#putdbdatabasenamespacesnameindexes) | Update index in namespace |
| POST | [/db/{database}/namespaces/{name}/indexes](#postdbdatabasenamespacesnameindexes) | Add new index to namespace |
| DELETE | [/db/{database}/namespaces/{name}/indexes/{indexname}](#deletedbdatabasenamespacesnameindexesindexname) | Drop index from namespace |
| GET | [/db/{database}/namespaces/{name}/schema](#getdbdatabasenamespacesnameschema) | Get namespace schema |
| PUT | [/db/{database}/namespaces/{name}/schema](#putdbdatabasenamespacesnameschema) | Set namespace schema |
| GET | [/db/{database}/protobuf_schema](#getdbdatabaseprotobuf_schema) | Get protobuf communication parameters schema |
| GET | [/db/{database}/query](#getdbdatabasequery) | Query documents from namespace |
| PUT | [/db/{database}/query](#putdbdatabasequery) | Update documents in namespace |
| POST | [/db/{database}/query](#postdbdatabasequery) | Query documents from namespace |
| DELETE | [/db/{database}/query](#deletedbdatabasequery) | Delete documents from namespace |
| POST | [/db/{database}/namespaces/{name}/transactions/begin](#postdbdatabasenamespacesnametransactionsbegin) | Begin transaction to namespace |
| POST | [/db/{database}/transactions/{tx_id}/commit](#postdbdatabasetransactionstx_idcommit) | Commit transaction |
| POST | [/db/{database}/transactions/{tx_id}/rollback](#postdbdatabasetransactionstx_idrollback) | Rollback transaction |
| PUT | [/db/{database}/transactions/{tx_id}/items](#putdbdatabasetransactionstx_iditems) | Update documents in namespace via transaction |
| POST | [/db/{database}/transactions/{tx_id}/items](#postdbdatabasetransactionstx_iditems) | Insert documents to namespace via transaction |
| DELETE | [/db/{database}/transactions/{tx_id}/items](#deletedbdatabasetransactionstx_iditems) | Delete documents from namespace via transaction |
| PATCH | [/db/{database}/transactions/{tx_id}/items](#patchdbdatabasetransactionstx_iditems) | Upsert documents in namespace via transaction |
| GET | [/db/{database}/transactions/{tx_id}/query](#getdbdatabasetransactionstx_idquery) | Delete/update queries for transactions |
| DELETE | [/db/{database}/transactions/{tx_id}/query](#deletedbdatabasetransactionstx_idquery) | Delete documents from namespace (transactions) |
| GET | [/db/{database}/suggest](#getdbdatabasesuggest) | Suggest for autocompletion of SQL query |
| POST | [/db/{database}/sqlquery](#postdbdatabasesqlquery) | Query documents from namespace |
| GET | [/check](#getcheck) | Get system information |
| POST | [/allocator/drop_cache](#postallocatordrop_cache) | Try to release free memory back to the operating system for reuse by other applications. |
| GET | [/allocator/info](#getallocatorinfo) | Get memory usage information |
| GET | [/user/role](#getuserrole) | Get user role |
| GET | [/db/{database}/namespaces/%23activitystats/items](#getdbdatabasenamespaces23activitystatsitems) | Get activity stats information |
| GET | [/db/{database}/namespaces/%23clientsstats/items](#getdbdatabasenamespaces23clientsstatsitems) | Get client connection information |
| GET | [/db/{database}/namespaces/%23replicationstats/items](#getdbdatabasenamespaces23replicationstatsitems) | Get replication statistics |
| GET | [/db/{database}/namespaces/%23memstats/items](#getdbdatabasenamespaces23memstatsitems) | Get memory stats information |
| GET | [/db/{database}/namespaces/%23perfstats/items](#getdbdatabasenamespaces23perfstatsitems) | Get performance stats information |
| GET | [/db/{database}/namespaces/%23queriesperfstats/items](#getdbdatabasenamespaces23queriesperfstatsitems) | Get SELECT queries performance stats information |
| GET | [/db/{database}/namespaces/%23config/items](#getdbdatabasenamespaces23configitems) | Get system configs |
| PUT | [/db/{database}/namespaces/%23config/items](#putdbdatabasenamespaces23configitems) | Update system config |
| GET | [/db/default_configs](#getdbdefault_configs) | Get default system configs |

## Reference Table

| Name | Path | Description |
| --- | --- | --- |
| SysInfo | [SysInfo](#sysinfo) |  |
| ActivityStats | [ActivityStats](#activitystats) |  |
| ClientsStats | [ClientsStats](#clientsstats) |  |
| ReplicationSyncStat | [ReplicationSyncStat](#replicationsyncstat) |  |
| GlobalReplicationStats | [GlobalReplicationStats](#globalreplicationstats) |  |
| Databases | [Databases](#databases) |  |
| Database | [Database](#database) |  |
| Namespaces | [Namespaces](#namespaces) |  |
| Namespace | [Namespace](#namespace) |  |
| Index | [Index](#index) |  |
| JsonObjectDef | [JsonObjectDef](#jsonobjectdef) |  |
| SchemaDef | [SchemaDef](#schemadef) |  |
| UpdateField | [UpdateField](#updatefield) |  |
| Query | [Query](#query) |  |
| SubQuery | [SubQuery](#subquery) | Subquery object. It must contain either 'select_filters' for the single field, single aggregation or must be matched against 'is null'/'is not null conditions' |
| EqualPositionDef | [EqualPositionDef](#equalpositiondef) | Array fields to be searched with equal array indexes |
| FilterDef | [FilterDef](#filterdef) | If contains 'filters' then cannot contain 'cond', 'field' and 'value'. If not contains 'filters' then 'field' and 'cond' are required. |
| KnnSearchParamsDef | [KnnSearchParamsDef](#knnsearchparamsdef) | Parameters for knn search |
| SortDef | [SortDef](#sortdef) | Specifies results sorting order |
| JoinedDef | [JoinedDef](#joineddef) |  |
| OnDef | [OnDef](#ondef) |  |
| AggregationsDef | [AggregationsDef](#aggregationsdef) |  |
| SubQueryAggregationsDef | [SubQueryAggregationsDef](#subqueryaggregationsdef) |  |
| AggregationsSortDef | [AggregationsSortDef](#aggregationssortdef) | Specifies facet aggregations results sorting order |
| FtStopWordObject | [FtStopWordObject](#ftstopwordobject) |  |
| FloatVectorConfig | [FloatVectorConfig](#floatvectorconfig) | Float Vector Index configuration |
| FulltextConfig | [FulltextConfig](#fulltextconfig) | Fulltext Index configuration |
| FulltextFieldConfig | [FulltextFieldConfig](#fulltextfieldconfig) | Configuration for certain field if it differ from whole index configuration |
| FulltextSynonym | [FulltextSynonym](#fulltextsynonym) | Fulltext synonym definition |
| FulltextTermsBoost | [FulltextTermsBoost](#fulltexttermsboost) | Fulltext terms boost definition |
| MetaInfo | [MetaInfo](#metainfo) | Meta info to be set |
| MetaListResponse | [MetaListResponse](#metalistresponse) | List of meta info of the specified namespace |
| MetaByKeyResponse | [MetaByKeyResponse](#metabykeyresponse) | Meta info of the specified namespace |
| Items | [Items](#items) |  |
| SuggestItems | [SuggestItems](#suggestitems) |  |
| QueryItems | [QueryItems](#queryitems) |  |
| Indexes | [Indexes](#indexes) |  |
| ExplainDef | [ExplainDef](#explaindef) |  |
| SingleQueryExplainDef | [SingleQueryExplainDef](#singlequeryexplaindef) | Explanations of query execution |
| MergedQueryExplainDef | [MergedQueryExplainDef](#mergedqueryexplaindef) |  |
| AggregationResDef | [AggregationResDef](#aggregationresdef) |  |
| DistincOneItemDef | [DistincOneItemDef](#distinconeitemdef) |  |
| DistinctMultiItemDef | [DistinctMultiItemDef](#distinctmultiitemdef) | Distinct fields values |
| QueryColumnDef | [QueryColumnDef](#querycolumndef) | Query columns for table outputs |
| StatusResponse | [StatusResponse](#statusresponse) |  |
| ItemsUpdateResponse | [ItemsUpdateResponse](#itemsupdateresponse) |  |
| UpdateResponse | [UpdateResponse](#updateresponse) |  |
| DatabaseMemStats | [DatabaseMemStats](#databasememstats) |  |
| NamespaceMemStats | [NamespaceMemStats](#namespacememstats) |  |
| IndexMemStat | [IndexMemStat](#indexmemstat) |  |
| EmbedderStatus | [EmbedderStatus](#embedderstatus) |  |
| EmbedderLastError | [EmbedderLastError](#embedderlasterror) |  |
| EmbeddersCacheMemStat | [EmbeddersCacheMemStat](#embedderscachememstat) |  |
| JoinCacheMemStats | [JoinCacheMemStats](#joincachememstats) | Join cache stats. Stores results of selects to right table by ON condition |
| QueryCacheMemStats | [QueryCacheMemStats](#querycachememstats) by Where conditions |
| IndexCacheMemStats | [IndexCacheMemStats](#indexcachememstats) keys |
| CacheMemStats | [CacheMemStats](#cachememstats) |  |
| ReplicationStats | [ReplicationStats](#replicationstats) | State of namespace replication |
| DatabasePerfStats | [DatabasePerfStats](#databaseperfstats) |  |
| NamespacePerfStats | [NamespacePerfStats](#namespaceperfstats) |  |
| CommonPerfStats | [CommonPerfStats](#commonperfstats) |  |
| UpdatePerfStats | [UpdatePerfStats](#updateperfstats) | Performance statistics for update operations |
| SelectPerfStats | [SelectPerfStats](#selectperfstats) | Performance statistics for select operations |
| TransactionsPerfStats | [TransactionsPerfStats](#transactionsperfstats) | Performance statistics for transactions |
| QueriesPerfStats | [QueriesPerfStats](#queriesperfstats) |  |
| QueryPerfStats | [QueryPerfStats](#queryperfstats) | Performance statistics per each query |
| LRUCachePerfStats | [LRUCachePerfStats](#lrucacheperfstats) | Performance statistics for specific LRU-cache instance |
| EmbedderCachePerfStat | [EmbedderCachePerfStat](#embeddercacheperfstat) | Performance statistics for specific Embedder LRU-cache instance |
| EmbedderPerfStat | [EmbedderPerfStat](#embedderperfstat) |  |
| SystemConfigItems | [SystemConfigItems](#systemconfigitems) |  |
| SystemConfigItem | [SystemConfigItem](#systemconfigitem) |  |
| ProfilingConfig | [ProfilingConfig](#profilingconfig) |  |
| LongQueriesLogging | [LongQueriesLogging](#longquerieslogging) | Parameters for logging long queries and transactions |
| SelectLogging | [SelectLogging](#selectlogging) |  |
| UpdateDeleteLogging | [UpdateDeleteLogging](#updatedeletelogging) |  |
| TransactionLogging | [TransactionLogging](#transactionlogging) |  |
| NamespacesConfig | [NamespacesConfig](#namespacesconfig) |  |
| ReplicationConfig | [ReplicationConfig](#replicationconfig) |  |
| AsyncReplicationConfig | [AsyncReplicationConfig](#asyncreplicationconfig) |  |
| EmbeddersConfig | [EmbeddersConfig](#embeddersconfig) |  |
| ActionCommand | [ActionCommand](#actioncommand) |  |
| BeginTransactionResponse | [BeginTransactionResponse](#begintransactionresponse) |  |
| UserRoleResponse | [UserRoleResponse](#userroleresponse) |  |
| OK | [OK](#ok) | Successful operation |
| BadRequest | [BadRequest](#badrequest) | Invalid arguments supplied |
| RequestTimeout | [RequestTimeout](#requesttimeout) | Context timeout |
| Forbidden | [Forbidden](#forbidden) | Forbidden |
| NotFound | [NotFound](#notfound) | Entry not found |
| UnexpectedError | [UnexpectedError](#unexpectederror) | Unexpected internal error |

## Path Details

***

### List available databases

```
[GET]/db
```

- Operation id  
describeDatabases


This operation will return list of all available databases.  


#### Parameters(Query)

```typescript
sort_order?: enum[asc, desc]
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Total count of databases
  total_items?: integer
  // Name of database
  items?: string[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Create new database

```
[POST]/db
```

- Operation id  
createDatabase


This operation will create new database. If database already exists, then error will be returned.  


#### RequestBody

- */*

```typescript
{
  // Name of database
  name?: string
}
```

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Drop database

```
[DELETE]/db/{database}
```

- Operation id  
dropDatabase


This operation will remove entire database from memory and disk. All data, including namespaces, their documents and indexes will be erased. Can not be undone. USE WITH CAUTION.

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### List available namespaces

```
[GET]/db/{database}/namespaces
```

- Operation id  
describeNamespaces


This operation will list all available namespaces in specified database.  
If database does not exist, then error will be returned.  


#### Parameters(Query)

```typescript
sort_order?: enum[asc, desc]
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  items: {
    // Name of namespace
    name?: string
  }[]
  // Total count of namespaces
  total_items?: integer
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Create namespace

```
[POST]/db/{database}/namespaces
```

- Operation id  
openNamespace


This operation will create new namespace in specified database.  
If namespace already exists, then operation does nothing.  


#### RequestBody

- */*

```typescript
{
  // Name of namespace
  name?: string
  storage: {
    // If true, then documents will be stored to disc storage, else all data will be lost on server shutdown
    enabled?: boolean
  }
  indexes: {
    // Name of index, can contains letters, digits and underscores
    name: string //default: id
    json_paths?: string //default: id[]
    // Field data type
    field_type: enum[int, int64, double, string, bool, composite, point]
    // Index structure type
    index_type: enum[hash, tree, text, rtree, ttl, -] //default: hash
    // Specify, time to live for ttl index, in seconds
    expire_after?: integer
    // Specifies, that index is primary key. The update operations will checks, that PK field is unique. The namespace MUST have only 1 PK index
    is_pk?: boolean
    // Specifies, that index is array. Array indexes can work with array fields, or work with multiple fields
    is_array?: boolean
    // Reduces the index size. For hash and tree it will save ~8 bytes per unique key value. Useful for indexes with high selectivity, but for tree and hash indexes with low selectivity can seriously decrease update performance;
    is_dense?: boolean
    // Reduces the index size. Allows to save ~(`stored_type_size` * `namespace_items_count`) bytes, where `stored_type_size` is the size of the type stored in the index, and `namespace_items_count` is the number of items in the namespace. May reduce performance;
    is_no_column?: boolean
    // Value of index may not present in the document, and therefore, reduce data size but decreases speed operations on index
    is_sparse?: boolean
    // Algorithm to construct RTree index
    rtree_type?: enum[linear, quadratic, greene, rstar] //default: rstar
    // Use simple tag instead of actual index, which will notice rx about possible field name for strict policies
    is_simple_tag?: boolean
    // String collate mode
    collate_mode?: enum[none, ascii, utf8, numeric] //default: none
    // Sort order letters
    sort_order_letters?: string
    config?: FulltextConfig | FloatVectorConfig
  }[]
}
```

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get namespace description

```
[GET]/db/{database}/namespaces/{name}
```

- Operation id  
describeCurrNamespace


This operation will return specified namespace description, including options of namespace, and available indexes  


#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Name of namespace
  name?: string
  storage: {
    // If true, then documents will be stored to disc storage, else all data will be lost on server shutdown
    enabled?: boolean
  }
  indexes: {
    // Name of index, can contains letters, digits and underscores
    name: string //default: id
    json_paths?: string //default: id[]
    // Field data type
    field_type: enum[int, int64, double, string, bool, composite, point]
    // Index structure type
    index_type: enum[hash, tree, text, rtree, ttl, -] //default: hash
    // Specify, time to live for ttl index, in seconds
    expire_after?: integer
    // Specifies, that index is primary key. The update operations will checks, that PK field is unique. The namespace MUST have only 1 PK index
    is_pk?: boolean
    // Specifies, that index is array. Array indexes can work with array fields, or work with multiple fields
    is_array?: boolean
    // Reduces the index size. For hash and tree it will save ~8 bytes per unique key value. Useful for indexes with high selectivity, but for tree and hash indexes with low selectivity can seriously decrease update performance;
    is_dense?: boolean
    // Reduces the index size. Allows to save ~(`stored_type_size` * `namespace_items_count`) bytes, where `stored_type_size` is the size of the type stored in the index, and `namespace_items_count` is the number of items in the namespace. May reduce performance;
    is_no_column?: boolean
    // Value of index may not present in the document, and therefore, reduce data size but decreases speed operations on index
    is_sparse?: boolean
    // Algorithm to construct RTree index
    rtree_type?: enum[linear, quadratic, greene, rstar] //default: rstar
    // Use simple tag instead of actual index, which will notice rx about possible field name for strict policies
    is_simple_tag?: boolean
    // String collate mode
    collate_mode?: enum[none, ascii, utf8, numeric] //default: none
    // Sort order letters
    sort_order_letters?: string
    config?: FulltextConfig | FloatVectorConfig
  }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Drop namespace

```
[DELETE]/db/{database}/namespaces/{name}
```

- Operation id  
dropNamespace


This operation will delete namespace completely from memory and disk.  
All documents, indexes and metadata from namespace will be removed.  
Can not be undone. USE WITH CAUTION.  


#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Truncate namespace

```
[DELETE]/db/{database}/namespaces/{name}/truncate
```

- Operation id  
truncateNamespace


This operation will delete all items from namespace.  


#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Rename namespace

```
[GET]/db/{database}/namespaces/{name}/rename/{newname}
```

- Operation id  
renameNamespace


This operation will rename namespace.  


#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get list of namespace's meta info

```
[GET]/db/{database}/namespaces/{name}/metalist
```

- Operation id  
getNamespaceMetalist


This operation will return list of keys of all meta of specified namespace  


#### Parameters(Query)

```typescript
sort_order?: enum[asc, desc]
```

```typescript
with_values?: boolean
```

```typescript
offset?: integer
```

```typescript
limit?: integer
```

#### Responses

- 200 successful operation

`application/json`

```typescript
// List of meta info of the specified namespace
{
  // Total count of meta info in the namespace
  total_items: integer
  meta: {
    key: string
    // Optional: Provided if 'with_values' = true
    value?: string
  }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get namespace's meta info by key

```
[GET]/db/{database}/namespaces/{name}/metabykey/{key}
```

- Operation id  
getNamespaceMetaByKey


This operation will return value of namespace's meta with specified key  


#### Responses

- 200 Successful operation

`application/json`

```typescript
// Meta info of the specified namespace
{
  key: string
  value: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Remove namespace's meta info for key

```
[DELETE]/db/{database}/namespaces/{name}/metabykey/{key}
```

- Operation id  
deleteNamespaceMetaByKey


This operation will remove meta with specified key from namespace  


#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Put namespace's meta info with specified key and value

```
[PUT]/db/{database}/namespaces/{name}/metabykey
```

- Operation id  
putNamespaceMetaByKey


This operation will set namespace's meta with specified key and value  


#### RequestBody

- */*

```typescript
// Meta info to be set
{
  key: string
  value: string
}
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Count of updated items
  updated?: integer
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get documents from namespace

```
[GET]/db/{database}/namespaces/{name}/items
```

- Operation id  
getItems


This operation will select documents from namespace with specified filters, and sort them by specified sort order. Paging with limit and offset are supported.  


#### Parameters(Query)

```typescript
limit?: integer
```

```typescript
offset?: integer
```

```typescript
sort_field?: string
```

```typescript
sort_order?: enum[asc, desc]
```

```typescript
filter?: string
```

```typescript
fields?: string
```

```typescript
format?: enum[json, msgpack, protobuf, csv-file]
```

```typescript
sharding?: enum[true, false]
```

```typescript
with_shard_ids?: enum[true, false]
```

```typescript
with_vectors?: enum[true, false]
```

```typescript
with_columns?: enum[true, false]
```

```typescript
width?: integer
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Total count of documents, matched specified filters
  total_items?: integer
  items: {
  }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Update documents in namespace

```
[PUT]/db/{database}/namespaces/{name}/items
```

- Operation id  
putItems


This operation will UPDATE documents in namespace, by their primary keys.  
Each document should be in request body as separate JSON object, e.g.  
```  
{"id":100, "name": "Pet"}  
{"id":101, "name": "Dog"}  
...  
```  


#### Parameters(Query)

```typescript
precepts?: string[]
```

```typescript
format?: enum[json, msgpack, protobuf]
```

#### RequestBody

- */*

```typescript
{
}
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Count of updated items
  updated?: integer
  items: {
  }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Insert documents to namespace

```
[POST]/db/{database}/namespaces/{name}/items
```

- Operation id  
postItems


This operation will INSERT documents to namespace, by their primary keys.  
Each document should be in request body as separate JSON object, e.g.  
```  
{"id":100, "name": "Pet"}  
{"id":101, "name": "Dog"}  
...  
```  


#### Parameters(Query)

```typescript
precepts?: string[]
```

```typescript
format?: enum[json, msgpack, protobuf]
```

#### RequestBody

- */*

```typescript
{
}
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Count of updated items
  updated?: integer
  items: {
  }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Delete documents from namespace

```
[DELETE]/db/{database}/namespaces/{name}/items
```

- Operation id  
deleteItems


This operation will DELETE documents from namespace, by their primary keys.  
Each document should be in request body as separate JSON object, e.g.  
```  
{"id":100}  
{"id":101}  
...  
```  


#### Parameters(Query)

```typescript
precepts?: string[]
```

#### RequestBody

- */*

```typescript
{
}
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Count of updated items
  updated?: integer
  items: {
  }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Upsert documents in namespace

```
[PATCH]/db/{database}/namespaces/{name}/items
```

- Operation id  
patchItems


This operation will UPSERT documents in namespace, by their primary keys.  
Each document should be in request body as separate JSON object, e.g.  
```  
{"id":100, "name": "Pet"}  
{"id":101, "name": "Dog"}  
...  
```  


#### Parameters(Query)

```typescript
precepts?: string[]
```

```typescript
format?: enum[json, msgpack, protobuf]
```

#### RequestBody

- */*

```typescript
{
}
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Count of updated items
  updated?: integer
  items: {
  }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### List available indexes

```
[GET]/db/{database}/namespaces/{name}/indexes
```

- Operation id  
describeIndexes


This operation will return list of available indexes, from specified database and namespace.  


#### Responses

- 200 successful operation

`application/json`

```typescript
{
  items: {
    // Name of index, can contains letters, digits and underscores
    name: string //default: id
    json_paths?: string //default: id[]
    // Field data type
    field_type: enum[int, int64, double, string, bool, composite, point]
    // Index structure type
    index_type: enum[hash, tree, text, rtree, ttl, -] //default: hash
    // Specify, time to live for ttl index, in seconds
    expire_after?: integer
    // Specifies, that index is primary key. The update operations will checks, that PK field is unique. The namespace MUST have only 1 PK index
    is_pk?: boolean
    // Specifies, that index is array. Array indexes can work with array fields, or work with multiple fields
    is_array?: boolean
    // Reduces the index size. For hash and tree it will save ~8 bytes per unique key value. Useful for indexes with high selectivity, but for tree and hash indexes with low selectivity can seriously decrease update performance;
    is_dense?: boolean
    // Reduces the index size. Allows to save ~(`stored_type_size` * `namespace_items_count`) bytes, where `stored_type_size` is the size of the type stored in the index, and `namespace_items_count` is the number of items in the namespace. May reduce performance;
    is_no_column?: boolean
    // Value of index may not present in the document, and therefore, reduce data size but decreases speed operations on index
    is_sparse?: boolean
    // Algorithm to construct RTree index
    rtree_type?: enum[linear, quadratic, greene, rstar] //default: rstar
    // Use simple tag instead of actual index, which will notice rx about possible field name for strict policies
    is_simple_tag?: boolean
    // String collate mode
    collate_mode?: enum[none, ascii, utf8, numeric] //default: none
    // Sort order letters
    sort_order_letters?: string
    config?: FulltextConfig | FloatVectorConfig
  }[]
  // Total count of indexes
  total_items?: integer
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Update index in namespace

```
[PUT]/db/{database}/namespaces/{name}/indexes
```

- Operation id  
putIndex


This operation will update index parameters. E.g. type of field or type of index.  
Operation synchronous, so it can take long time, if namespace contains bunch of documents  


#### RequestBody

- */*

```typescript
{
  // Name of index, can contains letters, digits and underscores
  name: string //default: id
  json_paths?: string //default: id[]
  // Field data type
  field_type: enum[int, int64, double, string, bool, composite, point]
  // Index structure type
  index_type: enum[hash, tree, text, rtree, ttl, -] //default: hash
  // Specify, time to live for ttl index, in seconds
  expire_after?: integer
  // Specifies, that index is primary key. The update operations will checks, that PK field is unique. The namespace MUST have only 1 PK index
  is_pk?: boolean
  // Specifies, that index is array. Array indexes can work with array fields, or work with multiple fields
  is_array?: boolean
  // Reduces the index size. For hash and tree it will save ~8 bytes per unique key value. Useful for indexes with high selectivity, but for tree and hash indexes with low selectivity can seriously decrease update performance;
  is_dense?: boolean
  // Reduces the index size. Allows to save ~(`stored_type_size` * `namespace_items_count`) bytes, where `stored_type_size` is the size of the type stored in the index, and `namespace_items_count` is the number of items in the namespace. May reduce performance;
  is_no_column?: boolean
  // Value of index may not present in the document, and therefore, reduce data size but decreases speed operations on index
  is_sparse?: boolean
  // Algorithm to construct RTree index
  rtree_type?: enum[linear, quadratic, greene, rstar] //default: rstar
  // Use simple tag instead of actual index, which will notice rx about possible field name for strict policies
  is_simple_tag?: boolean
  // String collate mode
  collate_mode?: enum[none, ascii, utf8, numeric] //default: none
  // Sort order letters
  sort_order_letters?: string
  config?: FulltextConfig | FloatVectorConfig
}
```

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Add new index to namespace

```
[POST]/db/{database}/namespaces/{name}/indexes
```

- Operation id  
postIndex


This operation will create new index. If index already exists with different parameters, then error will be returned.  
Operation synchronous, so it can take long time, if namespace contains bunch of documents.  


#### RequestBody

- */*

```typescript
{
  // Name of index, can contains letters, digits and underscores
  name: string //default: id
  json_paths?: string //default: id[]
  // Field data type
  field_type: enum[int, int64, double, string, bool, composite, point]
  // Index structure type
  index_type: enum[hash, tree, text, rtree, ttl, -] //default: hash
  // Specify, time to live for ttl index, in seconds
  expire_after?: integer
  // Specifies, that index is primary key. The update operations will checks, that PK field is unique. The namespace MUST have only 1 PK index
  is_pk?: boolean
  // Specifies, that index is array. Array indexes can work with array fields, or work with multiple fields
  is_array?: boolean
  // Reduces the index size. For hash and tree it will save ~8 bytes per unique key value. Useful for indexes with high selectivity, but for tree and hash indexes with low selectivity can seriously decrease update performance;
  is_dense?: boolean
  // Reduces the index size. Allows to save ~(`stored_type_size` * `namespace_items_count`) bytes, where `stored_type_size` is the size of the type stored in the index, and `namespace_items_count` is the number of items in the namespace. May reduce performance;
  is_no_column?: boolean
  // Value of index may not present in the document, and therefore, reduce data size but decreases speed operations on index
  is_sparse?: boolean
  // Algorithm to construct RTree index
  rtree_type?: enum[linear, quadratic, greene, rstar] //default: rstar
  // Use simple tag instead of actual index, which will notice rx about possible field name for strict policies
  is_simple_tag?: boolean
  // String collate mode
  collate_mode?: enum[none, ascii, utf8, numeric] //default: none
  // Sort order letters
  sort_order_letters?: string
  config?: FulltextConfig | FloatVectorConfig
}
```

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Drop index from namespace

```
[DELETE]/db/{database}/namespaces/{name}/indexes/{indexname}
```

- Operation id  
deleteIndex


This operation will remove index from namespace. No data will be erased.  
Operation synchronous, so it can take long time, if namespace contains bunch of documents.  


#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get namespace schema

```
[GET]/db/{database}/namespaces/{name}/schema
```

- Operation id  
getSchema


This operation will return current schema from specified database and namespace

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  required?: string[]
  properties: {
    field1:JsonObjectDef
    field2:JsonObjectDef
  }
  items:JsonObjectDef
  // Allow additional fields in this schema level. Allowed for objects only
  additionalProperties?: boolean
  // Entity type
  type?: enum[object, string, number, array]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Set namespace schema

```
[PUT]/db/{database}/namespaces/{name}/schema
```

- Operation id  
putSchema


This operation will set namespace schema (information about available fields and field types)  


#### RequestBody

- */*

```typescript
{
  required?: string[]
  properties: {
    field1:JsonObjectDef
    field2:JsonObjectDef
  }
  items:JsonObjectDef
  // Allow additional fields in this schema level. Allowed for objects only
  additionalProperties?: boolean
  // Entity type
  type?: enum[object, string, number, array]
}
```

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get protobuf communication parameters schema

```
[GET]/db/{database}/protobuf_schema
```

- Operation id  
getNsParamsSchema


This operation allows to get client/server communication parameters as google protobuf schema (content of .proto file)  


#### Parameters(Query)

```typescript
ns?: string[]
```

#### Responses

- 200 successful operation

- 400 Invalid arguments supplied

`text/plain`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`text/plain`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`text/plain`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`text/plain`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`text/plain`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Query documents from namespace

```
[GET]/db/{database}/query
```

- Operation id  
getQuery


This operation queries documents from namespace by SQL query. Query can be preceded by `EXPLAIN` statement, then query execution plan will be returned with query results. Two level paging is supported. At first, applied normal SQL `LIMIT` and `OFFSET`, then `limit` and `offset` from http request.

#### Parameters(Query)

```typescript
q: string
```

```typescript
limit?: integer
```

```typescript
offset?: integer
```

```typescript
with_columns?: boolean
```

```typescript
width?: integer
```

```typescript
format?: enum[json, msgpack, protobuf, csv-file]
```

```typescript
sharding?: enum[true, false]
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  items: {
  }[]
  namespaces?: string[]
  // Enables to client cache returned items. If false, then returned items has been modified  by reindexer, e.g. by select filter, or by functions, and can't be cached
  cache_enabled?: boolean
  // Total count of documents, matched query
  query_total_items?: integer
  aggregations: {
    fields?: string[]
    // Aggregation function
    type: enum[SUM, AVG, MIN, MAX, FACET, DISTINCT]
    // Value, calculated by aggregator
    value?: number
    facets: {
      values?: string[]
      // Count of elements these fields values
      count: integer
    }[]
    distincts?: DistincOneItemDef | DistinctMultiItemDef[]
  }[]
  equal_position?: string[]
  // Query columns for table outputs
  columns: {
    // Column name
    name?: string
    // Column width in percents of total width
    width_percents?: number
    // Column width in chars
    width_chars?: number
    // Maximum count of chars in column
    max_chars?: number
  }[]
  explain?: SingleQueryExplainDef | MergedQueryExplainDef
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Update documents in namespace

```
[PUT]/db/{database}/query
```

- Operation id  
updateQuery


This operation updates documents in namespace by DSL query.  


#### RequestBody

- */*

```typescript
{
  // Namespace name
  namespace: string
  // Type of query
  type?: enum[select, update, delete, truncate]
  // Maximum count of returned items
  limit?: integer
  // Offset of first returned item
  offset?: integer
  // Ask query to calculate total documents, match condition
  req_total?: enum[disabled, enabled, cached] //default: disabled
  // If contains 'filters' then cannot contain 'cond', 'field' and 'value'. If not contains 'filters' then 'field' and 'cond' are required.
  filters: {
    // Expression on the left side of the condition. It may be a field or function.
    left_expression: {
      // Explicit expression type
      type?: enum[field, expression]
      // Field name or function (as expression).
      value?: string
    }
    // Expression on the right side of the condition. It may be a field, function, or value.
    right_expression: {
      // Explicit expression type
      type?: enum[field, expression, values]
      // Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN. Function value as expression. Field name as a string.
      value: {
      }
    }
    // Condition operator
    cond?: enum[EQ, GT, GE, LE, LT, SET, ALLSET, EMPTY, RANGE, LIKE, DWITHIN, KNN]
    // Logic operator
    op?: enum[AND, OR, NOT]
    filters:FilterDef[]
    join_query: {
      // Namespace name
      namespace: string
      // Join type
      type: enum[LEFT, INNER, ORINNER]
      filters:FilterDef[]
      // Specifies results sorting order
      sort: {
        // Field or index name for sorting
        field: string
        values: {
        }[]
        // Descent or ascent sorting direction
        desc?: boolean
      }[]
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      on: {
        // Field from left namespace (main query namespace)
        left_field: string
        // Field from right namespace (joined query namespace)
        right_field: string
        // Condition operator
        cond: enum[EQ, GT, GE, LE, LT, SET]
        // Logic operator
        op?: enum[AND, OR, NOT]
      }[]
      select_filter?: string[]
    }
    // Subquery object. It must contain either 'select_filters' for the single field, single aggregation or must be matched against 'is null'/'is not null conditions'
    subquery: {
      // Namespace name
      namespace: string
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      // Ask query to calculate total documents, match condition
      req_total?: enum[disabled, enabled, cached] //default: disabled
      filters:FilterDef[]
      sort:SortDef[]
      select_filter?: string //default: id[]
      aggregations: {
        fields?: string[]
        // Aggregation function
        type: enum[SUM, AVG, MIN, MAX]
      }[]
    }
    // Array fields to be searched with equal array indexes
    equal_positions: {
      positions?: string[]
    }[]
    // Parameters for knn search
    params: {
      // Maximum count of returned vectors in KNN queries
      k?: integer
      // Raduis for filtering vectors by metric
      radius?: number
      // Applicable for HNSW index only. The size of the dynamic list for the nearest neighbors used during a query. Ef must be >= K. Default value = K
      ef?: integer
      // Applicable for IVF index only. The number of Voronoi cells to search during a query
      nprobe?: integer
    }
    // DEPRECATED. Use left_expression instead. Field json path or index name for filter
    field?: string
    // DEPRECATED. Use right_expression instead. Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN
    value: {
    }
    // DEPRECATED. Use left_expression instead. First field json path or index name for filter by two fields
    first_field?: string
    // DEPRECATED. Use right_expression instead. Second field json path or index name for filter by two fields
    second_field?: string
  }[]
  sort:SortDef[]
  merge_queries: {
    // Namespace name
    namespace: string
    // Type of query
    type?: enum[select, update, delete, truncate]
    // Maximum count of returned items
    limit?: integer
    // Offset of first returned item
    offset?: integer
    // Ask query to calculate total documents, match condition
    req_total?: enum[disabled, enabled, cached] //default: disabled
    filters:FilterDef[]
    sort:SortDef[]
    merge_queries:Query[]
    select_filter?: string //default: id[]
    select_functions?: string[]
    drop_fields?: string[]
    update_fields: {
      // field name
      name: string
      // update entry type
      type?: enum[object, expression, value]
      // is updated value an array
      is_array?: boolean
      values: {
      }[]
    }[]
    aggregations: {
      fields?: string[]
      // Aggregation function
      type: enum[SUM, AVG, MIN, MAX, FACET, DISTINCT]
      // Specifies facet aggregations results sorting order
      sort: {
        // Field or index name for sorting
        field: string
        // Descent or ascent sorting direction
        desc?: boolean
      }[]
      // Number of rows to get from result set. Allowed only for FACET
      limit?: integer
      // Index of the first row to get from result set. Allowed only for FACET
      offset?: integer
    }[]
    // Add query execution explain information
    explain?: boolean
    // Output fulltext or KNN rank in QueryResult. Allowed only with fulltext or KNN queries query
    select_with_rank?: boolean
    // Strict mode for query. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
    strict_mode?: enum[none, names, indexes] //default: names
  }[]
  select_filter?: string //default: id[]
  select_functions?: string[]
  drop_fields?: string[]
  update_fields:UpdateField[]
  aggregations:AggregationsDef[]
  // Add query execution explain information
  explain?: boolean
  // Output fulltext or KNN rank in QueryResult. Allowed only with fulltext or KNN queries query
  select_with_rank?: boolean
  // Strict mode for query. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
  strict_mode?: enum[none, names, indexes] //default: names
}
```

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Query documents from namespace

```
[POST]/db/{database}/query
```

- Operation id  
postQuery


This operation queries documents from namespace by DSL query.  


#### Parameters(Query)

```typescript
with_columns?: boolean
```

```typescript
width?: integer
```

```typescript
format?: enum[json, msgpack, protobuf, csv-file]
```

#### RequestBody

- */*

```typescript
{
  // Namespace name
  namespace: string
  // Type of query
  type?: enum[select, update, delete, truncate]
  // Maximum count of returned items
  limit?: integer
  // Offset of first returned item
  offset?: integer
  // Ask query to calculate total documents, match condition
  req_total?: enum[disabled, enabled, cached] //default: disabled
  // If contains 'filters' then cannot contain 'cond', 'field' and 'value'. If not contains 'filters' then 'field' and 'cond' are required.
  filters: {
    // Expression on the left side of the condition. It may be a field or function.
    left_expression: {
      // Explicit expression type
      type?: enum[field, expression]
      // Field name or function (as expression).
      value?: string
    }
    // Expression on the right side of the condition. It may be a field, function, or value.
    right_expression: {
      // Explicit expression type
      type?: enum[field, expression, values]
      // Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN. Function value as expression. Field name as a string.
      value: {
      }
    }
    // Condition operator
    cond?: enum[EQ, GT, GE, LE, LT, SET, ALLSET, EMPTY, RANGE, LIKE, DWITHIN, KNN]
    // Logic operator
    op?: enum[AND, OR, NOT]
    filters:FilterDef[]
    join_query: {
      // Namespace name
      namespace: string
      // Join type
      type: enum[LEFT, INNER, ORINNER]
      filters:FilterDef[]
      // Specifies results sorting order
      sort: {
        // Field or index name for sorting
        field: string
        values: {
        }[]
        // Descent or ascent sorting direction
        desc?: boolean
      }[]
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      on: {
        // Field from left namespace (main query namespace)
        left_field: string
        // Field from right namespace (joined query namespace)
        right_field: string
        // Condition operator
        cond: enum[EQ, GT, GE, LE, LT, SET]
        // Logic operator
        op?: enum[AND, OR, NOT]
      }[]
      select_filter?: string[]
    }
    // Subquery object. It must contain either 'select_filters' for the single field, single aggregation or must be matched against 'is null'/'is not null conditions'
    subquery: {
      // Namespace name
      namespace: string
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      // Ask query to calculate total documents, match condition
      req_total?: enum[disabled, enabled, cached] //default: disabled
      filters:FilterDef[]
      sort:SortDef[]
      select_filter?: string //default: id[]
      aggregations: {
        fields?: string[]
        // Aggregation function
        type: enum[SUM, AVG, MIN, MAX]
      }[]
    }
    // Array fields to be searched with equal array indexes
    equal_positions: {
      positions?: string[]
    }[]
    // Parameters for knn search
    params: {
      // Maximum count of returned vectors in KNN queries
      k?: integer
      // Raduis for filtering vectors by metric
      radius?: number
      // Applicable for HNSW index only. The size of the dynamic list for the nearest neighbors used during a query. Ef must be >= K. Default value = K
      ef?: integer
      // Applicable for IVF index only. The number of Voronoi cells to search during a query
      nprobe?: integer
    }
    // DEPRECATED. Use left_expression instead. Field json path or index name for filter
    field?: string
    // DEPRECATED. Use right_expression instead. Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN
    value: {
    }
    // DEPRECATED. Use left_expression instead. First field json path or index name for filter by two fields
    first_field?: string
    // DEPRECATED. Use right_expression instead. Second field json path or index name for filter by two fields
    second_field?: string
  }[]
  sort:SortDef[]
  merge_queries: {
    // Namespace name
    namespace: string
    // Type of query
    type?: enum[select, update, delete, truncate]
    // Maximum count of returned items
    limit?: integer
    // Offset of first returned item
    offset?: integer
    // Ask query to calculate total documents, match condition
    req_total?: enum[disabled, enabled, cached] //default: disabled
    filters:FilterDef[]
    sort:SortDef[]
    merge_queries:Query[]
    select_filter?: string //default: id[]
    select_functions?: string[]
    drop_fields?: string[]
    update_fields: {
      // field name
      name: string
      // update entry type
      type?: enum[object, expression, value]
      // is updated value an array
      is_array?: boolean
      values: {
      }[]
    }[]
    aggregations: {
      fields?: string[]
      // Aggregation function
      type: enum[SUM, AVG, MIN, MAX, FACET, DISTINCT]
      // Specifies facet aggregations results sorting order
      sort: {
        // Field or index name for sorting
        field: string
        // Descent or ascent sorting direction
        desc?: boolean
      }[]
      // Number of rows to get from result set. Allowed only for FACET
      limit?: integer
      // Index of the first row to get from result set. Allowed only for FACET
      offset?: integer
    }[]
    // Add query execution explain information
    explain?: boolean
    // Output fulltext or KNN rank in QueryResult. Allowed only with fulltext or KNN queries query
    select_with_rank?: boolean
    // Strict mode for query. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
    strict_mode?: enum[none, names, indexes] //default: names
  }[]
  select_filter?: string //default: id[]
  select_functions?: string[]
  drop_fields?: string[]
  update_fields:UpdateField[]
  aggregations:AggregationsDef[]
  // Add query execution explain information
  explain?: boolean
  // Output fulltext or KNN rank in QueryResult. Allowed only with fulltext or KNN queries query
  select_with_rank?: boolean
  // Strict mode for query. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
  strict_mode?: enum[none, names, indexes] //default: names
}
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  items: {
  }[]
  namespaces?: string[]
  // Enables to client cache returned items. If false, then returned items has been modified  by reindexer, e.g. by select filter, or by functions, and can't be cached
  cache_enabled?: boolean
  // Total count of documents, matched query
  query_total_items?: integer
  aggregations: {
    fields?: string[]
    // Aggregation function
    type: enum[SUM, AVG, MIN, MAX, FACET, DISTINCT]
    // Value, calculated by aggregator
    value?: number
    facets: {
      values?: string[]
      // Count of elements these fields values
      count: integer
    }[]
    distincts?: DistincOneItemDef | DistinctMultiItemDef[]
  }[]
  equal_position?: string[]
  // Query columns for table outputs
  columns: {
    // Column name
    name?: string
    // Column width in percents of total width
    width_percents?: number
    // Column width in chars
    width_chars?: number
    // Maximum count of chars in column
    max_chars?: number
  }[]
  explain?: SingleQueryExplainDef | MergedQueryExplainDef
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Delete documents from namespace

```
[DELETE]/db/{database}/query
```

- Operation id  
deleteQuery


This operation removes documents from namespace by DSL query.  


#### RequestBody

- */*

```typescript
{
  // Namespace name
  namespace: string
  // Type of query
  type?: enum[select, update, delete, truncate]
  // Maximum count of returned items
  limit?: integer
  // Offset of first returned item
  offset?: integer
  // Ask query to calculate total documents, match condition
  req_total?: enum[disabled, enabled, cached] //default: disabled
  // If contains 'filters' then cannot contain 'cond', 'field' and 'value'. If not contains 'filters' then 'field' and 'cond' are required.
  filters: {
    // Expression on the left side of the condition. It may be a field or function.
    left_expression: {
      // Explicit expression type
      type?: enum[field, expression]
      // Field name or function (as expression).
      value?: string
    }
    // Expression on the right side of the condition. It may be a field, function, or value.
    right_expression: {
      // Explicit expression type
      type?: enum[field, expression, values]
      // Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN. Function value as expression. Field name as a string.
      value: {
      }
    }
    // Condition operator
    cond?: enum[EQ, GT, GE, LE, LT, SET, ALLSET, EMPTY, RANGE, LIKE, DWITHIN, KNN]
    // Logic operator
    op?: enum[AND, OR, NOT]
    filters:FilterDef[]
    join_query: {
      // Namespace name
      namespace: string
      // Join type
      type: enum[LEFT, INNER, ORINNER]
      filters:FilterDef[]
      // Specifies results sorting order
      sort: {
        // Field or index name for sorting
        field: string
        values: {
        }[]
        // Descent or ascent sorting direction
        desc?: boolean
      }[]
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      on: {
        // Field from left namespace (main query namespace)
        left_field: string
        // Field from right namespace (joined query namespace)
        right_field: string
        // Condition operator
        cond: enum[EQ, GT, GE, LE, LT, SET]
        // Logic operator
        op?: enum[AND, OR, NOT]
      }[]
      select_filter?: string[]
    }
    // Subquery object. It must contain either 'select_filters' for the single field, single aggregation or must be matched against 'is null'/'is not null conditions'
    subquery: {
      // Namespace name
      namespace: string
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      // Ask query to calculate total documents, match condition
      req_total?: enum[disabled, enabled, cached] //default: disabled
      filters:FilterDef[]
      sort:SortDef[]
      select_filter?: string //default: id[]
      aggregations: {
        fields?: string[]
        // Aggregation function
        type: enum[SUM, AVG, MIN, MAX]
      }[]
    }
    // Array fields to be searched with equal array indexes
    equal_positions: {
      positions?: string[]
    }[]
    // Parameters for knn search
    params: {
      // Maximum count of returned vectors in KNN queries
      k?: integer
      // Raduis for filtering vectors by metric
      radius?: number
      // Applicable for HNSW index only. The size of the dynamic list for the nearest neighbors used during a query. Ef must be >= K. Default value = K
      ef?: integer
      // Applicable for IVF index only. The number of Voronoi cells to search during a query
      nprobe?: integer
    }
    // DEPRECATED. Use left_expression instead. Field json path or index name for filter
    field?: string
    // DEPRECATED. Use right_expression instead. Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN
    value: {
    }
    // DEPRECATED. Use left_expression instead. First field json path or index name for filter by two fields
    first_field?: string
    // DEPRECATED. Use right_expression instead. Second field json path or index name for filter by two fields
    second_field?: string
  }[]
  sort:SortDef[]
  merge_queries: {
    // Namespace name
    namespace: string
    // Type of query
    type?: enum[select, update, delete, truncate]
    // Maximum count of returned items
    limit?: integer
    // Offset of first returned item
    offset?: integer
    // Ask query to calculate total documents, match condition
    req_total?: enum[disabled, enabled, cached] //default: disabled
    filters:FilterDef[]
    sort:SortDef[]
    merge_queries:Query[]
    select_filter?: string //default: id[]
    select_functions?: string[]
    drop_fields?: string[]
    update_fields: {
      // field name
      name: string
      // update entry type
      type?: enum[object, expression, value]
      // is updated value an array
      is_array?: boolean
      values: {
      }[]
    }[]
    aggregations: {
      fields?: string[]
      // Aggregation function
      type: enum[SUM, AVG, MIN, MAX, FACET, DISTINCT]
      // Specifies facet aggregations results sorting order
      sort: {
        // Field or index name for sorting
        field: string
        // Descent or ascent sorting direction
        desc?: boolean
      }[]
      // Number of rows to get from result set. Allowed only for FACET
      limit?: integer
      // Index of the first row to get from result set. Allowed only for FACET
      offset?: integer
    }[]
    // Add query execution explain information
    explain?: boolean
    // Output fulltext or KNN rank in QueryResult. Allowed only with fulltext or KNN queries query
    select_with_rank?: boolean
    // Strict mode for query. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
    strict_mode?: enum[none, names, indexes] //default: names
  }[]
  select_filter?: string //default: id[]
  select_functions?: string[]
  drop_fields?: string[]
  update_fields:UpdateField[]
  aggregations:AggregationsDef[]
  // Add query execution explain information
  explain?: boolean
  // Output fulltext or KNN rank in QueryResult. Allowed only with fulltext or KNN queries query
  select_with_rank?: boolean
  // Strict mode for query. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
  strict_mode?: enum[none, names, indexes] //default: names
}
```

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Begin transaction to namespace

```
[POST]/db/{database}/namespaces/{name}/transactions/begin
```

- Operation id  
beginTx

#### Parameters(Query)

```typescript
format?: enum[json, msgpack, protobuf]
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Unique transaction id
  tx_id?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Commit transaction

```
[POST]/db/{database}/transactions/{tx_id}/commit
```

- Operation id  
commitTx

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Rollback transaction

```
[POST]/db/{database}/transactions/{tx_id}/rollback
```

- Operation id  
rollbackTx

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Update documents in namespace via transaction

```
[PUT]/db/{database}/transactions/{tx_id}/items
```

- Operation id  
putItemsTx


This will add UPDATE operation into transaction.  
It UPDATEs documents in namespace, by their primary keys.  
Each document should be in request body as separate JSON object, e.g.  
```  
{"id":100, "name": "Pet"}  
{"id":101, "name": "Dog"}  
...  
```  


#### Parameters(Query)

```typescript
precepts?: string[]
```

```typescript
format?: enum[json, msgpack, protobuf]
```

#### RequestBody

- */*

```typescript
{
}
```

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Insert documents to namespace via transaction

```
[POST]/db/{database}/transactions/{tx_id}/items
```

- Operation id  
postItemsTx


This will add INSERT operation into transaction.  
It INSERTs documents to namespace, by their primary keys.  
Each document should be in request body as separate JSON object, e.g.  
```  
{"id":100, "name": "Pet"}  
{"id":101, "name": "Dog"}  
...  
```  


#### Parameters(Query)

```typescript
precepts?: string[]
```

```typescript
format?: enum[json, msgpack, protobuf]
```

#### RequestBody

- */*

```typescript
{
}
```

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Delete documents from namespace via transaction

```
[DELETE]/db/{database}/transactions/{tx_id}/items
```

- Operation id  
deleteItemsTx


This will add DELETE operation into transaction.  
It DELETEs documents from namespace, by their primary keys.  
Each document should be in request body as separate JSON object, e.g.  
```  
{"id":100}  
{"id":101}  
...  
```  


#### Parameters(Query)

```typescript
precepts?: string[]
```

#### RequestBody

- */*

```typescript
{
}
```

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Upsert documents in namespace via transaction

```
[PATCH]/db/{database}/transactions/{tx_id}/items
```

- Operation id  
patchItemsTx


This will add UPSERT operation into transaction.  
It UPDATEs documents in namespace, by their primary keys.  
Each document should be in request body as separate JSON object, e.g.  
```  
{"id":100, "name": "Pet"}  
{"id":101, "name": "Dog"}  
...  
```  


#### Parameters(Query)

```typescript
precepts?: string[]
```

```typescript
format?: enum[json, msgpack, protobuf]
```

#### RequestBody

- */*

```typescript
{
}
```

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Delete/update queries for transactions

```
[GET]/db/{database}/transactions/{tx_id}/query
```

- Operation id  
getQueryTx


This will add DELETE/UPDATE SQL query into transaction.  
This query UPDATEs/DELETEs documents from namespace  


#### Parameters(Query)

```typescript
q: string
```

```typescript
width?: integer
```

```typescript
format?: enum[json, msgpack, protobuf]
```

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Delete documents from namespace (transactions)

```
[DELETE]/db/{database}/transactions/{tx_id}/query
```

- Operation id  
deleteQueryTx


This will add DELETE query into transaction.  
DELETE query removes documents from namespace by DSL query.  


#### Parameters(Query)

```typescript
tx_id?: string
```

#### RequestBody

- */*

```typescript
{
  // Namespace name
  namespace: string
  // Type of query
  type?: enum[select, update, delete, truncate]
  // Maximum count of returned items
  limit?: integer
  // Offset of first returned item
  offset?: integer
  // Ask query to calculate total documents, match condition
  req_total?: enum[disabled, enabled, cached] //default: disabled
  // If contains 'filters' then cannot contain 'cond', 'field' and 'value'. If not contains 'filters' then 'field' and 'cond' are required.
  filters: {
    // Expression on the left side of the condition. It may be a field or function.
    left_expression: {
      // Explicit expression type
      type?: enum[field, expression]
      // Field name or function (as expression).
      value?: string
    }
    // Expression on the right side of the condition. It may be a field, function, or value.
    right_expression: {
      // Explicit expression type
      type?: enum[field, expression, values]
      // Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN. Function value as expression. Field name as a string.
      value: {
      }
    }
    // Condition operator
    cond?: enum[EQ, GT, GE, LE, LT, SET, ALLSET, EMPTY, RANGE, LIKE, DWITHIN, KNN]
    // Logic operator
    op?: enum[AND, OR, NOT]
    filters:FilterDef[]
    join_query: {
      // Namespace name
      namespace: string
      // Join type
      type: enum[LEFT, INNER, ORINNER]
      filters:FilterDef[]
      // Specifies results sorting order
      sort: {
        // Field or index name for sorting
        field: string
        values: {
        }[]
        // Descent or ascent sorting direction
        desc?: boolean
      }[]
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      on: {
        // Field from left namespace (main query namespace)
        left_field: string
        // Field from right namespace (joined query namespace)
        right_field: string
        // Condition operator
        cond: enum[EQ, GT, GE, LE, LT, SET]
        // Logic operator
        op?: enum[AND, OR, NOT]
      }[]
      select_filter?: string[]
    }
    // Subquery object. It must contain either 'select_filters' for the single field, single aggregation or must be matched against 'is null'/'is not null conditions'
    subquery: {
      // Namespace name
      namespace: string
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      // Ask query to calculate total documents, match condition
      req_total?: enum[disabled, enabled, cached] //default: disabled
      filters:FilterDef[]
      sort:SortDef[]
      select_filter?: string //default: id[]
      aggregations: {
        fields?: string[]
        // Aggregation function
        type: enum[SUM, AVG, MIN, MAX]
      }[]
    }
    // Array fields to be searched with equal array indexes
    equal_positions: {
      positions?: string[]
    }[]
    // Parameters for knn search
    params: {
      // Maximum count of returned vectors in KNN queries
      k?: integer
      // Raduis for filtering vectors by metric
      radius?: number
      // Applicable for HNSW index only. The size of the dynamic list for the nearest neighbors used during a query. Ef must be >= K. Default value = K
      ef?: integer
      // Applicable for IVF index only. The number of Voronoi cells to search during a query
      nprobe?: integer
    }
    // DEPRECATED. Use left_expression instead. Field json path or index name for filter
    field?: string
    // DEPRECATED. Use right_expression instead. Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN
    value: {
    }
    // DEPRECATED. Use left_expression instead. First field json path or index name for filter by two fields
    first_field?: string
    // DEPRECATED. Use right_expression instead. Second field json path or index name for filter by two fields
    second_field?: string
  }[]
  sort:SortDef[]
  merge_queries: {
    // Namespace name
    namespace: string
    // Type of query
    type?: enum[select, update, delete, truncate]
    // Maximum count of returned items
    limit?: integer
    // Offset of first returned item
    offset?: integer
    // Ask query to calculate total documents, match condition
    req_total?: enum[disabled, enabled, cached] //default: disabled
    filters:FilterDef[]
    sort:SortDef[]
    merge_queries:Query[]
    select_filter?: string //default: id[]
    select_functions?: string[]
    drop_fields?: string[]
    update_fields: {
      // field name
      name: string
      // update entry type
      type?: enum[object, expression, value]
      // is updated value an array
      is_array?: boolean
      values: {
      }[]
    }[]
    aggregations: {
      fields?: string[]
      // Aggregation function
      type: enum[SUM, AVG, MIN, MAX, FACET, DISTINCT]
      // Specifies facet aggregations results sorting order
      sort: {
        // Field or index name for sorting
        field: string
        // Descent or ascent sorting direction
        desc?: boolean
      }[]
      // Number of rows to get from result set. Allowed only for FACET
      limit?: integer
      // Index of the first row to get from result set. Allowed only for FACET
      offset?: integer
    }[]
    // Add query execution explain information
    explain?: boolean
    // Output fulltext or KNN rank in QueryResult. Allowed only with fulltext or KNN queries query
    select_with_rank?: boolean
    // Strict mode for query. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
    strict_mode?: enum[none, names, indexes] //default: names
  }[]
  select_filter?: string //default: id[]
  select_functions?: string[]
  drop_fields?: string[]
  update_fields:UpdateField[]
  aggregations:AggregationsDef[]
  // Add query execution explain information
  explain?: boolean
  // Output fulltext or KNN rank in QueryResult. Allowed only with fulltext or KNN queries query
  select_with_rank?: boolean
  // Strict mode for query. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
  strict_mode?: enum[none, names, indexes] //default: names
}
```

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Suggest for autocompletion of SQL query

```
[GET]/db/{database}/suggest
```

- Operation id  
getSuggest


This operation pareses SQL query, and suggests autocompletion variants  


#### Parameters(Query)

```typescript
q: string
```

```typescript
pos: integer
```

```typescript
line: integer
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  suggests?: string[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Query documents from namespace

```
[POST]/db/{database}/sqlquery
```

- Operation id  
postSQLQuery


This operation queries documents from namespace by SQL query. Query can be preceded by `EXPLAIN` statement, then query execution plan will be returned with query results.  


#### Parameters(Query)

```typescript
with_columns?: boolean
```

```typescript
width?: integer
```

```typescript
format?: enum[json, msgpack, protobuf, csv-file]
```

#### RequestBody

- */*

```typescript
string
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  items: {
  }[]
  namespaces?: string[]
  // Enables to client cache returned items. If false, then returned items has been modified  by reindexer, e.g. by select filter, or by functions, and can't be cached
  cache_enabled?: boolean
  // Total count of documents, matched query
  query_total_items?: integer
  aggregations: {
    fields?: string[]
    // Aggregation function
    type: enum[SUM, AVG, MIN, MAX, FACET, DISTINCT]
    // Value, calculated by aggregator
    value?: number
    facets: {
      values?: string[]
      // Count of elements these fields values
      count: integer
    }[]
    distincts?: DistincOneItemDef | DistinctMultiItemDef[]
  }[]
  equal_position?: string[]
  // Query columns for table outputs
  columns: {
    // Column name
    name?: string
    // Column width in percents of total width
    width_percents?: number
    // Column width in chars
    width_chars?: number
    // Maximum count of chars in column
    max_chars?: number
  }[]
  explain?: SingleQueryExplainDef | MergedQueryExplainDef
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get system information

```
[GET]/check
```

- Operation id  
getSysInfo


This operation will return system information about server version, uptime, and resources consumption

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Server version
  version?: string
  // Server uptime in seconds
  uptime?: integer
  // Server start time in unix timestamp
  start_time?: integer
  // Current heap size in bytes
  heap_size?: integer
  // Current inuse allocated memory size in bytes
  current_allocated_bytes?: integer
  // Heap free size in bytes
  pageheap_free?: integer
  // Unmapped free heap size in bytes
  pageheap_unmapped?: integer
  // RPC server address
  rpc_address?: string
  // HTTP server address
  http_address?: string
  // Path to storage
  storage_path?: string
  // RPC server log path
  rpc_log?: string
  // HTTP server log path
  http_log?: string
  // Reindexer core log path
  core_log?: string
  // Reindexer server log path
  server_log?: string
  // Log level, should be one of these: trace, debug, info, warning, error, critical
  log_level?: string
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Try to release free memory back to the operating system for reuse by other applications.

```
[POST]/allocator/drop_cache
```

- Operation id  
postAllocatorDropCache


Try to release free memory back to the operating system for reuse. Only for tcmalloc allocator.

#### Responses

- 200 Successful operation

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get memory usage information

```
[GET]/allocator/info
```

- Operation id  
getAllocatorInfo


This operation will return memory usage information from tcmalloc allocator.

#### Responses

- 200 successful operation

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get user role

```
[GET]/user/role
```

- Operation id  
getUserRole


Get the role of the currently authorized user in the Reindexer. If authorization is disabled, the owner's role is returned

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // User role
  user_role?: enum[owner, db_admin, data_write, data_read, none, unauthorized]
}
```

- 401 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get activity stats information

```
[GET]/db/{database}/namespaces/%23activitystats/items
```

- Operation id  
getActivityStats


This operation will return detailed information about current activity of all connected to the database clients

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Total count of documents, matched specified filters
  total_items?: integer
  items: {
    // Client identifier
    client: string
    // User name
    user?: string
    // Query text
    query: string
    // Query identifier
    query_id: integer
    // Query start time
    query_start: string
    // Current operation state
    state: enum[in_progress, wait_lock, sending, indexes_lookup, select_loop, proxied_via_cluster_proxy, proxied_via_sharding_proxy]
    lock_description?: string
  }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get client connection information

```
[GET]/db/{database}/namespaces/%23clientsstats/items
```

- Operation id  
getClientsStats


This operation will return detailed information about all connections on the server

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Count of connected clients
  total_items?: integer
  items: {
    // Connection identifier
    connection_id: integer
    // Ip
    ip: string
    // User name
    user_name: string
    // User right
    user_rights: string
    // Database name
    db_name: string
    // Current activity
    current_activity: string
    // Server start time in unix timestamp
    start_time: integer
    // Receive byte
    recv_bytes: integer
    // Send byte
    sent_bytes: integer
    // Client version string
    client_version: string
    // Client's application name
    app_name: string
    // Count of currently opened transactions for this client
    tx_count: integer
    // Send buffer size
    send_buf_bytes?: integer
    // Timestamp of last send operation (ms)
    last_send_ts?: integer
    // Timestamp of last recv operation (ms)
    last_recv_ts?: integer
    // Current send rate (bytes/s)
    send_rate?: integer
    // Current recv rate (bytes/s)
    recv_rate?: integer
  }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get replication statistics

```
[GET]/db/{database}/namespaces/%23replicationstats/items
```

- Operation id  
getGlobalReplicationStats


This operation will return detailed information about replication status on this node or cluster

#### Parameters(Query)

```typescript
filter: string
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Total replication stat items count
  total_items?: integer
  items: {
    // Replication type. Either 'async' or 'cluster'
    type: string
    wal_sync: {
      // Syncs count
      count: integer
      // Average sync time
      avg_time_us: integer
      // Max sync time
      max_time_us: integer
    }
    force_sync:ReplicationSyncStat
    initial_sync: {
      wal_sync:ReplicationSyncStat
      force_sync:ReplicationSyncStat
      // Total time of initial sync
      total_time_us: integer
    }
    // Number of online updates waiting to be replicated
    pending_updates_count: integer
    // Number of online updates waiting to be released
    allocated_updates_count: integer
    // Total online updates' size in bytes
    allocated_updates_size: integer
    nodes: {
      // Node's dsn
      dsn: string
      // Node's server id
      server_id: integer
      // Online updates waiting to be replicated to this node
      pending_updates_count: integer
      // Network status
      status: enum[none, offline, online]
      // Replication role
      role: enum[none, follower, leader, candidate]
      // Replication mode for mixed 'sync cluster + async replication' configs
      replication_mode?: enum[default, from_sync_leader]
      // Shows synchronization state for raft-cluster node (false if node is outdated)
      is_synchronized?: boolean
      // Number of namespaces in initial synchronization queue
      queued_namespace_syncs?: integer
      namespaces?: string[]
    }[]
  }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get memory stats information

```
[GET]/db/{database}/namespaces/%23memstats/items
```

- Operation id  
getMemStats


This operation will return detailed information about database memory consumption

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Total count of documents, matched specified filters
  total_items?: integer
  items: {
    // Name of namespace
    name?: string
    // Type of namespace. For now it's 'namespace' or 'embedders'
    type?: string
    // Total count of documents in namespace
    items_count?: integer
    // Size of strings deleted from namespace, but still used in queryResults
    strings_waiting_to_be_deleted_size?: integer
    // [[deprecated]]. do not use
    updated_unix_nano?: integer
    // Status of disk storage (true, if storage is enabled and writable)
    storage_ok?: boolean
    // Shows if storage is enabled (however it may still be unavailable)
    storage_enabled?: boolean
    // More detailed info about storage status. May contain 'OK', 'DISABLED', 'NO SPACE LEFT' or last error description
    storage_status?: string
    // Filesystem path to namespace storage
    storage_path?: string
    // Background indexes optimization has been completed
    optimization_completed?: boolean
    // Summary of total namespace memory consumption
    total: {
      // Total memory size of stored documents, including system structures
      data_size?: integer
      // Total memory consumption of namespace's indexes
      indexes_size?: integer
      // Total memory consumption of namespace's caches. e.g. idset and join caches
      cache_size?: integer
      // Total memory size, occupation by index optimizer (in bytes)
      index_optimizer_memory?: integer
      // Total memory size, occupied by the AsyncStorage (in bytes)
      inmemory_storage_size?: integer
    }
    // Summary of total async storage memory consumption
    storage: {
      // Total memory size, occupied by synchronous proxy map of the AsyncStorage (in bytes)
      proxy_size?: integer
    }
    // Join cache stats. Stores results of selects to right table by ON condition
    join_cache?: CacheMemStats
    // Query cache stats. Stores results of SELECT COUNT(*) by Where conditions
    query_cache?: CacheMemStats
    // State of namespace replication
    replication: {
      // Last Log Sequence Number (LSN) of applied namespace modification represented as single integer
      last_lsn?: integer
      // Last Log Sequence Number (LSN) of applied namespace record
      last_lsn_v2: {
        // Server ID of record source node
        server_id?: integer
        // Record ID (incremental counter)
        counter?: integer
      }
      // Namespace version, assigned on namespace creation
      ns_version: {
        // Server ID of creater node
        server_id?: integer
        // Version (incremental counter)
        counter?: integer
      }
      // Cluster operation status for the namespace
      clusterization_status: {
        // Server ID of the namespace's leader
        leader_id?: integer
        // Namespace role in replication
        role?: enum[none, cluster_replica, simple_replica]
      }
      // Checksum of all records in namespace
      checksum?: integer
      // Write Ahead Log (WAL) records count
      wal_count?: integer
      // Total memory consumption of Write Ahead Log (WAL)
      wal_size?: integer
      // Current node ID
      server_id?: integer
      // Last update time
      updated_unix_nano?: integer
      // Items count in namespace
      data_count?: integer
      // Admissible replication token of the namespace
      admissible_token?: string
    }
    indexes: {
      // Name of index. There are special index with name `-tuple`. It's stores original document's json structure with non indexed fields
      name?: string
      // Count of unique keys values stored in index
      unique_keys_count?: integer
      // Total memory consumption (in bytes) of reverse index b-tree structures. For `dense` and `store` indexes always 0
      idset_btree_size?: integer
      // Total memory consumption (in bytes) of reverse index vectors. For `store` indexes always 0
      idset_plain_size?: integer
      // Total memory consumption (in bytes) of SORT statement and `GT`, `LT` conditions optimized structures. Applicable only to `tree` indexes
      sort_orders_size?: integer
      // Idset cache stats. Stores merged reverse index results of SELECT field IN(...) by IN(...) keys
      idset_cache?: CacheMemStats
      // Total memory consumption (in bytes) of the main indexing structures (fulltext, ANN, etc.)
      indexing_struct_size?: integer
      // Total memory consumation (in bytes) of shared vectors keeper structures (ANN indexes only)
      vectors_keeper_size?: integer
      // Total memory consumption (in bytes) of documents's data, held by index
      data_size?: integer
      // Updates count, pending in index updates tracker
      tracked_updates_count?: integer
      // Buckets count in index updates tracker map
      tracked_updates_buckets?: integer
      // Updates tracker map size in bytes
      tracked_updates_size?: integer
      // Updates tracker map overflow (number of elements, stored outside of the main buckets)
      tracked_updates_overflow?: integer
      // Shows whether KNN/fulltext indexing structure is fully built. If this field is missing, index does not require any specific build steps
      is_built?: boolean
      // Shows whether HNSW-index quantized. If this field is nil, index does not support quantization
      is_quantized?: boolean
      upsert_embedder: {
        // Last request execution status
        last_request_result?: enum[OK, ERROR]
        last_error: {
          // Error code. 0 - no error.
          code?: integer
          // Error message
          message?: string
        }
      }
      query_embedder:EmbedderStatus
    }[]
    embedding_caches: {
      // Tag of cache from configuration
      cache_tag?: string
      // Capacity of cache
      capacity?: integer
      cache: {
        // Total memory consumption by this cache
        total_size?: integer
        // Count of used elements stored in this cache
        items_count?: integer
        // Count of empty elements slots in this cache
        empty_count?: integer
        // Number of hits of queries, to store results in cache
        hit_count_limit?: integer
      }
      // Status of disk storage (true, if storage is enabled and writable)
      storage_ok?: boolean
      // More detailed info about storage status. May contain 'OK', 'DISABLED', 'FAILED' or last error description
      storage_status?: string
      // Filesystem path to namespace storage
      storage_path?: string
      // Disk space occupied by storage
      storage_size?: integer
    }[]
    // Status of tags matcher
    tags_matcher: {
      // Current count of tags in tags matcher
      tags_count?: integer
      // Maximum count of tags in tags matcher
      max_tags_count?: integer
      // Version of tags matcher
      version?: integer
      // State token of tags matcher
      state_token?: integer
    }
  }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get performance stats information

```
[GET]/db/{database}/namespaces/%23perfstats/items
```

- Operation id  
getPerfStats


This operation will return detailed information about database performance timings. By default performance stats is turned off.

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Total count of documents, matched specified filters
  total_items?: integer
  items: {
    // Name of namespace
    name?: string
    // Performance statistics for update operations
    updates?: CommonPerfStats
    // Performance statistics for select operations
    selects?: CommonPerfStats
    // Performance statistics for transactions
    transactions: {
      // Total transactions count for this namespace
      total_count?: integer
      // Total namespace copy operations
      total_copy_count?: integer
      // Average steps count in transactions for this namespace
      avg_steps_count?: integer
      // Minimum steps count in transactions for this namespace
      min_steps_count?: integer
      // Maximum steps count in transactions for this namespace
      max_steps_count?: integer
      // Average transaction preparation time usec
      avg_prepare_time_us?: integer
      // Minimum transaction preparation time usec
      min_prepare_time_us?: integer
      // Maximum transaction preparation time usec
      max_prepare_time_us?: integer
      // Average transaction commit time usec
      avg_commit_time_us?: integer
      // Minimum transaction commit time usec
      min_commit_time_us?: integer
      // Maximum transaction commit time usec
      max_commit_time_us?: integer
      // Average namespace copy time usec
      avg_copy_time_us?: integer
      // Maximum namespace copy time usec
      min_copy_time_us?: integer
      // Minimum namespace copy time usec
      max_copy_time_us?: integer
    }
    // Performance statistics for specific LRU-cache instance
    join_cache: {
      // Queries total count
      total_queries?: integer
      // Cache hit rate (hits / total_queries)
      cache_hit_rate?: number
      // Determines if cache is currently in use. Usually it has 'false' value for uncommitted indexes
      is_active?: boolean
    }
    query_count_cache:LRUCachePerfStats
    indexes: {
      // Name of index
      name?: string
      updates:UpdatePerfStats
      selects:SelectPerfStats
      cache:LRUCachePerfStats
      upsert_embedder: {
        // Total number of calls to a specific embedder
        total_queries_count?: integer
        // Total number of requested vectors
        total_embed_documents_count?: integer
        // Number of calls to the embedder in the last second
        last_sec_qps?: integer
        // Number of embedded documents in the last second
        last_sec_dps?: integer
        // Total number of errors accessing the embedder
        total_errors_count?: integer
        // Number of errors in the last second
        last_second_errors_count?: integer
        // Current number of connections in use
        conn_in_use?: integer
        // Average number of connections used over the last second
        last_sec_avg_conn_in_use?: integer
        // Average overall autoembedding latency (over all time)
        total_avg_latency_us?: integer
        // Average autoembedding latency (over the last second)
        last_sec_avg_latency_us?: integer
        // Maximum total autoembedding latency (all time)
        max_latency_us?: integer
        // Minimum overall autoembedding latency (all time)
        min_latency_us?: integer
        // Average latency of waiting for a connection from the pool (over all time)
        total_avg_conn_await_latency_us?: integer
        // Average latency of waiting for a connection from the pool (over the last second)
        last_sec_avg_conn_await_latency_us?: integer
        // Average auto-embedding latency on cache miss (over all time)
        total_avg_embed_latency_us?: integer
        // Average auto-embedding latency for cache misses (last second)
        last_sec_avg_embed_latency_us?: integer
        // Maximum auto-embedding latency on cache miss (all time)
        max_embed_latency_us?: integer
        // Minimum auto-embedding latency on cache miss (all time)
        min_embed_latency_us?: integer
        // Average auto-embedding latency for cache hits (over all time)
        total_avg_cache_latency_us?: integer
        // Average auto-embedding latency for cache hits (last second)
        last_sec_avg_cache_latency_us?: integer
        // Maximum auto-embedding latency on a cache hit (all time)
        max_cache_latency_us?: integer
        // Minimum auto-embedding latency for a cache hit (all time)
        min_cache_latency_us?: integer
        // Total amount of data received from the embedding service over the network
        input_traffic_total_bytes?: integer
        // Total amount of data sent to the embedding service over the network
        output_traffic_total_bytes?: integer
        // Performance statistics for specific Embedder LRU-cache instance
        cache: {
          // Name. Identifier for linking settings
          cache_tag?: string
          // Queries total count
          total_queries?: integer
          // Cache hit rate (hits / total_queries)
          cache_hit_rate?: number
          // Determines if cache is currently in use. Usually it has 'false' value for uncommitted indexes
          is_active?: boolean
        }
      }
      query_embedder:EmbedderPerfStat
    }[]
  }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get SELECT queries performance stats information

```
[GET]/db/{database}/namespaces/%23queriesperfstats/items
```

- Operation id  
getQueriesPerfStats


This operation will return detailed information about database memory consumption. By default quires performance stat is turned off.

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Total count of documents, matched specified filters
  total_items?: integer
  // Performance statistics per each query
  items?: CommonPerfStats & {
     // normalized SQL representation of query
     query?: string
     // not normalized SQL representation of longest query
     longest_query?: string
   }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get system configs

```
[GET]/db/{database}/namespaces/%23config/items
```

- Operation id  
getSystemConfigs


This operation will return system configs

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  items: {
    type: enum[profiling, namespaces, replication, async_replication, embedders, action] //default: profiling
    profiling: {
      // Enables tracking activity statistics
      activitystats?: boolean
      // Enables tracking memory statistics
      memstats?: boolean //default: true
      // Enables tracking overall performance statistics
      perfstats?: boolean
      // Enables record queries performance statistics
      queriesperfstats?: boolean
      // Minimum query execution time to be recorded in #queriesperfstats namespace
      queries_threshold_us?: integer
      // Parameters for logging long queries and transactions
      long_queries_logging: {
        select: {
          // Threshold value for logging SELECT queries, if -1 logging is disabled
          threshold_us?: integer
          // Output the query in a normalized form
          normalized?: boolean
        }
        update_delete: {
          // Threshold value for logging UPDATE and DELETE queries, if -1 logging is disabled
          threshold_us?: integer
          // Output the query in a normalized form
          normalized?: boolean
        }
        transaction: {
          // Threshold value for total transaction commit time, if -1 logging is disabled
          threshold_us?: integer
          // Threshold value for the average step duration time in a transaction, if -1 logging is disabled
          avg_step_threshold_us?: integer
        }
      }
    }
    namespaces: {
      // Name of namespace, or `*` for setting to all namespaces
      namespace?: string
      // Log level of queries core logger
      log_level?: enum[none, error, warning, info, trace]
      // Join cache mode
      join_cache_mode?: enum[aggressive, on, off] //default: off
      // Enable namespace copying for transaction with steps count greater than this value (if copy_politics_multiplier also allows this)
      start_copy_policy_tx_size?: integer //default: 10000
      // Disables copy policy if namespace size is greater than copy_policy_multiplier * start_copy_policy_tx_size
      copy_policy_multiplier?: integer //default: 5
      // Force namespace copying for transaction with steps count greater than this value
      tx_size_to_always_copy?: integer //default: 100000
      // Count of threads, that will be created during transaction's commit to insert data into multithread ANN-indexes
      tx_vec_insertion_threads?: integer //default: 4
      // Timeout before background indexes optimization start after last update. 0 - disable optimizations
      optimization_timeout_ms?: integer //default: 800
      // Maximum number of background threads of sort indexes optimization. 0 - disable sort optimizations
      optimization_sort_workers?: integer //default: 4
      // Maximum WAL size for this namespace (maximum count of WAL records)
      wal_size?: integer //default: 4000000
      // Maximum preselect size for optimization of inner join by insertion of filters. If max_preselect_size is 0, then only max_preselect_part will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied
      max_preselect_size?: integer //default: 1000
      // Maximum preselect part of namespace's items for optimization of inner join by insertion of filters. If max_preselect_part is 0, then only max_preselect_size will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied
      max_preselect_part?: number //default: 0.1
      // Minimum preselect size for optimization of inner join by insertion of filters. Min_preselect_size will be used as preselect limit if (max_preselect_part * ns.size) is less than this value
      min_preselect_size?: integer //default: 1000
      // Maximum number of IdSet iterations of namespace preliminary result size for optimization
      max_iterations_idset_preresult?: integer //default: 20000
      // Enables 'simple counting mode' for index updates tracker. This will increase index optimization time, however may reduce insertion time
      index_updates_counting_mode?: boolean
      // Enables synchronous storage flush inside write-calls, if async updates count is more than sync_storage_flush_limit. 0 - disables synchronous storage flush, in this case storage will be flushed in background thread only
      sync_storage_flush_limit?: integer //default: 20000
      // Delay between last and namespace update background ANN-indexes storage cache creation. Storage cache is required for ANN-indexes for faster startup. 0 - disables background cache creation (cache will still be created on the database shutdown)
      ann_storage_cache_build_timeout_ms?: integer //default: 5000
      // Strict mode for queries. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
      strict_mode?: enum[none, names, indexes] //default: names
      cache: {
        // Max size of the index IdSets cache in bytes (per index). Each index has it's own independent cache. This cache is used in any selections to store resulting sets of internal document IDs (it does not stores documents' content itself)
        index_idset_cache_size?: integer //default: 134217728
        // Default 'hits to cache' for index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
        index_idset_hits_to_cache?: integer //default: 2
        // Max size of the fulltext indexes IdSets cache in bytes (per index). Each fulltext index has it's own independent cache. This cache is used in any selections to store resulting sets of internal document IDs, FT ranks and highlighted areas (it does not stores documents' content itself)
        ft_index_cache_size?: integer //default: 134217728
        // Default 'hits to cache' for fulltext index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
        ft_index_hits_to_cache?: integer //default: 2
        // Max size of the index IdSets cache in bytes for each namespace. This cache will be enabled only if 'join_cache_mode' property is not 'off'. It stores resulting IDs, serialized JOINed queries and any other 'preselect' information for the JOIN queries (when target namespace is right namespace of the JOIN)
        joins_preselect_cache_size?: integer //default: 134217728
        // Default 'hits to cache' for joins preselect cache of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
        joins_preselect_hit_to_cache?: integer //default: 2
        // Max size of the cache for COUNT_CACHED() aggregation in bytes for each namespace. This cache stores resulting COUNTs and serialized queries for the COUNT_CACHED() aggregations
        query_count_cache_size?: integer //default: 134217728
        // Default 'hits to cache' for COUNT_CACHED() aggregation of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
        query_count_hit_to_cache?: integer //default: 2
      }
    }[]
    replication: {
      // Node identifier. Should be unique for each node in the replicated cluster (non-unique IDs are also allowed, but may lead to the inconsistency in some cases
      server_id?: integer
      // Cluster ID - must be same for client and for master
      cluster_id?: integer //default: 2
      admissible_replication_tokens: {
        token?: string
        namespaces: {
        }[]
      }[]
    }
    async_replication: {
      // Replication role
      role: enum[none, follower, leader]
      // Allows to configure async replication from sync raft-cluster (replicate either from each node, or from synchronous cluster leader)
      mode?: enum[default, from_sync_leader]
      // Application name, used by replicator as a login tag
      app_name?: string
      // Node response timeout for online-replication (seconds)
      online_updates_timeout_sec?: integer
      // Network timeout for communication with followers (for force and wal synchronization), in seconds
      sync_timeout_sec?: integer
      // Resync timeout on network errors
      retry_sync_interval_msec?: integer
      // Number of data replication threads
      sync_threads?: integer
      // Max number of concurrent force/wal sync's per thread
      syncs_per_thread?: integer
      // Number of coroutines for updates batching (per namespace). Higher value here may help to reduce networks triparound await time, but will require more RAM
      batching_routines_count?: integer
      // Delay between write operation and replication. Larger values here will leader to higher replication latency and buffering, but also will provide more effective network batching and CPU utilization
      online_updates_delay_msec?: integer
      // Enable network traffic compression
      enable_compression?: boolean
      // Maximum number of WAL records, which will be copied after force-sync
      max_wal_depth_on_force_sync?: integer
      // force resync on logic error conditions
      force_sync_on_logic_error?: boolean
      // force resync on wrong data hash conditions
      force_sync_on_wrong_data_hash?: boolean
      // Replication log level on replicator's startup
      log_level?: enum[none, error, warning, info, trace]
      namespaces?: string[]
      // Token of the current node that it sends to the follower for verification
      self_replication_token?: string
      nodes: {
        // Follower's DSN. Must have cproto-scheme
        dsn: string
        namespaces?: string[]
      }[]
    }
    embedders: {
      // Name. Identifier for linking settings. Special value '*' is supported (applies to all)
      cache_tag?: string
      // Maximum size of the embedding results cache in items. This cache will only be enabled if the 'max_cache_items' property is not 'off' (value 0). It stores the results of the embedding calculation
      max_cache_items?: integer //default: 1000000
      // Default 'hits to cache' for embedding calculation cache. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. 0 and 1 mean - when value added goes straight to the cache
      hit_to_cache?: integer //default: 1
    }[]
    action: {
      // Command to execute
      command: enum[restart_replication, reset_replication_role]
      // Namespace name for reset_replication_role. May be empty
      namespace?: string
    }
  }[]
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Update system config

```
[PUT]/db/{database}/namespaces/%23config/items
```

- Operation id  
putSystemConfig


This operation will update system configuration:  
- profiling configuration. It is used to enable recording of queries and overall performance;  
- log queries configuration.  


#### RequestBody

- */*

```typescript
{
  type: enum[profiling, namespaces, replication, async_replication, embedders, action] //default: profiling
  profiling: {
    // Enables tracking activity statistics
    activitystats?: boolean
    // Enables tracking memory statistics
    memstats?: boolean //default: true
    // Enables tracking overall performance statistics
    perfstats?: boolean
    // Enables record queries performance statistics
    queriesperfstats?: boolean
    // Minimum query execution time to be recorded in #queriesperfstats namespace
    queries_threshold_us?: integer
    // Parameters for logging long queries and transactions
    long_queries_logging: {
      select: {
        // Threshold value for logging SELECT queries, if -1 logging is disabled
        threshold_us?: integer
        // Output the query in a normalized form
        normalized?: boolean
      }
      update_delete: {
        // Threshold value for logging UPDATE and DELETE queries, if -1 logging is disabled
        threshold_us?: integer
        // Output the query in a normalized form
        normalized?: boolean
      }
      transaction: {
        // Threshold value for total transaction commit time, if -1 logging is disabled
        threshold_us?: integer
        // Threshold value for the average step duration time in a transaction, if -1 logging is disabled
        avg_step_threshold_us?: integer
      }
    }
  }
  namespaces: {
    // Name of namespace, or `*` for setting to all namespaces
    namespace?: string
    // Log level of queries core logger
    log_level?: enum[none, error, warning, info, trace]
    // Join cache mode
    join_cache_mode?: enum[aggressive, on, off] //default: off
    // Enable namespace copying for transaction with steps count greater than this value (if copy_politics_multiplier also allows this)
    start_copy_policy_tx_size?: integer //default: 10000
    // Disables copy policy if namespace size is greater than copy_policy_multiplier * start_copy_policy_tx_size
    copy_policy_multiplier?: integer //default: 5
    // Force namespace copying for transaction with steps count greater than this value
    tx_size_to_always_copy?: integer //default: 100000
    // Count of threads, that will be created during transaction's commit to insert data into multithread ANN-indexes
    tx_vec_insertion_threads?: integer //default: 4
    // Timeout before background indexes optimization start after last update. 0 - disable optimizations
    optimization_timeout_ms?: integer //default: 800
    // Maximum number of background threads of sort indexes optimization. 0 - disable sort optimizations
    optimization_sort_workers?: integer //default: 4
    // Maximum WAL size for this namespace (maximum count of WAL records)
    wal_size?: integer //default: 4000000
    // Maximum preselect size for optimization of inner join by insertion of filters. If max_preselect_size is 0, then only max_preselect_part will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied
    max_preselect_size?: integer //default: 1000
    // Maximum preselect part of namespace's items for optimization of inner join by insertion of filters. If max_preselect_part is 0, then only max_preselect_size will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied
    max_preselect_part?: number //default: 0.1
    // Minimum preselect size for optimization of inner join by insertion of filters. Min_preselect_size will be used as preselect limit if (max_preselect_part * ns.size) is less than this value
    min_preselect_size?: integer //default: 1000
    // Maximum number of IdSet iterations of namespace preliminary result size for optimization
    max_iterations_idset_preresult?: integer //default: 20000
    // Enables 'simple counting mode' for index updates tracker. This will increase index optimization time, however may reduce insertion time
    index_updates_counting_mode?: boolean
    // Enables synchronous storage flush inside write-calls, if async updates count is more than sync_storage_flush_limit. 0 - disables synchronous storage flush, in this case storage will be flushed in background thread only
    sync_storage_flush_limit?: integer //default: 20000
    // Delay between last and namespace update background ANN-indexes storage cache creation. Storage cache is required for ANN-indexes for faster startup. 0 - disables background cache creation (cache will still be created on the database shutdown)
    ann_storage_cache_build_timeout_ms?: integer //default: 5000
    // Strict mode for queries. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
    strict_mode?: enum[none, names, indexes] //default: names
    cache: {
      // Max size of the index IdSets cache in bytes (per index). Each index has it's own independent cache. This cache is used in any selections to store resulting sets of internal document IDs (it does not stores documents' content itself)
      index_idset_cache_size?: integer //default: 134217728
      // Default 'hits to cache' for index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
      index_idset_hits_to_cache?: integer //default: 2
      // Max size of the fulltext indexes IdSets cache in bytes (per index). Each fulltext index has it's own independent cache. This cache is used in any selections to store resulting sets of internal document IDs, FT ranks and highlighted areas (it does not stores documents' content itself)
      ft_index_cache_size?: integer //default: 134217728
      // Default 'hits to cache' for fulltext index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
      ft_index_hits_to_cache?: integer //default: 2
      // Max size of the index IdSets cache in bytes for each namespace. This cache will be enabled only if 'join_cache_mode' property is not 'off'. It stores resulting IDs, serialized JOINed queries and any other 'preselect' information for the JOIN queries (when target namespace is right namespace of the JOIN)
      joins_preselect_cache_size?: integer //default: 134217728
      // Default 'hits to cache' for joins preselect cache of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
      joins_preselect_hit_to_cache?: integer //default: 2
      // Max size of the cache for COUNT_CACHED() aggregation in bytes for each namespace. This cache stores resulting COUNTs and serialized queries for the COUNT_CACHED() aggregations
      query_count_cache_size?: integer //default: 134217728
      // Default 'hits to cache' for COUNT_CACHED() aggregation of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
      query_count_hit_to_cache?: integer //default: 2
    }
  }[]
  replication: {
    // Node identifier. Should be unique for each node in the replicated cluster (non-unique IDs are also allowed, but may lead to the inconsistency in some cases
    server_id?: integer
    // Cluster ID - must be same for client and for master
    cluster_id?: integer //default: 2
    admissible_replication_tokens: {
      token?: string
      namespaces: {
      }[]
    }[]
  }
  async_replication: {
    // Replication role
    role: enum[none, follower, leader]
    // Allows to configure async replication from sync raft-cluster (replicate either from each node, or from synchronous cluster leader)
    mode?: enum[default, from_sync_leader]
    // Application name, used by replicator as a login tag
    app_name?: string
    // Node response timeout for online-replication (seconds)
    online_updates_timeout_sec?: integer
    // Network timeout for communication with followers (for force and wal synchronization), in seconds
    sync_timeout_sec?: integer
    // Resync timeout on network errors
    retry_sync_interval_msec?: integer
    // Number of data replication threads
    sync_threads?: integer
    // Max number of concurrent force/wal sync's per thread
    syncs_per_thread?: integer
    // Number of coroutines for updates batching (per namespace). Higher value here may help to reduce networks triparound await time, but will require more RAM
    batching_routines_count?: integer
    // Delay between write operation and replication. Larger values here will leader to higher replication latency and buffering, but also will provide more effective network batching and CPU utilization
    online_updates_delay_msec?: integer
    // Enable network traffic compression
    enable_compression?: boolean
    // Maximum number of WAL records, which will be copied after force-sync
    max_wal_depth_on_force_sync?: integer
    // force resync on logic error conditions
    force_sync_on_logic_error?: boolean
    // force resync on wrong data hash conditions
    force_sync_on_wrong_data_hash?: boolean
    // Replication log level on replicator's startup
    log_level?: enum[none, error, warning, info, trace]
    namespaces?: string[]
    // Token of the current node that it sends to the follower for verification
    self_replication_token?: string
    nodes: {
      // Follower's DSN. Must have cproto-scheme
      dsn: string
      namespaces?: string[]
    }[]
  }
  embedders: {
    // Name. Identifier for linking settings. Special value '*' is supported (applies to all)
    cache_tag?: string
    // Maximum size of the embedding results cache in items. This cache will only be enabled if the 'max_cache_items' property is not 'off' (value 0). It stores the results of the embedding calculation
    max_cache_items?: integer //default: 1000000
    // Default 'hits to cache' for embedding calculation cache. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. 0 and 1 mean - when value added goes straight to the cache
    hit_to_cache?: integer //default: 1
  }[]
  action: {
    // Command to execute
    command: enum[restart_replication, reset_replication_role]
    // Namespace name for reset_replication_role. May be empty
    namespace?: string
  }
}
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  // Count of updated items
  updated?: integer
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 404 Entry not found

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

***

### Get default system configs

```
[GET]/db/default_configs
```

- Operation id  
getDefaultSystemConfigs


This operation will return default system configs.

#### Parameters(Query)

```typescript
type: enum[namespaces, replication, async_replication, profiling, embedders] //default: namespaces
```

#### Responses

- 200 successful operation

`application/json`

```typescript
{
  type: enum[profiling, namespaces, replication, async_replication, embedders, action] //default: profiling
  profiling: {
    // Enables tracking activity statistics
    activitystats?: boolean
    // Enables tracking memory statistics
    memstats?: boolean //default: true
    // Enables tracking overall performance statistics
    perfstats?: boolean
    // Enables record queries performance statistics
    queriesperfstats?: boolean
    // Minimum query execution time to be recorded in #queriesperfstats namespace
    queries_threshold_us?: integer
    // Parameters for logging long queries and transactions
    long_queries_logging: {
      select: {
        // Threshold value for logging SELECT queries, if -1 logging is disabled
        threshold_us?: integer
        // Output the query in a normalized form
        normalized?: boolean
      }
      update_delete: {
        // Threshold value for logging UPDATE and DELETE queries, if -1 logging is disabled
        threshold_us?: integer
        // Output the query in a normalized form
        normalized?: boolean
      }
      transaction: {
        // Threshold value for total transaction commit time, if -1 logging is disabled
        threshold_us?: integer
        // Threshold value for the average step duration time in a transaction, if -1 logging is disabled
        avg_step_threshold_us?: integer
      }
    }
  }
  namespaces: {
    // Name of namespace, or `*` for setting to all namespaces
    namespace?: string
    // Log level of queries core logger
    log_level?: enum[none, error, warning, info, trace]
    // Join cache mode
    join_cache_mode?: enum[aggressive, on, off] //default: off
    // Enable namespace copying for transaction with steps count greater than this value (if copy_politics_multiplier also allows this)
    start_copy_policy_tx_size?: integer //default: 10000
    // Disables copy policy if namespace size is greater than copy_policy_multiplier * start_copy_policy_tx_size
    copy_policy_multiplier?: integer //default: 5
    // Force namespace copying for transaction with steps count greater than this value
    tx_size_to_always_copy?: integer //default: 100000
    // Count of threads, that will be created during transaction's commit to insert data into multithread ANN-indexes
    tx_vec_insertion_threads?: integer //default: 4
    // Timeout before background indexes optimization start after last update. 0 - disable optimizations
    optimization_timeout_ms?: integer //default: 800
    // Maximum number of background threads of sort indexes optimization. 0 - disable sort optimizations
    optimization_sort_workers?: integer //default: 4
    // Maximum WAL size for this namespace (maximum count of WAL records)
    wal_size?: integer //default: 4000000
    // Maximum preselect size for optimization of inner join by insertion of filters. If max_preselect_size is 0, then only max_preselect_part will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied
    max_preselect_size?: integer //default: 1000
    // Maximum preselect part of namespace's items for optimization of inner join by insertion of filters. If max_preselect_part is 0, then only max_preselect_size will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied
    max_preselect_part?: number //default: 0.1
    // Minimum preselect size for optimization of inner join by insertion of filters. Min_preselect_size will be used as preselect limit if (max_preselect_part * ns.size) is less than this value
    min_preselect_size?: integer //default: 1000
    // Maximum number of IdSet iterations of namespace preliminary result size for optimization
    max_iterations_idset_preresult?: integer //default: 20000
    // Enables 'simple counting mode' for index updates tracker. This will increase index optimization time, however may reduce insertion time
    index_updates_counting_mode?: boolean
    // Enables synchronous storage flush inside write-calls, if async updates count is more than sync_storage_flush_limit. 0 - disables synchronous storage flush, in this case storage will be flushed in background thread only
    sync_storage_flush_limit?: integer //default: 20000
    // Delay between last and namespace update background ANN-indexes storage cache creation. Storage cache is required for ANN-indexes for faster startup. 0 - disables background cache creation (cache will still be created on the database shutdown)
    ann_storage_cache_build_timeout_ms?: integer //default: 5000
    // Strict mode for queries. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
    strict_mode?: enum[none, names, indexes] //default: names
    cache: {
      // Max size of the index IdSets cache in bytes (per index). Each index has it's own independent cache. This cache is used in any selections to store resulting sets of internal document IDs (it does not stores documents' content itself)
      index_idset_cache_size?: integer //default: 134217728
      // Default 'hits to cache' for index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
      index_idset_hits_to_cache?: integer //default: 2
      // Max size of the fulltext indexes IdSets cache in bytes (per index). Each fulltext index has it's own independent cache. This cache is used in any selections to store resulting sets of internal document IDs, FT ranks and highlighted areas (it does not stores documents' content itself)
      ft_index_cache_size?: integer //default: 134217728
      // Default 'hits to cache' for fulltext index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
      ft_index_hits_to_cache?: integer //default: 2
      // Max size of the index IdSets cache in bytes for each namespace. This cache will be enabled only if 'join_cache_mode' property is not 'off'. It stores resulting IDs, serialized JOINed queries and any other 'preselect' information for the JOIN queries (when target namespace is right namespace of the JOIN)
      joins_preselect_cache_size?: integer //default: 134217728
      // Default 'hits to cache' for joins preselect cache of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
      joins_preselect_hit_to_cache?: integer //default: 2
      // Max size of the cache for COUNT_CACHED() aggregation in bytes for each namespace. This cache stores resulting COUNTs and serialized queries for the COUNT_CACHED() aggregations
      query_count_cache_size?: integer //default: 134217728
      // Default 'hits to cache' for COUNT_CACHED() aggregation of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
      query_count_hit_to_cache?: integer //default: 2
    }
  }[]
  replication: {
    // Node identifier. Should be unique for each node in the replicated cluster (non-unique IDs are also allowed, but may lead to the inconsistency in some cases
    server_id?: integer
    // Cluster ID - must be same for client and for master
    cluster_id?: integer //default: 2
    admissible_replication_tokens: {
      token?: string
      namespaces: {
      }[]
    }[]
  }
  async_replication: {
    // Replication role
    role: enum[none, follower, leader]
    // Allows to configure async replication from sync raft-cluster (replicate either from each node, or from synchronous cluster leader)
    mode?: enum[default, from_sync_leader]
    // Application name, used by replicator as a login tag
    app_name?: string
    // Node response timeout for online-replication (seconds)
    online_updates_timeout_sec?: integer
    // Network timeout for communication with followers (for force and wal synchronization), in seconds
    sync_timeout_sec?: integer
    // Resync timeout on network errors
    retry_sync_interval_msec?: integer
    // Number of data replication threads
    sync_threads?: integer
    // Max number of concurrent force/wal sync's per thread
    syncs_per_thread?: integer
    // Number of coroutines for updates batching (per namespace). Higher value here may help to reduce networks triparound await time, but will require more RAM
    batching_routines_count?: integer
    // Delay between write operation and replication. Larger values here will leader to higher replication latency and buffering, but also will provide more effective network batching and CPU utilization
    online_updates_delay_msec?: integer
    // Enable network traffic compression
    enable_compression?: boolean
    // Maximum number of WAL records, which will be copied after force-sync
    max_wal_depth_on_force_sync?: integer
    // force resync on logic error conditions
    force_sync_on_logic_error?: boolean
    // force resync on wrong data hash conditions
    force_sync_on_wrong_data_hash?: boolean
    // Replication log level on replicator's startup
    log_level?: enum[none, error, warning, info, trace]
    namespaces?: string[]
    // Token of the current node that it sends to the follower for verification
    self_replication_token?: string
    nodes: {
      // Follower's DSN. Must have cproto-scheme
      dsn: string
      namespaces?: string[]
    }[]
  }
  embedders: {
    // Name. Identifier for linking settings. Special value '*' is supported (applies to all)
    cache_tag?: string
    // Maximum size of the embedding results cache in items. This cache will only be enabled if the 'max_cache_items' property is not 'off' (value 0). It stores the results of the embedding calculation
    max_cache_items?: integer //default: 1000000
    // Default 'hits to cache' for embedding calculation cache. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. 0 and 1 mean - when value added goes straight to the cache
    hit_to_cache?: integer //default: 1
  }[]
  action: {
    // Command to execute
    command: enum[restart_replication, reset_replication_role]
    // Namespace name for reset_replication_role. May be empty
    namespace?: string
  }
}
```

- 400 Invalid arguments supplied

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 403 Forbidden

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 408 Context timeout

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

- 500 Unexpected internal error

`application/json`

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

## References

### SysInfo

```typescript
{
  // Server version
  version?: string
  // Server uptime in seconds
  uptime?: integer
  // Server start time in unix timestamp
  start_time?: integer
  // Current heap size in bytes
  heap_size?: integer
  // Current inuse allocated memory size in bytes
  current_allocated_bytes?: integer
  // Heap free size in bytes
  pageheap_free?: integer
  // Unmapped free heap size in bytes
  pageheap_unmapped?: integer
  // RPC server address
  rpc_address?: string
  // HTTP server address
  http_address?: string
  // Path to storage
  storage_path?: string
  // RPC server log path
  rpc_log?: string
  // HTTP server log path
  http_log?: string
  // Reindexer core log path
  core_log?: string
  // Reindexer server log path
  server_log?: string
  // Log level, should be one of these: trace, debug, info, warning, error, critical
  log_level?: string
}
```

### ActivityStats

```typescript
{
  // Total count of documents, matched specified filters
  total_items?: integer
  items: {
    // Client identifier
    client: string
    // User name
    user?: string
    // Query text
    query: string
    // Query identifier
    query_id: integer
    // Query start time
    query_start: string
    // Current operation state
    state: enum[in_progress, wait_lock, sending, indexes_lookup, select_loop, proxied_via_cluster_proxy, proxied_via_sharding_proxy]
    lock_description?: string
  }[]
}
```

### ClientsStats

```typescript
{
  // Count of connected clients
  total_items?: integer
  items: {
    // Connection identifier
    connection_id: integer
    // Ip
    ip: string
    // User name
    user_name: string
    // User right
    user_rights: string
    // Database name
    db_name: string
    // Current activity
    current_activity: string
    // Server start time in unix timestamp
    start_time: integer
    // Receive byte
    recv_bytes: integer
    // Send byte
    sent_bytes: integer
    // Client version string
    client_version: string
    // Client's application name
    app_name: string
    // Count of currently opened transactions for this client
    tx_count: integer
    // Send buffer size
    send_buf_bytes?: integer
    // Timestamp of last send operation (ms)
    last_send_ts?: integer
    // Timestamp of last recv operation (ms)
    last_recv_ts?: integer
    // Current send rate (bytes/s)
    send_rate?: integer
    // Current recv rate (bytes/s)
    recv_rate?: integer
  }[]
}
```

### ReplicationSyncStat

```typescript
{
  // Syncs count
  count: integer
  // Average sync time
  avg_time_us: integer
  // Max sync time
  max_time_us: integer
}
```

### GlobalReplicationStats

```typescript
{
  // Total replication stat items count
  total_items?: integer
  items: {
    // Replication type. Either 'async' or 'cluster'
    type: string
    wal_sync: {
      // Syncs count
      count: integer
      // Average sync time
      avg_time_us: integer
      // Max sync time
      max_time_us: integer
    }
    force_sync:ReplicationSyncStat
    initial_sync: {
      wal_sync:ReplicationSyncStat
      force_sync:ReplicationSyncStat
      // Total time of initial sync
      total_time_us: integer
    }
    // Number of online updates waiting to be replicated
    pending_updates_count: integer
    // Number of online updates waiting to be released
    allocated_updates_count: integer
    // Total online updates' size in bytes
    allocated_updates_size: integer
    nodes: {
      // Node's dsn
      dsn: string
      // Node's server id
      server_id: integer
      // Online updates waiting to be replicated to this node
      pending_updates_count: integer
      // Network status
      status: enum[none, offline, online]
      // Replication role
      role: enum[none, follower, leader, candidate]
      // Replication mode for mixed 'sync cluster + async replication' configs
      replication_mode?: enum[default, from_sync_leader]
      // Shows synchronization state for raft-cluster node (false if node is outdated)
      is_synchronized?: boolean
      // Number of namespaces in initial synchronization queue
      queued_namespace_syncs?: integer
      namespaces?: string[]
    }[]
  }[]
}
```

### Databases

```typescript
{
  // Total count of databases
  total_items?: integer
  // Name of database
  items?: string[]
}
```

### Database

```typescript
{
  // Name of database
  name?: string
}
```

### Namespaces

```typescript
{
  items: {
    // Name of namespace
    name?: string
  }[]
  // Total count of namespaces
  total_items?: integer
}
```

### Namespace

```typescript
{
  // Name of namespace
  name?: string
  storage: {
    // If true, then documents will be stored to disc storage, else all data will be lost on server shutdown
    enabled?: boolean
  }
  indexes: {
    // Name of index, can contains letters, digits and underscores
    name: string //default: id
    json_paths?: string //default: id[]
    // Field data type
    field_type: enum[int, int64, double, string, bool, composite, point]
    // Index structure type
    index_type: enum[hash, tree, text, rtree, ttl, -] //default: hash
    // Specify, time to live for ttl index, in seconds
    expire_after?: integer
    // Specifies, that index is primary key. The update operations will checks, that PK field is unique. The namespace MUST have only 1 PK index
    is_pk?: boolean
    // Specifies, that index is array. Array indexes can work with array fields, or work with multiple fields
    is_array?: boolean
    // Reduces the index size. For hash and tree it will save ~8 bytes per unique key value. Useful for indexes with high selectivity, but for tree and hash indexes with low selectivity can seriously decrease update performance;
    is_dense?: boolean
    // Reduces the index size. Allows to save ~(`stored_type_size` * `namespace_items_count`) bytes, where `stored_type_size` is the size of the type stored in the index, and `namespace_items_count` is the number of items in the namespace. May reduce performance;
    is_no_column?: boolean
    // Value of index may not present in the document, and therefore, reduce data size but decreases speed operations on index
    is_sparse?: boolean
    // Algorithm to construct RTree index
    rtree_type?: enum[linear, quadratic, greene, rstar] //default: rstar
    // Use simple tag instead of actual index, which will notice rx about possible field name for strict policies
    is_simple_tag?: boolean
    // String collate mode
    collate_mode?: enum[none, ascii, utf8, numeric] //default: none
    // Sort order letters
    sort_order_letters?: string
    config?: FulltextConfig | FloatVectorConfig
  }[]
}
```

### Index

```typescript
{
  // Name of index, can contains letters, digits and underscores
  name: string //default: id
  json_paths?: string //default: id[]
  // Field data type
  field_type: enum[int, int64, double, string, bool, composite, point]
  // Index structure type
  index_type: enum[hash, tree, text, rtree, ttl, -] //default: hash
  // Specify, time to live for ttl index, in seconds
  expire_after?: integer
  // Specifies, that index is primary key. The update operations will checks, that PK field is unique. The namespace MUST have only 1 PK index
  is_pk?: boolean
  // Specifies, that index is array. Array indexes can work with array fields, or work with multiple fields
  is_array?: boolean
  // Reduces the index size. For hash and tree it will save ~8 bytes per unique key value. Useful for indexes with high selectivity, but for tree and hash indexes with low selectivity can seriously decrease update performance;
  is_dense?: boolean
  // Reduces the index size. Allows to save ~(`stored_type_size` * `namespace_items_count`) bytes, where `stored_type_size` is the size of the type stored in the index, and `namespace_items_count` is the number of items in the namespace. May reduce performance;
  is_no_column?: boolean
  // Value of index may not present in the document, and therefore, reduce data size but decreases speed operations on index
  is_sparse?: boolean
  // Algorithm to construct RTree index
  rtree_type?: enum[linear, quadratic, greene, rstar] //default: rstar
  // Use simple tag instead of actual index, which will notice rx about possible field name for strict policies
  is_simple_tag?: boolean
  // String collate mode
  collate_mode?: enum[none, ascii, utf8, numeric] //default: none
  // Sort order letters
  sort_order_letters?: string
  config?: FulltextConfig | FloatVectorConfig
}
```

### JsonObjectDef

```typescript
{
  required?: string[]
  properties: {
    field1: {
      required?: string[]
      properties: {
        field1:JsonObjectDef
        field2:JsonObjectDef
      }
      items:JsonObjectDef
      // Allow additional fields in this schema level. Allowed for objects only
      additionalProperties?: boolean
      // Entity type
      type?: enum[object, string, number, array]
    }
    field2:JsonObjectDef
  }
  items:JsonObjectDef
  // Allow additional fields in this schema level. Allowed for objects only
  additionalProperties?: boolean
  // Entity type
  type?: enum[object, string, number, array]
}
```

### SchemaDef

```typescript
{
  required?: string[]
  properties: {
    field1: {
      required?: string[]
      properties: {
        field1:JsonObjectDef
        field2:JsonObjectDef
      }
      items:JsonObjectDef
      // Allow additional fields in this schema level. Allowed for objects only
      additionalProperties?: boolean
      // Entity type
      type?: enum[object, string, number, array]
    }
    field2:JsonObjectDef
  }
  items:JsonObjectDef
  // Allow additional fields in this schema level. Allowed for objects only
  additionalProperties?: boolean
  // Entity type
  type?: enum[object, string, number, array]
}
```

### UpdateField

```typescript
{
  // field name
  name: string
  // update entry type
  type?: enum[object, expression, value]
  // is updated value an array
  is_array?: boolean
  values: {
  }[]
}
```

### Query

```typescript
{
  // Namespace name
  namespace: string
  // Type of query
  type?: enum[select, update, delete, truncate]
  // Maximum count of returned items
  limit?: integer
  // Offset of first returned item
  offset?: integer
  // Ask query to calculate total documents, match condition
  req_total?: enum[disabled, enabled, cached] //default: disabled
  // If contains 'filters' then cannot contain 'cond', 'field' and 'value'. If not contains 'filters' then 'field' and 'cond' are required.
  filters: {
    // Expression on the left side of the condition. It may be a field or function.
    left_expression: {
      // Explicit expression type
      type?: enum[field, expression]
      // Field name or function (as expression).
      value?: string
    }
    // Expression on the right side of the condition. It may be a field, function, or value.
    right_expression: {
      // Explicit expression type
      type?: enum[field, expression, values]
      // Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN. Function value as expression. Field name as a string.
      value: {
      }
    }
    // Condition operator
    cond?: enum[EQ, GT, GE, LE, LT, SET, ALLSET, EMPTY, RANGE, LIKE, DWITHIN, KNN]
    // Logic operator
    op?: enum[AND, OR, NOT]
    filters:FilterDef[]
    join_query: {
      // Namespace name
      namespace: string
      // Join type
      type: enum[LEFT, INNER, ORINNER]
      filters:FilterDef[]
      // Specifies results sorting order
      sort: {
        // Field or index name for sorting
        field: string
        values: {
        }[]
        // Descent or ascent sorting direction
        desc?: boolean
      }[]
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      on: {
        // Field from left namespace (main query namespace)
        left_field: string
        // Field from right namespace (joined query namespace)
        right_field: string
        // Condition operator
        cond: enum[EQ, GT, GE, LE, LT, SET]
        // Logic operator
        op?: enum[AND, OR, NOT]
      }[]
      select_filter?: string[]
    }
    // Subquery object. It must contain either 'select_filters' for the single field, single aggregation or must be matched against 'is null'/'is not null conditions'
    subquery: {
      // Namespace name
      namespace: string
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      // Ask query to calculate total documents, match condition
      req_total?: enum[disabled, enabled, cached] //default: disabled
      filters:FilterDef[]
      sort:SortDef[]
      select_filter?: string //default: id[]
      aggregations: {
        fields?: string[]
        // Aggregation function
        type: enum[SUM, AVG, MIN, MAX]
      }[]
    }
    // Array fields to be searched with equal array indexes
    equal_positions: {
      positions?: string[]
    }[]
    // Parameters for knn search
    params: {
      // Maximum count of returned vectors in KNN queries
      k?: integer
      // Raduis for filtering vectors by metric
      radius?: number
      // Applicable for HNSW index only. The size of the dynamic list for the nearest neighbors used during a query. Ef must be >= K. Default value = K
      ef?: integer
      // Applicable for IVF index only. The number of Voronoi cells to search during a query
      nprobe?: integer
    }
    // DEPRECATED. Use left_expression instead. Field json path or index name for filter
    field?: string
    // DEPRECATED. Use right_expression instead. Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN
    value: {
    }
    // DEPRECATED. Use left_expression instead. First field json path or index name for filter by two fields
    first_field?: string
    // DEPRECATED. Use right_expression instead. Second field json path or index name for filter by two fields
    second_field?: string
  }[]
  sort:SortDef[]
  merge_queries: {
    // Namespace name
    namespace: string
    // Type of query
    type?: enum[select, update, delete, truncate]
    // Maximum count of returned items
    limit?: integer
    // Offset of first returned item
    offset?: integer
    // Ask query to calculate total documents, match condition
    req_total?: enum[disabled, enabled, cached] //default: disabled
    filters:FilterDef[]
    sort:SortDef[]
    merge_queries:Query[]
    select_filter?: string //default: id[]
    select_functions?: string[]
    drop_fields?: string[]
    update_fields: {
      // field name
      name: string
      // update entry type
      type?: enum[object, expression, value]
      // is updated value an array
      is_array?: boolean
      values: {
      }[]
    }[]
    aggregations: {
      fields?: string[]
      // Aggregation function
      type: enum[SUM, AVG, MIN, MAX, FACET, DISTINCT]
      // Specifies facet aggregations results sorting order
      sort: {
        // Field or index name for sorting
        field: string
        // Descent or ascent sorting direction
        desc?: boolean
      }[]
      // Number of rows to get from result set. Allowed only for FACET
      limit?: integer
      // Index of the first row to get from result set. Allowed only for FACET
      offset?: integer
    }[]
    // Add query execution explain information
    explain?: boolean
    // Output fulltext or KNN rank in QueryResult. Allowed only with fulltext or KNN queries query
    select_with_rank?: boolean
    // Strict mode for query. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
    strict_mode?: enum[none, names, indexes] //default: names
  }[]
  select_filter?: string //default: id[]
  select_functions?: string[]
  drop_fields?: string[]
  update_fields:UpdateField[]
  aggregations:AggregationsDef[]
  // Add query execution explain information
  explain?: boolean
  // Output fulltext or KNN rank in QueryResult. Allowed only with fulltext or KNN queries query
  select_with_rank?: boolean
  // Strict mode for query. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
  strict_mode?: enum[none, names, indexes] //default: names
}
```

### SubQuery

```typescript
// Subquery object. It must contain either 'select_filters' for the single field, single aggregation or must be matched against 'is null'/'is not null conditions'
{
  // Namespace name
  namespace: string
  // Maximum count of returned items
  limit?: integer
  // Offset of first returned item
  offset?: integer
  // Ask query to calculate total documents, match condition
  req_total?: enum[disabled, enabled, cached] //default: disabled
  // If contains 'filters' then cannot contain 'cond', 'field' and 'value'. If not contains 'filters' then 'field' and 'cond' are required.
  filters: {
    // Expression on the left side of the condition. It may be a field or function.
    left_expression: {
      // Explicit expression type
      type?: enum[field, expression]
      // Field name or function (as expression).
      value?: string
    }
    // Expression on the right side of the condition. It may be a field, function, or value.
    right_expression: {
      // Explicit expression type
      type?: enum[field, expression, values]
      // Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN. Function value as expression. Field name as a string.
      value: {
      }
    }
    // Condition operator
    cond?: enum[EQ, GT, GE, LE, LT, SET, ALLSET, EMPTY, RANGE, LIKE, DWITHIN, KNN]
    // Logic operator
    op?: enum[AND, OR, NOT]
    filters:FilterDef[]
    join_query: {
      // Namespace name
      namespace: string
      // Join type
      type: enum[LEFT, INNER, ORINNER]
      filters:FilterDef[]
      // Specifies results sorting order
      sort: {
        // Field or index name for sorting
        field: string
        values: {
        }[]
        // Descent or ascent sorting direction
        desc?: boolean
      }[]
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      on: {
        // Field from left namespace (main query namespace)
        left_field: string
        // Field from right namespace (joined query namespace)
        right_field: string
        // Condition operator
        cond: enum[EQ, GT, GE, LE, LT, SET]
        // Logic operator
        op?: enum[AND, OR, NOT]
      }[]
      select_filter?: string[]
    }
    // Subquery object. It must contain either 'select_filters' for the single field, single aggregation or must be matched against 'is null'/'is not null conditions'
    subquery: {
      // Namespace name
      namespace: string
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      // Ask query to calculate total documents, match condition
      req_total?: enum[disabled, enabled, cached] //default: disabled
      filters:FilterDef[]
      sort:SortDef[]
      select_filter?: string //default: id[]
      aggregations: {
        fields?: string[]
        // Aggregation function
        type: enum[SUM, AVG, MIN, MAX]
      }[]
    }
    // Array fields to be searched with equal array indexes
    equal_positions: {
      positions?: string[]
    }[]
    // Parameters for knn search
    params: {
      // Maximum count of returned vectors in KNN queries
      k?: integer
      // Raduis for filtering vectors by metric
      radius?: number
      // Applicable for HNSW index only. The size of the dynamic list for the nearest neighbors used during a query. Ef must be >= K. Default value = K
      ef?: integer
      // Applicable for IVF index only. The number of Voronoi cells to search during a query
      nprobe?: integer
    }
    // DEPRECATED. Use left_expression instead. Field json path or index name for filter
    field?: string
    // DEPRECATED. Use right_expression instead. Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN
    value: {
    }
    // DEPRECATED. Use left_expression instead. First field json path or index name for filter by two fields
    first_field?: string
    // DEPRECATED. Use right_expression instead. Second field json path or index name for filter by two fields
    second_field?: string
  }[]
  sort:SortDef[]
  select_filter?: string //default: id[]
  aggregations:SubQueryAggregationsDef[]
}
```

### EqualPositionDef

```typescript
// Array fields to be searched with equal array indexes
{
  positions?: string[]
}
```

### FilterDef

```typescript
// If contains 'filters' then cannot contain 'cond', 'field' and 'value'. If not contains 'filters' then 'field' and 'cond' are required.
{
  // Expression on the left side of the condition. It may be a field or function.
  left_expression: {
    // Explicit expression type
    type?: enum[field, expression]
    // Field name or function (as expression).
    value?: string
  }
  // Expression on the right side of the condition. It may be a field, function, or value.
  right_expression: {
    // Explicit expression type
    type?: enum[field, expression, values]
    // Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN. Function value as expression. Field name as a string.
    value: {
    }
  }
  // Condition operator
  cond?: enum[EQ, GT, GE, LE, LT, SET, ALLSET, EMPTY, RANGE, LIKE, DWITHIN, KNN]
  // Logic operator
  op?: enum[AND, OR, NOT]
  // If contains 'filters' then cannot contain 'cond', 'field' and 'value'. If not contains 'filters' then 'field' and 'cond' are required.
  filters: {
    // Expression on the left side of the condition. It may be a field or function.
    left_expression: {
      // Explicit expression type
      type?: enum[field, expression]
      // Field name or function (as expression).
      value?: string
    }
    // Expression on the right side of the condition. It may be a field, function, or value.
    right_expression: {
      // Explicit expression type
      type?: enum[field, expression, values]
      // Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN. Function value as expression. Field name as a string.
      value: {
      }
    }
    // Condition operator
    cond?: enum[EQ, GT, GE, LE, LT, SET, ALLSET, EMPTY, RANGE, LIKE, DWITHIN, KNN]
    // Logic operator
    op?: enum[AND, OR, NOT]
    filters:FilterDef[]
    join_query: {
      // Namespace name
      namespace: string
      // Join type
      type: enum[LEFT, INNER, ORINNER]
      filters:FilterDef[]
      // Specifies results sorting order
      sort: {
        // Field or index name for sorting
        field: string
        values: {
        }[]
        // Descent or ascent sorting direction
        desc?: boolean
      }[]
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      on: {
        // Field from left namespace (main query namespace)
        left_field: string
        // Field from right namespace (joined query namespace)
        right_field: string
        // Condition operator
        cond: enum[EQ, GT, GE, LE, LT, SET]
        // Logic operator
        op?: enum[AND, OR, NOT]
      }[]
      select_filter?: string[]
    }
    // Subquery object. It must contain either 'select_filters' for the single field, single aggregation or must be matched against 'is null'/'is not null conditions'
    subquery: {
      // Namespace name
      namespace: string
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      // Ask query to calculate total documents, match condition
      req_total?: enum[disabled, enabled, cached] //default: disabled
      filters:FilterDef[]
      sort:SortDef[]
      select_filter?: string //default: id[]
      aggregations: {
        fields?: string[]
        // Aggregation function
        type: enum[SUM, AVG, MIN, MAX]
      }[]
    }
    // Array fields to be searched with equal array indexes
    equal_positions: {
      positions?: string[]
    }[]
    // Parameters for knn search
    params: {
      // Maximum count of returned vectors in KNN queries
      k?: integer
      // Raduis for filtering vectors by metric
      radius?: number
      // Applicable for HNSW index only. The size of the dynamic list for the nearest neighbors used during a query. Ef must be >= K. Default value = K
      ef?: integer
      // Applicable for IVF index only. The number of Voronoi cells to search during a query
      nprobe?: integer
    }
    // DEPRECATED. Use left_expression instead. Field json path or index name for filter
    field?: string
    // DEPRECATED. Use right_expression instead. Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN
    value: {
    }
    // DEPRECATED. Use left_expression instead. First field json path or index name for filter by two fields
    first_field?: string
    // DEPRECATED. Use right_expression instead. Second field json path or index name for filter by two fields
    second_field?: string
  }[]
  join_query:JoinedDef
  subquery:SubQuery
  equal_positions:EqualPositionDef[]
  params:KnnSearchParamsDef
  // DEPRECATED. Use left_expression instead. Field json path or index name for filter
  field?: string
  // DEPRECATED. Use right_expression instead. Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN
  value: {
  }
  // DEPRECATED. Use left_expression instead. First field json path or index name for filter by two fields
  first_field?: string
  // DEPRECATED. Use right_expression instead. Second field json path or index name for filter by two fields
  second_field?: string
}
```

### KnnSearchParamsDef

```typescript
// Parameters for knn search
{
  // Maximum count of returned vectors in KNN queries
  k?: integer
  // Raduis for filtering vectors by metric
  radius?: number
  // Applicable for HNSW index only. The size of the dynamic list for the nearest neighbors used during a query. Ef must be >= K. Default value = K
  ef?: integer
  // Applicable for IVF index only. The number of Voronoi cells to search during a query
  nprobe?: integer
}
```

### SortDef

```typescript
// Specifies results sorting order
{
  // Field or index name for sorting
  field: string
  values: {
  }[]
  // Descent or ascent sorting direction
  desc?: boolean
}
```

### JoinedDef

```typescript
{
  // Namespace name
  namespace: string
  // Join type
  type: enum[LEFT, INNER, ORINNER]
  // If contains 'filters' then cannot contain 'cond', 'field' and 'value'. If not contains 'filters' then 'field' and 'cond' are required.
  filters: {
    // Expression on the left side of the condition. It may be a field or function.
    left_expression: {
      // Explicit expression type
      type?: enum[field, expression]
      // Field name or function (as expression).
      value?: string
    }
    // Expression on the right side of the condition. It may be a field, function, or value.
    right_expression: {
      // Explicit expression type
      type?: enum[field, expression, values]
      // Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN. Function value as expression. Field name as a string.
      value: {
      }
    }
    // Condition operator
    cond?: enum[EQ, GT, GE, LE, LT, SET, ALLSET, EMPTY, RANGE, LIKE, DWITHIN, KNN]
    // Logic operator
    op?: enum[AND, OR, NOT]
    filters:FilterDef[]
    join_query: {
      // Namespace name
      namespace: string
      // Join type
      type: enum[LEFT, INNER, ORINNER]
      filters:FilterDef[]
      // Specifies results sorting order
      sort: {
        // Field or index name for sorting
        field: string
        values: {
        }[]
        // Descent or ascent sorting direction
        desc?: boolean
      }[]
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      on: {
        // Field from left namespace (main query namespace)
        left_field: string
        // Field from right namespace (joined query namespace)
        right_field: string
        // Condition operator
        cond: enum[EQ, GT, GE, LE, LT, SET]
        // Logic operator
        op?: enum[AND, OR, NOT]
      }[]
      select_filter?: string[]
    }
    // Subquery object. It must contain either 'select_filters' for the single field, single aggregation or must be matched against 'is null'/'is not null conditions'
    subquery: {
      // Namespace name
      namespace: string
      // Maximum count of returned items
      limit?: integer
      // Offset of first returned item
      offset?: integer
      // Ask query to calculate total documents, match condition
      req_total?: enum[disabled, enabled, cached] //default: disabled
      filters:FilterDef[]
      sort:SortDef[]
      select_filter?: string //default: id[]
      aggregations: {
        fields?: string[]
        // Aggregation function
        type: enum[SUM, AVG, MIN, MAX]
      }[]
    }
    // Array fields to be searched with equal array indexes
    equal_positions: {
      positions?: string[]
    }[]
    // Parameters for knn search
    params: {
      // Maximum count of returned vectors in KNN queries
      k?: integer
      // Raduis for filtering vectors by metric
      radius?: number
      // Applicable for HNSW index only. The size of the dynamic list for the nearest neighbors used during a query. Ef must be >= K. Default value = K
      ef?: integer
      // Applicable for IVF index only. The number of Voronoi cells to search during a query
      nprobe?: integer
    }
    // DEPRECATED. Use left_expression instead. Field json path or index name for filter
    field?: string
    // DEPRECATED. Use right_expression instead. Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5], 5.0]' for DWITHIN, or float vector for KNN
    value: {
    }
    // DEPRECATED. Use left_expression instead. First field json path or index name for filter by two fields
    first_field?: string
    // DEPRECATED. Use right_expression instead. Second field json path or index name for filter by two fields
    second_field?: string
  }[]
  sort:SortDef[]
  // Maximum count of returned items
  limit?: integer
  // Offset of first returned item
  offset?: integer
  on:OnDef[]
  select_filter?: string[]
}
```

### OnDef

```typescript
{
  // Field from left namespace (main query namespace)
  left_field: string
  // Field from right namespace (joined query namespace)
  right_field: string
  // Condition operator
  cond: enum[EQ, GT, GE, LE, LT, SET]
  // Logic operator
  op?: enum[AND, OR, NOT]
}
```

### AggregationsDef

```typescript
{
  fields?: string[]
  // Aggregation function
  type: enum[SUM, AVG, MIN, MAX, FACET, DISTINCT]
  // Specifies facet aggregations results sorting order
  sort: {
    // Field or index name for sorting
    field: string
    // Descent or ascent sorting direction
    desc?: boolean
  }[]
  // Number of rows to get from result set. Allowed only for FACET
  limit?: integer
  // Index of the first row to get from result set. Allowed only for FACET
  offset?: integer
}
```

### SubQueryAggregationsDef

```typescript
{
  fields?: string[]
  // Aggregation function
  type: enum[SUM, AVG, MIN, MAX]
}
```

### AggregationsSortDef

```typescript
// Specifies facet aggregations results sorting order
{
  // Field or index name for sorting
  field: string
  // Descent or ascent sorting direction
  desc?: boolean
}
```

### FtStopWordObject

```typescript
{
  // Stop word
  word?: string
  // If the value is true, the word can be included in search results in queries such as 'word*', 'word~' etc.
  is_morpheme?: boolean
}
```

### FloatVectorConfig

```typescript
// Float Vector Index configuration
{
  // Dimension of vectors in index
  dimension: integer
  metric: enum[l2, inner_product, cosine]
  // Start size of index. Allowed for HNSW and Brute Force indexes only.
  start_size?: integer //default: 1000
  // Number of bi-directional links created for every new element during construction. Allowed for HNSW indexes only.
  m?: integer //default: 16
  // Size of dynamic list for the nearest neighbors used during construction. Allowed for HNSW indexes only.
  ef_construction?: integer //default: 200
  // Multithread insertions. Allowed for HNSW indexes only.
  multithreading?: enum[0, 1]
  // Clusters count for construct IFV index. Required for IVF indexes. Allowed for IVF indexes only.
  centroids_count?: integer
  // Raduis for filtering vectors by metric in queries
  radius?: number
  // Quantization config. Supported only for HNSW-index
  quantization_config: {
    // Type of the quantization. Currently, only 8-bit scalar quantization is supported
    quantization_type?: enum[scalar_quantization_8_bit]
    // Quantile used to determine the clipping range for vector components during quantization. The value is automatically computed based on vector dimensionality. Changing it is recommended only if you understand the distribution of vector values and need to tune recall
    quantile?: number
    // Number of vectors sampled from the index to estimate the min/max range (with quantile clipping) used for quantization
    sample_size?: integer //default: 20000
    // Minimum number of vectors in the index required to trigger background quantization
    quantization_threshold?: integer //default: 100000
  }
  // Embedding configuration
  embedding: {
    // Upsert embedding configuration
    upsert_embedder: {
      // Embedder name. Optional
      name?: string
      // Embed service URL
      URL: string
      // tag is used to cache results of insertion
      cache_tag?: string
      fields?: string[]
      // Embedding application strategy
      embedding_strategy?: enum[always, empty_only, strict] //default: always
      // Connection pool configuration
      pool: {
        // Number connections to service
        connections?: integer //default: 10
        // Connection/reconnection timeout to any embedding service (milliseconds)
        connect_timeout_ms?: integer //default: 300
        // Timeout reading data from embedding service (milliseconds)
        read_timeout_ms?: integer //default: 5000
        // Timeout writing data from embedding service (milliseconds)
        write_timeout_ms?: integer //default: 5000
      }
    }
    // Query embedding configuration
    query_embedder: {
      // Embed service URL
      URL: string
      // tag is used to cache results of insertion
      cache_tag?: string
      // Connection pool configuration
      pool: {
        // Number connections to service
        connections?: integer //default: 10
        // Connection/reconnection timeout to any embedding service (milliseconds)
        connect_timeout_ms?: integer //default: 300
        // Timeout reading data from embedding service (milliseconds)
        read_timeout_ms?: integer //default: 5000
        // Timeout writing data from embedding service (milliseconds)
        write_timeout_ms?: integer //default: 5000
      }
    }
  }
}
```

### FulltextConfig

```typescript
// Fulltext Index configuration
{
  // Enable search of concatenated adjacent terms. e.g. terms 'di caprio' will match word 'dicaprio'
  enable_terms_concat?: boolean //default: true
  // Enable russian translit variants processing. e.g. term 'luntik' will match word 'лунтик'
  enable_translit?: boolean //default: true
  // Enable number variants processing. e.g. term '100' will match words one hundred
  enable_numbers_search?: boolean
  // Enable wrong keyboard layout variants processing. e.g. term 'keynbr' will match word 'лунтик'
  enable_kb_layout?: boolean //default: true
  // Log level of full text search engine
  log_level?: integer
  // Maximum documents count which will be processed in merge query results. Increasing this value may refine ranking of queries with high frequency words, but will decrease search speed
  merge_limit?: integer
  // List of symbols, which will be treated as word part delimiters
  word_part_delimiters?: string //default: -/+_`'
  // Min word part size for indexing and searching
  min_word_part_size?: integer //default: 3
  // List of symbols, which will be treated as word part, all other symbols will be treated as word separators, extra_word_symbols will be replenished with word_part_delimiters automatically at startup
  extra_word_symbols?: string //default: -/+_`'
  stop_words: {
    // Stop word
    word?: string
    // If the value is true, the word can be included in search results in queries such as 'word*', 'word~' etc.
    is_morpheme?: boolean
  }[]
  keep_diacritics?: string[]
  stemmers?: string[]
  // Fulltext synonym definition
  synonyms: {
    tokens?: string[]
    alternatives?: string[]
  }[]
  // Fulltext terms boost definition
  terms_boost: {
    terms?: string[]
    // Rank multiplier for boosted terms
    boost?: number
  }[]
  // Boost of bm25 ranking
  bm25_boost?: number //default: 1
  // Weight of bm25 rank in final rank 0: bm25 will not change final rank. 1: bm25 will affect to final rank in 0 - 100% range
  bm25_weight?: number //default: 0.1
  // Boost of search query term distance in found document
  distance_boost?: number //default: 1
  // Weight of search query terms distance in found document in final rank 0: distance will not change final rank. 1: distance will affect to final rank in 0 - 100% range
  distance_weight?: number //default: 0.5
  // Boost of search query term length
  term_len_boost?: number //default: 1
  // Weight of search query term length in final rank. 0: term length will not change final rank. 1: term length will affect to final rank in 0 - 100% range
  term_len_weight?: number //default: 0.3
  // Boost of search query term position
  position_boost?: number //default: 1
  // Weight of search query term position in final rank. 0: term position will not change final rank. 1: term position will affect to final rank in 0 - 100% range
  position_weight?: number //default: 0.1
  // Boost of full match of search phrase with doc
  full_match_boost?: number //default: 1.1
  // Decrease of relevancy in case of partial match by value: partial_match_decrease * (non matched symbols) / (matched symbols)
  partial_match_decrease?: integer
  // DEPRECATED. Minimum relevancy of found documents. 0: all found documents will be returned 1: only documents with rank >= 100 will be returned 
  min_relevancy?: number //default: 0.05
  // Minimum rank of found documents. 0: all found documents will be returned 255: only documents with rank = 255 will be returned 
  min_rank?: number //default: 5
  // Maximum possible typos in word. 0: typos is disabled, words with typos will not match. N: words with N possible typos will match. It is not recommended to set more than 2 possible typo -It will seriously increase RAM usage, and decrease search speed
  max_typos?: integer
  // Maximum word length for building and matching variants with typos.
  max_typo_len?: integer
  // Config for more precise typos algorithm tuning
  typos_detailed_config: {
    // Maximum distance between symbols in initial and target words to perform substitution
    max_typo_distance?: integer
    // Maximum distance between same symbols in initial and target words to perform substitution (to handle cases, when two symbols were switched with each other)
    max_symbol_permutation_distance?: integer
    // Maximum number of symbols, which may be removed from the initial term to transform it into the result word
    max_missing_letters?: integer
    // Maximum number of symbols, which may be added to the initial term to transform it into the result word
    max_extra_letters?: integer
  }
  // Maximum steps without full rebuild of ft - more steps faster commit slower select - optimal about 15.
  max_rebuild_steps?: integer
  // Maximum unique words to step
  max_step_size?: integer
  // Ratio to summation of ranks of match one term in several fields. For example, if value of this ratio is K, request is '@+f1,+f2,+f3 word', ranks of match in fields are R1, R2, R3 and R2 < R1 < R3, final rank will be R = R2 + K*R1 + K*K*R3
  sum_ranks_by_fields_ratio?: number
  // Optimize the index by memory or by cpu
  optimization?: enum[Memory, CPU] //default: Memory
  // Enable to execute others queries before the ft query
  enable_preselect_before_ft?: boolean
  // Max number of highlighted areas for each field in each document (for snippet() and highlight()). '-1' means unlimited
  max_areas_in_doc?: number
  // Max total number of highlighted areas in ft result, when result still remains cacheable. '-1' means unlimited
  max_total_areas_to_cache?: number
  // Config for document ranking function
  bm25_config: {
    // Coefficient k1 in the formula for calculating bm25. Coefficient that sets the saturation threshold for the frequency of the term. The higher the coefficient, the higher the threshold and the lower the saturation rate.
    bm25_k1?: number //default: 2
    // Coefficient b in the formula for calculating bm25. If b is bigger, the effects of the length of the document compared to the average length are more amplified.
    bm25_b?: number //default: 0.75
    // Formula for calculating document relevance (rx_bm25, bm25, word_count)
    bm25_type?: enum[rx_bm25, bm25, word_count] //default: rx_bm25
  }
  // Configuration for certain field if it differ from whole index configuration
  fields: {
    // Field name
    field_name?: string
    // Boost of bm25 ranking
    bm25_boost?: number //default: 1
    // Weight of bm25 rank in final rank 0: bm25 will not change final rank. 1: bm25 will affect to final rank in 0 - 100% range
    bm25_weight?: number //default: 0.1
    // Boost of search query term length
    term_len_boost?: number //default: 1
    // Weight of search query term length in final rank. 0: term length will not change final rank. 1: term length will affect to final rank in 0 - 100% range
    term_len_weight?: number //default: 0.3
    // Boost of search query term position
    position_boost?: number //default: 1
    // Weight of search query term position in final rank. 0: term position will not change final rank. 1: term position will affect to final rank in 0 - 100% range
    position_weight?: number //default: 0.1
  }[]
  // Config for subterm proc rank.
  base_ranking: {
    // Relevancy of full word match
    full_match_proc?: integer
    // Base relevancy of concatenated terms match
    concat_proc?: integer
    // Minimum relevancy of prefix word match
    prefix_min_proc?: integer
    // Minimum relevancy of suffix word match
    suffix_min_proc?: integer
    // Base relevancy of typo match
    base_typo_proc?: integer
    // Extra penalty for each word's permutation (addition/deletion of the symbol) in typo algorithm
    typo_proc_penalty?: integer
    // Penalty for the variants, created by stemming
    stemmer_proc_penalty?: integer
    // Relevancy of the match in incorrect kblayout
    kblayout_proc?: integer
    // Relevancy of the match in translit
    translit_proc?: integer
    // Relevancy of the synonym match
    synonyms_proc?: integer
    // Relevancy of the delimited word part match
    delimited_proc?: integer
  }
  // Text tokenization algorithm. 'fast' - splits text by spaces, special characters and unsupported UTF-8 symbols. Each token is a combination of letters from supported UTF-8 subset, numbers and extra word symbols. 'mmseg_cn' - algorithm based on friso implementation of mmseg for Chinese and English
  splitter?: enum[fast, mmseg_cn] //default: fast
}
```

### FulltextFieldConfig

```typescript
// Configuration for certain field if it differ from whole index configuration
{
  // Field name
  field_name?: string
  // Boost of bm25 ranking
  bm25_boost?: number //default: 1
  // Weight of bm25 rank in final rank 0: bm25 will not change final rank. 1: bm25 will affect to final rank in 0 - 100% range
  bm25_weight?: number //default: 0.1
  // Boost of search query term length
  term_len_boost?: number //default: 1
  // Weight of search query term length in final rank. 0: term length will not change final rank. 1: term length will affect to final rank in 0 - 100% range
  term_len_weight?: number //default: 0.3
  // Boost of search query term position
  position_boost?: number //default: 1
  // Weight of search query term position in final rank. 0: term position will not change final rank. 1: term position will affect to final rank in 0 - 100% range
  position_weight?: number //default: 0.1
}
```

### FulltextSynonym

```typescript
// Fulltext synonym definition
{
  tokens?: string[]
  alternatives?: string[]
}
```

### FulltextTermsBoost

```typescript
// Fulltext terms boost definition
{
  terms?: string[]
  // Rank multiplier for boosted terms
  boost?: number
}
```

### MetaInfo

```typescript
// Meta info to be set
{
  key: string
  value: string
}
```

### MetaListResponse

```typescript
// List of meta info of the specified namespace
{
  // Total count of meta info in the namespace
  total_items: integer
  meta: {
    key: string
    // Optional: Provided if 'with_values' = true
    value?: string
  }[]
}
```

### MetaByKeyResponse

```typescript
// Meta info of the specified namespace
{
  key: string
  value: string
}
```

### Items

```typescript
{
  // Total count of documents, matched specified filters
  total_items?: integer
  items: {
  }[]
}
```

### SuggestItems

```typescript
{
  suggests?: string[]
}
```

### QueryItems

```typescript
{
  items: {
  }[]
  namespaces?: string[]
  // Enables to client cache returned items. If false, then returned items has been modified  by reindexer, e.g. by select filter, or by functions, and can't be cached
  cache_enabled?: boolean
  // Total count of documents, matched query
  query_total_items?: integer
  aggregations: {
    fields?: string[]
    // Aggregation function
    type: enum[SUM, AVG, MIN, MAX, FACET, DISTINCT]
    // Value, calculated by aggregator
    value?: number
    facets: {
      values?: string[]
      // Count of elements these fields values
      count: integer
    }[]
    distincts?: DistincOneItemDef | DistinctMultiItemDef[]
  }[]
  equal_position?: string[]
  // Query columns for table outputs
  columns: {
    // Column name
    name?: string
    // Column width in percents of total width
    width_percents?: number
    // Column width in chars
    width_chars?: number
    // Maximum count of chars in column
    max_chars?: number
  }[]
  explain?: SingleQueryExplainDef | MergedQueryExplainDef
}
```

### Indexes

```typescript
{
  items: {
    // Name of index, can contains letters, digits and underscores
    name: string //default: id
    json_paths?: string //default: id[]
    // Field data type
    field_type: enum[int, int64, double, string, bool, composite, point]
    // Index structure type
    index_type: enum[hash, tree, text, rtree, ttl, -] //default: hash
    // Specify, time to live for ttl index, in seconds
    expire_after?: integer
    // Specifies, that index is primary key. The update operations will checks, that PK field is unique. The namespace MUST have only 1 PK index
    is_pk?: boolean
    // Specifies, that index is array. Array indexes can work with array fields, or work with multiple fields
    is_array?: boolean
    // Reduces the index size. For hash and tree it will save ~8 bytes per unique key value. Useful for indexes with high selectivity, but for tree and hash indexes with low selectivity can seriously decrease update performance;
    is_dense?: boolean
    // Reduces the index size. Allows to save ~(`stored_type_size` * `namespace_items_count`) bytes, where `stored_type_size` is the size of the type stored in the index, and `namespace_items_count` is the number of items in the namespace. May reduce performance;
    is_no_column?: boolean
    // Value of index may not present in the document, and therefore, reduce data size but decreases speed operations on index
    is_sparse?: boolean
    // Algorithm to construct RTree index
    rtree_type?: enum[linear, quadratic, greene, rstar] //default: rstar
    // Use simple tag instead of actual index, which will notice rx about possible field name for strict policies
    is_simple_tag?: boolean
    // String collate mode
    collate_mode?: enum[none, ascii, utf8, numeric] //default: none
    // Sort order letters
    sort_order_letters?: string
    config?: FulltextConfig | FloatVectorConfig
  }[]
  // Total count of indexes
  total_items?: integer
}
```

### ExplainDef

```typescript
undefined?: SingleQueryExplainDef | MergedQueryExplainDef
```

### SingleQueryExplainDef

```typescript
// Explanations of query execution
{
  // Namespace name
  namespace?: string
  // Total query execution time
  total_us?: integer
  // Intersection loop time
  loop_us?: integer
  // Indexes keys selection time
  indexes_us?: integer
  // Query post process time
  postprocess_us?: integer
  // Query preselect processing time
  preselect_us?: integer
  // Query prepare and optimize time
  prepare_us?: integer
  // Result sort time
  general_sort_us?: integer
  // Index, which used for sort results
  sort_index?: string
  // Optimization of sort by uncompleted index has been performed
  sort_by_uncommitted_index?: boolean
  selectors: {
    // Method, used to process condition
    method?: enum[scan, index, inner_join, left_join]
    // Field or index name
    field?: string
    // Shows which kind of the field was used for the filtration. Non-indexed fields are usually really slow for 'scan' and should be avoided
    field_type?: enum[non-indexed, indexed]
    // Count of scanned documents by this selector
    items?: integer
    // Count of processed documents, matched this selector
    matched?: integer
    // Count of comparators used, for this selector
    comparators?: integer
    // Cost expectation of this selector
    cost?: integer
    // Number of uniq keys, processed by this selector (may be incorrect, in case of internal query optimization/caching
    keys?: integer
    // Condition on the field
    condition?: string
    // Select iterator type
    type?: enum[Comparator, TwoFieldsComparison, Skipped, Forward, Reverse, SingleRange, SingleIdset, SingleIdSetWithDeferedSort, RevSingleRange, RevSingleIdset, RevSingleIdSetWithDeferedSort, OnlyComparator, Unsorted, UnbuiltSortOrdersIndex]
    // Description of the selector
    description?: string
    // Explanations of query execution
    explain_preselect: {
      // Namespace name
      namespace?: string
      // Total query execution time
      total_us?: integer
      // Intersection loop time
      loop_us?: integer
      // Indexes keys selection time
      indexes_us?: integer
      // Query post process time
      postprocess_us?: integer
      // Query preselect processing time
      preselect_us?: integer
      // Query prepare and optimize time
      prepare_us?: integer
      // Result sort time
      general_sort_us?: integer
      // Index, which used for sort results
      sort_index?: string
      // Optimization of sort by uncompleted index has been performed
      sort_by_uncommitted_index?: boolean
      selectors: {
        // Method, used to process condition
        method?: enum[scan, index, inner_join, left_join]
        // Field or index name
        field?: string
        // Shows which kind of the field was used for the filtration. Non-indexed fields are usually really slow for 'scan' and should be avoided
        field_type?: enum[non-indexed, indexed]
        // Count of scanned documents by this selector
        items?: integer
        // Count of processed documents, matched this selector
        matched?: integer
        // Count of comparators used, for this selector
        comparators?: integer
        // Cost expectation of this selector
        cost?: integer
        // Number of uniq keys, processed by this selector (may be incorrect, in case of internal query optimization/caching
        keys?: integer
        // Condition on the field
        condition?: string
        // Select iterator type
        type?: enum[Comparator, TwoFieldsComparison, Skipped, Forward, Reverse, SingleRange, SingleIdset, SingleIdSetWithDeferedSort, RevSingleRange, RevSingleIdset, RevSingleIdSetWithDeferedSort, OnlyComparator, Unsorted, UnbuiltSortOrdersIndex]
        // Description of the selector
        description?: string
        explain_preselect:SingleQueryExplainDef
        explain_select:SingleQueryExplainDef
      }[]
      on_conditions_insertions: {
        // Joinable ns name
        namespace?: string
        // Original ON-conditions clause. SQL-like string
        on_condition?: string
        // Total amount of time spent on checking and substituting all conditions
        total_time_us?: integer
        // Result of insertion attempt
        success?: boolean
        // Optional{succeed==false}. Explains condition insertion failure
        reason?: string
        // Values source: preselect values(by_value) or additional select(select)
        type?: string
        // Inserted condition. SQL-like string
        inserted_condition?: string
        conditions: {
          // single condition from Join ON section. SQL-like string
          condition?: string
          // total time elapsed from insertion attempt start till the end of substitution or rejection
          total_time_us?: integer
          explain_select:SingleQueryExplainDef
          // Optional. Aggregation type used in subquery
          agg_type?: enum[min, max, distinct]
          // result of insertion attempt
          success?: boolean
          // Optional. Explains condition insertion failure
          reason?: string
          // substituted inserted condition. SQL-like string
          new_condition?: string
          // resulting size of query values set
          values_count?: integer
        }[]
      }[]
      subqueries: {
        // Subquery's namespace name
        namespace?: string
        // Count of keys being compared with the subquery's result
        keys?: integer
        // Name of field being compared with the subquery's result
        field?: string
        explain:SingleQueryExplainDef
      }[]
    }
    explain_select:SingleQueryExplainDef
  }[]
  on_conditions_insertions: {
    // Joinable ns name
    namespace?: string
    // Original ON-conditions clause. SQL-like string
    on_condition?: string
    // Total amount of time spent on checking and substituting all conditions
    total_time_us?: integer
    // Result of insertion attempt
    success?: boolean
    // Optional{succeed==false}. Explains condition insertion failure
    reason?: string
    // Values source: preselect values(by_value) or additional select(select)
    type?: string
    // Inserted condition. SQL-like string
    inserted_condition?: string
    conditions: {
      // single condition from Join ON section. SQL-like string
      condition?: string
      // total time elapsed from insertion attempt start till the end of substitution or rejection
      total_time_us?: integer
      explain_select:SingleQueryExplainDef
      // Optional. Aggregation type used in subquery
      agg_type?: enum[min, max, distinct]
      // result of insertion attempt
      success?: boolean
      // Optional. Explains condition insertion failure
      reason?: string
      // substituted inserted condition. SQL-like string
      new_condition?: string
      // resulting size of query values set
      values_count?: integer
    }[]
  }[]
  subqueries: {
    // Subquery's namespace name
    namespace?: string
    // Count of keys being compared with the subquery's result
    keys?: integer
    // Name of field being compared with the subquery's result
    field?: string
    explain:SingleQueryExplainDef
  }[]
}
```

### MergedQueryExplainDef

```typescript
{
  // Namespace name
  namespace?: string
  // Total query execution time
  total_us?: integer
  // Intersection loop time (includes loop_us of all merged queries)
  loop_us?: integer
  // Indexes keys selection time (includes indexes_us of all merged queries)
  indexes_us?: integer
  // Query post process time (includes postprocess_us of all merged queries)
  postprocess_us?: integer
  // Query preselect processing time (includes preselect_us of all merged queries)
  preselect_us?: integer
  // Query prepare and optimize time (includes prepare_us of all merged queries)
  prepare_us?: integer
  // Result sort time (includes indexes_us of all merged queries and post-merge sort time of merged result)
  general_sort_us?: integer
  // Index, which used for sort results
  sort_index?: string
  // Explanations of query execution
  merged: {
    // Namespace name
    namespace?: string
    // Total query execution time
    total_us?: integer
    // Intersection loop time
    loop_us?: integer
    // Indexes keys selection time
    indexes_us?: integer
    // Query post process time
    postprocess_us?: integer
    // Query preselect processing time
    preselect_us?: integer
    // Query prepare and optimize time
    prepare_us?: integer
    // Result sort time
    general_sort_us?: integer
    // Index, which used for sort results
    sort_index?: string
    // Optimization of sort by uncompleted index has been performed
    sort_by_uncommitted_index?: boolean
    selectors: {
      // Method, used to process condition
      method?: enum[scan, index, inner_join, left_join]
      // Field or index name
      field?: string
      // Shows which kind of the field was used for the filtration. Non-indexed fields are usually really slow for 'scan' and should be avoided
      field_type?: enum[non-indexed, indexed]
      // Count of scanned documents by this selector
      items?: integer
      // Count of processed documents, matched this selector
      matched?: integer
      // Count of comparators used, for this selector
      comparators?: integer
      // Cost expectation of this selector
      cost?: integer
      // Number of uniq keys, processed by this selector (may be incorrect, in case of internal query optimization/caching
      keys?: integer
      // Condition on the field
      condition?: string
      // Select iterator type
      type?: enum[Comparator, TwoFieldsComparison, Skipped, Forward, Reverse, SingleRange, SingleIdset, SingleIdSetWithDeferedSort, RevSingleRange, RevSingleIdset, RevSingleIdSetWithDeferedSort, OnlyComparator, Unsorted, UnbuiltSortOrdersIndex]
      // Description of the selector
      description?: string
      explain_preselect:SingleQueryExplainDef
      explain_select:SingleQueryExplainDef
    }[]
    on_conditions_insertions: {
      // Joinable ns name
      namespace?: string
      // Original ON-conditions clause. SQL-like string
      on_condition?: string
      // Total amount of time spent on checking and substituting all conditions
      total_time_us?: integer
      // Result of insertion attempt
      success?: boolean
      // Optional{succeed==false}. Explains condition insertion failure
      reason?: string
      // Values source: preselect values(by_value) or additional select(select)
      type?: string
      // Inserted condition. SQL-like string
      inserted_condition?: string
      conditions: {
        // single condition from Join ON section. SQL-like string
        condition?: string
        // total time elapsed from insertion attempt start till the end of substitution or rejection
        total_time_us?: integer
        explain_select:SingleQueryExplainDef
        // Optional. Aggregation type used in subquery
        agg_type?: enum[min, max, distinct]
        // result of insertion attempt
        success?: boolean
        // Optional. Explains condition insertion failure
        reason?: string
        // substituted inserted condition. SQL-like string
        new_condition?: string
        // resulting size of query values set
        values_count?: integer
      }[]
    }[]
    subqueries: {
      // Subquery's namespace name
      namespace?: string
      // Count of keys being compared with the subquery's result
      keys?: integer
      // Name of field being compared with the subquery's result
      field?: string
      explain:SingleQueryExplainDef
    }[]
  }[]
}
```

### AggregationResDef

```typescript
{
  fields?: string[]
  // Aggregation function
  type: enum[SUM, AVG, MIN, MAX, FACET, DISTINCT]
  // Value, calculated by aggregator
  value?: number
  facets: {
    values?: string[]
    // Count of elements these fields values
    count: integer
  }[]
  distincts?: DistincOneItemDef | DistinctMultiItemDef[]
}
```

### DistincOneItemDef

```typescript
string
```

### DistinctMultiItemDef

```typescript
string[]
```

### QueryColumnDef

```typescript
// Query columns for table outputs
{
  // Column name
  name?: string
  // Column width in percents of total width
  width_percents?: number
  // Column width in chars
  width_chars?: number
  // Maximum count of chars in column
  max_chars?: number
}
```

### StatusResponse

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

### ItemsUpdateResponse

```typescript
{
  // Count of updated items
  updated?: integer
  items: {
  }[]
}
```

### UpdateResponse

```typescript
{
  // Count of updated items
  updated?: integer
}
```

### DatabaseMemStats

```typescript
{
  // Total count of documents, matched specified filters
  total_items?: integer
  items: {
    // Name of namespace
    name?: string
    // Type of namespace. For now it's 'namespace' or 'embedders'
    type?: string
    // Total count of documents in namespace
    items_count?: integer
    // Size of strings deleted from namespace, but still used in queryResults
    strings_waiting_to_be_deleted_size?: integer
    // [[deprecated]]. do not use
    updated_unix_nano?: integer
    // Status of disk storage (true, if storage is enabled and writable)
    storage_ok?: boolean
    // Shows if storage is enabled (however it may still be unavailable)
    storage_enabled?: boolean
    // More detailed info about storage status. May contain 'OK', 'DISABLED', 'NO SPACE LEFT' or last error description
    storage_status?: string
    // Filesystem path to namespace storage
    storage_path?: string
    // Background indexes optimization has been completed
    optimization_completed?: boolean
    // Summary of total namespace memory consumption
    total: {
      // Total memory size of stored documents, including system structures
      data_size?: integer
      // Total memory consumption of namespace's indexes
      indexes_size?: integer
      // Total memory consumption of namespace's caches. e.g. idset and join caches
      cache_size?: integer
      // Total memory size, occupation by index optimizer (in bytes)
      index_optimizer_memory?: integer
      // Total memory size, occupied by the AsyncStorage (in bytes)
      inmemory_storage_size?: integer
    }
    // Summary of total async storage memory consumption
    storage: {
      // Total memory size, occupied by synchronous proxy map of the AsyncStorage (in bytes)
      proxy_size?: integer
    }
    // Join cache stats. Stores results of selects to right table by ON condition
    join_cache?: CacheMemStats
    // Query cache stats. Stores results of SELECT COUNT(*) by Where conditions
    query_cache?: CacheMemStats
    // State of namespace replication
    replication: {
      // Last Log Sequence Number (LSN) of applied namespace modification represented as single integer
      last_lsn?: integer
      // Last Log Sequence Number (LSN) of applied namespace record
      last_lsn_v2: {
        // Server ID of record source node
        server_id?: integer
        // Record ID (incremental counter)
        counter?: integer
      }
      // Namespace version, assigned on namespace creation
      ns_version: {
        // Server ID of creater node
        server_id?: integer
        // Version (incremental counter)
        counter?: integer
      }
      // Cluster operation status for the namespace
      clusterization_status: {
        // Server ID of the namespace's leader
        leader_id?: integer
        // Namespace role in replication
        role?: enum[none, cluster_replica, simple_replica]
      }
      // Checksum of all records in namespace
      checksum?: integer
      // Write Ahead Log (WAL) records count
      wal_count?: integer
      // Total memory consumption of Write Ahead Log (WAL)
      wal_size?: integer
      // Current node ID
      server_id?: integer
      // Last update time
      updated_unix_nano?: integer
      // Items count in namespace
      data_count?: integer
      // Admissible replication token of the namespace
      admissible_token?: string
    }
    indexes: {
      // Name of index. There are special index with name `-tuple`. It's stores original document's json structure with non indexed fields
      name?: string
      // Count of unique keys values stored in index
      unique_keys_count?: integer
      // Total memory consumption (in bytes) of reverse index b-tree structures. For `dense` and `store` indexes always 0
      idset_btree_size?: integer
      // Total memory consumption (in bytes) of reverse index vectors. For `store` indexes always 0
      idset_plain_size?: integer
      // Total memory consumption (in bytes) of SORT statement and `GT`, `LT` conditions optimized structures. Applicable only to `tree` indexes
      sort_orders_size?: integer
      // Idset cache stats. Stores merged reverse index results of SELECT field IN(...) by IN(...) keys
      idset_cache?: CacheMemStats
      // Total memory consumption (in bytes) of the main indexing structures (fulltext, ANN, etc.)
      indexing_struct_size?: integer
      // Total memory consumation (in bytes) of shared vectors keeper structures (ANN indexes only)
      vectors_keeper_size?: integer
      // Total memory consumption (in bytes) of documents's data, held by index
      data_size?: integer
      // Updates count, pending in index updates tracker
      tracked_updates_count?: integer
      // Buckets count in index updates tracker map
      tracked_updates_buckets?: integer
      // Updates tracker map size in bytes
      tracked_updates_size?: integer
      // Updates tracker map overflow (number of elements, stored outside of the main buckets)
      tracked_updates_overflow?: integer
      // Shows whether KNN/fulltext indexing structure is fully built. If this field is missing, index does not require any specific build steps
      is_built?: boolean
      // Shows whether HNSW-index quantized. If this field is nil, index does not support quantization
      is_quantized?: boolean
      upsert_embedder: {
        // Last request execution status
        last_request_result?: enum[OK, ERROR]
        last_error: {
          // Error code. 0 - no error.
          code?: integer
          // Error message
          message?: string
        }
      }
      query_embedder:EmbedderStatus
    }[]
    embedding_caches: {
      // Tag of cache from configuration
      cache_tag?: string
      // Capacity of cache
      capacity?: integer
      cache: {
        // Total memory consumption by this cache
        total_size?: integer
        // Count of used elements stored in this cache
        items_count?: integer
        // Count of empty elements slots in this cache
        empty_count?: integer
        // Number of hits of queries, to store results in cache
        hit_count_limit?: integer
      }
      // Status of disk storage (true, if storage is enabled and writable)
      storage_ok?: boolean
      // More detailed info about storage status. May contain 'OK', 'DISABLED', 'FAILED' or last error description
      storage_status?: string
      // Filesystem path to namespace storage
      storage_path?: string
      // Disk space occupied by storage
      storage_size?: integer
    }[]
    // Status of tags matcher
    tags_matcher: {
      // Current count of tags in tags matcher
      tags_count?: integer
      // Maximum count of tags in tags matcher
      max_tags_count?: integer
      // Version of tags matcher
      version?: integer
      // State token of tags matcher
      state_token?: integer
    }
  }[]
}
```

### NamespaceMemStats

```typescript
{
  // Name of namespace
  name?: string
  // Type of namespace. For now it's 'namespace' or 'embedders'
  type?: string
  // Total count of documents in namespace
  items_count?: integer
  // Size of strings deleted from namespace, but still used in queryResults
  strings_waiting_to_be_deleted_size?: integer
  // [[deprecated]]. do not use
  updated_unix_nano?: integer
  // Status of disk storage (true, if storage is enabled and writable)
  storage_ok?: boolean
  // Shows if storage is enabled (however it may still be unavailable)
  storage_enabled?: boolean
  // More detailed info about storage status. May contain 'OK', 'DISABLED', 'NO SPACE LEFT' or last error description
  storage_status?: string
  // Filesystem path to namespace storage
  storage_path?: string
  // Background indexes optimization has been completed
  optimization_completed?: boolean
  // Summary of total namespace memory consumption
  total: {
    // Total memory size of stored documents, including system structures
    data_size?: integer
    // Total memory consumption of namespace's indexes
    indexes_size?: integer
    // Total memory consumption of namespace's caches. e.g. idset and join caches
    cache_size?: integer
    // Total memory size, occupation by index optimizer (in bytes)
    index_optimizer_memory?: integer
    // Total memory size, occupied by the AsyncStorage (in bytes)
    inmemory_storage_size?: integer
  }
  // Summary of total async storage memory consumption
  storage: {
    // Total memory size, occupied by synchronous proxy map of the AsyncStorage (in bytes)
    proxy_size?: integer
  }
  // Join cache stats. Stores results of selects to right table by ON condition
  join_cache?: CacheMemStats
  // Query cache stats. Stores results of SELECT COUNT(*) by Where conditions
  query_cache?: CacheMemStats
  // State of namespace replication
  replication: {
    // Last Log Sequence Number (LSN) of applied namespace modification represented as single integer
    last_lsn?: integer
    // Last Log Sequence Number (LSN) of applied namespace record
    last_lsn_v2: {
      // Server ID of record source node
      server_id?: integer
      // Record ID (incremental counter)
      counter?: integer
    }
    // Namespace version, assigned on namespace creation
    ns_version: {
      // Server ID of creater node
      server_id?: integer
      // Version (incremental counter)
      counter?: integer
    }
    // Cluster operation status for the namespace
    clusterization_status: {
      // Server ID of the namespace's leader
      leader_id?: integer
      // Namespace role in replication
      role?: enum[none, cluster_replica, simple_replica]
    }
    // Checksum of all records in namespace
    checksum?: integer
    // Write Ahead Log (WAL) records count
    wal_count?: integer
    // Total memory consumption of Write Ahead Log (WAL)
    wal_size?: integer
    // Current node ID
    server_id?: integer
    // Last update time
    updated_unix_nano?: integer
    // Items count in namespace
    data_count?: integer
    // Admissible replication token of the namespace
    admissible_token?: string
  }
  indexes: {
    // Name of index. There are special index with name `-tuple`. It's stores original document's json structure with non indexed fields
    name?: string
    // Count of unique keys values stored in index
    unique_keys_count?: integer
    // Total memory consumption (in bytes) of reverse index b-tree structures. For `dense` and `store` indexes always 0
    idset_btree_size?: integer
    // Total memory consumption (in bytes) of reverse index vectors. For `store` indexes always 0
    idset_plain_size?: integer
    // Total memory consumption (in bytes) of SORT statement and `GT`, `LT` conditions optimized structures. Applicable only to `tree` indexes
    sort_orders_size?: integer
    // Idset cache stats. Stores merged reverse index results of SELECT field IN(...) by IN(...) keys
    idset_cache?: CacheMemStats
    // Total memory consumption (in bytes) of the main indexing structures (fulltext, ANN, etc.)
    indexing_struct_size?: integer
    // Total memory consumation (in bytes) of shared vectors keeper structures (ANN indexes only)
    vectors_keeper_size?: integer
    // Total memory consumption (in bytes) of documents's data, held by index
    data_size?: integer
    // Updates count, pending in index updates tracker
    tracked_updates_count?: integer
    // Buckets count in index updates tracker map
    tracked_updates_buckets?: integer
    // Updates tracker map size in bytes
    tracked_updates_size?: integer
    // Updates tracker map overflow (number of elements, stored outside of the main buckets)
    tracked_updates_overflow?: integer
    // Shows whether KNN/fulltext indexing structure is fully built. If this field is missing, index does not require any specific build steps
    is_built?: boolean
    // Shows whether HNSW-index quantized. If this field is nil, index does not support quantization
    is_quantized?: boolean
    upsert_embedder: {
      // Last request execution status
      last_request_result?: enum[OK, ERROR]
      last_error: {
        // Error code. 0 - no error.
        code?: integer
        // Error message
        message?: string
      }
    }
    query_embedder:EmbedderStatus
  }[]
  embedding_caches: {
    // Tag of cache from configuration
    cache_tag?: string
    // Capacity of cache
    capacity?: integer
    cache: {
      // Total memory consumption by this cache
      total_size?: integer
      // Count of used elements stored in this cache
      items_count?: integer
      // Count of empty elements slots in this cache
      empty_count?: integer
      // Number of hits of queries, to store results in cache
      hit_count_limit?: integer
    }
    // Status of disk storage (true, if storage is enabled and writable)
    storage_ok?: boolean
    // More detailed info about storage status. May contain 'OK', 'DISABLED', 'FAILED' or last error description
    storage_status?: string
    // Filesystem path to namespace storage
    storage_path?: string
    // Disk space occupied by storage
    storage_size?: integer
  }[]
  // Status of tags matcher
  tags_matcher: {
    // Current count of tags in tags matcher
    tags_count?: integer
    // Maximum count of tags in tags matcher
    max_tags_count?: integer
    // Version of tags matcher
    version?: integer
    // State token of tags matcher
    state_token?: integer
  }
}
```

### IndexMemStat

```typescript
{
  // Name of index. There are special index with name `-tuple`. It's stores original document's json structure with non indexed fields
  name?: string
  // Count of unique keys values stored in index
  unique_keys_count?: integer
  // Total memory consumption (in bytes) of reverse index b-tree structures. For `dense` and `store` indexes always 0
  idset_btree_size?: integer
  // Total memory consumption (in bytes) of reverse index vectors. For `store` indexes always 0
  idset_plain_size?: integer
  // Total memory consumption (in bytes) of SORT statement and `GT`, `LT` conditions optimized structures. Applicable only to `tree` indexes
  sort_orders_size?: integer
  // Idset cache stats. Stores merged reverse index results of SELECT field IN(...) by IN(...) keys
  idset_cache?: CacheMemStats
  // Total memory consumption (in bytes) of the main indexing structures (fulltext, ANN, etc.)
  indexing_struct_size?: integer
  // Total memory consumation (in bytes) of shared vectors keeper structures (ANN indexes only)
  vectors_keeper_size?: integer
  // Total memory consumption (in bytes) of documents's data, held by index
  data_size?: integer
  // Updates count, pending in index updates tracker
  tracked_updates_count?: integer
  // Buckets count in index updates tracker map
  tracked_updates_buckets?: integer
  // Updates tracker map size in bytes
  tracked_updates_size?: integer
  // Updates tracker map overflow (number of elements, stored outside of the main buckets)
  tracked_updates_overflow?: integer
  // Shows whether KNN/fulltext indexing structure is fully built. If this field is missing, index does not require any specific build steps
  is_built?: boolean
  // Shows whether HNSW-index quantized. If this field is nil, index does not support quantization
  is_quantized?: boolean
  upsert_embedder: {
    // Last request execution status
    last_request_result?: enum[OK, ERROR]
    last_error: {
      // Error code. 0 - no error.
      code?: integer
      // Error message
      message?: string
    }
  }
  query_embedder:EmbedderStatus
}
```

### EmbedderStatus

```typescript
{
  // Last request execution status
  last_request_result?: enum[OK, ERROR]
  last_error: {
    // Error code. 0 - no error.
    code?: integer
    // Error message
    message?: string
  }
}
```

### EmbedderLastError

```typescript
{
  // Error code. 0 - no error.
  code?: integer
  // Error message
  message?: string
}
```

### EmbeddersCacheMemStat

```typescript
{
  // Tag of cache from configuration
  cache_tag?: string
  // Capacity of cache
  capacity?: integer
  cache: {
    // Total memory consumption by this cache
    total_size?: integer
    // Count of used elements stored in this cache
    items_count?: integer
    // Count of empty elements slots in this cache
    empty_count?: integer
    // Number of hits of queries, to store results in cache
    hit_count_limit?: integer
  }
  // Status of disk storage (true, if storage is enabled and writable)
  storage_ok?: boolean
  // More detailed info about storage status. May contain 'OK', 'DISABLED', 'FAILED' or last error description
  storage_status?: string
  // Filesystem path to namespace storage
  storage_path?: string
  // Disk space occupied by storage
  storage_size?: integer
}
```

### JoinCacheMemStats

```typescript
// Join cache stats. Stores results of selects to right table by ON condition
undefined?: CacheMemStats
```

### QueryCacheMemStats

```typescript
// Query cache stats. Stores results of SELECT COUNT(*) by Where conditions
undefined?: CacheMemStats
```

### IndexCacheMemStats

```typescript
// Idset cache stats. Stores merged reverse index results of SELECT field IN(...) by IN(...) keys
undefined?: CacheMemStats
```

### CacheMemStats

```typescript
{
  // Total memory consumption by this cache
  total_size?: integer
  // Count of used elements stored in this cache
  items_count?: integer
  // Count of empty elements slots in this cache
  empty_count?: integer
  // Number of hits of queries, to store results in cache
  hit_count_limit?: integer
}
```

### ReplicationStats

```typescript
// State of namespace replication
{
  // Last Log Sequence Number (LSN) of applied namespace modification represented as single integer
  last_lsn?: integer
  // Last Log Sequence Number (LSN) of applied namespace record
  last_lsn_v2: {
    // Server ID of record source node
    server_id?: integer
    // Record ID (incremental counter)
    counter?: integer
  }
  // Namespace version, assigned on namespace creation
  ns_version: {
    // Server ID of creater node
    server_id?: integer
    // Version (incremental counter)
    counter?: integer
  }
  // Cluster operation status for the namespace
  clusterization_status: {
    // Server ID of the namespace's leader
    leader_id?: integer
    // Namespace role in replication
    role?: enum[none, cluster_replica, simple_replica]
  }
  // Checksum of all records in namespace
  checksum?: integer
  // Write Ahead Log (WAL) records count
  wal_count?: integer
  // Total memory consumption of Write Ahead Log (WAL)
  wal_size?: integer
  // Current node ID
  server_id?: integer
  // Last update time
  updated_unix_nano?: integer
  // Items count in namespace
  data_count?: integer
  // Admissible replication token of the namespace
  admissible_token?: string
}
```

### DatabasePerfStats

```typescript
{
  // Total count of documents, matched specified filters
  total_items?: integer
  items: {
    // Name of namespace
    name?: string
    // Performance statistics for update operations
    updates?: CommonPerfStats
    // Performance statistics for select operations
    selects?: CommonPerfStats
    // Performance statistics for transactions
    transactions: {
      // Total transactions count for this namespace
      total_count?: integer
      // Total namespace copy operations
      total_copy_count?: integer
      // Average steps count in transactions for this namespace
      avg_steps_count?: integer
      // Minimum steps count in transactions for this namespace
      min_steps_count?: integer
      // Maximum steps count in transactions for this namespace
      max_steps_count?: integer
      // Average transaction preparation time usec
      avg_prepare_time_us?: integer
      // Minimum transaction preparation time usec
      min_prepare_time_us?: integer
      // Maximum transaction preparation time usec
      max_prepare_time_us?: integer
      // Average transaction commit time usec
      avg_commit_time_us?: integer
      // Minimum transaction commit time usec
      min_commit_time_us?: integer
      // Maximum transaction commit time usec
      max_commit_time_us?: integer
      // Average namespace copy time usec
      avg_copy_time_us?: integer
      // Maximum namespace copy time usec
      min_copy_time_us?: integer
      // Minimum namespace copy time usec
      max_copy_time_us?: integer
    }
    // Performance statistics for specific LRU-cache instance
    join_cache: {
      // Queries total count
      total_queries?: integer
      // Cache hit rate (hits / total_queries)
      cache_hit_rate?: number
      // Determines if cache is currently in use. Usually it has 'false' value for uncommitted indexes
      is_active?: boolean
    }
    query_count_cache:LRUCachePerfStats
    indexes: {
      // Name of index
      name?: string
      updates:UpdatePerfStats
      selects:SelectPerfStats
      cache:LRUCachePerfStats
      upsert_embedder: {
        // Total number of calls to a specific embedder
        total_queries_count?: integer
        // Total number of requested vectors
        total_embed_documents_count?: integer
        // Number of calls to the embedder in the last second
        last_sec_qps?: integer
        // Number of embedded documents in the last second
        last_sec_dps?: integer
        // Total number of errors accessing the embedder
        total_errors_count?: integer
        // Number of errors in the last second
        last_second_errors_count?: integer
        // Current number of connections in use
        conn_in_use?: integer
        // Average number of connections used over the last second
        last_sec_avg_conn_in_use?: integer
        // Average overall autoembedding latency (over all time)
        total_avg_latency_us?: integer
        // Average autoembedding latency (over the last second)
        last_sec_avg_latency_us?: integer
        // Maximum total autoembedding latency (all time)
        max_latency_us?: integer
        // Minimum overall autoembedding latency (all time)
        min_latency_us?: integer
        // Average latency of waiting for a connection from the pool (over all time)
        total_avg_conn_await_latency_us?: integer
        // Average latency of waiting for a connection from the pool (over the last second)
        last_sec_avg_conn_await_latency_us?: integer
        // Average auto-embedding latency on cache miss (over all time)
        total_avg_embed_latency_us?: integer
        // Average auto-embedding latency for cache misses (last second)
        last_sec_avg_embed_latency_us?: integer
        // Maximum auto-embedding latency on cache miss (all time)
        max_embed_latency_us?: integer
        // Minimum auto-embedding latency on cache miss (all time)
        min_embed_latency_us?: integer
        // Average auto-embedding latency for cache hits (over all time)
        total_avg_cache_latency_us?: integer
        // Average auto-embedding latency for cache hits (last second)
        last_sec_avg_cache_latency_us?: integer
        // Maximum auto-embedding latency on a cache hit (all time)
        max_cache_latency_us?: integer
        // Minimum auto-embedding latency for a cache hit (all time)
        min_cache_latency_us?: integer
        // Total amount of data received from the embedding service over the network
        input_traffic_total_bytes?: integer
        // Total amount of data sent to the embedding service over the network
        output_traffic_total_bytes?: integer
        // Performance statistics for specific Embedder LRU-cache instance
        cache: {
          // Name. Identifier for linking settings
          cache_tag?: string
          // Queries total count
          total_queries?: integer
          // Cache hit rate (hits / total_queries)
          cache_hit_rate?: number
          // Determines if cache is currently in use. Usually it has 'false' value for uncommitted indexes
          is_active?: boolean
        }
      }
      query_embedder:EmbedderPerfStat
    }[]
  }[]
}
```

### NamespacePerfStats

```typescript
{
  // Name of namespace
  name?: string
  // Performance statistics for update operations
  updates?: CommonPerfStats
  // Performance statistics for select operations
  selects?: CommonPerfStats
  // Performance statistics for transactions
  transactions: {
    // Total transactions count for this namespace
    total_count?: integer
    // Total namespace copy operations
    total_copy_count?: integer
    // Average steps count in transactions for this namespace
    avg_steps_count?: integer
    // Minimum steps count in transactions for this namespace
    min_steps_count?: integer
    // Maximum steps count in transactions for this namespace
    max_steps_count?: integer
    // Average transaction preparation time usec
    avg_prepare_time_us?: integer
    // Minimum transaction preparation time usec
    min_prepare_time_us?: integer
    // Maximum transaction preparation time usec
    max_prepare_time_us?: integer
    // Average transaction commit time usec
    avg_commit_time_us?: integer
    // Minimum transaction commit time usec
    min_commit_time_us?: integer
    // Maximum transaction commit time usec
    max_commit_time_us?: integer
    // Average namespace copy time usec
    avg_copy_time_us?: integer
    // Maximum namespace copy time usec
    min_copy_time_us?: integer
    // Minimum namespace copy time usec
    max_copy_time_us?: integer
  }
  // Performance statistics for specific LRU-cache instance
  join_cache: {
    // Queries total count
    total_queries?: integer
    // Cache hit rate (hits / total_queries)
    cache_hit_rate?: number
    // Determines if cache is currently in use. Usually it has 'false' value for uncommitted indexes
    is_active?: boolean
  }
  query_count_cache:LRUCachePerfStats
  indexes: {
    // Name of index
    name?: string
    updates:UpdatePerfStats
    selects:SelectPerfStats
    cache:LRUCachePerfStats
    upsert_embedder: {
      // Total number of calls to a specific embedder
      total_queries_count?: integer
      // Total number of requested vectors
      total_embed_documents_count?: integer
      // Number of calls to the embedder in the last second
      last_sec_qps?: integer
      // Number of embedded documents in the last second
      last_sec_dps?: integer
      // Total number of errors accessing the embedder
      total_errors_count?: integer
      // Number of errors in the last second
      last_second_errors_count?: integer
      // Current number of connections in use
      conn_in_use?: integer
      // Average number of connections used over the last second
      last_sec_avg_conn_in_use?: integer
      // Average overall autoembedding latency (over all time)
      total_avg_latency_us?: integer
      // Average autoembedding latency (over the last second)
      last_sec_avg_latency_us?: integer
      // Maximum total autoembedding latency (all time)
      max_latency_us?: integer
      // Minimum overall autoembedding latency (all time)
      min_latency_us?: integer
      // Average latency of waiting for a connection from the pool (over all time)
      total_avg_conn_await_latency_us?: integer
      // Average latency of waiting for a connection from the pool (over the last second)
      last_sec_avg_conn_await_latency_us?: integer
      // Average auto-embedding latency on cache miss (over all time)
      total_avg_embed_latency_us?: integer
      // Average auto-embedding latency for cache misses (last second)
      last_sec_avg_embed_latency_us?: integer
      // Maximum auto-embedding latency on cache miss (all time)
      max_embed_latency_us?: integer
      // Minimum auto-embedding latency on cache miss (all time)
      min_embed_latency_us?: integer
      // Average auto-embedding latency for cache hits (over all time)
      total_avg_cache_latency_us?: integer
      // Average auto-embedding latency for cache hits (last second)
      last_sec_avg_cache_latency_us?: integer
      // Maximum auto-embedding latency on a cache hit (all time)
      max_cache_latency_us?: integer
      // Minimum auto-embedding latency for a cache hit (all time)
      min_cache_latency_us?: integer
      // Total amount of data received from the embedding service over the network
      input_traffic_total_bytes?: integer
      // Total amount of data sent to the embedding service over the network
      output_traffic_total_bytes?: integer
      // Performance statistics for specific Embedder LRU-cache instance
      cache: {
        // Name. Identifier for linking settings
        cache_tag?: string
        // Queries total count
        total_queries?: integer
        // Cache hit rate (hits / total_queries)
        cache_hit_rate?: number
        // Determines if cache is currently in use. Usually it has 'false' value for uncommitted indexes
        is_active?: boolean
      }
    }
    query_embedder:EmbedderPerfStat
  }[]
}
```

### CommonPerfStats

```typescript
{
  // Total count of queries to this object
  total_queries_count?: integer
  // Average latency (execution time) for queries to this object
  total_avg_latency_us?: integer
  // Average waiting time for acquiring lock to this object
  total_avg_lock_time_us?: integer
  // Count of queries to this object, requested at last second
  last_sec_qps?: integer
  // Average latency (execution time) for queries to this object at last second
  last_sec_avg_latency_us?: integer
  // Average waiting time for acquiring lock to this object at last second
  last_sec_avg_lock_time_us?: integer
  // Standard deviation of latency values
  latency_stddev?: number
  // Minimal latency value
  min_latency_us?: integer
  // Maximum latency value
  max_latency_us?: integer
}
```

### UpdatePerfStats

```typescript
// Performance statistics for update operations
undefined?: CommonPerfStats
```

### SelectPerfStats

```typescript
// Performance statistics for select operations
undefined?: CommonPerfStats
```

### TransactionsPerfStats

```typescript
// Performance statistics for transactions
{
  // Total transactions count for this namespace
  total_count?: integer
  // Total namespace copy operations
  total_copy_count?: integer
  // Average steps count in transactions for this namespace
  avg_steps_count?: integer
  // Minimum steps count in transactions for this namespace
  min_steps_count?: integer
  // Maximum steps count in transactions for this namespace
  max_steps_count?: integer
  // Average transaction preparation time usec
  avg_prepare_time_us?: integer
  // Minimum transaction preparation time usec
  min_prepare_time_us?: integer
  // Maximum transaction preparation time usec
  max_prepare_time_us?: integer
  // Average transaction commit time usec
  avg_commit_time_us?: integer
  // Minimum transaction commit time usec
  min_commit_time_us?: integer
  // Maximum transaction commit time usec
  max_commit_time_us?: integer
  // Average namespace copy time usec
  avg_copy_time_us?: integer
  // Maximum namespace copy time usec
  min_copy_time_us?: integer
  // Minimum namespace copy time usec
  max_copy_time_us?: integer
}
```

### QueriesPerfStats

```typescript
{
  // Total count of documents, matched specified filters
  total_items?: integer
  // Performance statistics per each query
  items?: CommonPerfStats & {
     // normalized SQL representation of query
     query?: string
     // not normalized SQL representation of longest query
     longest_query?: string
   }[]
}
```

### QueryPerfStats

```typescript
// Performance statistics per each query
undefined?: CommonPerfStats & {
   // normalized SQL representation of query
   query?: string
   // not normalized SQL representation of longest query
   longest_query?: string
 }
```

### LRUCachePerfStats

```typescript
// Performance statistics for specific LRU-cache instance
{
  // Queries total count
  total_queries?: integer
  // Cache hit rate (hits / total_queries)
  cache_hit_rate?: number
  // Determines if cache is currently in use. Usually it has 'false' value for uncommitted indexes
  is_active?: boolean
}
```

### EmbedderCachePerfStat

```typescript
// Performance statistics for specific Embedder LRU-cache instance
{
  // Name. Identifier for linking settings
  cache_tag?: string
  // Queries total count
  total_queries?: integer
  // Cache hit rate (hits / total_queries)
  cache_hit_rate?: number
  // Determines if cache is currently in use. Usually it has 'false' value for uncommitted indexes
  is_active?: boolean
}
```

### EmbedderPerfStat

```typescript
{
  // Total number of calls to a specific embedder
  total_queries_count?: integer
  // Total number of requested vectors
  total_embed_documents_count?: integer
  // Number of calls to the embedder in the last second
  last_sec_qps?: integer
  // Number of embedded documents in the last second
  last_sec_dps?: integer
  // Total number of errors accessing the embedder
  total_errors_count?: integer
  // Number of errors in the last second
  last_second_errors_count?: integer
  // Current number of connections in use
  conn_in_use?: integer
  // Average number of connections used over the last second
  last_sec_avg_conn_in_use?: integer
  // Average overall autoembedding latency (over all time)
  total_avg_latency_us?: integer
  // Average autoembedding latency (over the last second)
  last_sec_avg_latency_us?: integer
  // Maximum total autoembedding latency (all time)
  max_latency_us?: integer
  // Minimum overall autoembedding latency (all time)
  min_latency_us?: integer
  // Average latency of waiting for a connection from the pool (over all time)
  total_avg_conn_await_latency_us?: integer
  // Average latency of waiting for a connection from the pool (over the last second)
  last_sec_avg_conn_await_latency_us?: integer
  // Average auto-embedding latency on cache miss (over all time)
  total_avg_embed_latency_us?: integer
  // Average auto-embedding latency for cache misses (last second)
  last_sec_avg_embed_latency_us?: integer
  // Maximum auto-embedding latency on cache miss (all time)
  max_embed_latency_us?: integer
  // Minimum auto-embedding latency on cache miss (all time)
  min_embed_latency_us?: integer
  // Average auto-embedding latency for cache hits (over all time)
  total_avg_cache_latency_us?: integer
  // Average auto-embedding latency for cache hits (last second)
  last_sec_avg_cache_latency_us?: integer
  // Maximum auto-embedding latency on a cache hit (all time)
  max_cache_latency_us?: integer
  // Minimum auto-embedding latency for a cache hit (all time)
  min_cache_latency_us?: integer
  // Total amount of data received from the embedding service over the network
  input_traffic_total_bytes?: integer
  // Total amount of data sent to the embedding service over the network
  output_traffic_total_bytes?: integer
  // Performance statistics for specific Embedder LRU-cache instance
  cache: {
    // Name. Identifier for linking settings
    cache_tag?: string
    // Queries total count
    total_queries?: integer
    // Cache hit rate (hits / total_queries)
    cache_hit_rate?: number
    // Determines if cache is currently in use. Usually it has 'false' value for uncommitted indexes
    is_active?: boolean
  }
}
```

### SystemConfigItems

```typescript
{
  items: {
    type: enum[profiling, namespaces, replication, async_replication, embedders, action] //default: profiling
    profiling: {
      // Enables tracking activity statistics
      activitystats?: boolean
      // Enables tracking memory statistics
      memstats?: boolean //default: true
      // Enables tracking overall performance statistics
      perfstats?: boolean
      // Enables record queries performance statistics
      queriesperfstats?: boolean
      // Minimum query execution time to be recorded in #queriesperfstats namespace
      queries_threshold_us?: integer
      // Parameters for logging long queries and transactions
      long_queries_logging: {
        select: {
          // Threshold value for logging SELECT queries, if -1 logging is disabled
          threshold_us?: integer
          // Output the query in a normalized form
          normalized?: boolean
        }
        update_delete: {
          // Threshold value for logging UPDATE and DELETE queries, if -1 logging is disabled
          threshold_us?: integer
          // Output the query in a normalized form
          normalized?: boolean
        }
        transaction: {
          // Threshold value for total transaction commit time, if -1 logging is disabled
          threshold_us?: integer
          // Threshold value for the average step duration time in a transaction, if -1 logging is disabled
          avg_step_threshold_us?: integer
        }
      }
    }
    namespaces: {
      // Name of namespace, or `*` for setting to all namespaces
      namespace?: string
      // Log level of queries core logger
      log_level?: enum[none, error, warning, info, trace]
      // Join cache mode
      join_cache_mode?: enum[aggressive, on, off] //default: off
      // Enable namespace copying for transaction with steps count greater than this value (if copy_politics_multiplier also allows this)
      start_copy_policy_tx_size?: integer //default: 10000
      // Disables copy policy if namespace size is greater than copy_policy_multiplier * start_copy_policy_tx_size
      copy_policy_multiplier?: integer //default: 5
      // Force namespace copying for transaction with steps count greater than this value
      tx_size_to_always_copy?: integer //default: 100000
      // Count of threads, that will be created during transaction's commit to insert data into multithread ANN-indexes
      tx_vec_insertion_threads?: integer //default: 4
      // Timeout before background indexes optimization start after last update. 0 - disable optimizations
      optimization_timeout_ms?: integer //default: 800
      // Maximum number of background threads of sort indexes optimization. 0 - disable sort optimizations
      optimization_sort_workers?: integer //default: 4
      // Maximum WAL size for this namespace (maximum count of WAL records)
      wal_size?: integer //default: 4000000
      // Maximum preselect size for optimization of inner join by insertion of filters. If max_preselect_size is 0, then only max_preselect_part will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied
      max_preselect_size?: integer //default: 1000
      // Maximum preselect part of namespace's items for optimization of inner join by insertion of filters. If max_preselect_part is 0, then only max_preselect_size will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied
      max_preselect_part?: number //default: 0.1
      // Minimum preselect size for optimization of inner join by insertion of filters. Min_preselect_size will be used as preselect limit if (max_preselect_part * ns.size) is less than this value
      min_preselect_size?: integer //default: 1000
      // Maximum number of IdSet iterations of namespace preliminary result size for optimization
      max_iterations_idset_preresult?: integer //default: 20000
      // Enables 'simple counting mode' for index updates tracker. This will increase index optimization time, however may reduce insertion time
      index_updates_counting_mode?: boolean
      // Enables synchronous storage flush inside write-calls, if async updates count is more than sync_storage_flush_limit. 0 - disables synchronous storage flush, in this case storage will be flushed in background thread only
      sync_storage_flush_limit?: integer //default: 20000
      // Delay between last and namespace update background ANN-indexes storage cache creation. Storage cache is required for ANN-indexes for faster startup. 0 - disables background cache creation (cache will still be created on the database shutdown)
      ann_storage_cache_build_timeout_ms?: integer //default: 5000
      // Strict mode for queries. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
      strict_mode?: enum[none, names, indexes] //default: names
      cache: {
        // Max size of the index IdSets cache in bytes (per index). Each index has it's own independent cache. This cache is used in any selections to store resulting sets of internal document IDs (it does not stores documents' content itself)
        index_idset_cache_size?: integer //default: 134217728
        // Default 'hits to cache' for index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
        index_idset_hits_to_cache?: integer //default: 2
        // Max size of the fulltext indexes IdSets cache in bytes (per index). Each fulltext index has it's own independent cache. This cache is used in any selections to store resulting sets of internal document IDs, FT ranks and highlighted areas (it does not stores documents' content itself)
        ft_index_cache_size?: integer //default: 134217728
        // Default 'hits to cache' for fulltext index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
        ft_index_hits_to_cache?: integer //default: 2
        // Max size of the index IdSets cache in bytes for each namespace. This cache will be enabled only if 'join_cache_mode' property is not 'off'. It stores resulting IDs, serialized JOINed queries and any other 'preselect' information for the JOIN queries (when target namespace is right namespace of the JOIN)
        joins_preselect_cache_size?: integer //default: 134217728
        // Default 'hits to cache' for joins preselect cache of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
        joins_preselect_hit_to_cache?: integer //default: 2
        // Max size of the cache for COUNT_CACHED() aggregation in bytes for each namespace. This cache stores resulting COUNTs and serialized queries for the COUNT_CACHED() aggregations
        query_count_cache_size?: integer //default: 134217728
        // Default 'hits to cache' for COUNT_CACHED() aggregation of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
        query_count_hit_to_cache?: integer //default: 2
      }
    }[]
    replication: {
      // Node identifier. Should be unique for each node in the replicated cluster (non-unique IDs are also allowed, but may lead to the inconsistency in some cases
      server_id?: integer
      // Cluster ID - must be same for client and for master
      cluster_id?: integer //default: 2
      admissible_replication_tokens: {
        token?: string
        namespaces: {
        }[]
      }[]
    }
    async_replication: {
      // Replication role
      role: enum[none, follower, leader]
      // Allows to configure async replication from sync raft-cluster (replicate either from each node, or from synchronous cluster leader)
      mode?: enum[default, from_sync_leader]
      // Application name, used by replicator as a login tag
      app_name?: string
      // Node response timeout for online-replication (seconds)
      online_updates_timeout_sec?: integer
      // Network timeout for communication with followers (for force and wal synchronization), in seconds
      sync_timeout_sec?: integer
      // Resync timeout on network errors
      retry_sync_interval_msec?: integer
      // Number of data replication threads
      sync_threads?: integer
      // Max number of concurrent force/wal sync's per thread
      syncs_per_thread?: integer
      // Number of coroutines for updates batching (per namespace). Higher value here may help to reduce networks triparound await time, but will require more RAM
      batching_routines_count?: integer
      // Delay between write operation and replication. Larger values here will leader to higher replication latency and buffering, but also will provide more effective network batching and CPU utilization
      online_updates_delay_msec?: integer
      // Enable network traffic compression
      enable_compression?: boolean
      // Maximum number of WAL records, which will be copied after force-sync
      max_wal_depth_on_force_sync?: integer
      // force resync on logic error conditions
      force_sync_on_logic_error?: boolean
      // force resync on wrong data hash conditions
      force_sync_on_wrong_data_hash?: boolean
      // Replication log level on replicator's startup
      log_level?: enum[none, error, warning, info, trace]
      namespaces?: string[]
      // Token of the current node that it sends to the follower for verification
      self_replication_token?: string
      nodes: {
        // Follower's DSN. Must have cproto-scheme
        dsn: string
        namespaces?: string[]
      }[]
    }
    embedders: {
      // Name. Identifier for linking settings. Special value '*' is supported (applies to all)
      cache_tag?: string
      // Maximum size of the embedding results cache in items. This cache will only be enabled if the 'max_cache_items' property is not 'off' (value 0). It stores the results of the embedding calculation
      max_cache_items?: integer //default: 1000000
      // Default 'hits to cache' for embedding calculation cache. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. 0 and 1 mean - when value added goes straight to the cache
      hit_to_cache?: integer //default: 1
    }[]
    action: {
      // Command to execute
      command: enum[restart_replication, reset_replication_role]
      // Namespace name for reset_replication_role. May be empty
      namespace?: string
    }
  }[]
}
```

### SystemConfigItem

```typescript
{
  type: enum[profiling, namespaces, replication, async_replication, embedders, action] //default: profiling
  profiling: {
    // Enables tracking activity statistics
    activitystats?: boolean
    // Enables tracking memory statistics
    memstats?: boolean //default: true
    // Enables tracking overall performance statistics
    perfstats?: boolean
    // Enables record queries performance statistics
    queriesperfstats?: boolean
    // Minimum query execution time to be recorded in #queriesperfstats namespace
    queries_threshold_us?: integer
    // Parameters for logging long queries and transactions
    long_queries_logging: {
      select: {
        // Threshold value for logging SELECT queries, if -1 logging is disabled
        threshold_us?: integer
        // Output the query in a normalized form
        normalized?: boolean
      }
      update_delete: {
        // Threshold value for logging UPDATE and DELETE queries, if -1 logging is disabled
        threshold_us?: integer
        // Output the query in a normalized form
        normalized?: boolean
      }
      transaction: {
        // Threshold value for total transaction commit time, if -1 logging is disabled
        threshold_us?: integer
        // Threshold value for the average step duration time in a transaction, if -1 logging is disabled
        avg_step_threshold_us?: integer
      }
    }
  }
  namespaces: {
    // Name of namespace, or `*` for setting to all namespaces
    namespace?: string
    // Log level of queries core logger
    log_level?: enum[none, error, warning, info, trace]
    // Join cache mode
    join_cache_mode?: enum[aggressive, on, off] //default: off
    // Enable namespace copying for transaction with steps count greater than this value (if copy_politics_multiplier also allows this)
    start_copy_policy_tx_size?: integer //default: 10000
    // Disables copy policy if namespace size is greater than copy_policy_multiplier * start_copy_policy_tx_size
    copy_policy_multiplier?: integer //default: 5
    // Force namespace copying for transaction with steps count greater than this value
    tx_size_to_always_copy?: integer //default: 100000
    // Count of threads, that will be created during transaction's commit to insert data into multithread ANN-indexes
    tx_vec_insertion_threads?: integer //default: 4
    // Timeout before background indexes optimization start after last update. 0 - disable optimizations
    optimization_timeout_ms?: integer //default: 800
    // Maximum number of background threads of sort indexes optimization. 0 - disable sort optimizations
    optimization_sort_workers?: integer //default: 4
    // Maximum WAL size for this namespace (maximum count of WAL records)
    wal_size?: integer //default: 4000000
    // Maximum preselect size for optimization of inner join by insertion of filters. If max_preselect_size is 0, then only max_preselect_part will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied
    max_preselect_size?: integer //default: 1000
    // Maximum preselect part of namespace's items for optimization of inner join by insertion of filters. If max_preselect_part is 0, then only max_preselect_size will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied
    max_preselect_part?: number //default: 0.1
    // Minimum preselect size for optimization of inner join by insertion of filters. Min_preselect_size will be used as preselect limit if (max_preselect_part * ns.size) is less than this value
    min_preselect_size?: integer //default: 1000
    // Maximum number of IdSet iterations of namespace preliminary result size for optimization
    max_iterations_idset_preresult?: integer //default: 20000
    // Enables 'simple counting mode' for index updates tracker. This will increase index optimization time, however may reduce insertion time
    index_updates_counting_mode?: boolean
    // Enables synchronous storage flush inside write-calls, if async updates count is more than sync_storage_flush_limit. 0 - disables synchronous storage flush, in this case storage will be flushed in background thread only
    sync_storage_flush_limit?: integer //default: 20000
    // Delay between last and namespace update background ANN-indexes storage cache creation. Storage cache is required for ANN-indexes for faster startup. 0 - disables background cache creation (cache will still be created on the database shutdown)
    ann_storage_cache_build_timeout_ms?: integer //default: 5000
    // Strict mode for queries. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
    strict_mode?: enum[none, names, indexes] //default: names
    cache: {
      // Max size of the index IdSets cache in bytes (per index). Each index has it's own independent cache. This cache is used in any selections to store resulting sets of internal document IDs (it does not stores documents' content itself)
      index_idset_cache_size?: integer //default: 134217728
      // Default 'hits to cache' for index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
      index_idset_hits_to_cache?: integer //default: 2
      // Max size of the fulltext indexes IdSets cache in bytes (per index). Each fulltext index has it's own independent cache. This cache is used in any selections to store resulting sets of internal document IDs, FT ranks and highlighted areas (it does not stores documents' content itself)
      ft_index_cache_size?: integer //default: 134217728
      // Default 'hits to cache' for fulltext index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
      ft_index_hits_to_cache?: integer //default: 2
      // Max size of the index IdSets cache in bytes for each namespace. This cache will be enabled only if 'join_cache_mode' property is not 'off'. It stores resulting IDs, serialized JOINed queries and any other 'preselect' information for the JOIN queries (when target namespace is right namespace of the JOIN)
      joins_preselect_cache_size?: integer //default: 134217728
      // Default 'hits to cache' for joins preselect cache of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
      joins_preselect_hit_to_cache?: integer //default: 2
      // Max size of the cache for COUNT_CACHED() aggregation in bytes for each namespace. This cache stores resulting COUNTs and serialized queries for the COUNT_CACHED() aggregations
      query_count_cache_size?: integer //default: 134217728
      // Default 'hits to cache' for COUNT_CACHED() aggregation of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
      query_count_hit_to_cache?: integer //default: 2
    }
  }[]
  replication: {
    // Node identifier. Should be unique for each node in the replicated cluster (non-unique IDs are also allowed, but may lead to the inconsistency in some cases
    server_id?: integer
    // Cluster ID - must be same for client and for master
    cluster_id?: integer //default: 2
    admissible_replication_tokens: {
      token?: string
      namespaces: {
      }[]
    }[]
  }
  async_replication: {
    // Replication role
    role: enum[none, follower, leader]
    // Allows to configure async replication from sync raft-cluster (replicate either from each node, or from synchronous cluster leader)
    mode?: enum[default, from_sync_leader]
    // Application name, used by replicator as a login tag
    app_name?: string
    // Node response timeout for online-replication (seconds)
    online_updates_timeout_sec?: integer
    // Network timeout for communication with followers (for force and wal synchronization), in seconds
    sync_timeout_sec?: integer
    // Resync timeout on network errors
    retry_sync_interval_msec?: integer
    // Number of data replication threads
    sync_threads?: integer
    // Max number of concurrent force/wal sync's per thread
    syncs_per_thread?: integer
    // Number of coroutines for updates batching (per namespace). Higher value here may help to reduce networks triparound await time, but will require more RAM
    batching_routines_count?: integer
    // Delay between write operation and replication. Larger values here will leader to higher replication latency and buffering, but also will provide more effective network batching and CPU utilization
    online_updates_delay_msec?: integer
    // Enable network traffic compression
    enable_compression?: boolean
    // Maximum number of WAL records, which will be copied after force-sync
    max_wal_depth_on_force_sync?: integer
    // force resync on logic error conditions
    force_sync_on_logic_error?: boolean
    // force resync on wrong data hash conditions
    force_sync_on_wrong_data_hash?: boolean
    // Replication log level on replicator's startup
    log_level?: enum[none, error, warning, info, trace]
    namespaces?: string[]
    // Token of the current node that it sends to the follower for verification
    self_replication_token?: string
    nodes: {
      // Follower's DSN. Must have cproto-scheme
      dsn: string
      namespaces?: string[]
    }[]
  }
  embedders: {
    // Name. Identifier for linking settings. Special value '*' is supported (applies to all)
    cache_tag?: string
    // Maximum size of the embedding results cache in items. This cache will only be enabled if the 'max_cache_items' property is not 'off' (value 0). It stores the results of the embedding calculation
    max_cache_items?: integer //default: 1000000
    // Default 'hits to cache' for embedding calculation cache. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. 0 and 1 mean - when value added goes straight to the cache
    hit_to_cache?: integer //default: 1
  }[]
  action: {
    // Command to execute
    command: enum[restart_replication, reset_replication_role]
    // Namespace name for reset_replication_role. May be empty
    namespace?: string
  }
}
```

### ProfilingConfig

```typescript
{
  // Enables tracking activity statistics
  activitystats?: boolean
  // Enables tracking memory statistics
  memstats?: boolean //default: true
  // Enables tracking overall performance statistics
  perfstats?: boolean
  // Enables record queries performance statistics
  queriesperfstats?: boolean
  // Minimum query execution time to be recorded in #queriesperfstats namespace
  queries_threshold_us?: integer
  // Parameters for logging long queries and transactions
  long_queries_logging: {
    select: {
      // Threshold value for logging SELECT queries, if -1 logging is disabled
      threshold_us?: integer
      // Output the query in a normalized form
      normalized?: boolean
    }
    update_delete: {
      // Threshold value for logging UPDATE and DELETE queries, if -1 logging is disabled
      threshold_us?: integer
      // Output the query in a normalized form
      normalized?: boolean
    }
    transaction: {
      // Threshold value for total transaction commit time, if -1 logging is disabled
      threshold_us?: integer
      // Threshold value for the average step duration time in a transaction, if -1 logging is disabled
      avg_step_threshold_us?: integer
    }
  }
}
```

### LongQueriesLogging

```typescript
// Parameters for logging long queries and transactions
{
  select: {
    // Threshold value for logging SELECT queries, if -1 logging is disabled
    threshold_us?: integer
    // Output the query in a normalized form
    normalized?: boolean
  }
  update_delete: {
    // Threshold value for logging UPDATE and DELETE queries, if -1 logging is disabled
    threshold_us?: integer
    // Output the query in a normalized form
    normalized?: boolean
  }
  transaction: {
    // Threshold value for total transaction commit time, if -1 logging is disabled
    threshold_us?: integer
    // Threshold value for the average step duration time in a transaction, if -1 logging is disabled
    avg_step_threshold_us?: integer
  }
}
```

### SelectLogging

```typescript
{
  // Threshold value for logging SELECT queries, if -1 logging is disabled
  threshold_us?: integer
  // Output the query in a normalized form
  normalized?: boolean
}
```

### UpdateDeleteLogging

```typescript
{
  // Threshold value for logging UPDATE and DELETE queries, if -1 logging is disabled
  threshold_us?: integer
  // Output the query in a normalized form
  normalized?: boolean
}
```

### TransactionLogging

```typescript
{
  // Threshold value for total transaction commit time, if -1 logging is disabled
  threshold_us?: integer
  // Threshold value for the average step duration time in a transaction, if -1 logging is disabled
  avg_step_threshold_us?: integer
}
```

### NamespacesConfig

```typescript
{
  // Name of namespace, or `*` for setting to all namespaces
  namespace?: string
  // Log level of queries core logger
  log_level?: enum[none, error, warning, info, trace]
  // Join cache mode
  join_cache_mode?: enum[aggressive, on, off] //default: off
  // Enable namespace copying for transaction with steps count greater than this value (if copy_politics_multiplier also allows this)
  start_copy_policy_tx_size?: integer //default: 10000
  // Disables copy policy if namespace size is greater than copy_policy_multiplier * start_copy_policy_tx_size
  copy_policy_multiplier?: integer //default: 5
  // Force namespace copying for transaction with steps count greater than this value
  tx_size_to_always_copy?: integer //default: 100000
  // Count of threads, that will be created during transaction's commit to insert data into multithread ANN-indexes
  tx_vec_insertion_threads?: integer //default: 4
  // Timeout before background indexes optimization start after last update. 0 - disable optimizations
  optimization_timeout_ms?: integer //default: 800
  // Maximum number of background threads of sort indexes optimization. 0 - disable sort optimizations
  optimization_sort_workers?: integer //default: 4
  // Maximum WAL size for this namespace (maximum count of WAL records)
  wal_size?: integer //default: 4000000
  // Maximum preselect size for optimization of inner join by insertion of filters. If max_preselect_size is 0, then only max_preselect_part will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied
  max_preselect_size?: integer //default: 1000
  // Maximum preselect part of namespace's items for optimization of inner join by insertion of filters. If max_preselect_part is 0, then only max_preselect_size will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied
  max_preselect_part?: number //default: 0.1
  // Minimum preselect size for optimization of inner join by insertion of filters. Min_preselect_size will be used as preselect limit if (max_preselect_part * ns.size) is less than this value
  min_preselect_size?: integer //default: 1000
  // Maximum number of IdSet iterations of namespace preliminary result size for optimization
  max_iterations_idset_preresult?: integer //default: 20000
  // Enables 'simple counting mode' for index updates tracker. This will increase index optimization time, however may reduce insertion time
  index_updates_counting_mode?: boolean
  // Enables synchronous storage flush inside write-calls, if async updates count is more than sync_storage_flush_limit. 0 - disables synchronous storage flush, in this case storage will be flushed in background thread only
  sync_storage_flush_limit?: integer //default: 20000
  // Delay between last and namespace update background ANN-indexes storage cache creation. Storage cache is required for ANN-indexes for faster startup. 0 - disables background cache creation (cache will still be created on the database shutdown)
  ann_storage_cache_build_timeout_ms?: integer //default: 5000
  // Strict mode for queries. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions
  strict_mode?: enum[none, names, indexes] //default: names
  cache: {
    // Max size of the index IdSets cache in bytes (per index). Each index has it's own independent cache. This cache is used in any selections to store resulting sets of internal document IDs (it does not stores documents' content itself)
    index_idset_cache_size?: integer //default: 134217728
    // Default 'hits to cache' for index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
    index_idset_hits_to_cache?: integer //default: 2
    // Max size of the fulltext indexes IdSets cache in bytes (per index). Each fulltext index has it's own independent cache. This cache is used in any selections to store resulting sets of internal document IDs, FT ranks and highlighted areas (it does not stores documents' content itself)
    ft_index_cache_size?: integer //default: 134217728
    // Default 'hits to cache' for fulltext index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
    ft_index_hits_to_cache?: integer //default: 2
    // Max size of the index IdSets cache in bytes for each namespace. This cache will be enabled only if 'join_cache_mode' property is not 'off'. It stores resulting IDs, serialized JOINed queries and any other 'preselect' information for the JOIN queries (when target namespace is right namespace of the JOIN)
    joins_preselect_cache_size?: integer //default: 134217728
    // Default 'hits to cache' for joins preselect cache of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
    joins_preselect_hit_to_cache?: integer //default: 2
    // Max size of the cache for COUNT_CACHED() aggregation in bytes for each namespace. This cache stores resulting COUNTs and serialized queries for the COUNT_CACHED() aggregations
    query_count_cache_size?: integer //default: 134217728
    // Default 'hits to cache' for COUNT_CACHED() aggregation of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
    query_count_hit_to_cache?: integer //default: 2
  }
}
```

### ReplicationConfig

```typescript
{
  // Node identifier. Should be unique for each node in the replicated cluster (non-unique IDs are also allowed, but may lead to the inconsistency in some cases
  server_id?: integer
  // Cluster ID - must be same for client and for master
  cluster_id?: integer //default: 2
  admissible_replication_tokens: {
    token?: string
    namespaces: {
    }[]
  }[]
}
```

### AsyncReplicationConfig

```typescript
{
  // Replication role
  role: enum[none, follower, leader]
  // Allows to configure async replication from sync raft-cluster (replicate either from each node, or from synchronous cluster leader)
  mode?: enum[default, from_sync_leader]
  // Application name, used by replicator as a login tag
  app_name?: string
  // Node response timeout for online-replication (seconds)
  online_updates_timeout_sec?: integer
  // Network timeout for communication with followers (for force and wal synchronization), in seconds
  sync_timeout_sec?: integer
  // Resync timeout on network errors
  retry_sync_interval_msec?: integer
  // Number of data replication threads
  sync_threads?: integer
  // Max number of concurrent force/wal sync's per thread
  syncs_per_thread?: integer
  // Number of coroutines for updates batching (per namespace). Higher value here may help to reduce networks triparound await time, but will require more RAM
  batching_routines_count?: integer
  // Delay between write operation and replication. Larger values here will leader to higher replication latency and buffering, but also will provide more effective network batching and CPU utilization
  online_updates_delay_msec?: integer
  // Enable network traffic compression
  enable_compression?: boolean
  // Maximum number of WAL records, which will be copied after force-sync
  max_wal_depth_on_force_sync?: integer
  // force resync on logic error conditions
  force_sync_on_logic_error?: boolean
  // force resync on wrong data hash conditions
  force_sync_on_wrong_data_hash?: boolean
  // Replication log level on replicator's startup
  log_level?: enum[none, error, warning, info, trace]
  namespaces?: string[]
  // Token of the current node that it sends to the follower for verification
  self_replication_token?: string
  nodes: {
    // Follower's DSN. Must have cproto-scheme
    dsn: string
    namespaces?: string[]
  }[]
}
```

### EmbeddersConfig

```typescript
{
  // Name. Identifier for linking settings. Special value '*' is supported (applies to all)
  cache_tag?: string
  // Maximum size of the embedding results cache in items. This cache will only be enabled if the 'max_cache_items' property is not 'off' (value 0). It stores the results of the embedding calculation
  max_cache_items?: integer //default: 1000000
  // Default 'hits to cache' for embedding calculation cache. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. 0 and 1 mean - when value added goes straight to the cache
  hit_to_cache?: integer //default: 1
}
```

### ActionCommand

```typescript
{
  // Command to execute
  command: enum[restart_replication, reset_replication_role]
  // Namespace name for reset_replication_role. May be empty
  namespace?: string
}
```

### BeginTransactionResponse

```typescript
{
  // Unique transaction id
  tx_id?: string
}
```

### UserRoleResponse

```typescript
{
  // User role
  user_role?: enum[owner, db_admin, data_write, data_read, none, unauthorized]
}
```

### OK

- application/json

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

### BadRequest

- application/json

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

### RequestTimeout

- application/json

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

### Forbidden

- application/json

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

### NotFound

- application/json

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```

### UnexpectedError

- application/json

```typescript
{
  success?: boolean
  // Duplicates HTTP response code
  response_code?: integer
  // Text description of error details
  description?: string
}
```