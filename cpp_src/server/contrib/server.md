# Reindexer REST API

<!-- toc -->

- [Overview](#overview)
  * [Version information](#version-information)
  * [License information](#license-information)
  * [URI scheme](#uri-scheme)
  * [Tags](#tags)
  * [Produces](#produces)
- [Paths](#paths)
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
  * [Get activity stats information](#get-activity-stats-information)
  * [Get client connection information](#get-client-connection-information)
  * [Get replication statistics](#get-replication-statistics)
  * [Get memory stats information](#get-memory-stats-information)
  * [Get performance stats information](#get-performance-stats-information)
  * [Get SELECT queries performance stats information](#get-select-queries-performance-stats-information)
  * [Update system config](#update-system-config)
- [Definitions](#definitions)
  * [ActionCommand](#actioncommand)
  * [ActivityStats](#activitystats)
  * [AggregationResDef](#aggregationresdef)
  * [AggregationsDef](#aggregationsdef)
  * [AggregationsSortDef](#aggregationssortdef)
  * [AsyncReplicationConfig](#asyncreplicationconfig)
  * [BeginTransactionResponse](#begintransactionresponse)
  * [CacheMemStats](#cachememstats)
  * [ClientsStats](#clientsstats)
  * [CommonPerfStats](#commonperfstats)
  * [Database](#database)
  * [DatabaseMemStats](#databasememstats)
  * [DatabasePerfStats](#databaseperfstats)
  * [Databases](#databases)
  * [EqualPositionDef](#equalpositiondef)
  * [ExplainDef](#explaindef)
  * [FilterDef](#filterdef)
  * [FtStopWordObject](#ftstopwordobject)
  * [FulltextConfig](#fulltextconfig)
  * [FulltextFieldConfig](#fulltextfieldconfig)
  * [FulltextSynonym](#fulltextsynonym)
  * [GlobalReplicationStats](#globalreplicationstats)
  * [Index](#index)
  * [IndexCacheMemStats](#indexcachememstats)
  * [IndexMemStat](#indexmemstat)
  * [Indexes](#indexes)
  * [Items](#items)
  * [ItemsUpdateResponse](#itemsupdateresponse)
  * [JoinCacheMemStats](#joincachememstats)
  * [JoinedDef](#joineddef)
  * [JsonObjectDef](#jsonobjectdef)
  * [LongQueriesLogging](#longquerieslogging)
  * [MetaByKeyResponse](#metabykeyresponse)
  * [MetaInfo](#metainfo)
  * [MetaListResponse](#metalistresponse)
  * [Namespace](#namespace)
  * [NamespaceMemStats](#namespacememstats)
  * [NamespacePerfStats](#namespaceperfstats)
  * [Namespaces](#namespaces)
  * [NamespacesConfig](#namespacesconfig)
  * [OnDef](#ondef)
  * [ProfilingConfig](#profilingconfig)
  * [QueriesPerfStats](#queriesperfstats)
  * [Query](#query)
  * [QueryCacheMemStats](#querycachememstats)
  * [QueryColumnDef](#querycolumndef)
  * [QueryItems](#queryitems)
  * [QueryPerfStats](#queryperfstats)
  * [ReplicationConfig](#replicationconfig)
  * [ReplicationStats](#replicationstats)
  * [ReplicationSyncStat](#replicationsyncstat)
  * [SchemaDef](#schemadef)
  * [SelectLogging](#selectlogging)
  * [SelectPerfStats](#selectperfstats)
  * [SortDef](#sortdef)
  * [StatusResponse](#statusresponse)
  * [SubQuery](#subquery)
  * [SubQueryAggregationsDef](#subqueryaggregationsdef)
  * [SuggestItems](#suggestitems)
  * [SysInfo](#sysinfo)
  * [SystemConfigItem](#systemconfigitem)
  * [TransactionLogging](#transactionlogging)
  * [TransactionsPerfStats](#transactionsperfstats)
  * [UpdateDeleteLogging](#updatedeletelogging)
  * [UpdateField](#updatefield)
  * [UpdatePerfStats](#updateperfstats)
  * [UpdateResponse](#updateresponse)

<!-- tocstop -->

## Overview
**Reindexer** is an embeddable, in-memory, document-oriented database with a high-level Query builder interface.
Reindexer's goal is to provide fast search with complex queries.
Reindexer is compact, fast and it does not have heavy dependencies.


### Version information
*Version* : 4.14.0


### License information
*License* : Apache 2.0  
*License URL* : http://www.apache.org/licenses/LICENSE-2.0.html  
*Terms of service* : null


### URI scheme
*BasePath* : /api/v1  
*Schemes* : HTTP


### Tags

* databases : Databases management
* indexes : Indexes management
* items : Documents management
* namespaces : Namespaces management
* queries : Queries to reindexer (dsl/sql)
* system : System methods


### Produces

* `application/json`





## Paths


### List available databases
```
GET /db
```


#### Description
This operation will output list of all available databases


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Query**|**sort_order**  <br>*optional*|Sort Order|enum (asc, desc)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[Databases](#databases)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* databases



### Create new database
```
POST /db
```


#### Description
This operation will create new database. If database is already exists, then error will be returned.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**body**  <br>*required*|Database definintion|[Database](#database)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* databases



### Drop database
```
DELETE /db/{database}
```


#### Description
This operation will remove complete database from memory and disk. 
All data, including namespaces, their documents and indexes will be erased. 
Can not be undone. USE WITH CAUTION.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* databases



### List available namespaces
```
GET /db/{database}/namespaces
```


#### Description
This operation will list all availavle namespaces in specified database.
If database is not exists, then error will be returned.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Query**|**sort_order**  <br>*optional*|Sort Order|enum (asc, desc)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[Namespaces](#namespaces)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* namespaces



### Create namespace
```
POST /db/{database}/namespaces
```


#### Description
This operation will create new namespace in specified database.
If namespace is already exists, then operation do not nothing.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Body**|**body**  <br>*required*|Namespace definintion|[Namespace](#namespace)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* namespaces



### Get namespace description
```
GET /db/{database}/namespaces/{name}
```


#### Description
This operation will return specified namespace description, including options of namespace, and available indexes


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[Namespace](#namespace)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* namespaces



### Drop namespace
```
DELETE /db/{database}/namespaces/{name}
```


#### Description
This operation will delete completely namespace from memory and disk.
All documents, indexes and metadata from namespace will be removed.
Can not be undone. USE WITH CAUTION.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* namespaces



### Truncate namespace
```
DELETE /db/{database}/namespaces/{name}/truncate
```


#### Description
This operation will delete all items from namespace.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* namespaces



### Rename namespace
```
GET /db/{database}/namespaces/{name}/rename/{newname}
```


#### Description
This operation will rename namespace.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|
|**Path**|**newname**  <br>*required*|Namespace new name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* namespaces



### Get list of namespace's meta info
```
GET /db/{database}/namespaces/{name}/metalist
```


#### Description
This operation will return list of keys of all meta of specified namespace


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string||
|**Path**|**name**  <br>*required*|Namespace name|string||
|**Query**|**limit**  <br>*optional*|If 0 - no limit|integer|`0`|
|**Query**|**offset**  <br>*optional*||integer|`0`|
|**Query**|**sort_order**  <br>*optional*|Sort Order|enum (asc, desc)||
|**Query**|**with_values**  <br>*optional*|Include values in response|boolean|`"false"`|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[MetaListResponse](#metalistresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* namespaces



### Get namespace's meta info by key
```
GET /db/{database}/namespaces/{name}/metabykey/{key}
```


#### Description
This operation will return value of namespace's meta with specified key


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**key**  <br>*required*|Meta key|string|
|**Path**|**name**  <br>*required*|Namespace name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[MetaByKeyResponse](#metabykeyresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* namespaces



### Put namespace's meta info with specified key and value
```
PUT /db/{database}/namespaces/{name}/metabykey
```


#### Description
This operation will set namespace's meta with specified key and value


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|
|**Body**|**meta_info**  <br>*required*|Meta info|[MetaInfo](#metainfo)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[UpdateResponse](#updateresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* namespaces



### Get documents from namespace
```
GET /db/{database}/namespaces/{name}/items
```


#### Description
This operation will select documents from namespace with specified filters, and sort them by specified sort order. Paging with limit and offset are supported.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|
|**Query**|**fields**  <br>*optional*|Comma-separated list of returned fields|string|
|**Query**|**filter**  <br>*optional*|Filter with SQL syntax, e.g: field1 = 'v1' AND field2 > 'v2'|string|
|**Query**|**format**  <br>*optional*|encoding data format|enum (json, msgpack, protobuf, csv-file)|
|**Query**|**limit**  <br>*optional*|Maximum count of returned items|integer|
|**Query**|**offset**  <br>*optional*|Offset of first returned item|integer|
|**Query**|**sharding**  <br>*optional*|if off then get items from current node only|enum (true, false)|
|**Query**|**sort_field**  <br>*optional*|Sort Field|string|
|**Query**|**sort_order**  <br>*optional*|Sort Order|enum (asc, desc)|
|**Query**|**with_shard_ids**  <br>*optional*|if sharding is enabled, then add the #shard_id field to the item|enum (true, false)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[Items](#items)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* items



### Update documents in namespace
```
PUT /db/{database}/namespaces/{name}/items
```


#### Description
This operation will UPDATE documents in namespace, by their primary keys.
Each document should be in request body as separate JSON object, e.g.
```
{"id":100, "name": "Pet"}
{"id":101, "name": "Dog"}
...
```


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|
|**Query**|**format**  <br>*optional*|encoding data format|enum (json, msgpack, protobuf)|
|**Query**|**precepts**  <br>*optional*|Precepts to be done|< string > array(multi)|
|**Body**|**body**  <br>*required*||object|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[ItemsUpdateResponse](#itemsupdateresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* items



### Insert documents to namespace
```
POST /db/{database}/namespaces/{name}/items
```


#### Description
This operation will INSERT documents to namespace, by their primary keys.
Each document should be in request body as separate JSON object, e.g.
```
{"id":100, "name": "Pet"}
{"id":101, "name": "Dog"}
...
```


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|
|**Query**|**format**  <br>*optional*|encoding data format|enum (json, msgpack, protobuf)|
|**Query**|**precepts**  <br>*optional*|Precepts to be done|< string > array(multi)|
|**Body**|**body**  <br>*required*||object|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[ItemsUpdateResponse](#itemsupdateresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* items



### Delete documents from namespace
```
DELETE /db/{database}/namespaces/{name}/items
```


#### Description
This operation will DELETE documents from namespace, by their primary keys.
Each document should be in request body as separate JSON object, e.g.
```
{"id":100}
{"id":101}
...
```


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|
|**Query**|**precepts**  <br>*optional*|Precepts to be done|< string > array(multi)|
|**Body**|**body**  <br>*required*||object|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[ItemsUpdateResponse](#itemsupdateresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* items



### Upsert documents in namespace
```
PATCH /db/{database}/namespaces/{name}/items
```


#### Description
This operation will UPSERT documents in namespace, by their primary keys.
Each document should be in request body as separate JSON object, e.g.
```
{"id":100, "name": "Pet"}
{"id":101, "name": "Dog"}
...
```


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|
|**Query**|**format**  <br>*optional*|encoding data format|enum (json, msgpack, protobuf)|
|**Query**|**precepts**  <br>*optional*|Precepts to be done|< string > array(multi)|
|**Body**|**body**  <br>*required*||object|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[ItemsUpdateResponse](#itemsupdateresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* items



### List available indexes
```
GET /db/{database}/namespaces/{name}/indexes
```


#### Description
This operation will return list of available indexes, from specified database and namespace.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[Indexes](#indexes)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* indexes



### Update index in namespace
```
PUT /db/{database}/namespaces/{name}/indexes
```


#### Description
This operation will update index parameters. E.g. type of field or type of index.
Operation  is synchronious, so it can take long time, if namespace contains bunch of documents


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|
|**Body**|**body**  <br>*required*|Index definition|[Index](#index)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* indexes



### Add new index to namespace
```
POST /db/{database}/namespaces/{name}/indexes
```


#### Description
This operation will create new index. If index is already exists with the different parameters, then error will be returned.
Operation is synchronious, so it can take long time, if namespace contains bunch of documents.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|
|**Body**|**body**  <br>*required*|Index definition|[Index](#index)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* indexes



### Drop index from namespace
```
DELETE /db/{database}/namespaces/{name}/indexes/{indexname}
```


#### Description
This operation will remove index from namespace. No data will be erased.
Operation  is synchronious, so it can take long time, if namespace contains bunch of documents.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**indexname**  <br>*required*|Index name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* indexes



### Get namespace schema
```
GET /db/{database}/namespaces/{name}/schema
```


#### Description
This operation will return current schema from specified database and namespace


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[SchemaDef](#schemadef)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* schema



### Set namespace schema
```
PUT /db/{database}/namespaces/{name}/schema
```


#### Description
This operation will set namespace schema (information about available fields and field types)


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|
|**Body**|**body**  <br>*required*|This operation will put new schema for specified database and namespace|[SchemaDef](#schemadef)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* schema



### Get protobuf communication parameters schema
```
GET /db/{database}/protobuf_schema
```


#### Description
This operation allows to get client/server communication parameters as google protobuf schema (content of .proto file)


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Query**|**ns**  <br>*required*|Namespace name|< string > array(multi)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|No Content|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Produces

* `text/plain`


#### Tags

* schema



### Query documents from namespace
```
GET /db/{database}/query
```


#### Description
This operation queries documents from namespace by SQL query. Query can be preced by `EXPLAIN` statement, then query execution plan will be returned with query results. 
Two level paging is supported. At first, applied normal SQL `LIMIT` and `OFFSET`,
then `limit` and `offset` from http request.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Query**|**format**  <br>*optional*|encoding data format|enum (json, msgpack, protobuf, csv-file)|
|**Query**|**limit**  <br>*optional*|Maximum count of returned items|integer|
|**Query**|**offset**  <br>*optional*|Offset of first returned item|integer|
|**Query**|**q**  <br>*required*|SQL query|string|
|**Query**|**sharding**  <br>*optional*|if off then execute SQL query on current node|enum (true, false)|
|**Query**|**width**  <br>*optional*|Total width in rows of view for table format output|integer|
|**Query**|**with_columns**  <br>*optional*|Return columns names and widths for table format output|boolean|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[QueryItems](#queryitems)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* queries



### Update documents in namespace
```
PUT /db/{database}/query
```


#### Description
This operation updates documents in namespace by DSL query.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Body**|**body**  <br>*required*|DSL query|[Query](#query)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* queries



### Query documents from namespace
```
POST /db/{database}/query
```


#### Description
This operation queries documents from namespace by DSL query.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Query**|**format**  <br>*optional*|encoding data format|enum (json, msgpack, protobuf, csv-file)|
|**Query**|**width**  <br>*optional*|Total width in rows of view for table format output|integer|
|**Query**|**with_columns**  <br>*optional*|Return columns names and widths for table format output|boolean|
|**Body**|**body**  <br>*required*|DSL query|[Query](#query)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[QueryItems](#queryitems)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* queries



### Delete documents from namespace
```
DELETE /db/{database}/query
```


#### Description
This operation removes documents from namespace by DSL query.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Body**|**body**  <br>*required*|DSL query|[Query](#query)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* queries



### Begin transaction to namespace
```
POST /db/{database}/namespaces/{name}/transactions/begin
```


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**name**  <br>*required*|Namespace name|string|
|**Query**|**format**  <br>*optional*|encoding data format|enum (json, msgpack, protobuf)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[BeginTransactionResponse](#begintransactionresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* transactions



### Commit transaction
```
POST /db/{database}/transactions/{tx_id}/commit
```


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**tx_id**  <br>*required*|transaction id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* transactions



### Rollback transaction
```
POST /db/{database}/transactions/{tx_id}/rollback
```


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**tx_id**  <br>*required*|transaction id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* transactions



### Update documents in namespace via transaction
```
PUT /db/{database}/transactions/{tx_id}/items
```


#### Description
This will add UPDATE operation into transaction.
It UPDATEs documents in namespace, by their primary keys.
Each document should be in request body as separate JSON object, e.g.
```
{"id":100, "name": "Pet"}
{"id":101, "name": "Dog"}
...
```


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**tx_id**  <br>*required*|transaction id|string|
|**Query**|**format**  <br>*optional*|encoding data format|enum (json, msgpack, protobuf)|
|**Query**|**precepts**  <br>*optional*|Precepts to be done|< string > array(multi)|
|**Body**|**body**  <br>*required*||object|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* transactions



### Insert documents to namespace via transaction
```
POST /db/{database}/transactions/{tx_id}/items
```


#### Description
This will add INSERT operation into transaction.
It INSERTs documents to namespace, by their primary keys.
Each document should be in request body as separate JSON object, e.g.
```
{"id":100, "name": "Pet"}
{"id":101, "name": "Dog"}
...
```


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**tx_id**  <br>*required*|transaction id|string|
|**Query**|**format**  <br>*optional*|encoding data format|enum (json, msgpack, protobuf)|
|**Query**|**precepts**  <br>*optional*|Precepts to be done|< string > array(multi)|
|**Body**|**body**  <br>*required*||object|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* transactions



### Delete documents from namespace via transaction
```
DELETE /db/{database}/transactions/{tx_id}/items
```


#### Description
This will add DELETE operation into transaction.
It DELETEs documents from namespace, by their primary keys.
Each document should be in request body as separate JSON object, e.g.
```
{"id":100}
{"id":101}
...
```


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**tx_id**  <br>*required*|transaction id|string|
|**Query**|**precepts**  <br>*optional*|Precepts to be done|< string > array(multi)|
|**Body**|**body**  <br>*required*||object|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* transactions



### Upsert documents in namespace via transaction
```
PATCH /db/{database}/transactions/{tx_id}/items
```


#### Description
This will add UPSERT operation into transaction.
It UPDATEs documents in namespace, by their primary keys.
Each document should be in request body as separate JSON object, e.g.
```
{"id":100, "name": "Pet"}
{"id":101, "name": "Dog"}
...
```


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**tx_id**  <br>*required*|transaction id|string|
|**Query**|**format**  <br>*optional*|encoding data format|enum (json, msgpack, protobuf)|
|**Query**|**precepts**  <br>*optional*|Precepts to be done|< string > array(multi)|
|**Body**|**body**  <br>*required*||object|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* transactions



### Delete/update queries for transactions
```
GET /db/{database}/transactions/{tx_id}/query
```


#### Description
This will add DELETE/UPDATE SQL query into transaction.
This query UPDATEs/DELETEs documents from namespace


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**tx_id**  <br>*required*|transaction id|string|
|**Query**|**format**  <br>*optional*|encoding data format|enum (json, msgpack, protobuf)|
|**Query**|**q**  <br>*required*|SQL query|string|
|**Query**|**width**  <br>*optional*|Total width in rows of view for table format output|integer|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* transactions



### Delete documents from namespace (transactions)
```
DELETE /db/{database}/transactions/{tx_id}/query
```


#### Description
This will add DELETE query into transaction.
DELETE query removes documents from namespace by DSL query.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Path**|**tx_id**  <br>*required*|transaction id|string|
|**Query**|**tx_id**  <br>*optional*|transaction id|string|
|**Body**|**body**  <br>*required*|DSL query|[Query](#query)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successful operation|[StatusResponse](#statusresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* transactions



### Suggest for autocompletion of SQL query
```
GET /db/{database}/suggest
```


#### Description
This operation pareses SQL query, and suggests autocompletion variants


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Query**|**line**  <br>*required*|Cursor line for suggest|integer|
|**Query**|**pos**  <br>*required*|Cursor position for suggest|integer|
|**Query**|**q**  <br>*required*|SQL query|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[SuggestItems](#suggestitems)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* queries



### Query documents from namespace
```
POST /db/{database}/sqlquery
```


#### Description
This operation queries documents from namespace by SQL query. Query can be preced by `EXPLAIN` statement, then query execution plan will be returned with query results.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Query**|**format**  <br>*optional*|encoding data format|enum (json, msgpack, protobuf, csv-file)|
|**Query**|**width**  <br>*optional*|Total width in rows of view for table format output|integer|
|**Query**|**with_columns**  <br>*optional*|Return columns names and widths for table format output|boolean|
|**Body**|**q**  <br>*required*|SQL query|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[QueryItems](#queryitems)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* queries



### Get system information
```
GET /check
```


#### Description
This operation will return system informatiom about server version, uptime, and resources consumtion


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[SysInfo](#sysinfo)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* system



### Try to release free memory back to the operating system for reuse by other applications.
```
POST /allocator/drop_cache
```


#### Description
Try to release free memory back to the operating system for reuse. Only for tcmalloc allocator.


#### Tags

* system



### Get memory usage information
```
GET /allocator/info
```


#### Description
This operation will return memory usage informatiom from tcmalloc allocator.


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|No Content|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* system



### Get activity stats information
```
GET /db/{database}/namespaces/%23activitystats/items
```


#### Description
This operation will return detailed informatiom about current activity of all connected to the database clients


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[ActivityStats](#activitystats)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* system



### Get client connection information
```
GET /db/{database}/namespaces/%23clientsstats/items
```


#### Description
This operation will return detailed informatiom about all connections on the server


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[ClientsStats](#clientsstats)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* system



### Get replication statistics
```
GET /db/{database}/namespaces/%23replicationstats/items
```


#### Description
This operation will return detailed informatiom about replication status on this node or cluster


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Query**|**filter**  <br>*required*|Filter with SQL syntax, e.g: field1 = 'v1' AND field2 > 'v2'. Has to filter by 'type' field: either 'async' or 'cluster'|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[GlobalReplicationStats](#globalreplicationstats)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* system



### Get memory stats information
```
GET /db/{database}/namespaces/%23memstats/items
```


#### Description
This operation will return detailed informatiom about database memory consumption


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[DatabaseMemStats](#databasememstats)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* system



### Get performance stats information
```
GET /db/{database}/namespaces/%23perfstats/items
```


#### Description
This operation will return detailed informatiom about database performance timings. By default performance stats is turned off.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[DatabasePerfStats](#databaseperfstats)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* system



### Get SELECT queries performance stats information
```
GET /db/{database}/namespaces/%23queriesperfstats/items
```


#### Description
This operation will return detailed informatiom about database memory consumption. By default qureis performance stat is turned off.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[QueriesPerfStats](#queriesperfstats)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* system



### Update system config
```
PUT /db/{database}/namespaces/%23config/items
```


#### Description
This operation will update system configuration:
- profiling configuration. It is used to enable recording of queries and overal performance;
- log queries configurating.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**database**  <br>*required*|Database name|string|
|**Body**|**body**  <br>*required*||[SystemConfigItem](#systemconfigitem)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|successful operation|[UpdateResponse](#updateresponse)|
|**400**|Invalid arguments supplied|[StatusResponse](#statusresponse)|
|**403**|Forbidden|[StatusResponse](#statusresponse)|
|**404**|Entry not found|[StatusResponse](#statusresponse)|
|**408**|Context timeout|[StatusResponse](#statusresponse)|
|**500**|Unexpected internal error|[StatusResponse](#statusresponse)|


#### Tags

* system





## Definitions


### ActionCommand

|Name|Description|Schema|
|---|---|---|
|**command**  <br>*required*|Command to execute|enum (restart_replication, reset_replication_role)|
|**namespace**  <br>*optional*|Namespace name for reset_replication_role. May be empty|string|



### ActivityStats

|Name|Description|Schema|
|---|---|---|
|**items**  <br>*optional*||< [items](#activitystats-items) > array|
|**total_items**  <br>*optional*|Total count of documents, matched specified filters|integer|


**items**

|Name|Description|Schema|
|---|---|---|
|**client**  <br>*required*|Client identifier|string|
|**lock_description**  <br>*optional*||string|
|**query**  <br>*required*|Query text|string|
|**query_id**  <br>*required*|Query identifier|integer|
|**query_start**  <br>*required*|Query start time|string|
|**state**  <br>*required*|Current operation state|enum (in_progress, wait_lock, sending, indexes_lookup, select_loop, proxied_via_cluster_proxy, proxied_via_sharding_proxy)|
|**user**  <br>*optional*|User name|string|



### AggregationResDef

|Name|Description|Schema|
|---|---|---|
|**distincts**  <br>*optional*|List of distinct values of the field|< string > array|
|**facets**  <br>*optional*|Facets, calculated by aggregator|< [facets](#aggregationresdef-facets) > array|
|**fields**  <br>*required*|Fields or indexes names for aggregation function|< string > array|
|**type**  <br>*required*|Aggregation function|enum (SUM, AVG, MIN, MAX, FACET, DISTINCT)|
|**value**  <br>*optional*|Value, calculated by aggregator|number|


**facets**

|Name|Description|Schema|
|---|---|---|
|**count**  <br>*required*|Count of elemens these fields values|integer|
|**values**  <br>*required*|Facet fields values|< string > array|



### AggregationsDef

|Name|Description|Schema|
|---|---|---|
|**fields**  <br>*required*|Fields or indexes names for aggregation function|< string > array|
|**limit**  <br>*optional*|Number of rows to get from result set. Allowed only for FACET  <br>**Minimum value** : `0`|integer|
|**offset**  <br>*optional*|Index of the first row to get from result set. Allowed only for FACET  <br>**Minimum value** : `0`|integer|
|**sort**  <br>*optional*|Specifies results sorting order. Allowed only for FACET|< [AggregationsSortDef](#aggregationssortdef) > array|
|**type**  <br>*required*|Aggregation function|enum (SUM, AVG, MIN, MAX, FACET, DISTINCT)|



### AggregationsSortDef
Specifies facet aggregations results sorting order


|Name|Description|Schema|
|---|---|---|
|**desc**  <br>*optional*|Descent or ascent sorting direction|boolean|
|**field**  <br>*required*|Field or index name for sorting|string|



### AsyncReplicationConfig

|Name|Description|Schema|
|---|---|---|
|**app_name**  <br>*optional*|Application name, used by replicator as a login tag|string|
|**batching_routines_count**  <br>*optional*|Number of coroutines for updates batching (per namespace). Higher value here may help to reduce networks triparound await time, but will require more RAM|integer|
|**enable_compression**  <br>*optional*|Enable network traffic compression|boolean|
|**log_level**  <br>*optional*|Replication log level on replicator's startup|enum (none, error, warning, info, trace)|
|**max_wal_depth_on_force_sync**  <br>*optional*|Maximum number of WAL records, which will be copied after force-sync|integer|
|**mode**  <br>*optional*|Allows to configure async replication from sync raft-cluster (replicate either from each node, or from synchronous cluster leader)|enum (default, from_sync_leader)|
|**namespaces**  <br>*required*|General list of namespaces for replication. Empty means all of the namespaces. All replicated namespaces will become read only for followers|< string > array|
|**nodes**  <br>*required*|Followers list|< [nodes](#asyncreplicationconfig-nodes) > array|
|**online_updates_delay_msec**  <br>*optional*|Delay between write operation and replication. Larger values here will leader to higher replication latency and bufferization, but also will provide more effective network batching and CPU untilization|integer|
|**online_updates_timeout_sec**  <br>*optional*|Node response timeout for online-replication (seconds)|integer|
|**retry_sync_interval_msec**  <br>*optional*|Resync timeout on network errors|integer|
|**role**  <br>*required*|Replication role|enum (none, follower, leader)|
|**sync_threads**  <br>*optional*|Number of data replication threads|integer|
|**sync_timeout_sec**  <br>*optional*|Network timeout for communication with followers (for force and wal synchronization), in seconds|integer|
|**syncs_per_thread**  <br>*optional*|Max number of concurrent force/wal sync's per thread|integer|


**nodes**

|Name|Description|Schema|
|---|---|---|
|**dsn**  <br>*required*|Follower's DSN. Must have cproto-scheme|string|
|**namespaces**  <br>*optional*|List of namespaces to replicate on this specific node. Empty means all of the namespaces. If field doesn't exists, then general list will be used|< string > array|



### BeginTransactionResponse

|Name|Description|Schema|
|---|---|---|
|**tx_id**  <br>*optional*|Unique transaction id|string|



### CacheMemStats

|Name|Description|Schema|
|---|---|---|
|**empty_count**  <br>*optional*|Count of empty elements slots in this cache|integer|
|**hit_count_limit**  <br>*optional*|Number of hits of queries, to store results in cache|integer|
|**items_count**  <br>*optional*|Count of used elements stored in this cache|integer|
|**total_size**  <br>*optional*|Total memory consumption by this cache|integer|



### ClientsStats

|Name|Description|Schema|
|---|---|---|
|**items**  <br>*optional*||< [items](#clientsstats-items) > array|
|**total_items**  <br>*optional*|Count of connected clients|integer|


**items**

|Name|Description|Schema|
|---|---|---|
|**app_name**  <br>*required*|Client's aplication name|string|
|**client_version**  <br>*required*|Client version string|string|
|**connection_id**  <br>*required*|Connection identifier|integer|
|**current_activity**  <br>*required*|Current activity|string|
|**db_name**  <br>*required*|Database name|string|
|**ip**  <br>*required*|Ip|string|
|**last_recv_ts**  <br>*optional*|Timestamp of last recv operation (ms)|integer|
|**last_send_ts**  <br>*optional*|Timestamp of last send operation (ms)|integer|
|**recv_bytes**  <br>*required*|Receive byte|integer|
|**recv_rate**  <br>*optional*|Current recv rate (bytes/s)|integer|
|**send_buf_bytes**  <br>*optional*|Send buffer size|integer|
|**send_rate**  <br>*optional*|Current send rate (bytes/s)|integer|
|**sent_bytes**  <br>*required*|Send byte|integer|
|**start_time**  <br>*required*|Server start time in unix timestamp|integer|
|**tx_count**  <br>*required*|Count of currently opened transactions for this client|integer|
|**user_name**  <br>*required*|User name|string|
|**user_rights**  <br>*required*|User right|string|



### CommonPerfStats

|Name|Description|Schema|
|---|---|---|
|**last_sec_avg_latency_us**  <br>*optional*|Average latency (execution time) for queries to this object at last second|integer|
|**last_sec_avg_lock_time_us**  <br>*optional*|Average waiting time for acquiring lock to this object at last second|integer|
|**last_sec_qps**  <br>*optional*|Count of queries to this object, requested at last second|integer|
|**latency_stddev**  <br>*optional*|Standard deviation of latency values|number|
|**max_latency_us**  <br>*optional*|Maximum latency value|integer|
|**min_latency_us**  <br>*optional*|Minimal latency value|integer|
|**total_avg_latency_us**  <br>*optional*|Average latency (execution time) for queries to this object|integer|
|**total_avg_lock_time_us**  <br>*optional*|Average waiting time for acquiring lock to this object|integer|
|**total_queries_count**  <br>*optional*|Total count of queries to this object|integer|



### Database

|Name|Description|Schema|
|---|---|---|
|**name**  <br>*optional*|Name of database  <br>**Pattern** : `"^[A-Za-z0-9_\\-]*$"`|string|



### DatabaseMemStats

|Name|Description|Schema|
|---|---|---|
|**items**  <br>*optional*|Documents, matched specified filters|< [NamespaceMemStats](#namespacememstats) > array|
|**total_items**  <br>*optional*|Total count of documents, matched specified filters|integer|



### DatabasePerfStats

|Name|Description|Schema|
|---|---|---|
|**items**  <br>*optional*|Documents, matched specified filters|< [NamespacePerfStats](#namespaceperfstats) > array|
|**total_items**  <br>*optional*|Total count of documents, matched specified filters|integer|



### Databases

|Name|Description|Schema|
|---|---|---|
|**items**  <br>*optional*||< string > array|
|**total_items**  <br>*optional*|Total count of databases|integer|



### EqualPositionDef
Array fields to be searched with equal array indexes


|Name|Schema|
|---|---|
|**positions**  <br>*optional*|< string > array|



### ExplainDef
Query execution explainings


|Name|Description|Schema|
|---|---|---|
|**general_sort_us**  <br>*optional*|Result sort time|integer|
|**indexes_us**  <br>*optional*|Indexes keys selection time|integer|
|**loop_us**  <br>*optional*|Intersection loop time|integer|
|**on_conditions_injections**  <br>*optional*|Describes Join ON conditions injections|< [on_conditions_injections](#explaindef-on_conditions_injections) > array|
|**postprocess_us**  <br>*optional*|Query post process time|integer|
|**prepare_us**  <br>*optional*|Query prepare and optimize time|integer|
|**preselect_us**  <br>*optional*|Query preselect processing time|integer|
|**selectors**  <br>*optional*|Filter selectors, used to proccess query conditions|< [selectors](#explaindef-selectors) > array|
|**sort_by_uncommitted_index**  <br>*optional*|Optimization of sort by uncompleted index has been performed|boolean|
|**sort_index**  <br>*optional*|Index, which used for sort results|string|
|**subqueries**  <br>*optional*|Explain of subqueries preselect|< [subqueries](#explaindef-subqueries) > array|
|**total_us**  <br>*optional*|Total query execution time|integer|


**on_conditions_injections**

|Name|Description|Schema|
|---|---|---|
|**conditions**  <br>*optional*|Individual conditions processing results|< [conditions](#explaindef-conditions) > array|
|**injected_condition**  <br>*optional*|Injected condition. SQL-like string|string|
|**namespace**  <br>*optional*|Joinable ns name|string|
|**on_condition**  <br>*optional*|Original ON-conditions clause. SQL-like string|string|
|**reason**  <br>*optional*|Optional{succeed==false}. Explains condition injection failure|string|
|**success**  <br>*optional*|Result of injection attempt|boolean|
|**type**  <br>*optional*|Values source: preselect values(by_value) or additional select(select)|string|


**conditions**

|Name|Description|Schema|
|---|---|---|
|**agg_type**  <br>*optional*|Optional. Aggregation type used in subquery|enum (min, max, distinct)|
|**condition**  <br>*optional*|single condition from Join ON section. SQL-like string|string|
|**explain_select**  <br>*optional*|Optional. Explain of Select subquery|[ExplainDef](#explaindef)|
|**new_condition**  <br>*optional*|substituted injected condition. SQL-like string|string|
|**reason**  <br>*optional*|Optional. Explains condition injection failure|string|
|**success**  <br>*optional*|result of injection attempt|boolean|
|**total_time_us**  <br>*optional*|total time elapsed from injection attempt start till the end of substitution or rejection|integer|
|**values_count**  <br>*optional*|resulting size of query values set|integer|


**selectors**

|Name|Description|Schema|
|---|---|---|
|**comparators**  <br>*optional*|Count of comparators used, for this selector|integer|
|**cost**  <br>*optional*|Cost expectation of this selector|integer|
|**description**  <br>*optional*|Description of the selector|string|
|**explain_preselect**  <br>*optional*|Preselect in joined namespace execution explainings|[ExplainDef](#explaindef)|
|**explain_select**  <br>*optional*|One of selects in joined namespace execution explainings|[ExplainDef](#explaindef)|
|**field**  <br>*optional*|Field or index name|string|
|**field_type**  <br>*optional*|Shows which kind of the field was used for the filtration. Non-indexed fields are usually really slow for 'scan' and should be avoided|enum (non-indexed, indexed)|
|**items**  <br>*optional*|Count of scanned documents by this selector|integer|
|**keys**  <br>*optional*|Number of uniq keys, processed by this selector (may be incorrect, in case of internal query optimization/caching|integer|
|**matched**  <br>*optional*|Count of processed documents, matched this selector|integer|
|**method**  <br>*optional*|Method, used to process condition|enum (scan, index, inner_join, left_join)|
|**type**  <br>*optional*|Type of the selector|string|


**subqueries**

|Name|Description|Schema|
|---|---|---|
|**explain**  <br>*optional*|Explain of the subquery's preselect|[ExplainDef](#explaindef)|
|**field**  <br>*optional*|Name of field being compared with the subquery's result|string|
|**keys**  <br>*optional*|Count of keys being compared with the subquery's result|integer|
|**namespace**  <br>*optional*|Subquery's namespace name|string|



### FilterDef
If contains 'filters' then cannot contain 'cond', 'field' and 'value'. If not contains 'filters' then 'field' and 'cond' are required.


|Name|Description|Schema|
|---|---|---|
|**always**  <br>*optional*|Boolean constant|boolean|
|**cond**  <br>*optional*|Condition operator|enum (EQ, GT, GE, LE, LT, SET, ALLSET, EMPTY, RANGE, LIKE, DWITHIN)|
|**equal_positions**  <br>*optional*|Array of array fields to be searched with equal array indexes|< [EqualPositionDef](#equalpositiondef) > array|
|**field**  <br>*optional*|Field json path or index name for filter|string|
|**filters**  <br>*optional*|Filter for results documents|< [FilterDef](#filterdef) > array|
|**first_field**  <br>*optional*|First field json path or index name for filter by two fields|string|
|**join_query**  <br>*optional*||[JoinedDef](#joineddef)|
|**op**  <br>*optional*|Logic operator|enum (AND, OR, NOT)|
|**second_field**  <br>*optional*|Second field json path or index name for filter by two fields|string|
|**subquery**  <br>*optional*|Subquery to compare its result|[SubQuery](#subquery)|
|**value**  <br>*optional*|Value of filter. Single integer or string for EQ, GT, GE, LE, LT condition, array of 2 elements for RANGE condition, variable len array for SET and ALLSET conditions, or something like that: '[[1, -3.5],5.0]' for DWITHIN|object|



### FtStopWordObject

|Name|Description|Schema|
|---|---|---|
|**is_morpheme**  <br>*optional*|If the value is true, the word can be included in search results in queries such as 'word*', 'word~' etc.  <br>**Default** : `false`|boolean|
|**word**  <br>*optional*|Stop word|string|



### FulltextConfig
Fulltext Index configuration


|Name|Description|Schema|
|---|---|---|
|**base_ranking**  <br>*optional*|Config for subterm proc rank.|[base_ranking](#fulltextconfig-base_ranking)|
|**bm25_boost**  <br>*optional*|Boost of bm25 ranking  <br>**Default** : `1.0`  <br>**Minimum value** : `0`  <br>**Maximum value** : `10`|number (float)|
|**bm25_config**  <br>*optional*|Config for document ranking function|[bm25_config](#fulltextconfig-bm25_config)|
|**bm25_weight**  <br>*optional*|Weight of bm25 rank in final rank 0: bm25 will not change final rank. 1: bm25 will affect to finl rank in 0 - 100% range  <br>**Default** : `0.1`  <br>**Minimum value** : `0`  <br>**Maximum value** : `1`|number (float)|
|**distance_boost**  <br>*optional*|Boost of search query term distance in found document  <br>**Default** : `1.0`  <br>**Minimum value** : `0`  <br>**Maximum value** : `10`|number (float)|
|**distance_weight**  <br>*optional*|Weight of search query terms distance in found document in final rank 0: distance will not change final rank. 1: distance will affect to final rank in 0 - 100% range  <br>**Default** : `0.5`  <br>**Minimum value** : `0`  <br>**Maximum value** : `1`|number (float)|
|**enable_kb_layout**  <br>*optional*|Enable wrong keyboard layout variants processing. e.g. term 'keynbr' will match word 'лунтик'  <br>**Default** : `true`|boolean|
|**enable_numbers_search**  <br>*optional*|Enable number variants processing. e.g. term '100' will match words one hundred  <br>**Default** : `false`|boolean|
|**enable_preselect_before_ft**  <br>*optional*|Enable to execute others queries before the ft query  <br>**Default** : `false`|boolean|
|**enable_translit**  <br>*optional*|Enable russian translit variants processing. e.g. term 'luntik' will match word 'лунтик'  <br>**Default** : `true`|boolean|
|**enable_warmup_on_ns_copy**  <br>*optional*|Enable auto index warmup after atomic namespace copy on transaction  <br>**Default** : `false`|boolean|
|**extra_word_symbols**  <br>*optional*|List of symbols, which will be threated as word part, all other symbols will be thrated as wors separators  <br>**Default** : `"-/+"`|string|
|**fields**  <br>*optional*|Configuration for certian field if it differ from whole index configuration|< [FulltextFieldConfig](#fulltextfieldconfig) > array|
|**full_match_boost**  <br>*optional*|Boost of full match of search phrase with doc  <br>**Default** : `1.1`  <br>**Minimum value** : `0`  <br>**Maximum value** : `10`|number (float)|
|**log_level**  <br>*optional*|Log level of full text search engine  <br>**Minimum value** : `0`  <br>**Maximum value** : `4`|integer|
|**max_areas_in_doc**  <br>*optional*|Max number of highlighted areas for each field in each document (for snippet() and highlight()). '-1' means unlimited  <br>**Maximum value** : `1000000000`|number|
|**max_rebuild_steps**  <br>*optional*|Maximum steps without full rebuild of ft - more steps faster commit slower select - optimal about 15.  <br>**Minimum value** : `0`  <br>**Maximum value** : `500`|integer|
|**max_step_size**  <br>*optional*|Maximum unique words to step  <br>**Minimum value** : `5`  <br>**Maximum value** : `1000000000`|integer|
|**max_total_areas_to_cache**  <br>*optional*|Max total number of highlighted areas in ft result, when result still remains cacheable. '-1' means unlimited  <br>**Maximum value** : `1000000000`|number|
|**max_typo_len**  <br>*optional*|Maximum word length for building and matching variants with typos.  <br>**Minimum value** : `0`  <br>**Maximum value** : `100`|integer|
|**max_typos**  <br>*optional*|Maximum possible typos in word. 0: typos is disabled, words with typos will not match. N: words with N possible typos will match. It is not recommended to set more than 2 possible typo -It will seriously increase RAM usage, and decrease search speed  <br>**Minimum value** : `0`  <br>**Maximum value** : `4`|integer|
|**merge_limit**  <br>*optional*|Maximum documents count which will be processed in merge query results. Increasing this value may refine ranking of queries with high frequency words, but will decrease search speed  <br>**Minimum value** : `0`  <br>**Maximum value** : `65000`|integer|
|**min_relevancy**  <br>*optional*|Minimum rank of found documents. 0: all found documents will be returned 1: only documents with relevancy >= 100% will be returned  <br>**Default** : `0.05`  <br>**Minimum value** : `0`  <br>**Maximum value** : `1`|number (float)|
|**optimization**  <br>*optional*|Optimize the index by memory or by cpu  <br>**Default** : `"Memory"`|enum (Memory, CPU)|
|**partial_match_decrease**  <br>*optional*|Decrease of relevancy in case of partial match by value: partial_match_decrease * (non matched symbols) / (matched symbols)  <br>**Minimum value** : `0`  <br>**Maximum value** : `100`|integer|
|**position_boost**  <br>*optional*|Boost of search query term position  <br>**Default** : `1.0`  <br>**Minimum value** : `0`  <br>**Maximum value** : `10`|number (float)|
|**position_weight**  <br>*optional*|Weight of search query term position in final rank. 0: term position will not change final rank. 1: term position will affect to final rank in 0 - 100% range  <br>**Default** : `0.1`  <br>**Minimum value** : `0`  <br>**Maximum value** : `1`|number (float)|
|**stemmers**  <br>*optional*|List of stemmers to use|< string > array|
|**stop_words**  <br>*optional*|List of objects of stop words. Words from this list will be ignored when building indexes|< [FtStopWordObject](#ftstopwordobject) > array|
|**sum_ranks_by_fields_ratio**  <br>*optional*|Ratio to summation of ranks of match one term in several fields. For example, if value of this ratio is K, request is '@+f1,+f2,+f3 word', ranks of match in fields are R1, R2, R3 and R2 < R1 < R3, final rank will be R = R2 + K*R1 + K*K*R3  <br>**Default** : `0.0`  <br>**Minimum value** : `0`  <br>**Maximum value** : `1`|number (float)|
|**synonyms**  <br>*optional*|List of synonyms for replacement|< [FulltextSynonym](#fulltextsynonym) > array|
|**term_len_boost**  <br>*optional*|Boost of search query term length  <br>**Default** : `1.0`  <br>**Minimum value** : `0`  <br>**Maximum value** : `10`|number (float)|
|**term_len_weight**  <br>*optional*|Weight of search query term length in final rank. 0: term length will not change final rank. 1: term length will affect to final rank in 0 - 100% range  <br>**Default** : `0.3`  <br>**Minimum value** : `0`  <br>**Maximum value** : `1`|number (float)|
|**typos_detailed_config**  <br>*optional*|Config for more precise typos algorithm tuning|[typos_detailed_config](#fulltextconfig-typos_detailed_config)|


**base_ranking**

|Name|Description|Schema|
|---|---|---|
|**base_typo_proc**  <br>*optional*|Base relevancy of typo match  <br>**Minimum value** : `0`  <br>**Maximum value** : `500`|integer|
|**full_match_proc**  <br>*optional*|Relevancy of full word match  <br>**Minimum value** : `0`  <br>**Maximum value** : `500`|integer|
|**kblayout_proc**  <br>*optional*|Relevancy of the match in incorrect kblayout  <br>**Minimum value** : `0`  <br>**Maximum value** : `500`|integer|
|**prefix_min_proc**  <br>*optional*|Mininum relevancy of prefix word match  <br>**Minimum value** : `0`  <br>**Maximum value** : `500`|integer|
|**stemmer_proc_penalty**  <br>*optional*|Penalty for the variants, created by stemming  <br>**Minimum value** : `0`  <br>**Maximum value** : `500`|integer|
|**suffix_min_proc**  <br>*optional*|Mininum relevancy of suffix word match  <br>**Minimum value** : `0`  <br>**Maximum value** : `500`|integer|
|**synonyms_proc**  <br>*optional*|Relevancy of the synonym match  <br>**Minimum value** : `0`  <br>**Maximum value** : `500`|integer|
|**translit_proc**  <br>*optional*|Relevancy of the match in translit  <br>**Minimum value** : `0`  <br>**Maximum value** : `500`|integer|
|**typo_proc_penalty**  <br>*optional*|Extra penalty for each word's permutation (addition/deletion of the symbol) in typo algorithm  <br>**Minimum value** : `0`  <br>**Maximum value** : `500`|integer|


**bm25_config**

|Name|Description|Schema|
|---|---|---|
|**bm25_b**  <br>*optional*|Coefficient b in the formula for calculating bm25. If b is bigger, the effects of the length of the document compared to the average length are more amplified.  <br>**Default** : `0.75`  <br>**Minimum value** : `0`  <br>**Maximum value** : `1`|number (float)|
|**bm25_k1**  <br>*optional*|Coefficient k1 in the formula for calculating bm25. Сoefficient that sets the saturation threshold for the frequency of the term. The higher the coefficient, the higher the threshold and the lower the saturation rate.  <br>**Default** : `2.0`  <br>**Minimum value** : `0`|number (float)|
|**bm25_type**  <br>*optional*|Formula for calculating document relevance (rx_bm25, bm25, word_count)  <br>**Default** : `"rx_bm25"`|enum (rx_bm25, bm25, word_count)|


**typos_detailed_config**

|Name|Description|Schema|
|---|---|---|
|**max_extra_letters**  <br>*optional*|Maximum number of symbols, which may be added to the initial term to transform it into the result word  <br>**Minimum value** : `-1`  <br>**Maximum value** : `2`|integer|
|**max_missing_letters**  <br>*optional*|Maximum number of symbols, which may be removed from the initial term to transform it into the result word  <br>**Minimum value** : `-1`  <br>**Maximum value** : `2`|integer|
|**max_symbol_permutation_distance**  <br>*optional*|Maximum distance between same symbols in initial and target words to perform substitution (to handle cases, when two symbolws were switched with each other)  <br>**Minimum value** : `-1`  <br>**Maximum value** : `100`|integer|
|**max_typo_distance**  <br>*optional*|Maximum distance between symbols in initial and target words to perform substitution  <br>**Minimum value** : `-1`  <br>**Maximum value** : `100`|integer|



### FulltextFieldConfig
Configuration for certian field if it differ from whole index configuration


|Name|Description|Schema|
|---|---|---|
|**bm25_boost**  <br>*optional*|Boost of bm25 ranking  <br>**Default** : `1.0`  <br>**Minimum value** : `0`  <br>**Maximum value** : `10`|number (float)|
|**bm25_weight**  <br>*optional*|Weight of bm25 rank in final rank 0: bm25 will not change final rank. 1: bm25 will affect to finl rank in 0 - 100% range  <br>**Default** : `0.1`  <br>**Minimum value** : `0`  <br>**Maximum value** : `1`|number (float)|
|**field_name**  <br>*optional*|Field name|string|
|**position_boost**  <br>*optional*|Boost of search query term position  <br>**Default** : `1.0`  <br>**Minimum value** : `0`  <br>**Maximum value** : `10`|number (float)|
|**position_weight**  <br>*optional*|Weight of search query term position in final rank. 0: term position will not change final rank. 1: term position will affect to final rank in 0 - 100% range  <br>**Default** : `0.1`  <br>**Minimum value** : `0`  <br>**Maximum value** : `1`|number (float)|
|**term_len_boost**  <br>*optional*|Boost of search query term length  <br>**Default** : `1.0`  <br>**Minimum value** : `0`  <br>**Maximum value** : `10`|number (float)|
|**term_len_weight**  <br>*optional*|Weight of search query term length in final rank. 0: term length will not change final rank. 1: term length will affect to final rank in 0 - 100% range  <br>**Default** : `0.3`  <br>**Minimum value** : `0`  <br>**Maximum value** : `1`|number (float)|



### FulltextSynonym
Fulltext synonym definition


|Name|Description|Schema|
|---|---|---|
|**alternatives**  <br>*optional*|List of alternatives, which will be used for search documents|< string > array|
|**tokens**  <br>*optional*|List source tokens in query, which will be replaced with alternatives|< string > array|



### GlobalReplicationStats

|Name|Description|Schema|
|---|---|---|
|**items**  <br>*optional*||< [items](#globalreplicationstats-items) > array|
|**total_items**  <br>*optional*|Total replication stat items count|integer|


**items**

|Name|Description|Schema|
|---|---|---|
|**allocated_updates_count**  <br>*required*|count of online updates, awaiting deallocation|integer|
|**allocated_updates_size**  <br>*required*|total online updates' size in bytes|integer|
|**force_sync**  <br>*required*||[ReplicationSyncStat](#replicationsyncstat)|
|**initial_sync**  <br>*optional*||[initial_sync](#globalreplicationstats-initial_sync)|
|**nodes**  <br>*required*|info about each node|< [nodes](#globalreplicationstats-nodes) > array|
|**pending_updates_count**  <br>*required*|count of online updates, awaiting replication|integer|
|**type**  <br>*required*|Replication type. Either 'async' or 'cluster'|string|
|**wal_sync**  <br>*required*||[ReplicationSyncStat](#replicationsyncstat)|


**initial_sync**

|Name|Description|Schema|
|---|---|---|
|**force_sync**  <br>*required*||[ReplicationSyncStat](#replicationsyncstat)|
|**total_time_us**  <br>*required*|Total time of initial sync|integer|
|**wal_sync**  <br>*required*||[ReplicationSyncStat](#replicationsyncstat)|


**nodes**

|Name|Description|Schema|
|---|---|---|
|**dsn**  <br>*required*|node's dsn|string|
|**is_synchronized**  <br>*optional*|shows synchroniztion state for raft-cluster node (false if node is outdated)|boolean|
|**namespaces**  <br>*required*|list of namespaces, which are configure for this node|< string > array|
|**pending_updates_count**  <br>*required*|online updates, awaiting replication to this node|integer|
|**replication_mode**  <br>*optional*|replication mode for mixed 'sync cluster + async replication' configs|enum (default, from_sync_leader)|
|**role**  <br>*required*|replication role|enum (none, follower, leader, candidate)|
|**server_id**  <br>*required*|node's server id|integer|
|**status**  <br>*required*|network status|enum (none, offline, online)|



### Index

|Name|Description|Schema|
|---|---|---|
|**collate_mode**  <br>*optional*|String collate mode  <br>**Default** : `"none"`|enum (none, ascii, utf8, numeric)|
|**config**  <br>*optional*||[FulltextConfig](#fulltextconfig)|
|**expire_after**  <br>*optional*|Specify, time to live for ttl index, in seconds|integer|
|**field_type**  <br>*required*|Field data type|enum (int, int64, double, string, bool, composite, point)|
|**index_type**  <br>*required*|Index structure type  <br>**Default** : `"hash"`|enum (hash, tree, text, rtree, ttl, -)|
|**is_array**  <br>*optional*|Specifies, that index is array. Array indexes can work with array fields, or work with multiple fields  <br>**Default** : `false`|boolean|
|**is_dense**  <br>*optional*|Reduces the index size. For hash and tree it will save ~8 bytes per unique key value. Useful for indexes with high selectivity, but for tree and hash indexes with low selectivity can seriously decrease update performance;  <br>**Default** : `false`|boolean|
|**is_pk**  <br>*optional*|Specifies, that index is primary key. The update operations will checks, that PK field is unique. The namespace MUST have only 1 PK index|boolean|
|**is_simple_tag**  <br>*optional*|Use simple tag instead of actual index, which will notice rx about possible field name for strict policies  <br>**Default** : `false`|boolean|
|**is_sparse**  <br>*optional*|Value of index may not present in the document, and threfore, reduce data size but decreases speed operations on index  <br>**Default** : `false`|boolean|
|**json_paths**  <br>*required*|Fields path in json object, e.g 'id' or 'subobject.field'. If index is 'composite' or 'is_array', than multiple json_paths can be specified, and index will get values from all specified fields.|< string > array|
|**name**  <br>*required*|Name of index, can contains letters, digits and underscores  <br>**Default** : `"id"`  <br>**Pattern** : `"^[A-Za-z0-9_\\-]*$"`|string|
|**rtree_type**  <br>*optional*|Algorithm to construct RTree index  <br>**Default** : `"rstar"`|enum (linear, quadratic, greene, rstar)|
|**sort_order_letters**  <br>*optional*|Sort order letters  <br>**Default** : `""`|string|



### IndexCacheMemStats
Idset cache stats. Stores merged reverse index results of SELECT field IN(...) by IN(...) keys

*Polymorphism* : Composition


|Name|Description|Schema|
|---|---|---|
|**empty_count**  <br>*optional*|Count of empty elements slots in this cache|integer|
|**hit_count_limit**  <br>*optional*|Number of hits of queries, to store results in cache|integer|
|**items_count**  <br>*optional*|Count of used elements stored in this cache|integer|
|**total_size**  <br>*optional*|Total memory consumption by this cache|integer|



### IndexMemStat

|Name|Description|Schema|
|---|---|---|
|**data_size**  <br>*optional*|Total memory consumption of documents's data, holded by index|integer|
|**fulltext_size**  <br>*optional*|Total memory consumption of fulltext search structures|integer|
|**idset_btree_size**  <br>*optional*|Total memory consumption of reverse index b-tree structures. For `dense` and `store` indexes always 0|integer|
|**idset_cache**  <br>*optional*||[IndexCacheMemStats](#indexcachememstats)|
|**idset_plain_size**  <br>*optional*|Total memory consumption of reverse index vectors. For `store` ndexes always 0|integer|
|**name**  <br>*optional*|Name of index. There are special index with name `-tuple`. It's stores original document's json structure with non indexe fields|string|
|**sort_orders_size**  <br>*optional*|Total memory consumption of SORT statement and `GT`, `LT` conditions optimized structures. Applicabe only to `tree` indexes|integer|
|**tracked_updates_buckets**  <br>*optional*|Buckets count in index updates tracker map|integer|
|**tracked_updates_count**  <br>*optional*|Updates count, pending in index updates tracker|integer|
|**tracked_updates_overflow**  <br>*optional*|Updates tracker map overflow (number of elements, stored outside of the main buckets)|integer|
|**tracked_updates_size**  <br>*optional*|Updates tracker map size in bytes|integer|
|**unique_keys_count**  <br>*optional*|Count of unique keys values stored in index|integer|



### Indexes

|Name|Description|Schema|
|---|---|---|
|**items**  <br>*optional*||< [Index](#index) > array|
|**total_items**  <br>*optional*|Total count of indexes|integer|



### Items

|Name|Description|Schema|
|---|---|---|
|**items**  <br>*optional*|Documents, matched specified filters|< object > array|
|**total_items**  <br>*optional*|Total count of documents, matched specified filters|integer|



### ItemsUpdateResponse

|Name|Description|Schema|
|---|---|---|
|**items**  <br>*optional*|Updated documents. Contains only if precepts were provided|< object > array|
|**updated**  <br>*optional*|Count of updated items|integer|



### JoinCacheMemStats
Join cache stats. Stores results of selects to right table by ON condition

*Polymorphism* : Composition


|Name|Description|Schema|
|---|---|---|
|**empty_count**  <br>*optional*|Count of empty elements slots in this cache|integer|
|**hit_count_limit**  <br>*optional*|Number of hits of queries, to store results in cache|integer|
|**items_count**  <br>*optional*|Count of used elements stored in this cache|integer|
|**total_size**  <br>*optional*|Total memory consumption by this cache|integer|



### JoinedDef

|Name|Description|Schema|
|---|---|---|
|**filters**  <br>*optional*|Filter for results documents|< [FilterDef](#filterdef) > array|
|**limit**  <br>*optional*|Maximum count of returned items|integer|
|**namespace**  <br>*required*|Namespace name|string|
|**offset**  <br>*optional*|Offset of first returned item|integer|
|**select_filter**  <br>*optional*|Filter fields of returned document. Can be dot separated, e.g 'subobject.field'|< string > array|
|**sort**  <br>*optional*||< [SortDef](#sortdef) > array|
|**on**  <br>*optional*|Join ON statement|< [OnDef](#ondef) > array|
|**type**  <br>*required*|Join type|enum (LEFT, INNER, ORINNER)|



### JsonObjectDef

|Name|Description|Schema|
|---|---|---|
|**additionalProperties**  <br>*optional*|Allow additional fields in this schema level. Allowed for objects only  <br>**Default** : `false`|boolean|
|**items**  <br>*optional*||[JsonObjectDef](#jsonobjectdef)|
|**properties**  <br>*optional*||[properties](#jsonobjectdef-properties)|
|**required**  <br>*optional*|Array of required fieldsl. Allowed for objects only|< string > array|
|**type**  <br>*optional*|Entity type|enum (object, string, number, array)|


**properties**

|Name|Schema|
|---|---|
|**field1**  <br>*optional*|[JsonObjectDef](#jsonobjectdef)|
|**field2**  <br>*optional*|[JsonObjectDef](#jsonobjectdef)|



### LongQueriesLogging
Parameters for logging long queries and transactions


|Name|Schema|
|---|---|
|**select**  <br>*optional*|[SelectLogging](#selectlogging)|
|**transaction**  <br>*optional*|[TransactionLogging](#transactionlogging)|
|**update_delete**  <br>*optional*|[UpdateDeleteLogging](#updatedeletelogging)|



### MetaByKeyResponse
Meta info of the specified namespace


|Name|Schema|
|---|---|
|**key**  <br>*required*|string|
|**value**  <br>*required*|string|



### MetaInfo
Meta info to be set


|Name|Schema|
|---|---|
|**key**  <br>*required*|string|
|**value**  <br>*required*|string|



### MetaListResponse
List of meta info of the specified namespace


|Name|Description|Schema|
|---|---|---|
|**meta**  <br>*required*||< [meta](#metalistresponse-meta) > array|
|**total_items**  <br>*required*|Total count of meta info in the namespace|integer|


**meta**

|Name|Description|Schema|
|---|---|---|
|**key**  <br>*required*||string|
|**value**  <br>*optional*|Optional: Provided if 'with_values' = true|string|



### Namespace

|Name|Description|Schema|
|---|---|---|
|**indexes**  <br>*optional*||< [Index](#index) > array|
|**name**  <br>*optional*|Name of namespace  <br>**Pattern** : `"^[A-Za-z0-9_\\-]*$"`|string|
|**storage**  <br>*optional*||[storage](#namespace-storage)|


**storage**

|Name|Description|Schema|
|---|---|---|
|**enabled**  <br>*optional*|If true, then documents will be stored to disc storage, else all data will be lost on server shutdown|boolean|



### NamespaceMemStats

|Name|Description|Schema|
|---|---|---|
|**indexes**  <br>*optional*|Memory consumption of each namespace index|< [IndexMemStat](#indexmemstat) > array|
|**items_count**  <br>*optional*|Total count of documents in namespace|integer|
|**join_cache**  <br>*optional*||[JoinCacheMemStats](#joincachememstats)|
|**name**  <br>*optional*|Name of namespace|string|
|**optimization_completed**  <br>*optional*|Background indexes optimization has been completed|boolean|
|**query_cache**  <br>*optional*||[QueryCacheMemStats](#querycachememstats)|
|**replication**  <br>*optional*||[ReplicationStats](#replicationstats)|
|**storage_enabled**  <br>*optional*|Shows if storage is enabled (hovewer it may still be unavailable)|boolean|
|**storage_ok**  <br>*optional*|Status of disk storage (true, if storage is enabled and writable)|boolean|
|**storage_path**  <br>*optional*|Filesystem path to namespace storage|string|
|**storage_status**  <br>*optional*|More detailed info about storage status. May contain 'OK', 'DISABLED', 'NO SPACE LEFT' or last error descrition|string|
|**strings_waiting_to_be_deleted_size**  <br>*optional*|Size of strings deleted from namespace, but still used in queryResults|integer|
|**total**  <br>*optional*|Summary of total namespace memory consumption|[total](#namespacememstats-total)|
|**updated_unix_nano**  <br>*optional*|[[deperecated]]. do not use|integer|


**total**

|Name|Description|Schema|
|---|---|---|
|**cache_size**  <br>*optional*|Total memory consumption of namespace's caches. e.g. idset and join caches|integer|
|**data_size**  <br>*optional*|Total memory size of stored documents, including system structures|integer|
|**index_optimizer_memory**  <br>*optional*|Total memory size, occupated by index optimizer (in bytes)|integer|
|**indexes_size**  <br>*optional*|Total memory consumption of namespace's indexes|integer|



### NamespacePerfStats

|Name|Description|Schema|
|---|---|---|
|**indexes**  <br>*optional*|Memory consumption of each namespace index|< [indexes](#namespaceperfstats-indexes) > array|
|**name**  <br>*optional*|Name of namespace|string|
|**selects**  <br>*optional*||[SelectPerfStats](#selectperfstats)|
|**transactions**  <br>*optional*||[TransactionsPerfStats](#transactionsperfstats)|
|**updates**  <br>*optional*||[UpdatePerfStats](#updateperfstats)|


**indexes**

|Name|Description|Schema|
|---|---|---|
|**name**  <br>*optional*|Name of index|string|
|**selects**  <br>*optional*||[SelectPerfStats](#selectperfstats)|
|**updates**  <br>*optional*||[UpdatePerfStats](#updateperfstats)|



### Namespaces

|Name|Description|Schema|
|---|---|---|
|**items**  <br>*optional*||< [items](#namespaces-items) > array|
|**total_items**  <br>*optional*|Total count of namespaces|integer|


**items**

|Name|Description|Schema|
|---|---|---|
|**name**  <br>*optional*|Name of namespace|string|



### NamespacesConfig

|Name|Description|Schema|
|---|---|---|
|**cache**  <br>*optional*||[cache](#namespacesconfig-cache)|
|**copy_policy_multiplier**  <br>*optional*|Disables copy policy if namespace size is greater than copy_policy_multiplier * start_copy_policy_tx_size|integer|
|**index_updates_counting_mode**  <br>*optional*|Enables 'simple counting mode' for index updates tracker. This will increase index optimization time, however may reduce insertion time|boolean|
|**join_cache_mode**  <br>*optional*|Join cache mode|enum (aggressive)|
|**lazyload**  <br>*optional*|Enable namespace lazy load (namespace shoud be loaded from disk on first call, not at reindexer startup)|boolean|
|**log_level**  <br>*optional*|Log level of queries core logger|enum (none, error, warning, info, trace)|
|**max_preselect_part**  <br>*optional*|Maximum preselect part of namespace's items for optimization of inner join by injection of filters. If max_preselect_part is 0, then only mmax_preselect_size will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied  <br>**Default** : `0.1`  <br>**Minimum value** : `0`  <br>**Maximum value** : `1`|number (float)|
|**max_preselect_size**  <br>*optional*|Maximum preselect size for optimization of inner join by injection of filters. If max_preselect_size is 0, then only max_preselect_part will be used. If max_preselect_size is 0 and max_preselect_part is 0, optimization with preselect will not be applied. If max_preselect_size is 0 and max_preselect_part is 1.0, then the optimization will always be applied  <br>**Minimum value** : `0`|integer|
|**min_preselect_size**  <br>*optional*|Minimum preselect size for optimization of inner join by injection of filters. Min_preselect_size will be used as preselect limit if (max_preselect_part * ns.size) is less than this value  <br>**Minimum value** : `0`|integer|
|**namespace**  <br>*optional*|Name of namespace, or `*` for setting to all namespaces|string|
|**optimization_sort_workers**  <br>*optional*|Maximum number of background threads of sort indexes optimization. 0 - disable sort optimizations|integer|
|**optimization_timeout_ms**  <br>*optional*|Timeout before background indexes optimization start after last update. 0 - disable optimizations|integer|
|**start_copy_policy_tx_size**  <br>*optional*|Enable namespace copying for transaction with steps count greater than this value (if copy_politics_multiplier also allows this)|integer|
|**sync_storage_flush_limit**  <br>*optional*|Enables synchronous storage flush inside write-calls, if async updates count is more than sync_storage_flush_limit. 0 - disables synchronous storage flush, in this case storage will be flushed in background thread only|integer|
|**tx_size_to_always_copy**  <br>*optional*|Force namespace copying for transaction with steps count greater than this value|integer|
|**unload_idle_threshold**  <br>*optional*|Unload namespace data from RAM after this idle timeout in seconds. If 0, then data should not be unloaded|integer|
|**wal_size**  <br>*optional*|Maximum WAL size for this namespace (maximum count of WAL records)|integer|


**cache**

|Name|Description|Schema|
|---|---|---|
|**ft_index_cache_size**  <br>*optional*|Max size of the fulltext indexes IdSets cache in bytes (per index). Each fulltext index has it's own independant cache. This cache is used in any selections to store resulting sets of internal document IDs, FT ranks and highlighted areas (it does not stores documents' content itself)|integer|
|**ft_index_hits_to_cache**  <br>*optional*|Default 'hits to cache' for fulltext index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast|integer|
|**index_idset_cache_size**  <br>*optional*|Max size of the index IdSets cache in bytes (per index). Each index has it's own independant cache. This cache is used in any selections to store resulting sets of internal document IDs (it does not stores documents' content itself)|integer|
|**index_idset_hits_to_cache**  <br>*optional*|Default 'hits to cache' for index IdSets caches. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast|integer|
|**joins_preselect_cache_size**  <br>*optional*|Max size of the index IdSets cache in bytes for each namespace. This cache will be enabled only if 'join_cache_mode' property is not 'off'. It stores resulting IDs, serialized JOINed queries and any other 'preselect' information for the JOIN queries (when target namespace is right namespace of the JOIN)|integer|
|**joins_preselect_hit_to_cache**  <br>*optional*|Default 'hits to cache' for joins preselect cache of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast|integer|
|**query_count_cache_size**  <br>*optional*|Max size of the cache for COUNT_CACHED() aggregation in bytes for each namespace. This cache stores resulting COUNTs and serialized queries for the COUNT_CACHED() aggregations|integer|
|**query_count_hit_to_cache**  <br>*optional*|Default 'hits to cache' for COUNT_CACHED() aggregation of the current namespace. This value determines how many requests required to put results into cache. For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast|integer|



### OnDef

|Name|Description|Schema|
|---|---|---|
|**cond**  <br>*required*|Condition operator|enum (EQ, GT, GE, LE, LT, SET)|
|**left_field**  <br>*required*|Field from left namespace (main query namespace)|string|
|**op**  <br>*optional*|Logic operator|enum (AND, OR, NOT)|
|**right_field**  <br>*required*|Field from right namespace (joined query namespace)|string|



### ProfilingConfig

|Name|Description|Schema|
|---|---|---|
|**activitystats**  <br>*optional*|Enables tracking activity statistics  <br>**Default** : `false`|boolean|
|**long_queries_logging**  <br>*optional*||[LongQueriesLogging](#longquerieslogging)|
|**memstats**  <br>*optional*|Enables tracking memory statistics  <br>**Default** : `true`|boolean|
|**perfstats**  <br>*optional*|Enables tracking overal perofrmance statistics  <br>**Default** : `false`|boolean|
|**queries_threshold_us**  <br>*optional*|Minimum query execution time to be recoreded in #queriesperfstats namespace|integer|
|**queriesperfstats**  <br>*optional*|Enables record queries perofrmance statistics  <br>**Default** : `false`|boolean|



### QueriesPerfStats

|Name|Description|Schema|
|---|---|---|
|**items**  <br>*optional*|Documents, matched specified filters|< [QueryPerfStats](#queryperfstats) > array|
|**total_items**  <br>*optional*|Total count of documents, matched specified filters|integer|



### Query

|Name|Description|Schema|
|---|---|---|
|**aggregations**  <br>*optional*|Ask query calculate aggregation|< [AggregationsDef](#aggregationsdef) > array|
|**drop_fields**  <br>*optional*|List of fields to be dropped|< string > array|
|**explain**  <br>*optional*|Add query execution explain information  <br>**Default** : `false`|boolean|
|**filters**  <br>*optional*|Filter for results documents|< [FilterDef](#filterdef) > array|
|**limit**  <br>*optional*|Maximum count of returned items|integer|
|**merge_queries**  <br>*optional*|Merged queries to be merged with main query|< [Query](#query) > array|
|**namespace**  <br>*required*|Namespace name|string|
|**offset**  <br>*optional*|Offset of first returned item|integer|
|**req_total**  <br>*optional*|Ask query to calculate total documents, match condition  <br>**Default** : `"disabled"`|enum (disabled, enabled, cached)|
|**select_filter**  <br>*optional*|Filter fields of returned document. Can be dot separated, e.g 'subobject.field'|< string > array|
|**select_functions**  <br>*optional*|Add extra select functions to query|< string > array|
|**select_with_rank**  <br>*optional*|Output fulltext rank in QueryResult. Allowed only with fulltext query  <br>**Default** : `false`|boolean|
|**sort**  <br>*optional*|Specifies results sorting order|< [SortDef](#sortdef) > array|
|**strict_mode**  <br>*optional*|Strict mode for query. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions  <br>**Default** : `"names"`|enum (none, names, indexes)|
|**type**  <br>*optional*|Type of query|enum (select, update, delete, truncate)|
|**update_fields**  <br>*optional*|Fields to be updated|< [UpdateField](#updatefield) > array|



### QueryCacheMemStats
Query cache stats. Stores results of SELECT COUNT(*) by Where conditions

*Polymorphism* : Composition


|Name|Description|Schema|
|---|---|---|
|**empty_count**  <br>*optional*|Count of empty elements slots in this cache|integer|
|**hit_count_limit**  <br>*optional*|Number of hits of queries, to store results in cache|integer|
|**items_count**  <br>*optional*|Count of used elements stored in this cache|integer|
|**total_size**  <br>*optional*|Total memory consumption by this cache|integer|



### QueryColumnDef
Query columns for table outputs


|Name|Description|Schema|
|---|---|---|
|**max_chars**  <br>*optional*|Maximum count of chars in column|number|
|**name**  <br>*optional*|Column name|string|
|**width_chars**  <br>*optional*|Column width in chars|number|
|**width_percents**  <br>*optional*|Column width in percents of total width|number|



### QueryItems

|Name|Description|Schema|
|---|---|---|
|**aggregations**  <br>*optional*|Aggregation functions results|< [AggregationResDef](#aggregationresdef) > array|
|**cache_enabled**  <br>*optional*|Enables to client cache returned items. If false, then returned items has been modified  by reindexer, e.g. by select filter, or by functions, and can't be cached|boolean|
|**columns**  <br>*optional*|Columns for table output|< [QueryColumnDef](#querycolumndef) > array|
|**equal_position**  <br>*optional*|Array fields to be searched with equal array indexes|< string > array|
|**explain**  <br>*optional*||[ExplainDef](#explaindef)|
|**items**  <br>*optional*|Documents, matched query|< object > array|
|**namespaces**  <br>*optional*|Namespaces, used in query|< string > array|
|**query_total_items**  <br>*optional*|Total count of documents, matched query|integer|



### QueryPerfStats
Performance statistics per each query

*Polymorphism* : Composition


|Name|Description|Schema|
|---|---|---|
|**last_sec_avg_latency_us**  <br>*optional*|Average latency (execution time) for queries to this object at last second|integer|
|**last_sec_avg_lock_time_us**  <br>*optional*|Average waiting time for acquiring lock to this object at last second|integer|
|**last_sec_qps**  <br>*optional*|Count of queries to this object, requested at last second|integer|
|**latency_stddev**  <br>*optional*|Standard deviation of latency values|number|
|**longest_query**  <br>*optional*|not normalized SQL representation of longest query|string|
|**max_latency_us**  <br>*optional*|Maximum latency value|integer|
|**min_latency_us**  <br>*optional*|Minimal latency value|integer|
|**query**  <br>*optional*|normalized SQL representation of query|string|
|**total_avg_latency_us**  <br>*optional*|Average latency (execution time) for queries to this object|integer|
|**total_avg_lock_time_us**  <br>*optional*|Average waiting time for acquiring lock to this object|integer|
|**total_queries_count**  <br>*optional*|Total count of queries to this object|integer|



### ReplicationConfig

|Name|Description|Schema|
|---|---|---|
|**cluster_id**  <br>*optional*|Cluser ID - must be same for client and for master|integer|
|**server_id**  <br>*optional*|Node identifier. Should be unique for each node in the replicated cluster (non-unique IDs are also allowed, but may lead to the inconsistency in some cases  <br>**Maximum value** : `999`|integer|



### ReplicationStats
State of namespace replication


|Name|Description|Schema|
|---|---|---|
|**cluster_id**  <br>*optional*|Cluster ID - must be same for client and for master|integer|
|**data_count**  <br>*optional*|Items count in namespace|integer|
|**data_hash**  <br>*optional*|Hashsum of all records in namespace|integer|
|**error_code**  <br>*optional*|Error code of last replication|integer|
|**error_message**  <br>*optional*|Error message of last replication|string|
|**incarnation_counter**  <br>*optional*|Number of storage's master <-> slave switches|integer|
|**last_lsn**  <br>*optional*|Last Log Sequence Number (LSN) of applied namespace modification|integer|
|**master_state**  <br>*optional*|State of current master namespace|[master_state](#replicationstats-master_state)|
|**slave_mode**  <br>*optional*|If true, then namespace is in slave mode|boolean|
|**status**  <br>*optional*|Current replication status for this namespace|enum (idle, error, fatal, syncing, none)|
|**updated_unix_nano**  <br>*optional*|Last update time|integer|
|**wal_count**  <br>*optional*|Write Ahead Log (WAL) records count|integer|
|**wal_size**  <br>*optional*|Total memory consumption of Write Ahead Log (WAL)|integer|


**master_state**

|Name|Description|Schema|
|---|---|---|
|**data_count**  <br>*optional*|Items count in master namespace|integer|
|**data_hash**  <br>*optional*|Hashsum of all records in namespace|integer|
|**last_lsn**  <br>*optional*|Last Log Sequence Number (LSN) of applied namespace modification|integer|
|**updated_unix_nano**  <br>*optional*|Last update time|integer|



### ReplicationSyncStat

|Name|Description|Schema|
|---|---|---|
|**avg_time_us**  <br>*required*|Average sync time|integer|
|**count**  <br>*required*|Syncs count|integer|
|**max_time_us**  <br>*required*|Max sync time|integer|



### SchemaDef

|Name|Description|Schema|
|---|---|---|
|**additionalProperties**  <br>*optional*|Allow additional fields in this schema level. Allowed for objects only  <br>**Default** : `false`|boolean|
|**items**  <br>*optional*||[JsonObjectDef](#jsonobjectdef)|
|**properties**  <br>*optional*||[properties](#schemadef-properties)|
|**required**  <br>*optional*|Array of required fieldsl. Allowed for objects only|< string > array|
|**type**  <br>*optional*|Entity type|enum (object, string, number, array)|


**properties**

|Name|Schema|
|---|---|
|**field1**  <br>*optional*|[JsonObjectDef](#jsonobjectdef)|
|**field2**  <br>*optional*|[JsonObjectDef](#jsonobjectdef)|



### SelectLogging

|Name|Description|Schema|
|---|---|---|
|**normalized**  <br>*optional*|Output the query in a normalized form  <br>**Default** : `false`|boolean|
|**threshold_us**  <br>*optional*|Threshold value for logging SELECT queries, if -1 logging is disabled|integer|



### SelectPerfStats
Performance statistics for select operations

*Polymorphism* : Composition


|Name|Description|Schema|
|---|---|---|
|**last_sec_avg_latency_us**  <br>*optional*|Average latency (execution time) for queries to this object at last second|integer|
|**last_sec_avg_lock_time_us**  <br>*optional*|Average waiting time for acquiring lock to this object at last second|integer|
|**last_sec_qps**  <br>*optional*|Count of queries to this object, requested at last second|integer|
|**latency_stddev**  <br>*optional*|Standard deviation of latency values|number|
|**max_latency_us**  <br>*optional*|Maximum latency value|integer|
|**min_latency_us**  <br>*optional*|Minimal latency value|integer|
|**total_avg_latency_us**  <br>*optional*|Average latency (execution time) for queries to this object|integer|
|**total_avg_lock_time_us**  <br>*optional*|Average waiting time for acquiring lock to this object|integer|
|**total_queries_count**  <br>*optional*|Total count of queries to this object|integer|



### SortDef
Specifies results sorting order


|Name|Description|Schema|
|---|---|---|
|**desc**  <br>*optional*|Descent or ascent sorting direction|boolean|
|**field**  <br>*required*|Field or index name for sorting|string|
|**values**  <br>*optional*|Optional: Documents with this values of field will be returned first|< object > array|



### StatusResponse

|Name|Description|Schema|
|---|---|---|
|**description**  <br>*optional*|Text description of error details|string|
|**response_code**  <br>*optional*|Duplicates HTTP response code|integer|
|**success**  <br>*optional*||boolean|



### SubQuery
Subquery object. It must contain either 'select_filters' for the single field, single aggregation or must be matched againts 'is null'/'is not null conditions'


|Name|Description|Schema|
|---|---|---|
|**aggregations**  <br>*optional*|Ask query calculate aggregation|< [SubQueryAggregationsDef](#subqueryaggregationsdef) > array|
|**filters**  <br>*optional*|Filter for results documents|< [FilterDef](#filterdef) > array|
|**limit**  <br>*optional*|Maximum count of returned items|integer|
|**namespace**  <br>*required*|Namespace name|string|
|**offset**  <br>*optional*|Offset of first returned item|integer|
|**req_total**  <br>*optional*|Ask query to calculate total documents, match condition  <br>**Default** : `"disabled"`|enum (disabled, enabled, cached)|
|**select_filter**  <br>*optional*|Filter fields of returned document. Can be dot separated, e.g 'subobject.field'|< string > array|
|**sort**  <br>*optional*|Specifies results sorting order|< [SortDef](#sortdef) > array|



### SubQueryAggregationsDef

|Name|Description|Schema|
|---|---|---|
|**fields**  <br>*required*|Fields or indexes names for aggregation function|< string > array|
|**type**  <br>*required*|Aggregation function|enum (SUM, AVG, MIN, MAX)|



### SuggestItems

|Name|Description|Schema|
|---|---|---|
|**suggests**  <br>*optional*|Suggested query autocompletion variants|< string > array|



### SysInfo

|Name|Description|Schema|
|---|---|---|
|**core_log**  <br>*optional*|Reindexer core log path|string|
|**current_allocated_bytes**  <br>*optional*|Current inuse allocated memory size in bytes|integer|
|**heap_size**  <br>*optional*|Current heap size in bytes|integer|
|**http_address**  <br>*optional*|HTTP server address|string|
|**http_log**  <br>*optional*|HTTP server log path|string|
|**log_level**  <br>*optional*|Log level, should be one of these: trace, debug, info, warning, error, critical|string|
|**pageheap_free**  <br>*optional*|Heap free size in bytes|integer|
|**pageheap_unmapped**  <br>*optional*|Unmapped free heap size in bytes|integer|
|**rpc_address**  <br>*optional*|RPC server address|string|
|**rpc_log**  <br>*optional*|RPC server log path|string|
|**server_log**  <br>*optional*|Reindexer server log path|string|
|**start_time**  <br>*optional*|Server start time in unix timestamp|integer|
|**storage_path**  <br>*optional*|Path to storage|string|
|**uptime**  <br>*optional*|Server uptime in seconds|integer|
|**version**  <br>*optional*|Server version|string|



### SystemConfigItem

|Name|Description|Schema|
|---|---|---|
|**action**  <br>*optional*||[ActionCommand](#actioncommand)|
|**async_replication**  <br>*optional*||[AsyncReplicationConfig](#asyncreplicationconfig)|
|**namespaces**  <br>*optional*||< [NamespacesConfig](#namespacesconfig) > array|
|**profiling**  <br>*optional*||[ProfilingConfig](#profilingconfig)|
|**replication**  <br>*optional*||[ReplicationConfig](#replicationconfig)|
|**type**  <br>*required*|**Default** : `"profiling"`|enum (profiling, namespaces, replication, action)|



### TransactionLogging

|Name|Description|Schema|
|---|---|---|
|**avg_step_threshold_us**  <br>*optional*|Threshold value for the average step duration time in a transaction, if -1 logging is disabled|integer|
|**threshold_us**  <br>*optional*|Threshold value for total transaction commit time, if -1 logging is disabled|integer|



### TransactionsPerfStats
Performance statistics for transactions


|Name|Description|Schema|
|---|---|---|
|**avg_commit_time_us**  <br>*optional*|Average transaction commit time usec|integer|
|**avg_copy_time_us**  <br>*optional*|Average namespace copy time usec|integer|
|**avg_prepare_time_us**  <br>*optional*|Average transaction preparation time usec|integer|
|**avg_steps_count**  <br>*optional*|Average steps count in transactions for this namespace|integer|
|**max_commit_time_us**  <br>*optional*|Maximum transaction commit time usec|integer|
|**max_copy_time_us**  <br>*optional*|Minimum namespace copy time usec|integer|
|**max_prepare_time_us**  <br>*optional*|Maximum transaction preparation time usec|integer|
|**max_steps_count**  <br>*optional*|Maximum steps count in transactions for this namespace|integer|
|**min_commit_time_us**  <br>*optional*|Minimum transaction commit time usec|integer|
|**min_copy_time_us**  <br>*optional*|Maximum namespace copy time usec|integer|
|**min_prepare_time_us**  <br>*optional*|Minimum transaction preparation time usec|integer|
|**min_steps_count**  <br>*optional*|Minimum steps count in transactions for this namespace|integer|
|**total_copy_count**  <br>*optional*|Total namespace copy operations|integer|
|**total_count**  <br>*optional*|Total transactions count for this namespace|integer|



### UpdateDeleteLogging

|Name|Description|Schema|
|---|---|---|
|**normalized**  <br>*optional*|Output the query in a normalized form  <br>**Default** : `false`|boolean|
|**threshold_us**  <br>*optional*|Threshold value for logging UPDATE and DELETE queries, if -1 logging is disabled|integer|



### UpdateField

|Name|Description|Schema|
|---|---|---|
|**is_array**  <br>*optional*|is updated value an array|boolean|
|**name**  <br>*required*|field name|string|
|**type**  <br>*optional*|update entry type|enum (object, expression, value)|
|**values**  <br>*required*|Values to update field with|< object > array|



### UpdatePerfStats
Performance statistics for update operations

*Polymorphism* : Composition


|Name|Description|Schema|
|---|---|---|
|**last_sec_avg_latency_us**  <br>*optional*|Average latency (execution time) for queries to this object at last second|integer|
|**last_sec_avg_lock_time_us**  <br>*optional*|Average waiting time for acquiring lock to this object at last second|integer|
|**last_sec_qps**  <br>*optional*|Count of queries to this object, requested at last second|integer|
|**latency_stddev**  <br>*optional*|Standard deviation of latency values|number|
|**max_latency_us**  <br>*optional*|Maximum latency value|integer|
|**min_latency_us**  <br>*optional*|Minimal latency value|integer|
|**total_avg_latency_us**  <br>*optional*|Average latency (execution time) for queries to this object|integer|
|**total_avg_lock_time_us**  <br>*optional*|Average waiting time for acquiring lock to this object|integer|
|**total_queries_count**  <br>*optional*|Total count of queries to this object|integer|



### UpdateResponse

|Name|Description|Schema|
|---|---|---|
|**updated**  <br>*optional*|Count of updated items|integer|





