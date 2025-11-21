# Version 5.9.0 (21.11.2025)
## Core
- [fea] Added direct support for nested arrays storing/indexing in `JSON`, `CJSON` and `MsgPack` (i.e. JSONs like this `{ "id": 7, "arr": [ 1, "string", [ 1, 2, 3], { "field": 10 }] }` now may be stored into database)
- [fea] Allowed to sort `null`-values in `hash`-indexes (including `null's` inside arrays). `Nulls`-order is now consistent for different indexes/fields: `null` is considering less than any other value. **This changes behavior for some queries with `sparse tree` indexes: previously `nulls`-order was inconsistent and had depent on the selection plan and index/field type**
- [fea] Added `TagsMatcher's` info into `#memstats`
- [fea] Added fields check according to current `StrictMode` for joined fields inside `ON`-clause
- [fea] Added `Distinct`-support for `composite`-indexes
- [fix] Fixed assertion in ordered queries with `Distinct` over fulltext-indexes
- [fix] Fixed background index optimization in cases, when target index contains `null`-values
- [fix] Fixed `CJSON`-corruption after `UPDATE`-queries with non-existing array indexes

## Fulltext
- [fea] Improved merging logic, when [MergeLimit](fulltext.md#base-config-parameters) is exceeded. Search engine will try to find documents with maximum corresponding terms. This may be slower, but provides better quality. You may set environment variable `REINDEXER_NO_2PHASE_FT_MERGE=1` to disable 2-phase merging and fallback to the old merge logic
- [fea] Supported [select functions](fulltext.md#using-select-functions) for array values in composite indexes
- [fix] Allowed to index `null`-fields in `fulltext composite` indexes

## Vector indexes
- [fea] Added performance metrics for [auto-embedding logic](float_vector.md#knn-search-with-auto-embedding). Check `indexes` performance stats in `#perfstats` namespace for details (make sure, that `perfstats` are enabled in `#config`)
- [fix] Fixed segmentation fault in KNN-queries with `radius`, when target index is empty
- [fix] Disabled vector indexes update/create operations, when namespace does not have PK-index (it could led to disk storage corruption)

## Go connector
- [fea] Added support for nested arrays into `CJSON`-coding/decoding
- [fix] Fixed Transactions with `Update`/`Delete`-queries. Now such transactions will return actual count of items, affected by the queries

## Reindexer server
- [fea] Added [Prometheus-metrics](cpp_src/readme.md#prometheus-server-side) for [auto-embedding logic](float_vector.md#knn-search-with-auto-embedding)
- [fix] Fixed screening in `api/v1/db/:db/namespaces/:ns/meta*` endpoints

## Reindexer tool
- [fix] Fixed screening in `\meta`-calls

## Face
- [fea] Added total for `Memory Statistics` of namespace
- [fix] Fixed issue appeared on `Performance Statistics` refresh
- [fix] Fixed Embedder's URL validation to allow local domains

# Version 5.8.1 (05.11.2025)
## Core
- [fix] Fixed `INNER JOIN` in composition with multicolumn sort by `tree` index and `LIMIT`. Previously, `joined`-array could be missing in the result items, although the result itself was correct

# Version 5.8.0 (23.10.2025)
## Core
- [fea] Added [new EqualPosition syntax](readme.md#search-in-array-fields-with-matching-indexes-using-grouping) to perform grouping conditions over object arrays
- [fea] Added [MERGE support](hybrid.md#merging-queries-results) for hybrid select results
- [fea] Optimized comparator for multifield `Distinct` (for conditions like `Distinct(field1,field2,...)`)
- [fea] Added `Distinct` support for fulltext indexed (works the same way as `Distinct` for regular indexes)
- [fea] Optimized selection plan for empty query results
- [fix] Fixed error handling in `composite`-index update/delete operations
- [fix] Fixed [DSN masking](cpp_src/readme.md#masking) in `#config`-namespace
- [fix] Fixed token's positions in SQL parsing error descriptions
- [fix] Fixed data race in namespaces renaming

## Fulltext
- [fea] Added extra strict validation for non-existing fields/indexes in [fulltext dsl](fulltext.md#text-query-format)

## Vector indexes
- [fea] Added vector's data sharing between multiple query results to reduce peak memory footprint for results, containing vectors

## Replication
- [fix] Fixed possible "split mind" in RAFT-cluster

## Sharding
- [fea] Added forced RAFT-leader elections request after proxying errors during `#replicationstats` request for more stable errors handling
- [fix] Disabled sharding by vector indexes

## Reindexer tool
- [fix] Fixed interactive mode termination after error

## Face
- [fea] Added new `vectors keeper size` field to the Statistics page

# Version 5.7.0 (18.09.2025)
## Core
- [fea] Added support for [sorting](readme.md#sort) with array fields (i.e. `ORDER BY array_field`)
- [fea] Json-paths ordering for [composite indexes](readme.md#complex-primary-keys-and-composite-indexes) was made consistent and now depends on initial json-paths ordering in indexes definition array
- [fea] Improved error messages in cases, when user tries to create new PK-index over the field with duplicated values
- [fea] Optimized dynamic memory allocations count in [JOIN-queries](readme.md#join)
- [fix] Fixed crash during `null`-values handling in [equal_position](readme.md#search-in-array-fields-with-matching-array-indexes)
- [fix] Fixed quotes handling in [sort expressions](readme.md#sort)

## Fulltext
- [fea] Sufficiently optimized ranks merging loop for queries with large relevant results count (up to 25% performance boost according to our [CPP-benchmarks](cpp_src/gtests/bench))
- [fix] Fixed composite fulltext indexes update when target index has individual fields configs
- [fix] Fixed crash when indexing arrays with `enable_numbers_search`
- [fix] Fixed fast-path index update

## Reindexer server
- [fea] Added support for transaction in Protobuf and MsgPack format (in `/api/v1/db/:db/namespaces/:ns/transactions` endpoint)
- [fix] Fixed crash on incorrect JSON for `equal_positions` and `join_query` fields in Query DSL parser
- [fix] Fixed response for [GRPC EnumNamespaces](cpp_src/readme.md#grpc-api) with `onlyNames`-option

## CXX API
- [fea] Added few more safety checks for `client::Reindexer`
- [fix] Fixed handling of nested json-paths in `reindexer::Item::operator[]` (i.e. cases like `item["obj.field"] = 10`)
- [ref] Method `Select(std::string_view sql)` was renamed to `ExecSQL(std::string_view sql)`
- [ref] Removed deprecated `temporary` flag from namespace's `#memstat`

## Deploy
- [upd] Updated base docker image from `alpine:3.21` to `alpine:3.22`

# Version 5.6.0 (29.08.2025)
## Core
- [fea] Added subqueries and `or inner join` support for [UPDATE](readme.md#update-queries-with-inner-joins-and-subqueries) and [DELETE](readme.md#delete-queries) queries
- [fea] Added more informative error message in case of unsuccessful index creation
- [fea] Improved [anti-join](#anti-join) handling (excessive braces do not required anymore)
- [fea] Added more strict validation for incorrect conditions with LEFT JOINS
- [fea] Improved protobuf/msgpack content validation
- [fea] Added more strict validation for [UPDATE-queries](#update-queries) targeting non-array fields
- [fix] Fixed SQL/DSL(JSON) parsing of `NOT`-operator inside JOIN's ON-clause
- [fix] Fixed case-insensitive namespaces names in DSL(JSON) queries
- [fix] Fixed automatic indexes substitution for array-indexes with multiple `jsonpaths` and `sparse`-indexes
- [fix] Fixed compatibility in empty arrays JOINs
- [fix] Fixed incorrect LIMIT handling in queries with combination of array-field [DISTINCT/multi-DISTINCT](readme.md#aggregations) and [forced sort](readme.md#forced-sort)
- [fix] Fixed `matched` field value in `explain` results for conditions with `NOT` operators

## Vector indexes
- [fea] Added automatic fallback in [hybrid query](hybrid.md), when [embedder](float_vector.md#embedding-configuration) is not available. This query will be executed as pure fulltext query without KNN-part
- [fix] Fixed incorrect handling of the deleted vectors by [KNN-conditions](float_vector.md#knn-search) with `radius`
- [fix] Changed [embedders](float_vector.md#embedding-configuration) validation logic to avoid indexes creation error on startup

## Replication
- [fea] Added proxying for UPDATE and DELETE queries with subqueries and inner joins

## Reindexer tool
- [fea] Added [storage convertion tool](cpp_src/readme.md#converting-storage-type-for-existing-database)

## Deploy
- [upd] Added deployment for `debian:13` (trixie)
- [upd] Removed deployment for `debian:11` (bookworm)

## Face
- [fea] Added new fields to fulltext index config (`keep_diacritics`, `min_word_part_size` and `word_part_delimiters`)
- [fix] Fixed ms measure for statistics column titles

# Version 5.5.0 (31.07.2025)
## Core
- [fea] Added support for `INNER JOIN` (as filters) in `UPDATE`/`DELETE`-queries, including queries with self-joins (`UPDATE ns1 SET v=1 INNER JOIN ns1 ON ns1.idx IN ns1.allowed_ids INNER JOIN ns2 ON ns1.prices = ns2.price_id`)
- [fea] Added support for mixed field types (scalars + arrays) into [multifield DISTINCT](readme.md#aggregations)
- [fea] Added support for `JOINs` between null-values
- [fix] Fixed multi-fields sort by `composite` indexes with `tree`-type (i.e. cases with `ORDER BY tree_composite_1, other_filed`)
- [fix] Fixed PK index validation: now it can't be created, if there are duplicated values in the target field

## Vector indexes
- [fea] Change [auto-embedding API](embedders_api.yaml) for more embedding flexibility and future chunking support
- [fix] Fixed data-race in distance calculation in multithread HNSW index
- [fix] Fixed crash in `QueryResults` containing combination of multiple null/non-null vectors fields
- [fix] Fixed error handling in case of inappropriate index update

## Fulltext
- [fea] Added specific handling for composite words with delimiters (e.g., `resident's`, that may be splitted into `resident` and `s`, or `Biot–Savart`, that may be splitted into `Biot` and `Savart`). Check `WordPartDelimiters` and `MinWordPartSize` [config fields](fulltext.md#base-config-parameters). Default value of the `ExtraWordSymbols` was also changed due to this feature
- [fix] Fixed phrase search with `composite fulltext` indexes

## Replication
- [fix] Fixed logical race in async replication role switch of the target follower (may lead to unnecessary resync)
- [fix] Added check for the replication role in `DropNamespace`-call (now follower's namespaces can't be deleted by user, if replication is active)

## Go connector
- [fea] Change interface of `NewReindex()`-call. Now it returns current status in `error` to force user to check DB's status
- [fix] Changed `SetDefaultQueryDebug()` for better corner cases handling

## Reindexer server
- [fea] Support [multifield DISTINCT](readme.md#aggregations) in proto-schemas
- [fix] Fixed integer types support in [Protobuf interface](cpp_src/readme.md#protobuf). **This breaks compatibility with old protobuf clients and requires repeated client generation for the new schema**

## Reindexer tool
- [fea] Added `-n`/`--namespaces` options to specify namespaces list, that will be restored from the dump file
- [fix] Fixed erros output during dump restoration process

## Face
- [fea] Added validation of URL field in Auto-embedding Config form
- [fea] Added blocking of `is_no_column` field for the composite field type
- [fea] Added measures conversion for "String waiting to be deleted size" field
- [fea] Removed default value for `radius` field in vector indexes config
- [fix] Fixed error that appeared on statistics reset

# Version 5.4.0 (27.06.2025)
## Core
- [fix] Added ignore empty sorting expression
- [fix] Fixed invalidation of index cache
- [fix] Added validation of types of arguments of condition `Set` in query
- [fix] Added validation of types of arguments of forced sort
- [fix] Turned off of optimization of forced sort by fulltext index
- [fix] Fixed parsing of invalid item json
- [fix] Added validation of operation `OR` in `join` `ON` statement

## Vector indexes
- [fea] Added [hybrid](hybrid.md) Fulltext and KNN query
- [fea] Added [`radius`](float_vector.md#knn-search) search-parameter in KNN query
- [fea] Added [`radius`](float_vector.md#creation) parameter in KNN index
- [fea] Added [embedding action](float_vector.md#create-embedding-for-existing-documents)
- [fea] Added selective rebuild vector index on the index's config update

## Fulltext
- [fea] Added ignore accent and vocalization marks. Check `keep_diacritics` in the fulltext index config
- [fix] Fixed search by phrase with binary operator

## Replication
- [fea] Removed compatibility with V3-followers
- [fix] Fixed possible `lsn` breaking during `WAL sync`

## Go connector
- [fix] Added support for condition `LIKE` in dsl query parser
- [fix] Added validation of item values types in cjson parser

## Reindexer server
- [fix] Added parameter `width` in `HTTP` method `GET /db/{database}/namespaces/{name}/items`
- [fix] Fixed format of result of `HTTP` method `POST /db/{database}/query`

## Face
- [fea] Added the index name locking during editing
- [fea] Improved validation of the embedding configuration
- [fea] Added the `radius` option to the vector index configuration
- [fix] Fixed the inability to save an item if any of the item fields has a null value
- [fix] Fixed a validation error that made it impossible to clear the Query embedding configuration
- [fix] Fixed an error when canceling selected items for deletion
- [fix] Fixed `max_typos` tooltips
- [fix] Fixed the error of substituting default values of the profiling config

# Version 5.3.0 (27.05.2025)
## Core
- [fea] Added random sorting via `hash()`/`hash(seed)` functions in [sort expressions](readme.md#sort)
- [fea] Added support for exponential numbers in SQL parser
- [fea] Allow to switch index type (from `array` to `scalar` and from `scalar` to `array`) for empty namespaces via `UpdateIndex` interface

## Vector indexes
- [fea] Added optional built-in [auto-embedding cache](float_vector.md#embedding-cache-configuration) with hybrid (RAM+disk) structure
- [fea] Optimize single modification requests for `HNSW` indexes with enabled `multithreading` option. Now they have the same performance as `HNSW`-indexes with disabled `multithreading`
- [fix] Fixed `AVX512` implementation of `L2` distance calculation for some specific dimensions count

## Replication
- [fix] Fixed possible hanging/timeout in `set_leader_node` config action

## Sharding
- [fix] Fixed incorrect sorting by expressions in distributed queries

## Reindexer server
- [fix] Fixed race in connections Listener during server termination

## Reindexer tool
- [fea] Added multithreading into dump restoration process. This allows to restore dumps with multiple namespaces 2.5-3 times faster if network is good enough
- [fea] Added optional transactions into dump restoration process (`--txsize` option). This allows to speedup dumps restoration on local hosts a little more

## Go connector
- [fea] Improved parsing for `ttl`-index tags. Now `expire_after=xxx` option may be combined with any other options (like `dense` or `is_no_column`)
- [fix] Fixed [events stream](readme.md#events-subscription) drop on idle connections

## Build
- [fea] Added `uninstall` target for `make`

## Face
- [fea] Added default configs requests from the backend
- [fea] Added new fields for auto-embedding cache to `Statistics` -> `Memory`
- [fix] Fixed items view on using query with `like`
- [fix] Fixed Vector values disappearing issue that appeared on items deleting
- [fix] Fixed result value view on using query with `distinct`

# Version 5.2.1 (16.05.2025)
## Core
- [fix] Fixed [forced sort](readme.md#forced-sort) with `sparse` indexes

## Fulltext
- [fix] Fixed zero `rank` values in cached fulltext results

## Replication
- [fix] Added missing timeouts into replication requests/connections

## Reindexer server
- [fix] Fixed conflict between `OpenSSL's` handshake and connection rebalancing. Previously, this could lead to connections hanging

## Go connector
- [fix] Added missing `is_sortable` and `conditions` fields into `IndexDescription`. Information in these fields was actualized according to the current indexes behavior

# Version 5.2.0 (29.04.2025)
## Core
- [fea] Added support for `distinct` with multiple fields (i.e. something like `distinct(field1, field2, field3)`)
- [fea] Allowed `null`-values inside `IN()`-clause (they automatically will be transformed into `OR IS NULL`)
- [fea] Made `IS NULL`/`IS NOT NULL` behavior more consistent between `sparse`-indexes and `non-indexed`-fields. Check [readme](readme.md#null-values-filtration) for more details
- [fea] Added extra validation for `sprase` indexes. Previously incorrect values in those indexes were silently ignored and from now they will produce errors on insertion
- [fix] Fixed crash in vector index during `index drop` operation
- [fix] Fixed timings calculations in `#perfstats`/`#queriesperfstats`
- [ref] Changed `Connect()`-method behavior. Now this call is required before any other database calls. **This may require changes in C++ code, that uses Reindexer**

## Replication
- [fea] Added optional [replication_token](replication.md#configuration) mechanism for extra validation
- [fea] Added execution timeouts for all replication's queries
- [fix] Fixed possible transaction's steps reordering in synchronous cluster proxy

## Reindexer server
- [fea] Added `GET /api/v1/db/default_configs` method to get default config JSONs
- [fix] Fixed possible heap-use-after-free during RPC server termination
- [fix] Fixed segfault in case of incorrect items format

## Go connector
- [fea] Added `DBMSVersion`-method to get builtin/remote `reindexer` version
- [fea] Added [events](readme.md#events-subscription) on `forced`/`WAL` synchronization
- [fea] Added support for multi-fields `distinct`. `AggregationResult` struct was slightly changed. **This requires changes in Go code that uses Distinct: for the single field distinct just take values with 0 index in each slice**
- [fix] Fixed heap-user-after free in `DB.Close()`-call when `ActivityStats` flag was enabled

## Build
- [fix] Updated min cmake versions to fix build with latest `cmake`

## Deploy
- [fea] Added prebuilt package for `fedora:42`
- [upd] Deprecated `fedora:40` repository

## Face
- [fea] Added embedding settings for the `float_vector` index types
- [fea] Disabled the `is_no_column` flag for `sparse`-indexes
- [fea] Changed the `is_no_column` flag visibility from disappearing to disabling
- [fix] Fixed the Save button on the Config form after switching to the Schema tab

# Version 5.1.0 (07.04.2025)
## Core
- [fea] Added separate `is_no_column` index option, which allows to disable column subindex (previously this option was included into `is_dense`)
- [fix] Fixed race on concurrent creation of the same namespace by multiple users/replication
- [fix] Fixed crash in strings comparator for `sparse`-indexes
- [fix] Fixed timeout handling in `UPDATE`-queries
- [fix] Disallowed to create `sparse PK` indexes

## Vector indexes
- [fea] Allow to use empty/null vector values in `UPDATE`-queries with `set`
- [fea] Added support for `IS NULL`/`IS NOT NULL` conditions with indexed vector fields
- [fea] Added [autoembedding logic](float_vectors.md#embedding-configuration) with external user's service for single documents insertion/modification, transactions and [SELECT-queries](float_vectors.md#knn-search-with-auto-embedding)
- [fix] Fixed race in concurrent deleted point reusing in `HNSW` multithread transactions
- [fix] Fixed vector index rebuild on config update

## Replication
- [fix] Fixed replicated `WAL` size on the `follower` after `force sync`

## Reindexer server
- [ref] Removed `autorepair` logic and related flags due to undesirable side effects. LevelDB's repair call could lead to sufficient storage slowdown, so any repair operations should be intentionally called via `reindexer_tool`

## Go connector
- [fix] Fixed possible `heap-use-after-free` in background results recycling logic after database closing (in `builtin`/`builtinserver` modes)
- [fix] Fixed possible `heap-use-after-free` in `UnsubscribeUpdated()`-call during `builtinserver` termination

## Face
- [fea] Added `sync_state` labels for async and sync replications
- [fea] Added new NC config fields: `ann_storage_cache_build_timeout_ms` and `tx_vec_insertion_threads`
- [fea] Added new parameters for the `float_vector` index
- [fea] Added `is_no_column` field to Indexes
- [fix] Added info message about exceeding the acceptable MAX_SAFE_INTEGER value
- [fix] Blocked unnecessary scheme saving

# Version 5.0.1 (13.03.2025)
## Core
- [fix] Fixed incorrect aggregations (`min`, `max`, `avg`, `sum`) interaction with [force sort](readme.md#forced-sort)/`hash`-index sort and `LIMIT`
- [fix] Fixed undefined behaviour in one of the background threads (it was the reason of the stalls on `Windows`-platform)
- [fix] Fixed OSX build for `python` connector

## Vector indexes
- [fix] Fixed `cosine` normalization coefficient update in `HNSW` after corresponding vector reuse
- [fix] Fixed multithread `HNSW` transactions with empty/null vector values
- [fix] Fixed multithread `HNSW` transactions with multiple updates of the same item
- [fix] Fixed possible incorrect `DELETE`-queries handling in multithread `HNSW` transactions
- [fix] Fixed data race in multithread `HNSW` transactions after deleted vector reuse

## Face
- [fix] Fixed issue on the item list getting with checked `with_vectors` field
- [fix] Changed disabled selectors background
- [fix] Changed some column titles on the `Statistics` -> `Memory` -> `NS` (RU version)
- [fix] Fixed displaying of aggregation fields view

# Version 5.0.0 (04.03.2025)
## Core
- [fea] Added `HNSW`, `IVF` and `bruteforce` indexes for [ANN-search](float_vector.md)
- [fea] Optimized internal memory layout for [key_strings](cpp_src/core/payload/readme.md#key_string) (allows to reduce memory consumption for each indexed string and each unique `-tuple`)
- [fea] Added separate CJSON tag for float values for more effective memory consumption and JSON serialization (CPP/Go-bindings will use it automatically)
- [fix] Fixed internal meta flush on namespace close (fixes false positive warning about datahash missmatch on database load)
- [fix] Fixed incorrect `DISTINCT`, [force sort](readme.md#forced-sort) and `LIMIT` interaction
- [fix] Fixed incorrect `force sort` and `always_false` virtual query entry interaction
- [fix] Fixed possible `heap-use-after-free` error in documents with deep nested object-arrays
- [fix] Fixed `segfault` on attempt to create array value with `precept`

## Replication
- [fix] Disabled buggy statement-based replication for `DELETE`-queries
- [fix] Fixed WAL references cleanup for `TRUNCATE`-queries and `DELETE`-queries

## Reindexer server
- [fea] Added `--version` flag to output version information
- [fea] Migrated to `openapi 3.0.1` in [REST API description](cpp_src/server/contrib/server.yml)

## Reindexer tool
- [fea] Added `--version` flag to output local version information and `\version` command to output remote server version information
- [fix] Fixed `with_shard_id` env behavior

## Go connector
- [fea] Added support for tags related to `vector indexes` configuration
- [fix] Fixed nil-values handling in item modification operations (`Insert`, `Delete`, etc.)

## Build
- [upd] Updated to C++20

## Deploy
- [fea] Enabled `ENABLE_V3_FOLLOWERS` flag for all prebuilt packages (this flag allows to Reindexer v5 to be a `leader` for Reindexer v3 followers). This is temporary functionality
- [upd] Deprecated deploy for `ubuntu:20.04` packages
- [upd] Packages were renamed to `reindexer-dev` and `reindexer-server` (without explicit major version)

## Face
- [fea] Added new Field type: float_vector.
- [fea] Renamed fulltext_size to indexing_struct_size on the Memstats page
- [fea] Added the "Vector fields" toggle to get the Vector fields
- [fix] Fixed the Index configuration filling for non-text and non-vector indexes
- [fix] Fixed the namespace name position in the page title

# Version 4.20.0 (04.02.2025)
## Core
- [fea] Optimized indexed strings memory layout (each unique indexed string now requires 20-36 bytes less memery, depending on platform)
- [fea] Optimized non-built btree-index selection
- [fea] Optimized selections with large documents count in results
- [fea] Reduced allocations count in inserts/upserts and select-queries
- [fea] Changed index compatibility check in `AddIndex`: now `dense` and `regular` indexes are treated compatible
- [fix] Fixed parsing for [EQUAL_POSITION](readme.md#search-in-array-fields-with-matching-array-indexes) with brackets in SQL

## Replication
- [fea] Improved full sync performance for namespaces with large amount of empty documents

## Reindexer server
- [fix] Added more TCP-requests format checks
- [fix] Fixed [server.yml](cpp_src/server/contrib/server.yml) format errors

## Go connector
- [fea] Added support for absolute path on Windows platforms in `builtin`-binding

## Build
- [fix] Fixed build for ARM docker image

## Ported
- [fea/fix] Ported all the fixes and features from [v3.31.0](https://github.com/Restream/reindexer/releases/tag/v3.31.0)

# Version 4.19.0 (17.12.2024)

## Core
- [fea] Added automatic masking for reindexer user's credentials in log files and cluster/sharding JSON's (except for `async_replication`-config in `#config`-namespace)
- [fix] Fixed assertion on attempt to use 'null'-values with `=`, `IN()`, `<`, `>`, `<=`, `>=` and `RANGE()` operators

## Replication
- [fea] Optimized memory footprint for online-updates queue

## Reindexer server
- [fea] Added `OpenSSL` support for HTTP-server (`https`) and RPC-server (`cprotos`). [Read more...](cpp_src/readme.md#tls-support)
- [fea] Added special user roles `sharding` and `replication` with all required rights and restrictions for corresponding scenarios
- [fea] Added support for SHA256/SHA512-based encryption for user's passwords in `users.yml`. [Read more...](cpp_src/readme.md#authentication)
- [fea] Added HTTP method `GET api/v1/user/role` to check current user's role
- [fix] Fixed possible update buffer overflow for [events subscription](readme.md#events-subscription) in TCP-mode

## Ported
- [fea/fix] Ported all the fixes and features from [v3.30.0](https://github.com/Restream/reindexer/releases/tag/v3.30.0)

## Face
- [fea] Added `column_size` field to the Memory table
- [fea] Added `splitter` field to the Full text index config
- [fix] Fixed wrong info message about disabled statistics when the database didn't have Namespaces
- [fix] Fixed last row position in the Memory table
- [fix] Fixed wrong input field clearing on cache settings page
- [fix] Fixed sort order icon on the tables
- [fix] Fixed Replication Statistics table behavior on tabs switching
- [fix] Fixed data saving in JSON view on the Fulltext index settings page

# Version 4.18.0 (31.10.2024) *beta*
## Replication
- [fix] Fixed possible origin LSN missmatch in snapshots during WAL/Force syncs

## Reindexer server
- [fix] Fixed logical operations (`or`, `not`) parsing in JSON DSL joined queries

## Ported
- [fea/fix] Ported all the fixes and features from [v3.29.0](https://github.com/Restream/reindexer/releases/tag/v3.29.0)

## Face
- [fea] Added disabled mode for selectors on NS config page
- [fix] Fixed `shard_id` filter on the Items table after the Item editing
- [fix] Fixed scroll position on NS menu 
- [fix] Fixed the issue related to NS config changes saving on tabs switching
- [fix] Fixed duplicated requests on Indexes and Items pages

# Version 4.17.2 *beta* (19.09.2024)
## Core
- [fix] Fixed data race in cached comparators (`joins cache` may cause incorrect comparators deletion)
- [fix] Changed `select fields filters` behavior: incorrect fields in the filter will be ignored (instead of full filter reset)

## Go connector
- [fix] Unexported fields and fields, marked with `"json":-`, will not create indexes anymore (including nested ones). Check updated example [here](readme.md#nested-structs)
- [fix] Unexported fields, marked with `joined` now produce explicit error (previously such fields silently did not work)

## Deploy
- [fea] Added `RedOS 8` prebuilt packages

## Face
- [fea] Changed column tooltips on `Statistics->Queries` page
- [fea] Grouped slow log settings on `Database config` page
- [fix] Fixed the `JOIN` links in `Explain result` table

# Version 4.17.1 *beta* (05.09.2024)
## Core
- [fix] Fixed pagination for `MERGE` fulltext queries. Previously those queries could return duplicates on different pages due to missing ordering guarantees
- [fix] Fixed fields filters serialization for `DISTINCT` aggregations (affects SQL logging only)
- [fix] Temporary disabled default values logic for indexed fields from v4.16.0 - this logic may cause stability issues and will be reworked in further releases
- [fix] Add extra check for composites indexes substitution
- [fix] Fix composites substitution after indexes update

## Reindexer tool
- [fix] Fixed crash in `\quit`-command after previous errors
- [fix] Fixed crash in `git bash` on `Windows` platforms

## Face
- [fea] Added RU language in UI
- [fea] Changed BM25 settings layout
- [fea] Added new documentation links
- [fix] Fixed database deletion
- [fix] Fixed default value of the stop words in fulltext config
- [fix] Fixed default value of the rtree-indexes in item creation tab
- [fix] Fixed last namespace displaying
- [fix] Fixed the issue with removing of 'Statistics -> Replication' table on the page reloading
- [fix] Renamed 'Stat(ms)' field on Replication page
- [fix] Fixed XSS vulnerability in table view

# Version 4.17.0 *beta* (16.08.2024)
## Core
- [fea] Updated [logging library](https://github.com/gabime/spdlog) to v1.14.1 and [formatting library](https://github.com/fmtlib/fmt) to v11.0.2
- [fea] Optimized log level checks to avoid excessive serialization in core logs
- [fea] Added support for [array_remove](readme.md#remove-array-elements-by-values) with scalar values in SQL
- [fea] Added support for [array_remove](readme.md#remove-array-elements-by-values) with non-integral values
- [fix] Disabled default values creation for object array indexes to avoid Go/Java connectors incompatibility
- [fix] Fixed sorted JOIN-subqueries over optimized `tree`-indexes with unordered conditions

## Reindexer server
- [fix] Fixed [docker's](https://hub.docker.com/r/reindexer/reindexer) SEGFAULT on M1 CPU
- [fix] Disable network compression on Windows (it some cases it may lead to crashes)

## Reindexer tool
- [fix] Fixed possible hang in interactive mode on Windows
## Replication
# Version 4.16.0 *beta* (26.07.2024)

## Reindexer server
- [fea] Added RPC API for updates subscription

## Go connector
- [fea] Added database [events subscription](readme.md#events-subscription)

## Sharding
- [fea] Improved errors handling in proxied transactions

## Ported
- [fea/fix] Ported all the fixes and features from [v3.25.0](https://github.com/Restream/reindexer/releases/tag/v3.25.0), [v3.26.0](https://github.com/Restream/reindexer/releases/tag/v3.26.0) and [v3.27.0](https://github.com/Restream/reindexer/releases/tag/v3.27.0)

## Deploy
- [fea] Added build for Ubuntu 24.04
- [ref] CentOS 7 reindexer repo is no longer supported due to CentOS 7 EOL

## Face
- [fea] Increased max allowed value of the `position_boost` field
- [fea] Added "Minimum preselect size for optimization of inner join by injection of filters" field to NS config 
- [fea] Added `UUID` index type to PK options
- [fix] Fixed the issue related to Aggregation result displaying
- [fix] Fixed the pagination issue
- [fix] Fixed the console issue in Meta section
- [fix] Fixed Explain tab layout on SQL result page
- [fix] Fixed placeholder layout for "Forced sort values" field and filer condition on Query Builder page
- [fix] Fixed icons and tabs layout on NS page
- [fix] Fixed "Tags" input layout in Index config modal 
- [fix] Fixed default value for array fields
- [fix] Fixed tabs layout on NS page after Explain request
- [fix] Fixed input width in the settings

# Version 4.15.0 *beta* (22.04.2024)

## Core
- [fea] Optimized comparators execution logic to avoid excessive runtime checks
- [fea] Rewritten comparators for the composite indexes. The new version does not extract fields from the tuple in each iteration. Overall performance boost for queries with composite conditions is up to ~40%
- [fea] Added extra column subindex for the `hash`/`tree`-indexes. It requires extra memory, but gives ~20-50%% overall speedup (depending on the indexes' selectivity). The column subindex may be disabled with the `dense` index option
- [fea] Optimized general sorting logic for the `hash`/`store`-indexes: now it uses column subindexes if possible for the cache efficiency. In some cases this approach provides up to 45% performance gain for the low-selectivity indexes
- [fea] Added extra column subindex for the `string` `store(-)` indexes. Previously `store(-)` indexes have used the column for `int`, `int64`, `double`, `uuid` and `bool` types only. The column subindex may be disabled with the `dense` index option
- [fix] Fixed types conversions for non-index fields in the select queries

## Go connector
- [fix] Fixed `WhereUUID`-method. Now it works for non-index fields too

## Ported
- [fea/fix] Ported all the fixes and features from [v3.23.0](https://github.com/Restream/reindexer/releases/tag/v3.23.0), [v3.23.1](https://github.com/Restream/reindexer/releases/tag/v3.23.1) and [v3.24.0](https://github.com/Restream/reindexer/releases/tag/v3.24.0)

## Deploy
- [fea] Added [APT-repo](cpp_src/readme.md#altlinux) for `AltLinux P10`

## Face
- [fea] Changed the fonts
- [fea] Added the ability to delete NS Meta data 
- [fea] Added the Cache settings to the NS config
- [fea] Made the new UI component for the pagination
- [fea] Added redirect to the created NS
- [fea] Added the new settings to the Full text config
- [fea] Moved field descriptions to the tooltips on the DB Config page
- [fea] Added Git documentation link for the Bm25 config
- [fea] Improved the layout of the Stop words and Synonyms lists
- [fea] Changed empty data to the null value in the Grid view
- [fix] Fixed the Pin NS button
- [fix] Fixed the search panel layout on the NS page
- [fix] Fixed the incorrect message about the empty result of the Explain operation
- [fix] Fixed caching of the NS config 
- [fix] Fixed inform window that appeared on the Cancel button on the NS Config page
- [fix] Removed ESlint popup
- [fix] Fixed the layout issues on the Index form
- [fix] Fixed "see & edit" link on the Queries Perfstats page
- [fix] Fixed the console issue appeared on the SQL -> Explain query
- [fix] Fixed extra data uploading on the Performance page
- [fix] Fixed the console issues on the add/edit indexes
- [fix] Fixed the mergeLimit variable

# Version 4.14.0 *beta* (22.02.2024)

## Core
- [fea] In C++ `Reindexer::Connect`- call is now thread-safe

## Replication
- [fea] Added more data consistency checks for the force syncs
- [fix] Fixed situation, when some concurrently written documents could be lost during RAFT leader resync in case of leader's switch
- [fix] Fixed possible request timeouts on the user's `set_leader_node` command
- [fix] Fixed possible request timeouts during leadership transition
- [fix] Fixed possible deadlock on the initial leader sync during sharding config synchronization
- [fix] Fixed logical race on the cluster's startup (when some of the user's requests could be handled, while RAFT layer is not initialized yet)

## Sharding
- [fea] Added namespaces' content check during sharding reconfiguration: now it is possible to set sharding config at runtime for non-empty namespace if contents of all the namespaces corresponds to sharding keys distribution
- [fea] Added support for subqueries to the sharded namespaces
- [fea] Added `config_rollback_timeout_sec` into `sharding.conf` (option for debug purposes)

## Ported
- [fea/fix] Ported all the fixes from [v3.22.0](https://github.com/Restream/reindexer/releases/tag/v3.22.0) and [v3.22.1](https://github.com/Restream/reindexer/releases/tag/v3.22.1)

## Face
- [fea] Added the ability to use hot keys to navigate over the UI
- [fea] Changed the Server ID range
- [fea] Added the default values to the config form when the default config is using
- [fea] Upgraded Webpack to 5.х
- [fea] Added the default values to the NS config during the mode changing
- [fea] Added a new tooltip with the UTC converted date to the Timestamp data on the NS Grid 
- [fea] Made yarn upgrade
- [fea] Added Prettify button to the SQL Query
- [fea] Added the subqueries field to the explain mode
- [fix] Fixed Check request
- [fix] Added bottom padding to the Performance Updates chart
- [fix] Fixed the pagination on the SQL query result page
- [fix] Fixed NS config popup opened from the Statistics page
- [fix] Fixed the available column list in the grid settings after NS changing 
- [fix] Fixed the message about the outdated browser version after Chrome upgraded to v120.
- [fix] Fixed the settings panel layout on the Performance page, which was overlapped by the message about the outdated browser version
- [fix] Fixed the table columns auto resizing
- [fix] Fixed the table header width issue that appeared on the table resizing
- [fix] Fixed the table layout that crashed on scrolling
- [fix] Fixed the empty space between the last NS and the Total section on the Memory page
- [fix] Fixed the title changing on the NS page during a new NS creating
- [fix] Fixed the tooltip position in the sidebar menu
- [fix] Fixed “+” button for the Expires after field

# Version 4.13.0 *beta* (22.12.2023)

## Core
- [ref] Deprecated `EnableStorage` API was removed

## Replication
- [fea] Added compatibility mode for the replication. If reindexer v4 was built with `ENABLE_V3_FOLLOWERS` cmake option, it is able to replicate to the reindexer v3 followers (v3.21.0 or newer)

## Sharding
- [fea] Sharding configuration [command](sharding.md#runtime-sharding-configuration) is now works with RAFT-cluster shards (on the empty namespaces)
- [fix] Fixed #config-namespace rollback on the sharding configuration errors

## Go connector
- [fea] Go object cache (LRU) is now able to work with sharded clusters, RAFT-clusters and async replicas 

## Ported
- [fea/fix] Ported all the fixes from [v3.20.0](https://github.com/Restream/reindexer/releases/tag/v3.20.0) and [v3.21.0](https://github.com/Restream/reindexer/releases/tag/v3.21.0)

# Version 4.12.0 *beta* (13.10.2023)
## Sharding
- [fea] Added [commands](sharding.md#runtime-sharding-configuration) for the runtime sharding configuration (on the empty namespaces)

## Go connector
- [fix] Fixed potential deadlock in builtin binding, when `SetLogger` method is called multiple

## Ported
- [fea/fix] Ported all the fixes from [v3.17.0](https://github.com/Restream/reindexer/releases/tag/v3.17.0), [v3.18.0](https://github.com/Restream/reindexer/releases/tag/v3.18.0) and [v3.19.0](https://github.com/Restream/reindexer/releases/tag/v3.19.0)

## Face
- [fea] Improved the drop-down section behavior on the Query builder page
- [fea] Added a link to the online documentation
- [fea] Added new proc settings to the Index config
- [fea] Changed the scale window icon for textareas
- [fea] Added the background color to the Close icon in the search history on the Namespace page
- [fea] Improved the buttons' behavior on the Query builder page
- [fea] Added the database name size limit
- [fea] Added the ability to use spaces for JSON paths
- [fea] Changed the numeric values position in the Grid
- [fix] Fixed the columns' settings resetting after the Perfstats page reloading
- [fix] Removed the double requests on the Perfstats page
- [fix] Fixed the JSON Paths tooltip description
- [fix] Fixed the pie chart position in Safari
- [fix] Fixed the popup window size for the long text
- [fix] Fixed the bottom padding on the statistics legend window
- [fix] Fixed the modal window to inform about disabled memory statistics
- [fix] Fixed the filter removal
- [fix] Fixed the filter result page when the filter is removed
- [fix] Fixed the redirect to the wrong page after all items were removed
- [fix] Fixed the Statistics chart for undefined replication.wal_size
- [fix] Fixed the column set for the namespace items during the namespace switching
- [fix] Fixed the JSON paths view for values included spaces
- [fix] Changed the value format for width on the integer for sqlquery 
- [fix] Fixed the bug related to the query history on the Namespace Items list 
- [fix] Fixed the column titles in the table settings menu on the Performance page
- [fix] Added the validation of the negative values for the index settings
- [fix] Fixed the SQL query result table
- [fix] Fixed the aggregation panel
- [fix] Fixed the items sorting 
- [fix] Fixed the last column settings

# Version 4.11.0 *beta* (09.06.2023)
## Server
- [fix] Fixed HTTP-transactions timeout handling

## Replication
- [fix] Fixed SEGFAULT on 'set_leader_node'-command, when RAFT cluster is not configured

## Face
- [fix] Fixed the issue with the total section of the Memory/Grid table appeared on scrolling
- [fix] Added the tab choice to the local storage on the Statistics-> Replication page

## Ported
- [fea/fix] Ported all the fixes from [v3.15.0](https://github.com/Restream/reindexer/releases/tag/v3.15.0) and [v3.16.0](https://github.com/Restream/reindexer/releases/tag/v3.16.0)

# Version 4.10.1 *beta* (05.04.2023)
## Core
- [fix] Reduced preselect limit for joined queries (previous values from v4.10.0 could cause some performance issues)

## Ported
- [fix] Ported all the fixes from [v3.14.1](https://github.com/Restream/reindexer/blob/v3.14.1/changelog.md#version-3141-31032023) and [v3.14.2](https://github.com/Restream/reindexer/blob/v3.14.2/changelog.md#version-3142-05042023)

# Version 4.10.0 *beta* (25.03.2023)
## Core
- [fix] Fixed cost calculation for 'between fields' comparators

## Reindexer server
- [fix] Fixed server connections drops after outdated Close() call from RPC-client

## Go connector
- [fix] Fixed client connections drops after some queries time outs (CPROTO)

## Replication
- [fix] Fixed server ID validation

## Sharding
- [fea] Changed default sorting order for distributed query results. If explicit sort was not requested, results will be sorted by shard IDs
- [fix] Fixed reconnect between shards with RAFT cluster in case, when shard config does not contain all the RAFT nodes

## Ported
- [fea/fix] Ported all the features and fixes from [v3.13.2](https://github.com/Restream/reindexer/blob/v3.13.2/changelog.md#version-3132-23022023) and [v3.14.0](https://github.com/Restream/reindexer/blob/v3.14.0/changelog.md#version-3140-18032023)

## Face
- [fea] Added the information about supported browsers
- [fea] Replaced 'Create new database' label to the Choose a database in the main menu
- [fea] Forbade entering cyrillic symbols for DB and NS titles
- [fea] Added the ability to rollback to the default DB config
- [fea] Improved the filtering on the Namespace page
- [fea] Replaced the empty page with the inform message on the Meta page
- [fea] Disabled the Create NS button if none DB exists
- [fea] Added the ability to export the SQL result in CSV
- [fea] Added the settings of the slow requests logging to the NS config
- [fea] Fixed the ability to change the Item limit on the page if it exceeds the item total
- [fix] Added the redirect from a selected namespace to the index page during the DB changing
- [fix] Fixed the pagination on the Connections page
- [fix] Fixed minor issues with Queries Perfstats and Explain features
- [fix] Fixed the filtered list of Namespaces on the Memory page
- [fix] Fixed the item list after removing of all items on a page
- [fix] Fixed the displaying of the empty and fact result at the same time
- [fix] Fixed the redirect to the Explain page during loading new items on the List and Grid list on the QUERY -> SQL page
- [fix] Fixed the error appeared on the list resizing on the Query Builder page
- [fix] Fixed the infinity requests to namespaces on the Config page
- [fix] Fixed boolean values displaying in the Grid view 
- [fix] Fixed the validator of the tag field
- [fix] Fixed the error on the Explain page
- [fix] Fixed issues with the Position boost
- [fix] Replaced the step from 0.001 to 0.05 for input fields
- [fix] Fixed the sorting pointer icon on the Queries table
- [fix] Fixed the column settings for the Statistics table
- [fix] Improved the Statistics UI
- [fix] Fixed the SQL -> truncate response

# Version 4.9.0 *beta* (26.01.2023)
## Core
- [fix] Fixed potential memory access error on cluster's termination

## Sharding
- [fea] Added support for range-based based shard configs (check [sharding config example](cpp_src/cluster/sharding/sharding.conf) for details)
- [fix] Fixed distributed multishard queries for namespaces with upper case names

## Go connector
- [ref] Added version postfix to the modules' name (.../reindexer -> .../reindexer/v3)

## Ported
- [fea/fix] Ported all the features and fixes from v3.13.0 and v3.13.1

## Build/Deploy
- [fea] Switched Fedora's versions for prebuilt packages to Fedora 36/37

## Face
- [fea] Added the bulk deleting items on the list
- [fea] Changed the lifetime of the editing form for the Item adding operation
- [fix] Fixed collapsing/expanding details for the list items after the page changing
- [fix] Changed the double quotes with the quotes for strings in json-view-tree
- [fix] Fixed the Index list displaying after the DB removing

# Version 4.8.0 *beta* (27.12.2022)
## Core
- [fea] Added logging for slow queries and transactions (check `profiling.long_queries_logging` section in `#config`-namespace)

## Reindexer server
- [fix] Fixed connections balancing and threads creation in default (shared) mode
- [fix] Fixed cleanup for balancing mode for reused connections

# Version 4.7.0 *beta* (13.12.2022)
## Replication
- [fea] Added `online_updates_delay_msec` param for async replication config. It significantly improves online updates batching and reduces CPU consumption for async online-replication.
- [fea] Added config-action `set_log_level` and config-option `log_level` for sync/async replication to control replication logs independently from the main logs.
- [fea] Improved overall online-replication performance
- [fix] Fixed stacking on local namespaces requests, when cluster leader is not chosen
- [fix] Fixed compression flag (it was also fixed for C++ Reindexer's client)

## Build/Deploy
- [fea] Added support for RedOS 7

## Face
- [fea] Added tooltips to the sidebar buttons
- [fea] Renewed the Onboarding UI
- [fea] Replaced the scroll component with the vue-scroll one
- [fea] Removed the "Pended updates" field from Statistics->Connections
- [fea] Added strings_waiting_to_be_deleted_size to Statistics -> Memory for NC
- [fea] Added the data-test attribute
- [fea] Added tooltips to the action bar on the Namespace page
- [fea] Added a default value for Rtree type
- [fea] Made visible the default options of namespaces
- [fix] Fixed console errors appeared on hover for the Client cell in the Current Statistics
- [fix] Fixed disappearing of the Item table part on the Namespace page
- [fix] Fixed sending NULL value for max_preselect_part
- [fix] Removed use of obsolete libraries
- [fix] Fixed the column width resizing on the page reloading
- [fix] Fixed the Expand/Collapse actions for lists
- [fix] Fixed the Collapse all button on the Namespace page
- [fix] Fixed the Statistics menu pointer

## Ported
- [fea/fix] Ported all the features and fixes from v3.11.0

# Version 4.6.1 *beta* (17.11.2022)
## Go connector
- [fea] Add go.mod file with dependencies versions
- [ref] Cproto binding now requires explicit import of the `_ "github.com/restream/reindexer/bindings/cproto"`-module

## Repo
- [ref] Move benchmarks into separate [repository](https://github.com/Restream/reindexer-benchmarks)

# Version 4.6.0 *beta* (11.11.2022)
## Sharding
- [fix] Fix sorting in distributed queries for cases, when json path is not equal to index's name

## Core
- [fea] Totally replace deprecated CPP-client with the new one

## Reindexer server
- [fea] Add new logic for shared thread pool, which allows to create thread on request. Dedicated mode is no longer required for cluster and sharding anymore
- [fea] `enable-cluster` flags was deprecated. Cluster does not need any explicit options now

## Reindexer tool
- [fix] Fix 'binary buffer broken' error on large proxied selects

## Face
- [fea] Add the Namespace settings button to the Namespace list
- [fea] Add the ability to pin Namespaces in the Namespace list
- [fea] Add the enable_preselect_before_ft option to the indexconfig
- [fea] Improve the Precepts UI
- [fea] Make the "Gear" button visible for tables
- [fea] Redesign the Statistics->Memory page
- [fea] Redesign the Statistics->Performance page
- [fea] Improve snackbars view
- [fea] Redesign the feature of the column resizing
- [fix] Fix UI availability with DB config issues
- [fix] Fix a misprint in the database config
- [fix] Fix the issue with Scroll on the Statistic page
- [fix] Fix the title of the index editor
- [fix] Remove the search bar from the Indexes page

## Ported
- [fea/fix] Port all features and fixes from v3.9.0, v3.9.1, v3.10.0

# Version 4.5.0 *beta* (21.09.2022)
## Sharding
- [fea] Optimize proxying (proxying via raw buffers)

## Reindexer client
- [fea] Optimize status request

## Go connector
- [fea] Add sharding support for `builtinserver`
- [fix] Fix potential connection leak

## Face
- [fea] Increase cache life-time of a few resources basing on Google recommendation
- [fea] Add strict_mode to the NS config
- [fix] Fix the uptime issue
- [fea] Add `max_areas_in_doc`, `max_total_areas_to_cache`, `optimization` to Indexes
- [fea] Add `index_optimizer_memory`, `tracked_updates_size` to Statistics -> Memory
- [fea] Improve the Precepts UI
- [fea] Add the pagination instead of the 'load more' feature
- [fix] Fix the value array clearing
- [fix] Change column headers on the Statistics -> Queries page
- [fea] Add a flag of the server unavailability 
- [fea] Add the parsing of the 500 code response in the log
- [fea] Add `sync_storage_flush_limit` to the config
- [fix] Fix the column list for the grid view on the Statistics -> Memory page
- [fea] Add the description of the 5хх codes to the message body

## Ported
- [fea/fix] Port all features and fixes from v3.7.0, v3.8.0

# Version 4.4.0 *beta* (29.06.2022)
## Core
- [fea] Add 'last error' field into replication statistics
- [fix] Fix replicas network status logic

## Sharding
- [fea] Add sorting support for distributed (sharded) select queries
- [fea] Add aggregation support (min, max, sum) for distributed (sharded) select queries
- [fea] Make transactions' proxying asynchronous
- [fea] Add multithreading support for sharding proxy
- [fea] Add activity statistics support for sharding and cluster proxy
- [fea] Add parallel execution for distributed (sharded) select queries

## Reindexer tool
- [fea] Add #shard_id field for sharded select results

## Face
- [fea] Add `with_shard_ids` param to Items query
- [fix] Fix the default shard displaying
- [fix] Fix the limit in the request of the items on the SQL page
- [fix] Fix the NavigationDuplicated error

## Ported
- [fea/fix] Port all features and fixes from v3.6.0, v3.6.1(v3.6.2)

# Version 4.3.0 *beta* (24.05.2022)
## Core
- [fea] Improve cluster/sharding proxy performance (+add multiple proxying threads support)
- [fea] Add `local` keyword to SQL syntax. This allows to disable sharding proxying via SQL
- [fea] Add `from_sync_leader` mode for async replication from cluster. In this mode async namespaces will always be replicated from current sync cluster leader
- [fea] Improve tmp namespaces autoremove logic
- [fix] Fix `set_leader` behavior
- [fix] Fix `count` and `count_cached` aggregation for distributed sharding requests
- [fix] Fix reconnect interval for replicator

## Reindexer server
- [fea] Add `with_shard_ids` option for `GET /db/{database}/namespaces/{name}/items`. Allows to get `#shard_id` for each item

## Go connector
- [fix] Fix context timeout/deadline handling for cproto connect

## Face
- [fix] Fix sharding indexes duplication in indexes tab
- [fix] Remove old replication settings page
- [fix] Fix `current shard` switch
- [fix] Fix fulltext index config settings

## Ported
- [fea/fix] Port all features and fixes from v3.3.3, v3.4.0, v3.5.0 and v3.5.2

# Version 4.2.1 *beta* (14.03.2022)
## Core
- [fix] Fix compatibility with builtin java connector
- [fix] Fix error handling in queryresults iterator

# Version 4.2.0 *beta* (20.02.2022)
## Core
- [fea] Add sharding support. Check [sharding.md](sharding.md) for details
- [fea] Add support for mixed replication setups (now it's possible to replicate some the sync cluster namespaces asynchronously)
- [fix] Now transactions will return error on commit, if there were any errors in previous operations with this transaction

## Build
- [fea] Add build for debian-11 (bullseye)

## Reindexer tool
- [fea] Add different modes for \dump to support sharding

## Go connector
- [fea] Add support for sharding in LRU cproto items cache
- [fix] Fix LRU cproto items cache invalidation

## Face
- [fea] Add sharding support
- [fix] Fix 'explain' visualization for queries with facet()

# Version 4.1.0 *beta* (25.01.2022)
## Core
- [fea] Optimize tags updates replication
- [fea] Add multiple connections to cluster proxy to improve parallel proxying

## Reindexer server
- [fix] Fix vector overflow in json-parsing (when json ends with '\n')

## Go connector
- [fea] Add different reconnect strategies for async and sync cluster setups

## Docker
- [fea] Add flags for http timeouts

## Face
- [fix] Fix the issue with the Replication page opening

## Ported
- [fea/fix] Port all features and fixes from v3.2.6, v3.3.0, v3.3.1 and v3.3.2

# Version 4.0.0 *beta* (15.11.2021)
## Core
- [fea] Add synchronous replication support (RAFT-like cluster)
- [fea] Rewrite asynchronous replication logic

*WARNING*: This release breaks compatibility for replication and requires reconfiguration [replication.md](replication.md)
Storages for v3 and v4 are compatible in both ways.

## Face
[fea] Add replication statistics

# Version 3.2.5 (30.10.2021)
## Build
- [fix] Fix building in directories, containing dots (also fixes homebrew installation)

# Version 3.2.4 (14.10.2021)
## Core
- [fix] Fix memory leak in SetObject for non-index fields
- [fea] Optimize join queries with filters in both main and joined query
- [fix] Fix join queries SQL serialization
- [fix] Fix join queries with '<', '<=', '>' and '>=" operators in join condition

## Reindexer tool
- [fix] Fix \quit command

## Face
- [fea] Changed position of the Config menu for Namespaces
- [fix] Changed the log level input with selector
- [fea] Added the possibility to keep synonyms with spaces
- [fix] Removed excess requests in the tutorial
- [fix] Fixed the message popup position
- [fea] Changed the first item in the list view with namespace_<pk_name>_<pk_value>
- [fix] Fixed the set of columns on Statistics after forwarding from a Namespace
- [fix] Fixed the displaying of the enabled columns on Statistics -> Performance
- [fix] Fixed the format of the "start-time" value on the index page
- [fix] Fixed the scroll that appeared on the SQL page after enabling the disabled columns
- [fix] Fixed the long-name issue in the popup of the namespace deleting

# Version 3.2.3 (27.08.2021)
## Core
- [fix] Fixed indexes for client stats
- [fix] Fixed optimization cancelling for concurrent queries
- [fix] Do not rebuild composite indexes after update
- [fix] Removed TSAN suppression for tests

## Face
- [fea] Added tooltips to Grid columns
- [fea] Changed the DB Config menu position
- [fix] Fixed the issue with an empty table appearing on the Query result page after another query applying
- [fea] Added the ability to create PK during the Namespace creation
- [fix] Removed incorrect functions of editing and deleting items on the SQL result page
- [fix] Fixed the issue with the "Top-up" button
- [fix] Fixed the 'Desc' sorting on Statistics -> Grid
- [fix] Fixed the issue with losing a namespace focus during tabs changing
- [fix] Performed yarn upgrade
- [fix] Fixed the issue with the sorting params keeping 
- [fix] Fixed the issue with case-sensitive field names during the grid building
- [fix] Fixed the issue with slow 3g in the Namespace list
- [fix] Fixed the "Default stop words" option on the "Add index" form
- [fix] Fixed the issue with the full-text config and full-text synonyms definition config areas on the "Add index" form
- [fix] Fixed the table settings displaying 
- [fix] Fixed the converting from timestamp to human-readable date
- [fix] Fixed issues appeared after the bootstrap upgrade 
- [fix] Fixed the issue with the grid building appeared for indexes with multiple JSON paths

# Version 3.2.2 (16.07.2021)
## Core
- [fea] Optimize string refs counting for wide-range queries
- [fix] Fix merge limit handling for deleted values in fulltext index
- [fix] Fix cascade replication for nodes without storage
- [fix] Fix sorted indexes update

## Reindexer server
- [fix] Fix authorization for http update sql-queries and schema getters

## Go connector
- [fix] Fix pprof segfault during CPU profiling of builtin/builtinserver

## Build
- [fix] Fix special characters escaping in asm files regexp

# Version 3.2.1 (16.06.2021)
## Core
- [fea] Added optional fulltext index warmup after atomic namespace copy (EnableWarmupOnNsCopy)
- [fix] Fixed assertion on namespace reconfiguration, while force-sync is in progress
- [fix] Fixed fulltext index rebuilding after max_typos config update
- [fix] Fixed sort by expression in joined queries
- [fix] Fixed numeric strings handling (for cases, when value is greater than "2147483648")
- [fix] Fixed assertion in upsert with multiple jsonpaths (now it returns error instead)
- [fix] Fixed indexing for null-value
- [fea] Covered more text representation for huge numbers in fulltext search

## Reindexer tool
- [fix] Fixed sigabort on remote's server disconnect

## Face
- [fix] Collate mode became required for the 'string' field type only
- [fea] Added enable_warmup_on_ns_copy option to the full text index
- [fix] Fixed issue with the namespace config during database switching
- [fix] Slight design changes in the top menu and left sidebar
- [fix] Fixed type errors for Namespace -> Grid

# Version 3.2.0 (21.05.2021)
## Core
- [fea] Added fields ranks summation syntax for fulltext queries
- [fea] Changed typos handling config for fulltext (new max_typos option)
- [fix] Fixed error handling for fulltext PK

## Reindexer server
- [fea] Added separate options for http and rpc threading modes
- [fea] Added option for max http request size
- [fix] Fixed dangling threads for http-connections in dedicated threading mode

## Reindexer tool
- [fix] Fixed sigabort in \bench

## Face
- [fix] Fixed double requests during the sorting in the Performance table.
- [fea] Changed the step of the Bm25 weight option 
- [fea] Changed max_typos_in_word to max_typos 
- [fea] Added the sum_ranks_by_fields_ratio option
- [fea] Changed data type to integer for max_typo_len and max_rebuild_steps

# Version 3.1.4 (30.04.2021)
## Reindexer server
- [fix] Disable dedicated threads mode for HTTP-server

# Version 3.1.3 (26.04.2021)
## Build
- [fea] C++17 is now required to build reindexer from sources

## Core
- [fix] Fixed segfault in fulltext query with brackets
- [fix] Fixed deadlock in selector in case of concurrent namespace removing
- [fix] Fixed true/false tokens parsing inside query to composite index

## Reindexer server
- [fea] Optional dedicated threads mode for RPC and HTTP server
- [fix] Fixed heap use after free in concurrent requests to HTTP server
- [fix] Fixed stack use after free on RPC client disconnect and server shutdown

## Face
- [fix] Fixed bug on Statistics -> Performance -> Selects/Updates 
- [fix] Fixed default config for join_cache_mode
- [fix] Centered table data 
- [fea] Design changes on Performance statistics
- [fix] Limited responsive layout by 1280px
- [fea] Design changes for DB creation 
- [fea] Added DB name to DB removing UI 
- [fea] Added extra actions to Reset to default config and Save changes buttons
- [fea] Added samples to namespace search panel 
- [fix] Replaced color for rtree tips on Indexes
- [fix] Changed font to Hack for code in popups
- [fix] Removed sorting from Query table
- [fix] Fixed Load more error on Query -> SQL
- [fix] Fixed zero value displaying in Grid view

# Version 3.1.2 (9.04.2021)
## Face
- [fix] Fixed hot-key S for SQL query on tablets
- [fix] Fixed scroll issue for tables

# Version 3.1.1 (29.03.2021)
## Core
- [fix] Bug in full text query with single mandatory word fixed
- [fix] Bug in query with condition ALLSET by non indexed field fixed
- [fix] Bug in query with merge and join by the same namespace fixed
- [fix] Simultaneous update of field and whole object fixed
- [fix] Build on aarch64 architecture fixed
- [fix] Fixed replication updates limit tracking, and possible infinity full namespace sync
- [fix] Fixed web face page corruption on Windows builds

## Reindexer server
- [fea] GRPC module included in docker container and enabled by default
- [fix] Select with DISTINCT by GRPC fixed

## Face
- [fix] Fix for removing a single database
- [fix] Increased the edit label width of the index row
- [fea] Added the server configs to the homepage
- [fea] Added relevance between "Fields specific config" and "JSON paths" fields
- [fix] Upgraded Lo-Dash to the last version.
- [fix] Changed the font style of the "Create new database" menu.

# Version 3.1.0 (12.03.2021)
## Core
- [fix] Start of reindexer with DB containing indexes with invalid names fixed
- [fix] Mandatory terms with multiword synonyms in fulltext queries fixed
- [fea] Verification of EQUAL_POSITION by the same field added
- [fea] Added new syntax for update of array's elements
- [fea] Improved verification of fulltext index configuration

## Reindexer server
- [fea] api/v1/check returns more information
- [fea] GRPC module moved to dynamic library

## Go connector
- [fea] Call of Go signal handler improved

## Face
- [fix] Fixed the tables sorting
- [fix] Limited the modal window width
- [fix] Fixed the Statistics -> Queries view
- [fix] Fixed the horizontal scroll for /statistics-memory_stats and /statistics-perfstats
- [fix] Fixed the memory issue for big data
- [fea] Changes for the full-text config
- [fea] Added new fields to the Clientstats
- [fea] Added the ability to save column set for tables

# Version 3.0.3 (15.02.2021)
## Core
- [fix] Crash on full text query with empty variants
- [fix] Race namespace caches on transactions with atomic ns change
- [fix] Race on force sync replication
- [fea] Improved position search in fulltext
- [fea] Improved timeouts handling in select operations

## Reindexer server
- [fea] Added support for Content-Encoding: gzip in http server

## Go connector
- [fix] Build builtin/builtinserver on mingw

## Face
- [fea] Added tooltips to the longest query
- [fix] Fixed the query view on the Query -> SQL page
- [fix] Added checking for unsaved data during the window closing 
- [fix] Bug with the pagination in the List mode
- [fea] Added validation for the Server ID field 
- [fea] Added validation for the positive value of the Cluster ID field

# Version 3.0.2 (29.01.2021)
## Core
- [fea] Extra parameters added to fulltext config
- [fea] Validate PK index to be not scan, strict index names validation
- [fea] COUNT and COUNT_CACHED results added to aggregations results
- [fix] DISTINCT with 800k+ unique value execution time on Linux improved
- [fix] SQL representation of JOIN queries
- [fea] Optimize indexes before atomic transactions ns exchange
- [fea] Choose IN condition execution plan depend on another condition selectivity

## Reindexer server
- [fea] Added configurable size limit of online replication updates buffer
- [fix] Cascade replication fixes
- [fix] Shutdown time reduced

# Version 3.0.1 (31.12.2020)

## Core
- [fix] Search by multi-word synonyms is fixed
- [fix] Comparator performance issue of condition IN (many strings) 

## Face
- [fix] List view performance issues
- [fix] Enable edit items in query results page
# Version 3.0 (22.12.2020)
## Core
- [fea] Sort by distance
- [fea] Geo indexes performance improved
- [fix] Disable rename ns on slave
- [fix] Disable fulltext log in case loglevel is set to 0
- [fix] Overlap and possible assert on serial int32 overflow

## Reindexer server
- [fea] Added GRPC support. [reindexer.proto](cpp_src/server/proto/reindexer.proto)
- [fea] Added reindexer version to prometheus metrics
- [fix] Decreased time of shutdown reindexer server with enabled prometheus
## Reindexer tool
- [fea] Completely rewritten network cproto client
- [fix] reindexer.SetDefaultQueryDebug behavior fixed in case missed default namespace config 
## Face
- [fea] Completely redesigned

## Go connector 
- [fea] Added debug traces for Query and Iterator misuse. To enable tracer set REINDEXER_GODEBUG env

# Version 2.14.1 (02.11.2020)

## Core
- [fix] Binary incompatibility of UpdateQuery with pre v2.14.0 fixed, except case with Update array with single element
- [fix] Binary incompatibility of SetSchema WAL Record with pre v2.14.0 fixed. 

# Version 2.14.0 (30.10.2020)

## Core
- [fea] Add geometry indexes support
- [fea] Add logs for current query in case of crash
- [fix] Fix secondary slave replication after main master restart
- [fix] Fix set and update methods for non-indexed array fields
- [fix] Fix select filters for aggregation functions

## Reindexer server
- [fea] Add protobuf schema generating and protobuf data handling
- [fea] Add upsert command to http-api
- [fix] Fix outdated namespace removing from prometheus stats

## Reindexer tool
- [fix] Fix command execution interrupts on SIGINT
- [fix] Disable replicator for reindexer_tool

## Go connector
- [fix] Fix objects cache size setting
- [fix] Fix Status()-call for cproto-connector

# Version 2.13.0 (18.09.2020)

## Core
- [fea] Added extra parameter to clients stats
- [fea] Added update, delete, truncate statement in DSL
- [fix] Added support for equal_positions in sql suggester
- [fix] Crash on distinct with composite index
- [fix] Crash on query with incorrect index type after index conversion

## Reindexer tool 
- [fix] Crash on upsert array object as first json tag

## Version 2.12.2 (15.09.2020)

## Go connector
- [fix] Cproto connection freezes if timeout occurs while reading from socket

# Version 2.12.1 (10.09.2020)

- [fix] Crash on fulltext build, if typos map exceed 2GB
- [fix] Crash on DSL query with IN (string,...) conditions

## Go connector
- [fix] Cproto connection freezes on async transaction timeout

# Version 2.12.0 (02.09.2020)

## Core
- [fea] WAL size settings via #config namespace
- [fea] WAL updates filtering
- [fea] Return error for queries with EqualPosition to fields with IS NULL condition

# Reindexer server
- [fea] Transactions HTTP API
- [fea] Add RPC-transactions limit for each connection
- [fea] More advanced JSON DSL validation for queries

# Go connector
- [fea] Items cache size limits setting
- [fix] Fix SEGFAULT on unauthorized builtin server

# Version 2.11.2 (25.08.2020)

# Core
- [fix] Fulltext snippet location in text
- [fix] Unneeded force replication fixed

# Go connector
- [fix] Reset state on reconnect to different server

# Version 2.11.1 (14.08.2020)

# Core
- [fix] Increased performance of queries with custom sort order
- [fix] Fixed behavior of SQL UPDATE statement with true/false/null values
- [fix] Do not reset expire_after value of ttl indexes after copying tx
- [fix] Error loading of system records (indexes/tags/replState) after copying tx
- [fix] To config added default value of `optimization_sort_workers` 
- [fix] Windows specific error with IN () condition executed by comparator

# Reindexer server
- [fea] Removed default limit=10 in GET :db/namespaces/:ns/items method
- [fea] Added `REINDEXER_NOREUSEIDLE` env var for disable server connection reuse

# Go connector
- [fix] Fixed async tx goroutines leak

# Version 2.11.0 (29.07.2020)

# Core
- [fix] Crash with SEGV or assert on ordered queries after copying tx
- [fix] Correct normalization of SELECT COUNT(*) SQL queries
- [fea] More efficient replication startup on active master updates
- [fix] Namespace indexes optimization after load was not started
- [fix] Centos 7 build fixed

# Version 2.10.0 (05.07.2020)

# Core
- [fea] Cascade replication
- [fea] MsgPack format support for query and updates
- [fea] Initial JsonSchema v4 support. 
- [fea] Query strict mode support - Check indexes or fields names before query execution

# Reindexer server
- [fea] MsgPack format support for http and cproto

# Python connector
- [fea] Moved to separate repository


# Version 2.9.2 (24.06.2020)
- [fix] Optimize indexes after ns load
- [fix] Correct Drop fields by json path 
- [opt] Execution plan with wide IN condition to low selectivity indexes

# Version 2.9.1 (10.06.2020)

# Core 
- [fix] Fulltext search kb layout filter with symbols like ',' ']'
- [fix] Segfault with distinct on field with different types

# Reindexer tool 
- [fix] Behavior of --help command line argument
- [fix] Do not report error on restore exiting namespaces with compatible indexes

# Reindexer server
- [fix] Backtrace behavior with musl builds

# Go connector
- [fix] Turn cgo backtrace symbolizer off by default (fixes random crashes on go 1.14)
- [fix] Alpine linux build

# Version 2.9.0 (15.05.2020)

# Core
- [fea] Output relevancy rank information to json format
- [fea] Optimized query to #memstats. Now it's O(1) 
- [fea] Added app_name field #clientsstats
- [fix] Fixed SQL suggestion behavior on concatenated tokens

# Go connector
- [fix] Fixed and enabled multiple dsn support in cproto

# Version 2.8.1 (12.05.2020)

- [fix] unexpected "terminate called" 

# Version 2.8.0 (24.04.2020)

# Core

- [fea] Fulltext - rank by word position
- [fea] Explain information improved
- [fix] Memory corruption on copying tx-es

# Reindexer tool
- [fix] Fixed crash on query with joins

# Reindexer server
- [del] Removed useless "storage_enabled" flag from GET db/namespaces method response

# Face

- [fea] Query builder refactored
- [fea] New document page now modal

# Misc

- [ci] Ubuntu 20.04 build added

# Version 2.7.1 (14.04.2020)

# Go connector
- [rev] Disabled multiple dsn support in cproto

# Version 2.7.0 (10.04.2020)

# Core
- [fix] Requests with general sort and offset without limit can produce wrong sort
- [fix] Full text synonyms can not work after transactions
- [fea] More efficient arrays packing from json source
- [fix] NOT operator in full text queries
- [fix] Tuple memory leak on Update by condition
- [fea] Multi-word synonyms support 
- [fix] Potential large memory allocation on DISTINCT queries
- [fix] Potential crash with aggregation on object fields

# Reindexer server
- [fea] Optional snappy network traffic compression
- [fea] OnWALUpdates now used shared buffers - can seriously decrease memory usage with many slave
- [fix] Replication fixes on bad network connections
- [fix] Connection ID overflow, and bad connID error fixed

# Reindexer tool
- [fea] Added --createdb flag.
- [fea] Improved behavior while input is redirected

# Go connector
- [fix] Enable to create multiple instances of built-in server
- [fea] Multiple dsn support in cproto

# Face
- [fix] Fixed precepts of new item
- [fea] Rename namespace feature
- [fea] Namespace config moved to dedicated page

# Version 2.6.4 (31.03.2020)

# Reindexer server
- [fix] Wrong conn id full fix 

# Version 2.6.3 (27.03.2020)

# Core
- [fix] Full text search ranking with synonyms
- [fea] Distinct execution plan optimization
- [fix] Crash on incorrect json in config (array, when object expected)
- [fix] Crash on concurrent full text updates/selects
- [fix] Search by second field in full text, while term present in first field
- [fea] Multiple dsn support in cproto client

# Reindexer server
- [fix] Do not create users.yml in in-memory only mode
- [fix] Wrong conn id fix

# Reindexer tool
- [fix] Hang on error in batch mode
- [fix] db name begins with cproto or builtin correct handling 

 # go connector
- [fix] Hang on parsing recursive types references in struct

# Version 2.6.2 (13.03.2020)

# Core
- [fea] Multiple distinct in single query using indexes
- [fix] Unload by idle timeout feature is disabled
- [fea] Update Query now can update object or array of objects

# Reindexer server
- [fea] Enable tx.Rollback after tx.Commit
- [fix] Crash on Upsert to tables with composite pk with non indexed fields as part of PK
- [fea] Stronger txID & queryID validation and isolation between clients
- [fix] Correct lsn & id invalidation after UpdateQuery
- [fea] Clients connection statistics added


# go connector
- [fea] Enable tx.Rollback after tx.Commit, 
- [fea] Added read tx.Query support
- [fea] Log warning about reindexer library and client mismatch
- [fix] Unmarshall []bool indexed fields
- [fix] Unsafe Query flag reset after recycle from sync.Pool
- [fea] DB.SetDefaultQueryDebug now copies default ns settings


# Version 2.6.1 (02.03.2020)

# Core
- [fea] Synonyms feature in fulltext search
- [fea] New full_match_boost parameter in fulltext search
- [fix] last_updated_time of ns will not change on restart

# Reindexer server
- [fea] Enable server startup without storage
- [fea] Backward compatibility of tx replication with old reindexer versions

## go connector
- [fix] Windows build

# Version 2.6.0 (21.02.2020)

## Core
- [fea] Atomic applying transaction with namespace copy & replace
- [fea] Transaction performance statistics
- [fix] Equal position behavior fixed
- [fea] Sort queries by right namespace field 
- [fix] UPDATE statement fixes 
- [fix] Transaction replication fixes 
- [fix] Select fields filter fix for right namespace

# Reindexer server
- [fea] web static resources are embedded to server binary by default

# Version 2.5.5 (07.02.2020)

# Reindexer server
- [fix] Fixed backward compat of RPC with 2.5.2-

# Version 2.5.3 (06.02.2020)
## Core

- [fea] Added rename namespace API
- [fea] Improved -dev package behavior
- [fix] Transactions memory consumption are seriously decreased

# Reindexer tool
- [fea] Added `\namespace rename` command

# Reindexer server
- [add] Added HTTP API method /namespaces/rename
- [fix] Fixed docker image

## go connector
- [fea] Auto pass list of detected C/C++ dependencies libraries to go 
- [fea] Added RenameNamespace function

# Version 2.4.6 (24.01.2020)
## Core
- [fea] Replication of transactions
- [fea] EqualPosition support in sql in dsl
- [fix] Forced sort fixes
- [fix] Distinct keyword fixes in sql-dsl parsers/encoders 
- [fix] Sql auto-complete fixes
- [fix] Replication lsn mismatch fix
- [fix] Merge queries with joins fix 

## Reindexer tool
- [fix] Mac OS X table-view paging fix

# Version 2.4.5 (30.12.2019)

## Core
- [fix] Fix: forced replication can lead to infinite loop
- [fea] Add .pkg file and autotest in -dev package
- [fix] Replication of meta in forcedSync

## go connector
- [fea] Canceling of connecting to server if the deadline is expired

## Reindexer tool
- [fix] Fix of incorrect paging when output to file or stdout redirection
- [fix] Fix of suggest of '\<key word>'

# Version 2.4.4 (17.12.2019)

## Core
- [fix] Do not lock preResult values if from cache fixed potential assert on queries with join with used join cache
- [fix] Fix assert in sort by composite indexes
- [fea] Add composite values parsing for SQL select
- [fix] Make circular accumulator for stddev performance statistic
- [fix] Fix unhandled exception while calculating perf stat

## go connector
- [fix] RawBuffer leak due to unclosed iterators in transactions

# Version 2.4.3 (06.12.2019)

## Core
- [fea] Add '\' as a special escaping symbol to FtDSL
- [fix] Merge-join queries fixes and optimizations
- [fix] Fix hit_count_to_cache overflow for idset cache

## Reindexer server
- [fea] Add master's config check on slave connect
- [fea] Disable automatic database creation on RPC-connect
- [fix] Add force-resync after online replication errors
- [fix] Fix lsn overflow after conversion to int

## go connector
- [fea] Add replication status to memstats

# Version 2.4.2 (21.11.2019)

## Core
- [fix] Joins optimizations

## go connector
- [fix] cjson lock + deep copy performance fixes

# Version 2.4.1 (15.11.2019)

## Core
- [fea] Sort by expressions
- [fea] Optimized lock time for joins with small pre-result set
- [fea] Added more info about replication state to #memstat namespace
- [fix] LSN on row-based query replication (possible assert on server startup)
- [fix] Replication clusterID for namespaces without storage
- [fix] PK precepts replication

## Reindexer server
- [fix] Query results row calculation for http queries with limit

## Reindexer tool
- [fea] Table pagination
- [fea] Queries cancellation

## go connector
- [fix] Tags race on concurrent transactions

# Version 2.3.4 (29.10.2019)

## go connector
- [fix] Iterator.NextObj(): performance fix

## Core
- [fix] Fix of Windows build

# Version 2.3.3 (29.10.2019)

## Core
- [fix] disabled OR operator for Fulltext indexes
- [fix] replication: using row records instead of statements for UpdateQuery/DeleteQuery (performance improvement)

## Reindexer server
- [fea] new statistics parameter: items count (for prometheus)

## Reindexer tool
- [fix] UpdateQuery/DeleteQuery crash fix
- [fea] Cancelling queries execution by Ctrl+C

## go connector
- [fea] Iterator.NextObj() unmarshal data to any user provided struct

# Version 2.3.2 (25.10.2019)

# Core
- [fix] wrong WAL ring buffer size calculation on load from storage
- [fix] Make storage auto-repair optional
- [fix] firstSortIndex assert on sort by hash indexes

# Version 2.3.0 (11.10.2019)

## Core

- [fix] Possible deadlock on tx, if it is canceled by timeout
- [fea] Join optimization (use indexes from left ns, instead of scan) if possible
- [fix] Replication races fixes
- [fea] Atomic namespace replace on forced sync replication
- [fea] Try to auto repair storage if reindexer does not shutdown correctly

## Reindexer tool
- [fea] Table mode output

## Reindexer server

- [fea] Added columns pre calculation for table output  in GET /query request

## go connector

- [fea] Added averaged cgo limiter usage statistics
- [fea] CGOSymbolizer added, output mixed go/c++ backtraces on crash

## Face

- [fea] Improved SQL editor component
- [fix] Fixed behavior of keyboard shortcuts
- [fix] Total count of update/select operation in perf stats

# Version 2.2.4 (30.09.2019)

## Core

- [fix] Idset cache invalidation on upsert/delete null values to indexes
- [fix] Possible crash if sort orders disabled
- [fix] Wrong lowercase field name on SQL UPDATE query
- [fea] Delete & Update queries in transactions

## Reindexer tool

- [fea] Add command `databases` for operating with databases
- [fea] Add suggestions for commands and databases names
- [fix] Replxx dependency reverted to stable version
- [fea] Using default dsn cproto:://127.0.0.1:6534/

## go connector

- [fea] Delete & Update queries in transactions
# Version 2.2.3 (18.09.2019)

## Core 

- [fix] Fulltext queries sort by another field
- [fea] Number of background threads for sort optimization can be changed from #config namespace
- [fix] Sort optimization choose logic is improved

## go connector

- [fix] leak seq from chan
- [fix] check ctx.Done while waiting on cgo limiter


# Version 2.2.2 (15.09.2019)

## Core 

- [fea] More effective usage of btree index for GT/LT and sort in concurrent read write operations
- [fix] Potential crash on index update or deletion
- [fea] Timeout of background indexes optimization can be changed from #config namespace

## Reindexer server

- [fea] User list moved from users.json to users.yml
- [fea] Hash is used instead of plain password in users.yml file
- [fix] Pass operation timeout from cproto client to core

# Version 2.2.1 (07.09.2019)

## Core 

- [fea] Updated behaviour of Or InnerJoin statement
- [fea] Store backups of system records in storage
- [fix] Replicator can start before db initialization completed
- [fix] Search prefixes if enabled only postfixes

## Reindexer server

- [fea] Added prometheus /metrics handler

## Face

- [fea] Fulltext config editor
- [fea] Quick copy item's JSON from list view

# Version 2.2.0 (27.08.2019)

## Core

- [fea] Dynamic switch of replication role
- [fea] Facets by array fields 
- [fea] JOIN now can be used in expression with another query conditions 
- [fea] Support rocksdb as storage engine
- [fix] Race on concurrent read from system namespaces
- [fix] Replication config sync fixed

## Reindexer tool

- [fea] Add repair storage function

## go connector

- [fix] Async tx goroutine fixed
- [fix] Replication in builtin mode fixed

# Version 2.1.4 (15.08.2019)

- [fea] Reindexer server will not start if storage is corrupted, `startwitherrors` config flag is used to override
- [fix] Do not write to WAL empty update queries
- [fix] Replication config sync

# Version 2.1.3 (14.08.2019)

## Core

- [fea] Added two-way sync of replication config and namespace
- [fea] Memory usage of indexes decreased (tsl::sparesmap has been added)
- [fea] Added non-normalized query in queries stats
- [fea] Add truncate namespace function
- [fix] Fixed unexpected hang and huge memory alloc on select by uncommitted indexes
- [fix] Correct usage of '*' entry as default in namespaces config
- [fix] Memory statistics calculation are improved
- [fix] Slave will not try to clear expired by ttl records

# Version 2.1.2 (04.08.2019)

## Core

- [fea] Added requests execution timeouts and cancellation contexts
- [fea] Join memory consumption optimization
- [fea] Current database activity statistics
- [fea] Use composite indexes for IN condition to index's fields
- [fea] Reset performance and queries statistics by write to corresponding namespace
- [fix] Crashes on index removal
- [fix] Do not lock namespace on tx operations
- [fix] SQL dumper will not add exceeded brackets
- [fea] Added `updated_at` field to namespace attributes


# go connector

- [fea] Added requests execution timeouts and cancellation contexts
- [fea] Added async tx support
- [fea] Removed (moved to core) `updated_at` legacy code


# Version 2.1.1 (12.06.2019)

## Core 

- [fix] sparse index crashes on type conflicts
- [fix] fixed memory stats for dataSize of string hash indexes 

# Reindexer server

- [fix] possible crash on http meta update
- [fix] return back item data via cproto  on UpdateQuery operation 

# go connector

- [fix] Build builtinserver with libunwind conflict fixed
- [fix] Query.Update panic fixed
- [fix] Stronger check of namespace's item's type (PkgPath is included to check)

# Version 2.1.0 (08.06.2019)

## Core

- [fea] Brackets in DSL & SQL queries
- [fix] Crash on LRUCache fast invalidation
- [fix] Relaxed JSON validation. Symbols with codes < 0x20 now are valid
- [fix] The '\0' character in JSON will not break the parser
- [fea] Backtrace with line numbers for debug builds
- [fix] Replication fixes
- [fea] Support for jemalloc pprof features
- [fea] Detect tcmalloc pprof features in runtime
- [opt] Optimized memory statistics calculation

# Reindexer server
- [fea] Return back item after update with atomic on update functions

# Golang connector
- [fea] Add timeout/deadline support
- [fea] Add *Ctx methods with context.Context
- [fea] Return back item after update with atomic on update functions

## Face

- [fea] Added table view of memory statistics
- [fea] Added red mark of error in SQL editor


# Version 2.0.3 (04.04.2019)

## Core
- [fea] Facets API improved. Multiply fields and SORT features
- [fea] TTL added
- [fea] `LIKE` condition added 
- [fea] Add expressions support in SQL `UPDATE` statement
- [fix] Invalid JSON generation with empty object name
- [fix] Unnecessary updating of tagsmatcher on transactions
- [fix] LRUCache invalidation crash fix

# Reindexer server

- [fea] Added metadata manipulation methods

## Face

- [fea] Added metadata manipulation GUI
- [fix] Performance statistics GUI improvements

# Version 2.0.2 (08.03.2019)

## Core 
- [fea] Update fields of documents, with SQL `UPDATE` statement support
- [fea] Add SQL query suggestions
- [fea] Add `DISTINCT` support to SQL query
- [fea] Queries to non-nullable indexes with NULL condition will return error
- [fix] Fixes of full text search, raised on incremental index build
- [fix] Queries with forced sort order can return wrong sequences
- [fix] RPC client&replicator multithread races
- [fix] DISTINCT condition to store indexes
- [fix] Caches crash on too fast data invalidation
- [fix] Disable execution of delete query from namespace in slave mode
- [fix] Rebuild fulltext index if configuration changed
- [fix] Fixed handling SQL numeric conditions values with extra leading 0

# Golang connector
- [fea] `Query.Update` method added
- [doc] Updated documentation and mapping for system Namespaces struct
- [fix] Support of POD types derived custom types
- [fix] Added `MATCH` condition support (alias `EQ`) to DSL

# Reindexer server
- [fix] Report error if config file is not found

# Reindexer tool
- [fea] Add `UPDATE` statement support

# Face
- [fea] Add SQL query suggest
- [fea] GUI for database configuration
- [fea] GUI and charts for performance and memory statistics

# Version 2.0.0 (07.01.2019) 

## Core
- [fea] Master-slave replication added (beta). [Documentation](replication.md)
- [fea] Transactions support
- [fea] min/max/stddev values added to perfstat
- [fea] storage lazy load option added

## Reindexer tool

- [fea] \subscribe command added


# Version 1.10.4 (20.12.2018)

## Core

- [fix] int64 and double conversion in JSON parser loose precision
- [fea] `DELETE FROM` SQL statement added
- [fix] `SELECT *,COUNT(*)` will not return items
- [fix] Crash on condition with arguments of different type to same index
- [fea] Aggregation function name added to aggregations results
- [fix] Incorrect limit/offset calculation on queries with `SORT` by non indexed fields

## Go connector

- [fix] Struct verifier incorrect validation of composite `reindex` tags
- [fea] Pool usage statistics added to `DB.Status()` method

## Reindexer server

- [fea] Added fields `namespaces` and `enable_cache` to GET|POST /db/:db/query method

## Face

- [fea] Query builder added
- [fea] `Delete all` button added to items page
- [fea] Aggregation results view
- [fea] Edit/Delete function of query results added
- [fea] JSON index configuration editor
- [fea] Memory usage statistics round precision

# Version 1.10.3 (02.12.2018)

## Core 

- [fix] Potential crash on delete from store string index
- [fix] Slow cache cleanup
- [fix] Print precision of double values decreased to actual double range

## Reindexer server
- [fix] Invalid http redirects, if compiled with -DLINK_RESOURCES

## Reindexer tool
- [fix] Unhandled exception in case trying of create output file in non-existing directory
- [fix] RPC client optimizations and races fixes
- [fea] \bench command added


# Version 1.10.2 (20.11.2018)

## Core

- [fea] Indexes rebuilding now is non-blocking background task, concurrent R-W queries performance increased
- [fix] Fulltext index incremental rebuild memory grow fixed

## Reindexer server

- [fix] Logger buffer increased, and will not block on errors
- [fix] DELETE Query method fixed
- [fix] urldecode of database names fixed

## Reindexer tool

- [fix] Pretty print fixed

# Version 1.10.1 (06.11.2018)

## Core

- [fea] Incremental fulltext search index rebuild 
- [fea] Async C++ RPC client
- [fix] Fixed incorrect behaviour with non indexed field conditions
- [fix] Extra non indexed fields tests added
- [fix] Fixed Json parser memory leak

## Reindexer server

- [fea] REST API documentation improved
- [fea] Optimized performance

## Reindexer tool

- [fea] Operation speed is improved


# Version 1.10.0 (29.10.2018)

## Core

- [fea] Added explain query feature
- [fea] Added min, max and facets aggregation functions
- [fea] Added EqualPosition query function
- [fea] In SQL query parser/printer added is NULL condition
- [fea] Native bool data type support
- [fea] Filter nested fields in SelectFilter statement
- [fix] Fix crash on comparator with Join queries
- [fix] Incorrect sort order using with Join queries in join cache optimization step 
- [ref] Composite indexes now using json_paths for list of fields
- [ref] Storage format has been changed, and incompatible with this version. To migrate date from previous version use [reindexer_tool](cpp_src/cmd/reindexer_tool/readme.md)

## Go connector

- [fea] reindexer.Status method added, to check connector status after initialization
- [fea] OpenNamespace now register namespace <-> struct mapping without server connection requirement
- [fix] int type is now converted to int32/int64 depends on architecture

## Python connector

- [fea] [Python connector](connectors/py_reindexer) v.0.0.1 is released.

## Reindexer server

- [fea] Added fields filter to method GET /api/v1/:db/:namespace:/items matched
- [fea] Added method DELETE /api/v1/:db/query
- [fea] Added poll loop backend (osx,bsd)
- [ref] `json_path` renamed to `json_paths`, and now array
- [ref] Binary cproto protocol has been optimized and changed. Please consider to update clients
- [fix] EnumMeta method has been fixed
- [fix] ModifyItem will retry attempt if tags <-> names state is invalidated

## Face

- [fea] Added EXPLAIN query stats view
- [fea] Added tags selector component for json_paths to index create/modify form

# Version 1.9.7 (28.09.2018)

## Core
- [fea] Storing index configuration in storage
- [fea] Concurrent R-W queries performance optimization
- [fea] Added indexes runtime performance statistics
- [fix] Incorrect NOT behaviour on queries with only comparators
- [fix] Race condition on shutdown
- [fix] Type conversion on queries without indexes

## Go connector
- [fea] Added public AddIndex/DropIndex/UpdateIndex methods
- [ref] ConfigureIndex method deprecated
- [ref] `appendable` logic has been moved from C++ core to golang connector
- [fix] Multiple database support in `embeded` mode.

## Reindexer tool
- [fix] Fixed restoring namespaces with index names non-equal to json paths

# Version 1.9.6 (03.09.2018)

## Core
- [fea] Merge with Join queries support
- [fea] Sort by multiple columns/indexes
- [fix] Case insensitivity for index/namespaces names
- [fix] Sparse indexes behavior fixed
- [fix] Full text index - correct calculation of distance between words
- [fix] Race condition on concurrent ConfigureIndex requests

## Reindexer server
- [fea] Added modify index method

## Go connector
- [fea] New built-in server binding: builtin mode for go application + bundled server for external clients
- [fea] Improved validation of go struct `reindex` tags

# Version 1.9.5 (04.08.2018)

## Core

- [fea] Sparse indexes
- [fix] Fixed errors in conditions for non-indexed fields
- [fix] Fulltext terms relevancy, then query contains 2 terms included to single word
- [fea] Customizable symbols set of "words" symbols for fulltext
- [fix] Incorrect behavior on addition index with duplicated json path of another index

## Reindexer tool

- [fix] Fixed deadlock on linux
- [fix] Fixed \dump command namespace list parsing

## Reindexer server

- [fea] Added method /api/v1/check with system information about memory consumption, uptime and version
- [fea] Passing to RPC client information about version and uptime
- [fea] Optional embed web resources to server binary

## Face

- [fix] Incorrect urlencoded for document update API url
- [fix] Namespace view layout updated, jsonPath added to table

## Go connector

- [fix] Fixed error after closing connection by timeout
- [fix] Caches invalidation on server restart

# Version 1.9.4 (25.07.2018)

## Core

- [fea] Conditions to any fields, even not indexed
- [fea] cproto network client added 
- [fix] Query execution plan optimization fixes.

## Reindexer tool

- [fea] Command line editor. tool has been mostly rewritten at all
- [fea] Interoperation with standalone server

## Reindexer server

- [fea] Pagination in the GET sqlquery HTTP method
- [fea] Filtration in the GET items HTTP method

## Face

- [fea] Table view of items page
- [fea] Filtration in the items page
- [fea] SQL queries history

# Version 1.9.3 (20.06.2018)

## Core

- [fea] Added system namespaces #memstats #profstats #queriesstats #namespaces with execution and profiling statistics
- [fea] Added system namespace #config with runtime profiling configuration
- [fix] Join cache memory limitation
- [fix] Fixed bug with cjson parsing in nested objects on delete
- [fix] 32 bit reference counter for records instead of 16 bit
- [fix] Crash on load namespaces from storage with custom sort order
- [fix] Crash on select from composite index, after delete last shared string copy for sub part of composite index
- [fix] Full text build threads limited to 8

## Reindexer Server

- [fea] Load data in multiple threads on startup
- [fea] Auto re-balance connection between worker threads
- [fix] "Authorization" http header case insensitivity lookup
- [fix] Unexpected exit on SIGPIPE
- [fix] Namespaces names are now url decoded

## Go connector

- [fea] Ability to pass extra options to bindings
- [ref] cgo buffers cleanup optimization

# Version 1.9.2 (04.06.2018)

## Core
- [fea] SQL parser performance improved
- [fea] Caching of intermediate join results
- [fix] Assert on error cjson parser disabled: server will not crash on application errors
- [fix] Relaxed check of fields type on index addition and loading from storage
- [fix] Potential field data corruption on runtime index addition

## Reindexer server
- [fix] Correct grow of input buffer for http and cproto connections
- [ref] HTTP parser and router refactored.
- [fix] gprof handlers fixes
- [fix] packages dependencies fixed, installation errors on centos fixes, path to webroot fixed
- [ci] added sanity tests of packages installations

## Go connector
- [fea] Checking for duplicate names in `json` structs tags on OpenNamespace
- [fea] Checking DeepCopy interface for correct return value on OpenNamespace
- [fix] Fixed error with sync payload types, on json queries
- [fix] Local imports of ./repo in benchmarks package broke gb

# Version 1.9.1 (17.05.2018)

## Reindexer server
- [fix] path to queries API

## Face
- [fix] error on create indexes
- [fix] show 'new database windows 'in case of start without any databases 

# Version 1.9.0 (16.05.2018)

## C++ core

- [fea] Search digits by words representation (now only for Russian language) in ft1
- [fix] Forced flush all batched writes to disk on exit
- [fix] Incorrect total count for queries without where clause
- [fea] custom collate now accepts sequence of letters
- [fix] Build issues with clang 3.9
- [fix] Cmake build dependencies, leveldb and snappy optionally added as ExternalProject
- [port] Windows and BSD build
- [port] 32 bit build

## Reindexer tool
- [fea] Ability to input/output dump from stdin/stdout

## Reindexer server
- [ref] Flags `create_if_missed` and `drop_on_format_error` are removed from namespaces API
- [fix] Correct behavior on logging different loggers to the same file
- [fix] CPU profiler error in case of 
- [port] Ability to run as Windows service and cmake Windows NSIS installer

## Go connector
- [fea] Removed cproto connector dependency on cgo
- [fea] cproto connector are compiled by default

# Version 1.8.1 (26.04.2018)

## C++ core

- [fix] Fix KeyValue::operator == for strings (IdSetCache isn't worked due this)
- [fix] Describe for composite indexes will return name alias
- [fix] Check for invalid PK indexes: Throw error on '-' (scan) PK
- [fix] Add missed comma in IndexDef JSON serializer 
- [fix] Relevancy in case when first term from query found in non boosted field first
- [opt] Optimize AND performance in ft1
- [fea] Add extra debug output to ft1


## Go connector
- [fix] Golang SQL query pseudo pre-parser accept all whitespace chars, not only ' ' 
- [fix] Correct composite PK handling in QueryTest
- [fix] Support of multiply index options in struct tag


# Version 1.8.0 (17.04.2018)

## C++ core

- [fea] Support of custom letters order set for string collates
- [fea] Full text indexing memory consumption optimization
- [fea] Thread russian letter `ё` as `е` in full text index
- [fix] Fixed incorrect behavior of full text search with term `*<stop-word>`
- [fix] Fixed full text behavior with FtDSL started with `+`
- [fix] Fix conflict of with Leveldb's and reindexer's tcmalloc library

## Reindexer server
- [fea] Added daemonize mode to reindexer_server
- [fix] init.d script fixes
- [fea] Added CPU profiling mode to built-in HTTP pprof server

# Version 1.7.0 (05.04.2018)

## C++ core

- [fea] Support join, marge, aggregations in json DSL & SQL queries
- [fea] Added multiline form and comments in SQL query
- [fix] Last symbol of documents was not used by fulltext indexer
- [fix] Potential data corruption after removing index

## Go connector
- [fea] Batching of free c-buffers calls
- [fea] Added cgo limiter for deleteQuery call

## Reindexer server beta
- [fea] POST /api/v1/:db/sqlquery method
- [fea] Added sort_order to GET /api/v1/db method

# Version 1.6.0 (18.03.2018)

## C++ core
- [fea] Composite indexes direct queries support
- [fea] Fulltext support functions `snippet` and `highlight`
- [fix] Added utf8 strings validation
- [fix] WrSerializer::Printf incorrect buffer maxlen calculation
- [ref] utf8 collates does not allocate extra memory
- [ref] sort by unordered indexes optimizations
- [ref] Join queries optimizations

## Go connector
- [ref] Raise panic on Query reuse in Join
- [fea] Composite indexes direct queries support

## Reindexer server beta
- [fea] Write components logs to separate files
- [ref] Protocol breaking changes

# Version 1.5.0 (02.03.2018)

## Reindexer server beta released:
- [fea] Added cmake package target for RPM & DEB based systems
- [fea] sysv5 init script added
- [fea] Binary cproto RPC protocol introduced
- [fea] Graceful server shutdown on SIGTERM and SIGINT
- [fea] Multiply databases support was implemented
- [fea] HTTP REST API changed: db name added to path, optional HTTP basic authorization (disabled by default)
- [fea] HTTP REST API for add/drop indexes added
- [fea] Configuration moved to separate .yml file

## C++ Core
- [ref] Item and Reindexer classes API has been redesigned. C++ tests was refactored to run with new API
- [fix] Potential race conditions and memory leak was fixed
- [fix] Fixed Query optimization bug. Queries with Join and precondition with GT/LT condition can return incorrect results
- [fea] Add/Drop indexes without reloading data
- [ref] Redundant Makefiles removed
- [del] Ugly RenameNamespace & CloneNamespace API was removed
- [fix] Fixed unnecessary copy results of precondition Join Query

## Go connector
- [fea] cproto connector to standalone server
- [fix] Potential races was fixed
- [del] Tx now alias to Batch. Old update logic was deprecated
- [fix] Limit cgo execution to 2K goroutines to avoid exceed of OS threads limit 
- [ref] EnableStorage method was deprecated
- [fix] Query builder did not reset opOR after InnerJoin
