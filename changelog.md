# Version 3.18.0 (29.07.2023)
## Core
- [fea] Increased max indexes count for each namespace up to 255 user-defined indexes (previously it was 63)
- [fea] Added more info to the [slow logger's](readme.md#slow-actions-logging) output: mutexes timing for transactions and basic `explain` info for `select`-queries
- [fea] Improved logic of the cost evaluation for the btree indexes usage in situations, when backgroud indexes ordering was not completed (right after write operations). Expecting more optimal execution plan in those cases
- [fix] Changed logic of the `ALLSET` operator. Now `ALLSET` condition returns `false` for empty values sets and the result behavior is similar to MongoDB [$all](https://www.mongodb.com/docs/manual/reference/operator/query/all/)
- [fix] Fixed automatic conversion for numeric strings with leading or trailing spaces (i.e. ' 1234' or '1234 ') into integers/floats in `WHERE`/`ORDER BY`
- [fix] Allowed non-unique values in forced sort (`ORDER BY (id,4,2,2,5)`). If forced sort order contains same values on the different positions (i.e. `ORDER BY (id,1,2,1,5)`), then the first occurance of the value will be used for sorting
- [fix] Added limits for the large values sets in the composite indexes substitution algorithm, introduced in v3.15.0 (due to performance issues in some cases). If the result size of the set is exceeding corresponding limit, reindexer will try to find another composite index or skip the substitution

## Go connector
- [fea] Added support for JOINs and brackets into [JSON DSL wrapper](dsl/dsl.go)

## Build
- [fea] Added support and deploy for Debian 12 (bookworm). Debian 10 (buster) build was deprecated
- [fea] Enabled SSE4.2 for the default reindexer's builds and for the prebuilt packages. SSE may still be disabled by passing `-DENABLE_SSE=OFF` to `cmake` command

## Face
- [fea] Changed the scale window icon for textareas
- [fea] Added the background color to the Close icon in the search history on the Namespace page
- [fea] Improved the buttons' behavior on the Query builder page
- [fea] Added the database name size limit.
- [fea] Improved the drop-down section behavior on the Query builder page
- [fea] Added new proc settings to the Index config
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

# Version 3.17.0 (06.07.2023)
## Core
- [fea] Optimized namespaces' locks for queries to the system namespaces, containing explicit list of names (for example, `SELECT * FROM #memstats WHERE "name" IN ('ns1', 'nsx', 'ns19')` now requires shared locks for the listed namespaces only)
- [fea] Added update queries for the arrays of objects (i.e. queries like `UPDATE ns SET object_array[11].field = 999`)
- [fea] Added more fulltext index options for the base rankings tuning. Check `FtBaseRanking` struct in the [config doc](fulltext.md#configuration)
- [fix] Fixed update queries for the sparse indexed arrays
- [fix] Fixed selection plans and cost calculation for `NOT`-conditions in queries
- [fix] Disabled composite indexes over non-indexed fields (except fulltext indexes) and sparse composite indexes. Previously those indexes could be created, but was not actually implemented, so queries with them could lead to unexpected errors
- [fix] Fixed merging of the conditions by the same index in queries' optimizer (previously it could sometimes cause SIGABORT in case of the empty resulting set)
- [fix] Disabled LIMIT for internal merged queries (it was not implemented and did not work properly on previous versions)
- [fix] Fixed cache behavior for fulltext queires with JOINs and `enable_preselect_before_ft: true`

## Reindexer server
- [fea] Optimized perfstats config access
- [fea] Added support for CSV results for select queries via HTTP (experimental, probably this API will be modified later on)

## Go connector
- [fea] Added `SortStPointDistance` and `SortStFieldDistance` (simplified aliases for the long constructions with `ST_Distance`)
- [ref] Added `reindexer.Point` type instead of `[2]float64`

## Build
- [fea] Added support and deploy for RedOS 7.3.2

# Version 3.16.0 (09.06.2023)
## Core
- [fea] Added UUID field type for indexes
- [fix] Fixed fulltext areas highlighting for the queries with prefixes/suffixes/postfixes
- [fix] Fixed segfault on max unique json names overflowing
- [fix] Dissalowed system namespaces deletion/renaming

## Go connector
- [fea] Added support for OpenTelemetry traces. [Details](readme.md#tracing)
- [fea] Added prometheus client-side metrics. [Details](cpp_src/readme.md#prometheus-client-side-go)
- [fix] Fixed bool value parsing/validation in DSL package

## Face
- [fea] Added the Namespace name length limitation
- [fea] Added the UUID Index type
- [fea] Added the notification about doubles in the Query Builder filter
- [fix] Fixed the scroll in the Index table
- [fix] Fixed the empty table in Statistics->Memory
- [fix] Removed the select_functions parameter from the JSON preview of Query Builder
- [fix] Fixed the URL clearing after clicking by opened Namespace tabs
- [fix] Fixed the Memory table layout
- [fix] Fixed the console errors on the replication config page
- [fix] Fixed the column displaying for the Query -> Facet request
- [fix] Fixed the synonyms displaying for the UI view
- [fix] Fixed the console errors on the namespace pin icon clicking 
- [fix] Added the bottom padding to the legend of the Statistics
- [fix] Fixed the NavigationDuplicated console error on the pagination using on the Queries page

# Version 3.15.0 (24.04.2023)
## Core
- [fea] Improved typos handling agorithm for `text`-indexes. New options: `max_typo_distance`,`max_symbol_permutation_distance`,`max_missing_letters` and `max_extra_letters` were added
- [fea] Improved composite indexes substitution. Now optimizer is able to find composite index' parts on almost any positions in query and choose the largest corresponding composite index if multiple substitutions are available
- [fea] Improved sorting by fields from joined-namespaces (for `inner joins`). Added support for string types and improved conversion logic (previously this type of sorting were able to sort only numeric fields)
- [fea] Added more options for `snippet_n`: `with_area`, `left_bound` and `right_bound`. [Details](fulltext.md#snippet_n)
- [fix] Fixed handling of update-queries (with `set`), which tried to update non-existing array element. Now those queries will return error and will not change target array-content

## Reindexer server
- [fea] Added support for non-string map keys for msgpack decoded (positive and negative integer values may be used as tag names in msgpack messages now) 

## Go connector
- [fea] Added `OptionConnPoolLoadBalancing`. This options allows to chose load balancing algorithm for cproto connections

## Face
- [fea] Automized filling the key field on the Meta creation page
- [fea] Added new fields to the full-text config
- [fea] Improved the layout of the Namespace renaming window 
- [fea] Replaced the load more feature with the pagination one on the Connection and Queries pages
- [fea] Changed the "Close" button title with "Save changes" on the namespace page
- [fix] Fixed a few small issues on the Statistics page
- [fix] Fixed the onboarding pointer in the case of undefined namespace
- [fix] Fixed the namespace choosing on the page reloading on the Indexes and Meta tabs
- [fix] Improved UI for long values in the Add item window
- [fix] Fixed the layout of the error message on the Query page
- [fix] Fixed the notification about necessity of the statistics activation on the Memory page
- [fix] Fixed the index value with precept 
- [fix] Fixed the console issue during the namespace switching
- [fix] Fixed the wrong namespace redirecting on the Horizontal bar and Pie chart in Statistics 
- [fix] Changed the column title on the Performance page
- [fix] Fixed the spelling issues
- [fix] Fixed the Add Database action
- [fix] Fixed grammar in the "Query performance statistics is disabled" popup
- [fix] Fixed the database list layout when the list is big
- [fix] Changed the design of the tooltip appeared on the "Collapse the sidebar" icon
- [fix] Fixed the NavigationDuplicate console issue
- [fix] Fixed the issue with the Gear button on the Statistics -> memory page

# Version 3.14.2 (05.04.2023)
## Core
- [fix] Reduced preselect limit for joined queries (previous values from v3.14.0 could cause some performance issues)

# Version 3.14.1 (31.03.2023)
## Core
- [fix] Fixed cost calculation for 'between fields' comparators
- [fix] Improved namespaces loading order

## Reindexer server
- [fix] Fixed server connections drops after outdated Close() call from RPC-client
- [fix] Fixed 'SetSchema' RPC-call logging
- [fix] Enlarged stop attemtps before SIGKILL for reindexer service

## Go connector
- [fix] Fixed client connections drops after some of the queries time outs (CPROTO)

## Face
- [fix] Fixed minor issues with queriesperfstats and explain
- [fix] Fixed the visible field set storing for the same SQL queries
- [fix] Restored the Load more feature to the Connections page

# Version 3.14.0 (18.03.2023)
## Core
- [fea] Reworked fulltext search for multiword phrases in quotes (check description [here](fulltext.md#phrase-search))
- [fea] Optimized database/server termination
- [fea] Improved and optimized suffix tree selection for terms from stemmers in fulltext index
- [fea] Optimized select queries with limit/offset and ordered index in conditions, but without explicit sort orders
- [fea] Changed weights for ordered queries with limit/offset and ordered index in `where` or `order by` sections
- [fea] Improved performance for large joined namespaces
- [fea] Improved selection plan for non-index fields and optimized non-index field filtration
- [fea] Added [snippet_n](fulltext.md#snippet_n) function for fulltext highlighting. It will get more functionality later on
- [fix] Fixed aggregation results for empty namespaces and non-existing fields

## Reindexer server
- [fea] Optimized aggregations results sending. From now all the aggregations results will be sent only once

## Go connector
- [fea] Changed aggregations interface to support optional aggregations results

## Face
- [fea] Added the information about supported browsers
- [fix] Fixed the column settings for the Statistics table
- [fix] Improved the Statistics UI
- [fix] Fixed the SQL -> truncate response
- [fix] Fixed the infinity requests to namespases on the Config page
- [fix] Fixed the validator of the tag field
- [fix] Fixed the error on the Explain page
- [fix] Fixed issues with the Position boost
- [fix] Replaced the step from 0.001 to 0.05 for input fields
- [fix] Fixed the sorting pointer icon on the Queries table

# Version 3.13.2 (23.02.2023)
## Core
- [fix] Fixed collisions resolving in indexes hash map (worst case scenarious were significantly improved)
- [fix] Fixed errors handling for custom indexes sort orderes in case of incorrect sort_orders config

## Go connector
- [fix] Enlarged default fetch limit for cproto binding

## Reindexer tool
- [fix] Fixed large join requests execution (with 10k+ documents in result)
- [fix] Fixed 'pretty' output mode (now reindexer_tool will not stuck on formating after request)

## Build/Deploy
- [fea] Switched Fedora's versions for prebuilt packages to Fedora 36/37

## Face
- [fea] Added the bulk deleting items on the list
- [fea] Chnaged the lifetime of the editing form for the Item adding operation
- [fea] Replaced the Create new database label to the Choose a database in the main menu
- [fea] Forbade entering cyrillic symbols for DB and NS titles
- [fea] Added the ability to rollback to the default DB config
- [fea]  Improved the filtering on the Namespace page
- [fea] Replaced the empty page with the inform message on the Meta page
- [fea] Disabled the Create NS button if none DB exists
- [fea] Added the ability to export the SQL result in CSV
- [fea] Added the settings of the slow requests logging to the NS config
- [fea] Fixed the ability to change the Item limit on the page if it exceeds the item total
- [fix] Fixed collapsing/expanding details for the list items after the page changing
- [fix] Changed the double quotes with the quotes for strings in json-view-tree
- [fix] Fixed the Index list displaying after the DB removing
- [fix] Fixed the filtered list of Namespaces on the Memory page
- [fix] Fixed the item list after removing of all items
- [fix] Added the redirect from a selected namespace to the index page during the DB changing
- [fix] Fixed boolean values displaying in the Grid view 
- [fix] Fixed the displaying of the empty and fact result at the same time
- [fix] Fixed the redirect to the Explain page during loading new items on the List and Grid list on the QUERY -> SQL page
- [fix] Fixed the error appeared on the list resizing on the Query Builder page

# Version 3.13.1 (19.01.2023)
## Core
- [fix] Fixed wrong sort order for unbuilt tree-indexes combined with unordered conditions (SET/IN/ALLSET)
- [fix] Fixed assertion for sparse indexes with multiple JSON-paths
- [fix] Fixed assertion for wrong CJSON tags in storage
- [fix] Fixed double values restrictions for JSON-documents (previously in some cases negative double values could cause JSON parsing exceptions)

## Go connector
- [fix] Fixed Query.Set method with arrays of objects and maps (now it calls SetObject in those cases)

# Version 3.13.0 (12.01.2023)
## Core
- [fix] Fixed composit indexes fields update after new indexes addition
- [fix] Fixed SEGFAULT on string precepts

## Go connector
- [ref] Added version postfix to the modules' name (.../reindexer -> .../reindexer/v3)

## Reindexer server
- [fix] Allowed SET condition with 0 arguments in DSL query

# Version 3.12.0 (23.12.2022)
## Core
- [fea] Added logging for slow queries and transactions (check `profiling.long_queries_logging` section in `#config`-namespace)

## Reindexer server
- [fix] Fixed connections balancing and threads creation in default (shared) mode
- [fix] Fixed cleanup for balancing mode for reused connections

## Face
- [fea] Renewed the Onboarding UI
- [fea] Added the data-test attribute
- [fix] Fixed console errors appeared on hover for the Client cell in the Current Statistics
- [fix] Fixed the column width resizing on the page reloading
- [fix] Fixed disapiaring of the Item table part on the Namespace page

# Version 3.11.0 (02.12.2022)
## Core
- [fea] Added background TCMalloc's cache sizes managment (this feature was also supported for Go-builtin, Go-builtinserver and standalone modes). Check `allocator-cache-*` section in server's config
- [fix] Fixed handling of the `no space left` errors for asynchronous storage
- [fix] Fixed and improved composite indexes substitution optimization in select-queries
- [fix] Fixed SEGFAULT on large merge limit values in fulltest indexes

## Reindexer server
- [fea] Added more info about the storages' states into prometheus metrcis
- [fix] Fixed precepts parsing in HTTP API

## Face
- [fea] Replaced the Scrolly component with the Vue-scroll one
- [fea] Added strings_waiting_to_be_deleted_size to Statistics -> Memory for NC
- [fea] Added tooltips to the action bar on the Namespace page
- [fea] Added a default value for Rtree type
- [fea] Added tooltips to the sidbar buttons
- [fea] Made visible the default options of namespaces
- [fea] Changed the Disabled view of the Resent button on the SQL page
- [fix] Fixed sending NULL value for max_preselect_part 
- [fix] Removed the search bar from the Indexes page
- [fix] Fixed the title of the index editor
- [fix] Removed oldated libraries usage
- [fix] Fixed the Expand/Collapse actions for lists
- [fix] Fixed the Collapse all button on the Namespace page
- [fix] Fixed the Statistics menu pointer 

# Version 3.10.1 (17.11.2022)
## Go connector
- [fea] Add go.mod file with dependecies versions
- [ref] Cproto binding now requires explicit import of the `_ "github.com/restream/reindexer/bindings/cproto"`-module

## Repo
- [ref] Move benchmarks into separate [repository](https://github.com/Restream/reindexer-benchmarks)

# Version 3.10.0 (28.10.2022)
## Core
- [fea] Improve conditions injection from joined queries into main query. This optimization now covers much wider class of queries
- [fea] Add parenthesis expansion for queries' conditions
- [fea] Switch yaml parser to more functional one
- [fix] Fix priority of `NOT` operations in queries (now it always has highest priority among all the others logical operations)
- [fix] Fix race condition in indexes optimization logic

## Reindexer server
- [fea] Add support for separate connections threads by request
- [fea] Add support for large JSON-strings (larger than 16 MB)
- [fea] Add HTTP method to force drop of TCMalloc caches
- [fix] Fix SIGABORT after incorrect RPC query results' reuse

## Face
- [fea] Make the "Gear" button visible for tables
- [fea] Redesign the Statistics->Memory page
- [fea] Redesign the Statistics->Performance page
- [fea] Add the Namespace settings button to the Namespace list
- [fea] Add the ability to pin Namespaces in the Namespace list
- [fea] Redesign the feature of the column resizing
- [fix] Fix UI availability with DB config issues
- [fix] Fix the issue with Scrolly on the Statistics page

# Version 3.9.1 (12.10.2022)
## Core
- [fix] Fix SEGFAULT in fulltext index after documents deletion

# Version 3.9.0 (06.10.2022)
## Core
- [fea] Add `enable_preselect_before_ft` option for fulltext indexes. It allows to prioritize non-fultext indexes in query
- [fix] Fix runtime index convertations (from `sparse` and to `sparse`)
- [fix] Fix fulltext index memstats race
- [fix] Fix query results overflow for large joined requests (in cases, when joined items count is more than 10 billions)

## Go connector
- [fix] Fix rare query results leak after request timeout
- [fix] Fix RPC connections leak after reconnect

## Face
- [fea] Add the `enable_preselect_before_ft` option to the fulltext indexconfig
- [fea] Improve snackbars view
- [fea] Add the pagination instead of the 'load more' feature
- [fea] Improve the Precepts UI
- [fea] Add a message about ofline status on 500 server response
- [fea] Add the description of the 5хх codes to the message body
- [fix] Fix a misprint in the database config
- [fix] Fix the value array clearing

# Version 3.8.0 (25.08.2022)
## Core
- [fea] Optimize selects with large id sets
- [fea] Optimize max index structures resize time on documents insertion/deletion/modification
- [fea] Add max highlighted areas setting for fulltext indexes (`max_areas_in_doc` and `max_total_areas_to_cache`)
- [fix] Add some of the missing memory into #memstats

## Reindexer server
- [fea] Reduce allocations count for query result responses
- [fix] Fix assertion in RPC query results container
- [fix] Fix assertion on response buffer overflow
- [fix] Fix SEGFAULT in Go/JSON queries to WAL
- [fix] Server will clear unknown flags in cproto responses

## Face
- [fea] Increase cache lifetime for a number of resources
- [fea] Add `strict_mode` to config 
- [fea] Add `sync_storage_flush_limit` to config
- [fea] Add new ft-index fields (`max_areas_in_doc`, `max_total_areas_to_cache`, `optimization`)
- [fea] Add new fields in memory statistics (`index_optimizer_memory`,  `tracked_updates_size`)
- [fea] Add server unavailability indicator 
- [fea] Add parsing error messages from responses with 500 status code (for error log)
- [fix] Fix uptime indicator behavior
- [fix] Fix labels and table filters views in the Statistics tab

# Version 3.7.0 (05.08.2022)
## Core
- [fea] Optimize fulltext index
- [fea] Add optimization mode for fulltext index (Memory or CPU. Check ft index configs)
- [fea] Optimize unordered facets (ordering was disabled by default)
- [fea] Optimize storage reading on startup (multithread storage reading)
- [fea] Optimize storage flush (write locks will not affect storage flush anymore)
- [fix] Fix conflict in idset and composite index merge optimizations

## Face
- [fix] Fix the NavigationDuplicated issue
- [fix] Fix the limit in the request of the items on the SQL page

# Version 3.6.2 (same as 3.6.1) (30.06.2022)
## Reindexer server
- [fix] Fix timed out query results reuse
- [fix] Fix exception handling on connection drop

# Version 3.6.0 (29.06.2022)
## Core
- [fea] Add numeric fields names support in sort expressions
- [fea] Add optional counting mode for index updates tracker
- [fix] Fix temporary namespaces bloat by replicator in case of namespace creation error
- [fix] Fix timeout and strict mode logic in transaction queries
- [fix] Fix ALLSET for non-index fields
- [fix] Fix forced sort parsing/encoding

## Reindexer server
- [fea] Add idle timeout for inactive query results
- [fea] Add fields filter support for joined queries in queries DSL

## Go connector
- [fix] Fix cproto `Status()` update, when remote server becomes available

## Reindexer tool
- [fea] Add line numbers output in case of errors in dump handling (while performing restoration)

## Face
- [fea] Add new values to the condition list on the Query Builder page
- [fea] Add the Return back button to the second level of the Statistic table
- [fea] Add the Loader icon for heavy requests
- [fea] Change the full-text config
- [fea] Add the logging of the error messages
- [fea] Add the action disabling for the Add button in the JSON editor when an syntax error is detected
- [fix] Fix filters on the Query builder page
- [fix] Fix the pagination issue on the SQL page
- [fix] Fix the Namespace settings button position during the browser window resizing
- [fix] Fix the NavigationDuplicated issue
- [fix] Fix the auto scrolling page up during the infinity scrolling of the data table
- [fix] Fix the cursor position during the index creating
- [fix] Fix the incorrect redirect to the Explain tab from the List/Grid view on the SQL page

# Version 3.5.2 (06.05.2022)
## Core
- [fix] Fix sigabort after querie's filters merging
- [fix] Fix crash query report for joined queries
- [fix] Fix SQL parsing for numeric field names (in update-queries and in filter conditions)

## Face
- [fea] Add the 'More' button for the long text fields on the List view
- [fix] Exclude the es5-ext library

# Version 3.5.0 (14.04.2022)
## Core
- [fea] Optimize composite fulltext indexes rebuilding. Composite ft index will not be rebuild after update, if non of the index's parts were actually changed
- [fea] Add current query to backtrace for builtin/builtinserver modes
- [fix] Fix string copying for array indexes
- [fix] Fix ALLSET keyword parsing for SQL

## Face
- [fea] Improve `explain` representation for queries with joins
- [fea] Add 'copy' button for fro json preview in Query Builder
- [fix] Fix floating point numbers' behavior in numeric inputs in 'config' tab
- [fix] Fix 'x' button in 'meta' tab
- [fix] Fix type in 'partitial_match_decrease' field in index config

# Version 3.4.0 (28.03.2022)
## Core
- [fea] Now time in TTL indexes may be changed without index deletion/recreation
- [fea] Improve `equal postion` (add support for OR operator in combinations with `equal position`)

## Reindexer server
- [ref] Change `equal position` format in HTTP Query DSL (*incompatible with old `equal position's` format`*)

## Face
- [fea] Improved the aggregation result UI
- [fix] Fixed scrolling on the Indexes and Meta pages in Firefox

# Version 3.3.3 (24.03.2022)
## Core
- [fix] Fix segfault on max indexes count constraint check
- [fix] Fix segfault on composite indexes replication (in some corner cases)
- [fix] Fix assertion on joins by non-index fields
- [fix] Fix incorrect time counting for left joins in explain

## Build
- [fix] Fix build with clang 13.0

# Version 3.3.2 (25.01.2022)
## Core
- [fix] Fix index cache invalidation after items modification
- [fix] Fix heap-use-after-free in 'delete *' query

## Face
- [fix] Fix the undefined fields for the "List" view
- [fix] Fix the kebab menu for the "List" view

# Version 3.3.1 (13.12.2021)
## Face
- [fea] Replace the list component
- [fix] Fix the issue related to namespace settings
- [fix] Fix the issue related to the "Statistics -> Performance" and "Statistics -> Queries" settings
- [fix] Fix the issue with the "load more" button for the "Aggregations" and "Explain" sections
- [fix] Fix the issue with the empty popup during the index editing

# Version 3.3.0 (29.11.2021)
## Core
- [fea] Allow comparison between fields in queries' filters
- [fea] (*BREAKS COMPATIBILITY*) More strict SQL syntax: composite indexes must be enclosed in double quotes and string literals - in single quotes
- [fix] Fix inner joins for sparse indexes

## Reindexer server
- [fix] Fix memory corruption on item modify, when json had some whitespace symbols at the end

# Version 3.2.6 (24.11.2021)
## Core
- [fix] Fix default behaviour for joins optimization from v3.2.4 and add more settings for it in #config

## Build
- [fix] Fix build for msvc 14.29.30133

## Doc
- [fix] Fix doc for FT-rank

## Face
- [fea] Add settings for inner join optimization to NS config
- [fix] Remove ‘id’ from the ‘Also create primary key index (id)’ label
- [fix] Fix the wrong message during the item editing

# Version 3.2.5 (30.10.2021)
## Build
- [fix] Fix building in directories, containing dots (also fixes homebrew installation)

# Version 3.2.4 (14.10.2021)
## Core
- [fea] Add e2k support
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
- [fix] Removed TSAN suppressions for tests

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
- [fix] Fixed the issue with case sensitive field names during the grid building
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
- [fix] Fix merge limit handling for deleted values in fultext index
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
- [fix] Fixed deadlock in selecter in case of concurrent namespace removing
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
- [fix] Bug in query with condition ALLSET by nonindexed field fixed
- [fix] Bug in query with merge and join by the same namespace fixed
- [fix] Simultaneous update of field and whole object fixed
- [fix] Build on aarch64 architecture fixed
- [fix] Fixed replication updates limit trackig, and possible inifity full namespace sync
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
- [fea] Impoved verification of fulltext index configuration

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
- [fea] Added tooltips to longest query
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
- [fix] Search by multi word synonyms is fixed
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
- [fix] Fix command execution iterrupts on SIGINT
- [fix] Disable replicator for reindexer_tool

## Go connector
- [fix] Fix objects cache size setting
- [fix] Fix Status()-call for cproto-connector

# Version 2.13.0 (18.09.2020)

## Core
- [fea] Added extra parameter to clients stats
- [fea] Added update, delete, truncate statement in DSL
- [fix] Added support for equal_positions in sql suggester
- [fix] Crash on distinct whith composite index
- [fix] Crash on query whith incorrect index type after index conversion

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
- [fix] Enable to create multiple instances of builtinserver
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
- [fea] web static resources are embeded to server binary by default

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
- [fix] Fix unhandled exception while caclulating perf stat

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
- [fix] Fix lsn overflow after convertion to int

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
- [fea] Optimized lock time for joins with small preresult set
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
- [fea] Iterator.NextObj() unmarshals data to any user provided struct

# Version 2.3.2 (25.10.2019)

# Core
- [fix] wrong WAL ring buffer size calculation on load from storage
- [fix] Make storage autorepair optional
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
- [fix] Wrong lowercasing field name on SQL UPDATE query
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
- [fix] Sort optimization choose logic is improoved

## go connector

- [fix] leak seq from chan
- [fix] check ctx.Done while waiting on cgo limiter


# Version 2.2.2 (15.09.2019)

## Core 

- [fea] More effective usage of btree index for GT/LT and sort in concurent read write operations
- [fix] Potential crash on index update or deletion
- [fea] Timeout of background indexes optimization can be changed from #config namespace

## Reindexer server

- [fea] User list moved from users.json to users.yml
- [fea] Hash is used insead of plain password in users.yml file
- [fix] Pass operation timeout from cproto client to core

# Version 2.2.1 (07.09.2019)

## Core 

- [fea] Updated behaviour of Or InnerJoin statement
- [fea] Store backups of system records in storage
- [fix] Replicator can start before db initalization completed
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
- [fix] Race on concurent read from system namespaces
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

- [fea] Added two way sync of replication config and namespace
- [fea] Memory usage of indexes decreased (tsl::sparesmap has been added)
- [fea] Added non-normalized query in queries stats
- [fea] Add truncate namespace function
- [fix] Fixed unexpected hang and huge memory alloc on select by uncommited indexes
- [fix] Correct usage of '*' entry as default in namespaces config
- [fix] Memory statistics calculation are improoved
- [fix] Slave will not try to clear expired by ttl records

# Version 2.1.2 (04.08.2019)

## Core

- [fea] Added requests execution timeouts and cancelation contexts
- [fea] Join memory consumption optimization
- [fea] Current database activity statistics
- [fea] Use composite indexes for IN condition to index's fields
- [fea] Reset perfomance and queries statistics by write to corresponding namespace
- [fix] Crashes on index removal
- [fix] Do not lock namespace on tx operations
- [fix] SQL dumper will not add exceeded bracets
- [fea] Added `updated_at` field to namespace attributes


# go connector

- [fea] Added requests execution timeouts and cancelation contexts
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

- [fix] Build builinserver with libunwind conflict fixed
- [fix] Query.Update panic fixed
- [fix] Stronger check of namespace's item's type (PkgPath is included to check)

# Version 2.1.0 (08.06.2019)

## Core

- [fea] Bracets in DSL & SQL queries
- [fix] Crash on LRUCache fast invalidation
- [fix] Relaxed JSON validation. Symbols with codes < 0x20 now are valid
- [fix] '\0' symbol in JSON will not broke parser
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
- [fea] Facets API improoved. Multiply fields and SORT features
- [fea] TTL added
- [fea] `LIKE` condition added 
- [fea] Add expressions support in SQL `UPDATE` statement
- [fix] Invalid JSON generation with empty object name
- [fix] Unneccessary updating of tagsmatcher on transactions
- [fix] LRUCache invalidation crash fix

# Reindexer server

- [fea] Added metadata maniplulation methods

## Face

- [fea] Added metadata maniplulation GUI
- [fix] Performance statistics GUI improovements

# Version 2.0.2 (08.03.2019)

## Core 
- [fea] Update fields of documents, with SQL `UPDATE` statement support
- [fea] Add SQL query suggestions
- [fea] Add `DISTINCT` support to SQL query
- [fea] Queries to non nullable indexes with NULL condition will return error
- [fix] Fixes of full text search, raised on incremental index build
- [fix] Queries with forced sort order can return wrong sequences
- [fix] RPC client&replicator multithread races
- [fix] DISTINCT condition to store indexes
- [fix] Caches crash on too fast data invalidation
- [fix] Disable execiton of delete query from namespace in slave mode
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

- [fix] Struct verificator incorrect validation of composite `reindex` tags
- [fea] Pool usage statistics added to `DB.Status()` method

## Reindexer server

- [fea] Added fields `namespaces` and `enable_cache` to GET|POST /db/:db/query method

## Face

- [fea] Query builder added
- [fea] `Delete all` button added to items page
- [fea] Aggregations results view
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
- [fix] Unhandled exception in case trying of create output file in unexisting directory
- [fix] RPC client optimizations and races fixes
- [fea] \bench command added


# Version 1.10.2 (20.11.2018)

## Core

- [fea] Indexes rebuilding now is non blocking background task, concurrent R-W queries performance increased
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
- [fix] Json parser memleak fixed

## Reindexer server

- [fea] REST API documentation improoved
- [fea] Optimized performance

## Reindexer tool

- [fea] Operation speed is improoved


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
- [fea] OpenNamespace now register namespace <-> struct mapping without server connection requiriment
- [fix] int type is now converted to int32/int64 depends on architecture

## Python connector

- [fea] [Python connector](connectors/py_reindexer) v.0.0.1 is released.

## Reindexer server

- [fea] Added fields filter to method GET /api/v1/:db/:namespace:/items mathed
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
- [fea] Concurent R-W queries performance optimization
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
- [fix] Fixed restoring namespaces with index names non equal to json paths

# Version 1.9.6 (03.09.2018)

## Core
- [fea] Merge with Join queries support
- [fea] Sort by multiple columns/indexes
- [fix] Case insensivity for index/namespaces names
- [fix] Sparse indexes behavior fixed
- [fix] Full text index - correct calculatuon of distance between words
- [fix] Race condition on concurent ConfigureIndex requests

## Reindexer server
- [fea] Added modify index method

## Go connector
- [fea] New builtinserver binding: builtin mode for go application + bundled server for external clients
- [fea] Improoved validation of go struct `reindex` tags

# Version 1.9.5 (04.08.2018)

## Core

- [fea] Sparse indexes
- [fix] Fixed errors on conditions to unindexed fields
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

- [fix] Incorrect urlencode for document update API url
- [fix] Namespace view layout updated, jsonPath added to table

## Go connector

- [fix] Fixed error after closing connection by timeout
- [fix] Caches invalidation on server restart

# Version 1.9.4 (25.07.2018)

## Core

- [fea] Conditions to any fields, even not indexed
- [fea] cproto network client added 
- [fix] Query execution plan optimizator fixes. 

## Reindexer tool

- [fea] Command line editor. tool has been mostly rewritten at all
- [fea] Interopertion with standalone server

## Reindexer server

- [fea] Pagination in the GET sqlquery HTTP method
- [fea] Filtration in the GET items HTTP method

## Face

- [fea] Table view of items page
- [fea] Filtration in the items page
- [fea] SQL queries history

# Version 1.9.3 (20.06.2018)

## Core

- [fea] Added system namespaces #memstats #profstats #queriesstats #namespaces with executuin and profiling statistics
- [fea] Added system namespace #config with runtime profiling configuration
- [fix] Join cache memory limitation
- [fix] Fixed bug with cjson parsing in nested objects on delete
- [fix] 32 bit reference counter for records instead of 16 bit
- [fix] Crash on load namespaces from storage with custom sort order
- [fix] Crash on select from composite index, after delete last shared string copy for sub part of composite index
- [fix] Full text build threads limited to 8

## Reindexer Server

- [fea] Load data in multiple threads on startup
- [fea] Auto rebalance connection between worker threads
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
- [fea] Checking for duplicate names in `json` struct's tags on OpenNamespace
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
- [fix] Fix conflict of with leveldb's and reindexer's tcmalloc library

## Reindexer server
- [fea] Added daemonize mode to reindexer_server
- [fix] init.d script fixes
- [fea] Added CPU profiling mode to built-in HTTP pprof server

# Version 1.7.0 (05.04.2018)

## C++ core

- [fea] Support join, marge, aggregations in json DSL & SQL queris
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
- [fea] sysv5 initscript added
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

## Misc

