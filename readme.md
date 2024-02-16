# Reindexer

[![GoDoc](https://godoc.org/github.com/Restream/reindexer?status.svg)](https://godoc.org/github.com/Restream/reindexer)
[![Build Status](https://github.com/Restream/reindexer/workflows/build/badge.svg?branch=master)](https://github.com/Restream/reindexer/actions)
[![Build Status](https://ci.appveyor.com/api/projects/status/yonpih8vx3acaj86?svg=true)](https://ci.appveyor.com/project/olegator77/reindexer)

**Reindexer** is an embeddable, in-memory, document-oriented database with a high-level Query builder interface.

Reindexer's goal is to provide fast search with complex queries. We at Restream weren't happy with Elasticsearch and created Reindexer as a more performant alternative.

The core is written in C++ and the application level API is in Go.

This document describes Go connector and its API. To get information
about reindexer server and HTTP API refer to
[reindexer documentation](cpp_src/readme.md)

# Versions overview

There are two LTS-versions of reindexer available: v3.x.x and v4.x.x.

3.x.x is currently our mainstream branch and 4.x.x (release/4 branch) is beta-version with experimental RAFT-cluster and sharding support.
Storages are compatible between those versions, hovewer, replication configs are totally different. Versions 3 and 4 are geting all the same bugfixes and features (except replication-related ones).

# Table of contents:

- [Features](#features)
  - [Performance](#performance)
  - [Memory Consumption](#memory-consumption)
  - [Full text search](#full-text-search)
  - [Disk Storage](#disk-storage)
  - [Replication](#replication)
  - [Sharding](#sharding)
- [Usage](#usage)
  - [SQL compatible interface](#sql-compatible-interface)
- [Installation](#installation)
  - [Installation for server mode](#installation-for-server-mode)
    - [Official docker image](#official-docker-image)
  - [Installation for embedded mode](#installation-for-embedded-mode)
    - [Prerequirements](#prerequirements)
    - [Get Reindexer using go.mod](#get-reindexer-using-gomod)
    - [Get Reindexer using go.mod and replace](#get-reindexer-using-gomod-and-replace)
    - [Get Reindexer for apps without go.mod (vendoring)](#get-reindexer-for-apps-without-gomod-vendoring)
    - [Get Reindexer using go.mod (vendoring)](#get-reindexer-using-gomod-vendoring)
- [Advanced Usage](#advanced-usage)
  - [Index Types and Their Capabilities](#index-types-and-their-capabilities)
  - [Nested Structs](#nested-structs)
  - [Sort](#sort)
  - [Counting](#counting)
  - [Text pattern search with LIKE condition](#text-pattern-search-with-like-condition)
  - [Update queries](#update-queries)
  - [Transactions and batch update](#transactions-and-batch-update)
    - [Synchronous mode](#synchronous-mode)
    - [Async batch mode](#async-batch-mode)
    - [Transactions commit strategies](#transactions-commit-strategies)
    - [Implementation notes](#implementation-notes)
  - [Join](#join)
    - [Anti-join](#anti-join)
    - [Joinable interface](#joinable-interface)
  - [Subqueries (nested queries)](#subqueries-nested-queries)
  - [Complex Primary Keys and Composite Indexes](#complex-primary-keys-and-composite-indexes)
  - [Aggregations](#aggregations)
  - [Search in array fields with matching array indexes](#search-in-array-fields-with-matching-array-indexes)
  - [Atomic on update functions](#atomic-on-update-functions)
  - [Expire Data from Namespace by Setting TTL](#expire-data-from-namespace-by-setting-ttl)
  - [Direct JSON operations](#direct-json-operations)
    - [Upsert data in JSON format](#upsert-data-in-json-format)
    - [Get Query results in JSON format](#get-query-results-in-json-format)
  - [Using object cache](#using-object-cache)
    - [DeepCopy interface](#deepcopy-interface)
    - [Get shared objects from object cache (USE WITH CAUTION)](#get-shared-objects-from-object-cache-use-with-caution)
    - [Limit size of object cache](#limit-size-of-object-cache)
    - [Geometry](#geometry)
- [Logging, debug, profiling and tracing](#logging-debug-profiling-and-tracing)
  - [Turn on logger](#turn-on-logger)
  - [Slow actions logging](#slow-actions-logging)
  - [Debug queries](#debug-queries)
  - [Custom allocators support](#custom-allocators-support)
  - [Profiling](#profiling)
    - [Heap profiling](#heap-profiling)
    - [CPU profiling](#cpu-profiling)
    - [Known profiling issues](#known-profiling-issues)
  - [Tracing](#tracing)
- [Integration with other program languages](#integration-with-other-program-languages)
  - [Reindexer for python](#pyreindexer-for-python)
  - [Reindexer for java](#reindexer-for-java)
    - [Spring wrapper](#spring-wrapper)
  - [3rd party open source connectors](#3rd-party-open-source-connectors)
    - [PHP](#php)
    - [Rust](#rust)
    - [.NET](#net)
- [Limitations and known issues](#limitations-and-known-issues)
- [Getting help](#getting-help)
- [References](#references)

## Features

Key features:

- Sortable indices
- Aggregation queries
- Indices on array fields
- Complex primary keys
- Composite indices
- Join operations
- Full-text search
- Up to 256 indexes (255 user's index + 1 internal index) for each namespace
- ORM-like query interface
- SQL queries

### Performance

Performance has been our top priority from the start, and we think we managed to get it pretty good. Benchmarks show that Reindexer's performance is on par with a typical key-value database. On a single CPU core, we get:

- up to 500K queries/sec for queries `SELECT * FROM items WHERE id='?'`
- up to 50K queries/sec for queries `SELECT * FROM items WHERE year > 2010 AND name = 'string' AND id IN (....)`
- up to 20K queries/sec for queries `SELECT * FROM items WHERE year > 2010 AND name = 'string' JOIN subitems ON ...`

See benchmarking results and more details in [benchmarking repo](https://github.com/Restream/reindexer-benchmarks)

### Memory Consumption

Reindexer aims to consume as little memory as possible; most queries are processed without memory allocs at all.

To achieve that, several optimizations are employed, both on the C++ and Go level:

- Documents and indices are stored in dense binary C++ structs, so they don't impose any load on Go's garbage collector.

- String duplicates are merged.

- Memory overhead is about 32 bytes per document + ≈4-16 bytes per each search index.

- There is an object cache on the Go level for deserialized documents produced after query execution. Future queries use pre-deserialized documents, which cuts repeated deserialization and allocation costs

- The Query interface uses `sync.Pool` for reusing internal structures and buffers.
  Combining of these techings lets Reindexer execute most of queries without any allocations.

### Full text search

Reindexer has internal full text search engine. Full text search usage documentation and examples are [here](fulltext.md)

### Disk Storage

Reindexer can store documents to and load documents from disk via LevelDB. Documents are written to the storage backend asynchronously by large batches automatically in background.

When a namespace is created, all its documents are stored into RAM, so the queries on these documents run entirely in in-memory mode.

### Replication

Reindexer supports synchronious and asynchronious replication. Check replication documentation [here](replication.md)

### Sharding

Reindexer has some basic support for sharding. Check sharding documentation [here](sharding.md)

## Usage

Here is complete example of basic Reindexer usage:

```go
package main

// Import package
import (
	"fmt"
	"math/rand"

	"github.com/restream/reindexer/v4"
	// choose how the Reindexer binds to the app (in this case "builtin," which means link Reindexer as a static library)
	_ "github.com/restream/reindexer/v4/bindings/builtin"

	// OR use Reindexer as standalone server and connect to it via TCP or unix domain socket (if available).
	// _ "github.com/restream/reindexer/v4/bindings/cproto"

	// OR link Reindexer as static library with bundled server.
	// _ "github.com/restream/reindexer/v4/bindings/builtinserver"
	// "github.com/restream/reindexer/v4/bindings/builtinserver/config"

)

// Define struct with reindex tags
type Item struct {
	ID       int64  `reindex:"id,,pk"`    // 'id' is primary key
	Name     string `reindex:"name"`      // add index by 'name' field
	Articles []int  `reindex:"articles"`  // add index by articles 'articles' array
	Year     int    `reindex:"year,tree"` // add sortable index by 'year' field
}

func main() {
	// Init a database instance and choose the binding (builtin)
	db := reindexer.NewReindex("builtin:///tmp/reindex/testdb")

	// OR - Init a database instance and choose the binding (connect to server via TCP sockets)
	// Database should be created explicitly via reindexer_tool or via WithCreateDBIfMissing option:
	// If server security mode is enabled, then username and password are mandatory
	// db := reindexer.NewReindex("cproto://user:pass@127.0.0.1:6534/testdb", reindexer.WithCreateDBIfMissing())

	// OR - Init a database instance and choose the binding (connect to server via unix domain sockets)
	// Unix domain sockets are available on the unix systems only (socket file has to be explicitly set on the server's side with '--urpcaddr' option)
	// Database should be created explicitly via reindexer_tool or via WithCreateDBIfMissing option:
	// If server security mode is enabled, then username and password are mandatory
	// db := reindexer.NewReindex("ucproto://user:pass@/tmp/reindexer.socket:/testdb", reindexer.WithCreateDBIfMissing())

	// OR - Init a database instance and choose the binding (builtin, with bundled server)
	// serverConfig := config.DefaultServerConfig ()
	// If server security mode is enabled, then username and password are mandatory
	// db := reindexer.NewReindex("builtinserver://user:pass@testdb",reindexer.WithServerConfig(100*time.Second, serverConfig))

	// Create new namespace with name 'items', which will store structs of type 'Item'
	db.OpenNamespace("items", reindexer.DefaultNamespaceOptions(), Item{})

	// Generate dataset
	for i := 0; i < 100000; i++ {
		err := db.Upsert("items", &Item{
			ID:       int64(i),
			Name:     "Vasya",
			Articles: []int{rand.Int() % 100, rand.Int() % 100},
			Year:     2000 + rand.Int()%50,
		})
		if err != nil {
			panic(err)
		}
	}

	// Query a single document
	elem, found := db.Query("items").
		Where("id", reindexer.EQ, 40).
		Get()

	if found {
		item := elem.(*Item)
		fmt.Println("Found document:", *item)
	}

	// Query multiple documents
	query := db.Query("items").
		Sort("year", false).                          // Sort results by 'year' field in ascending order
		WhereString("name", reindexer.EQ, "Vasya").   // 'name' must be 'Vasya'
		WhereInt("year", reindexer.GT, 2020).         // 'year' must be greater than 2020
		WhereInt("articles", reindexer.SET, 6, 1, 8). // 'articles' must contain one of [6,1,8]
		Limit(10).                                    // Return maximum 10 documents
		Offset(0).                                    // from 0 position
		ReqTotal()                                    // Calculate the total count of matching documents

	// Execute the query and return an iterator
	iterator := query.Exec()
	// Iterator must be closed
	defer iterator.Close()

	fmt.Println("Found", iterator.TotalCount(), "total documents, first", iterator.Count(), "documents:")

	// Iterate over results
	for iterator.Next() {
		// Get the next document and cast it to a pointer
		elem := iterator.Object().(*Item)
		fmt.Println(*elem)
	}
	// Check the error
	if err := iterator.Error(); err != nil {
		panic(err)
	}
}
```

There are also some basic samples for C++ and Go [here](samples)

### SQL compatible interface

As alternative to Query builder Reindexer provides SQL compatible query interface. Here is sample of SQL interface usage:

```go
    ...
	iterator := db.ExecSQL ("SELECT * FROM items WHERE name='Vasya' AND year > 2020 AND articles IN (6,1,8) ORDER BY year LIMIT 10")
    ...
```

Please note, that Query builder interface is preferable way: It have more features, and faster than SQL interface

String literals should be enclosed in single quotes.

Composite indexes should be enclosed in double quotes.
```sql
	SELECT * FROM items WHERE "field1+field2" = 'Vasya'
```

If the field name does not start with alpha, '_' or '#' it must be enclosed in double quotes, examples:
```sql
	UPDATE items DROP "123"
```

```sql
	SELECT * FROM ns WHERE "123" = 'some_value'
```

```sql
	SELECT * FROM ns WHERE "123abc" = 123
```

```sql
	DELETE FROM ns WHERE "123abc123" = 111
```

Simple Joins may be done via default SQL syntax:
```sql
SELECT * FROM ns INNER JOIN ns2 ON ns2.id = ns.fk_id WHERE a > 0
```

Joins with condition on the left namespace must use subquery-like syntax:
```sql
SELECT * FROM ns WHERE a > 0 AND INNER JOIN (SELECT * FROM ns2 WHERE b > 10 AND c = 1) ON ns2.id = ns.fk_id
```

Subquery can also be a part of the WHERE-condition:
```sql
SELECT * FROM ns WHERE (SELECT * FROM ns2 WHERE id < 10 LIMIT 0) IS NOT NULL
```

```sql
SELECT * FROM ns WHERE id = (SELECT id FROM ns2 WHERE id < 10)
```

```sql
SELECT * FROM ns WHERE (SELECT COUNT(*) FROM ns2 WHERE id < 10) > 18
```

## Installation

Reindexer can run in 3 different modes:

- `embedded (builtin)` Reindexer is embedded into application as static library, and does not reuqire separate server proccess.
- `embedded with server (builtinserver)` Reindexer is embedded into application as static library, and start server. In this mode other
  clients can connect to application via cproto, ucproto or http.
- `standalone` Reindexer run as standalone server, application connects to Reindexer via network or unix domain sockets.

### Installation for server mode

In this mode reindexer's Go-binding does not depend on reindexer's static library.

1.  [Install Reindexer Server](cpp_src/readme.md#installation)
2.  go get -a github.com/restream/reindexer/v4

#### Official docker image

The simplest way to get reindexer server, is pulling & run docker image from [dockerhub](https://hub.docker.com/r/reindexer/reindexer/).

```bash
docker run -p9088:9088 -p6534:6534 -it reindexer/reindexer
```

[Dockerfile](cpp_src/cmd/reindexer_server/contrib/Dockerfile)

### Installation for embedded mode

#### Prerequirements

Reindexer's core is written in C++17 and uses LevelDB as the storage backend, so the Cmake, C++17 toolchain and LevelDB must be installed before installing Reindexer.

To build Reindexer, g++ 8+, clang 7+ or [mingw64](https://sourceforge.net/projects/mingw-w64/) is required.

In those modes reindexer's Go-binding depends on reindexer's static libraries (core, server and resource).

#### Get Reindexer using go.mod

This way is recommended and will fit for the most scenarios.

Go modules with go.mod do not allow to build C++ libraries in modules' directories. Go-binding will use [pkg-config](https://pkg.go.dev/github.com/rjeczalik/pkgconfig/cmd/pkg-config) to detect libraries' directories.

Reindexer's libraries must be either installed from [sources](cpp_src/readme.md#installation-from-sources) or from [prebuilt package via package manager](cpp_src/readme.md#linux).

Then get the module:

```bash
go get -a github.com/restream/reindexer/v4
```

#### Get Reindexer using go.mod and replace

If you need modified reindexer's sources, you can use `replace` like that.

1. Download and build reindexer:
```bash
# Clone reindexer via git. It's also possible to use 'go get -a github.com/restream/reindexer/v4', but it's behavior may vary depending on Go's version
git clone --branch release/4 https://github.com/restream/reindexer.git $GOPATH/src/reindexer
bash $GOPATH/src/reindexer/dependencies.sh
# Generate builtin binding
cd $GOPATH/src/reindexer
go generate ./bindings/builtin
# Optional (build builtin server binding)
go generate ./bindings/builtinserver
```

2. Add reindexer's module into your application's go.mod and the replace it with local package:
```bash
# Go to your app's directory
cd /your/app/path
go get -a github.com/restream/reindexer/v4
go mod edit -replace github.com/restream/reindexer/v4=$GOPATH/src/reindexer
```

In this case, Go-binding will generate explicit libraries' and paths' list and will not use pkg-config.

#### Get Reindexer for apps without go.mod (vendoring)

If you're not using go.mod it's possible to get and build reindexer from sources this way:

```bash
export GO111MODULE=off # Disable go1.11 modules
# Go to your app's directory
cd /your/app/path
# Clone reindexer via git. It's also possible to use 'go get -a github.com/restream/reindexer', but it's behavior may vary depending on Go's version
git clone --branch release/4 https://github.com/restream/reindexer.git vendor/github.com/restream/reindexer/v4
# Generate builtin binding
go generate -x ./vendor/github.com/restream/reindexer/v4/bindings/builtin
# Optional (build builtin server binding)
go generate -x ./vendor/github.com/restream/reindexer/v4/bindings/builtinserver
```

#### Get Reindexer using go.mod (vendoring)

Go does not support proper vendoring for CGO code (https://github.com/golang/go/issues/26366), however, it's possible to use [vend](https://github.com/nomad-software/vend) to copy reindexer's sources into vendor-directory.

With `vend` you'll be able to call `go generate -mod=vendor` for `builtin` and `builtinserver`, placed in your vendor-directory.

It's also possible to copy simply copy reindexer's sources into yout project, using `git clone`.

In this cases all the dependecies from reindexer's [go.mod](go.mod) must be installed manually with proper versions.

## Advanced Usage

### Index Types and Their Capabilities

Internally, structs are split into two parts:

- indexed fields, marked with `reindex` struct tag
- tuple of non-indexed fields

Queries are possible only on the indexed fields, marked with `reindex` tag. The `reindex` tag contains the index name, type, and additional options:

`reindex:"<name>[[,<type>],<opts>]"`

- `name` – index name.
- `type` – index type:
  - `hash` – fast select by EQ and SET match. Used by default. Allows _slow_ and inefficient sorting by field.
  - `tree` – fast select by RANGE, GT, and LT matches. A bit slower for EQ and SET matches than `hash` index. Allows fast sorting results by field.
  - `text` – full text search index. Usage details of full text search is described [here](fulltext.md)
  - `-` – column index. Can't perform fast select because it's implemented with full-scan technic. Has the smallest memory overhead.
  - `ttl` - TTL index that works only with int64 fields. These indexes are quite convenient for representation of date fields (stored as UNIX timestamps) that expire after specified amount of seconds.
  - `rtree` - available only DWITHIN match. Acceptable only for `[2]float64` (or `reindexer.Point`) field type. For details see [geometry subsection](#geometry).
- `opts` – additional index options:
  - `pk` – field is part of a primary key. Struct must have at least 1 field tagged with `pk`
  - `composite` – create composite index. The field type must be an empty struct: `struct{}`.
  - `joined` – field is a recipient for join. The field type must be `[]*SubitemType`.
  - `dense` - reduce index size. For `hash` and `tree` it will save 8 bytes per unique key value. For `-` it will save 4-8 bytes per each element. Useful for indexes with high selectivity, but for `tree` and `hash` indexes with low selectivity can seriously decrease update performance. Also `dense` will slow down wide fullscan queries on `-` indexes, due to lack of CPU cache optimization.
  - `sparse` - Row (document) contains a value of Sparse index only in case if it's set on purpose - there are no empty (or default) records of this type of indexes in the row (document). It allows to save RAM but it will cost you performance - it works a bit slower than regular indexes.
  - `collate_numeric` - create string index that provides values order in numeric sequence. The field type must be a string.
  - `collate_ascii` - create case-insensitive string index works with ASCII. The field type must be a string.
  - `collate_utf8` - create case-insensitive string index works with UTF8. The field type must be a string.
  - `collate_custom=<ORDER>` - create custom order string index. The field type must be a string. `<ORDER>` is sequence of letters, which defines sort order.
  - `linear`, `quadratic`, `greene` or `rstar` - specify algorithm for construction of `rtree` index (by default `rstar`). For details see [geometry subsection](#geometry).
  - `uuid` - store this value as UUID. This is much more effective from the RAM/network consumation standpoint for UUIDs, than strings. Only `hash` and `-` index types are supported for UUIDs. Can be used with any UUID variant, except variant 0

Fields with regular indexes are not nullable. Condition `is NULL` is supported only by `sparse` and `array` indexes.

### Nested Structs

By default Reindexer scans all nested structs and adds their fields to the namespace (as well as indexes specified).

```go
type Actor struct {
	Name string `reindex:"actor_name"`
}

type BaseItem struct {
	ID        int64  `reindex:"id,hash,pk"`
	UUIDValue string `reindex:"uuid_value,hash,uuid"`
}

type ComplexItem struct {
	BaseItem         // Index fields of BaseItem will be added to reindex
	Actor    []Actor // Index fields of Actor will be added to reindex as arrays
	Name     string  `reindex:"name"`      // Hash-index for "name"
	Year     int     `reindex:"year,tree"` // Tree-index for "year"
	Value    int     `reindex:"value,-"`   // Store(column)-index for "value"
	Metainfo int     `json:"-"`            // Field "MetaInfo" will not be stored in reindexer
	Parent   *Item   `reindex:"-"`         // Index fields of parent will NOT be added to reindex
}
```

### Sort

Reindexer can sort documents by fields (including nested and fields of the joined namespaces) or by expressions in ascending or descending order.

To sort by non-index fields all the values must be convertible to each other, i.e. either have the same types or be one of th numeric types (`bool`, `int`, `int64` or `float`).

Sort expressions can contain:
 - fields and indexes names (including nested fields and fields of the joined namespaces) of `bool`, `int`, `int64`, `float` or `string` types. All the values must be convertible to numbers ignoring leading and finishing spaces;
 - numbers;
 - functions `rank()`, `abs()` and `ST_Distance()`;
 - parenthesis;
 - arithmetic operations: `+`, `-` (unary and binary), `*` and `/`.

If field name followed by `+` they must be separated by space to distinguish composite index name.
Fields of the joined namespaces must be written like this: `joined_namespace.field`.

`Abs()` means absolute value of an argument.

`Rank()` means fulltext rank of match and is applicable only in fulltext query.

`ST_Distance()` means distance between geometry points (see [geometry subsection](#geometry)). The points could be columns in current or joined namespaces or fixed point in format `ST_GeomFromText('point(1 -3)')`

In SQL query sort expression must be quoted.

```go
type Person struct {
	Name string `reindex:"name"`
	Age  int    `reindex:"age"`
}

type City struct {
	Id                 int               `reindex:"id"`
	NumberOfPopulation int               `reindex:"population"`
	Center             reindexer.Point `reindex:"center,rtree,linear"`
}

type Actor struct {
	ID          int               `reindex:"id"`
	PersonData  Person            `reindex:"person"`
	Price       int               `reindex:"price"`
	Description string            `reindex:"description,text"`
	BirthPlace  int               `reindex:"birth_place_id"`
	Location    reindexer.Point `reindex:"location,rtree,greene"`
}
....

query := db.Query("actors").Sort("id", true)           // Sort by field
....
query = db.Query("actors").Sort("person.age", true)   // Sort by nested field
....
// Sort by joined field
// Works for inner join only, when each item from left namespace has exactly one joined item from right namespace
query = db.Query("actors").
	InnerJoin(db.Query("cities")).On("birth_place_id", reindexer.EQ, "id").
	Sort("cities.population", true)
....
// Sort by expression:
query = db.Query("actors").Sort("person.age / -10 + price / 1000 * (id - 5)", true)
....
query = db.Query("actors").Where("description", reindexer.EQ, "ququ").
    Sort("rank() + id / 100", true)   // Sort with fulltext rank
....
// Sort by geometry distance
query = db.Query("actors").
    Join(db.Query("cities")).On("birth_place_id", reindexer.EQ, "id").
    SortStPointDistance(cities.center, reindexer.Point{1.0, -3.0}, true).
    SortStFieldDistance("location", "cities.center", true)
....
// In SQL query:
iterator := db.ExecSQL ("SELECT * FROM actors ORDER BY person.name ASC")
....
iterator := db.ExecSQL ("SELECT * FROM actors WHERE description = 'ququ' ORDER BY 'rank() + id / 100' DESC")
....
iterator := db.ExecSQL ("SELECT * FROM actors ORDER BY 'ST_Distance(location, ST_GeomFromText(\'point(1 -3)\'))' ASC")
....
iterator := db.ExecSQL ("SELECT * FROM actors ORDER BY 'ST_Distance(location, cities.center)' ASC")
```

It is also possible to set a custom sort order like this

```go
type SortModeCustomItem struct {
	ID      int    `reindex:"id,,pk"`
	InsItem string `reindex:"item_custom,hash,collate_custom=a-zA-Z0-9"`
}
```

or like this

```go
type SortModeCustomItem struct {
	ID      int    `reindex:"id,,pk"`
	InsItem string `reindex:"item_custom,hash,collate_custom=АаБбВвГгДдЕеЖжЗзИиКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЭ-ЯAaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0-9ЁёЙйэ-я"`
}
```

The very first character in this list has the highest priority, priority of the last character is the smallest one. It means that sorting algorithm will put items that start with the first character before others. If some characters are skipped their priorities would have their usual values (according to characters in the list).

### Counting

Reindexer supports 2 versiong of counting aggregration:
- `count()` - this aggregation counts all the documents, which match query's filters;
- `count_cached()` - this allows reindexer to use cached count results. Generally this method is more fast (especially for queries with low `limit()` value), however may return outdated results, if cache was not invalidated after some documents' changes.

Go example for `count()`:
```go
query := db.Query("items").
		WhereString("name", reindexer.EQ, "Vasya").
		Limit(10).
		ReqTotal()
it := query.MustExec()
it.TotalCount() // Get total count of items, matching condition "WhereString("name", reindexer.EQ, "Vasya")"
```

Go example for `count_cached()`:
```go
query := db.Query("items").
		WhereString("name", reindexer.EQ, "Vasya").
		Limit(10).
		CachedTotal()
it := query.MustExec()
it.TotalCount() // Get total count of items, matching condition "WhereString("name", reindexer.EQ, "Vasya"), using queries cache"
```

If you need to get total count field in JSON representation via Go-API, you have to set name for this field (otherwise it won't be serialized to JSON):
```go
query := db.Query("items").
		WhereString("name", reindexer.EQ, "Vasya").
		Limit(10).
		ReqTotal("my_total_count_name")
json, err := query.ExecToJson().FetchAll()
```

### Text pattern search with LIKE condition

For simple searching text pattern in string fields condition `LIKE` can be used. It search strings which match a pattern. In the pattern `_` means any char and `%` means any sequence of chars.

Go example:
```go
	query := db.Query("items").
		Where("field", reindexer.LIKE, "pattern")
```
SQL example:
```sql
	SELECT * FROM items WHERE fields LIKE 'pattern'
```

'me_t' corresponds to 'meet', 'meat', 'melt' and so on
'%tion' corresponds to 'tion', 'condition', 'creation' and so on

*CAUTION*: condition LIKE uses scan method. It can be used for debug purposes or within queries with another good selective conditions.

Generally for full text search with reasonable speed we recommend to use fulltext index.

### Update queries

UPDATE queries are used to modify existing items of a namespace.
There are several kinds of update queries: updating existing fields, adding new fields and dropping existing non-indexed fields.

UPDATE Sql-Syntax

```sql
UPDATE nsName
SET field1 = value1, field2 = value2, ..
WHERE condition;
```

It is also possible to use arithmetic expressions with +, -, /, \* and brackets

```sql
UPDATE NS SET field1 = field2+field3-(field4+5)/2
```

including functions like `now()`, `sec()` and `serial()`. To use expressions from Golang code `SetExpression()` method needs to be called instead of `Set()`.

To make an array-field empty

```sql
UPDATE NS SET arrayfield = [] where id = 100
```

and set it to null

```sql
UPDATE NS SET field = null where id > 100
```

In case of non-indexed fields, setting its value to a value of a different type will replace it completely; in case of indexed fields, it is only possible to convert it from adjacent type (integral types and bool), numeric strings (like "123456") to integral types and back. Setting indexed field to null resets it to a default value.

It is possible to add new fields to existing items

```sql
UPDATE Ns set newField = 'Brand new!' where id > 100
```

and even add a new field by a complex nested path like this

```sql
UPDATE Ns set nested.nested2.nested3.nested4.newField = 'new nested field!' where id > 100
```

will create the following nested objects: nested, nested2, nested3, nested4 and newField as a member of object nested4.

Example of using Update queries in golang code:

```go
db.Query("items").Where("id", reindexer.EQ, 40).Set("field1", value1).Set("field2", value2).Update()
```
- there can be multiple `.Set` expressoins - one for a field. Also, it is possible to combine several `Set...` expressions of different types in one query, like this: `Set().SetExpression().SetObject()...`

Reindexer enables to update and add object fields. Object can be set by either a struct, a map or a byte array (that is a JSON version of object representation).

```go
type ClientData struct {
    Name          string `reindex:"name" json:"name"`
	Age           int    `reindex:"age" json:"age"`
	Address       int    `reindex:"year" json:"year"`
	Occupation    string `reindex:"occupation" json:"occupation"`
	TaxYear       int    `reindex:tax_year json:"tax_year"`
	TaxConsultant string `reindex:tax_consultant json:"tax_consultant"`
}
type Client struct {
	ID      int         `reindex:"id" json:"id"`
	Data    ClientData  `reindex:"client_data" json:"client_data"`
	...
}
clientData := updateClientData(clientId)
db.Query("clients").Where("id", reindexer.EQ, 100).SetObject("client_data", clientData).Update()
```

In this case, `Map` in golang can only work with string as a key. `map[string]interface{}` is a perfect choice.

Updating of object field by Sql statement:

```sql
UPDATE clients SET client_data = {"Name":"John Doe","Age":40,"Address":"Fifth Avenue, Manhattan","Occupation":"Bank Manager","TaxYear":1999,"TaxConsultant":"Jane Smith"} where id = 100;
```

UPDATE Sql-Syntax of queries that drop existing non-indexed fields:

```sql
UPDATE nsName
DROP field1, field2, ..
WHERE condition;
```

```go
db.Query("items").Where("id", reindexer.EQ, 40).Drop("field1").Drop("field2").Update()
```

Reindexer update mechanism enables to modify array fields: to modify a certain item of an existing array or even to replace an entire field.

To update an item subscription operator syntax is used:

```sql
update ns set array[*].prices[0] = 9999 where id = 5
```

where `*` means all items.

To update entire array the following is used:

```sql
update ns set prices = [999, 1999, 2999] where id = 9
```

any non-indexed field can be easily converted to array using this syntax.

Reindexer also allows to update items of object arrays:

```sql
update ns set extra.objects[0] = {"Id":0,"Description":"Updated!"} where id = 9
```

also like this

```golang
db.Query("clients").Where("id", reindexer.EQ, 100).SetObject("extra.objects[0]", updatedValue).Update()
```

To add items to an existing array the following syntax is supported:

```sql
update ns set integer_array = integer_array || [5,6,7,8]
```

and

```sql
update ns set integer_array = [1,2,3,4,5] || integer_array
```

The first one adds elements to the end of `integer_array`, the second one adds 5 items to the front of it. To make this code work in Golang `SetExpression()` should be used instead of `Set()`.

To remove item by index you should do the following:

```sql
update ns drop array[5]
```

### Transactions and batch update

Reindexer supports transactions. Transaction are performs atomic namespace update. There are synchronous and async transaction available. To start transaction method `db.BeginTx()` is used. This method creates transaction object, which provides usual Update/Upsert/Insert/Delete interface for application.
For RPC clients there is transactions count limitation - each connection can't has more than 1024 opened transactions at the same time.

#### Synchronous mode

```go

	// Create new transaction object
	tx, err := db.BeginTx("items");
	if err != nil {
		panic(err)
	}
	// Fill transaction object
	tx.Upsert(&Item{ID: 100})
	tx.Upsert(&Item{ID: 101})
	tx.Query().WhereInt("id", reindexer.EQ, 102).Set("Name", "Petya").Update()
	// Apply transaction
	if err := tx.Commit(); err != nil {
		panic(err)
	}

```

#### Async batch mode

For speed up insertion of bulk records async mode can be used.

```go

	// Create new transaction object
	tx, err := db.BeginTx("items");
	if err != nil {
		panic(err)
	}
	// Prepare transaction object async.
	tx.UpsertAsync(&Item{ID: 100},func(err error) {})
	tx.UpsertAsync(&Item{ID: 100},func(err error) {})
	// Wait for async operations done, and apply transaction.
	if err := tx.Commit(); err != nil {
		panic(err)
	}
```

The second argument of `UpsertAsync` is completion function, which will be called after receiving server response. Also, if any error occurred during prepare process, then `tx.Commit` should
return an error.
So it is enough, to check error returned by `tx.Commit` - to be sure, that all data has been successfully committed or not.

#### Transactions commit strategies

Depending on amount of changes in transaction there are 2 possible Commit strategies:

- Locked atomic update. Reindexer locks namespace and applying all changes under common lock. This mode is used with small amounts of changes.
- Copy & atomic replace. In this mode Reindexer makes namespace's snapshot, applying all changes to this snapshot, and atomically replaces namespace without lock

Data amount for choosing Commit strategy can be choose in namespaces config. Check fields `StartCopyPolicyTxSize`, `CopyPolicyMultiplier` and `TxSizeToAlwaysCopy` in `struct DBNamespacesConfig`([describer.go](describer.go))

#### Implementation notes

1. Transaction object is not thread safe and can't be used from different goroutines;
2. Transaction object holds Reindexer's resources, therefore application should explicitly call Rollback or Commit, otherwise resources will leak;
3. It is safe to call Rollback after Commit;
4. It is possible to call Query from transaction by call `tx.Query("ns").Exec() ...`;
5. Only serializable isolation is available, i.e. each transaction takes exclusive lock over the target namespace until all of the steps of the transaction commited.

### Join

Reindexer can join documents from multiple namespaces into a single result:

```go
type Actor struct {
	ID        int    `reindex:"id"`
	Name      string `reindex:"name"`
	IsVisible bool   `reindex:"is_visible"`
}

type ItemWithJoin struct {
	ID          int      `reindex:"id"`
	Name        string   `reindex:"name"`
	ActorsIDs   []int    `reindex:"actors_ids"`
	ActorsNames []int    `reindex:"actors_names"`
	Actors      []*Actor `reindex:"actors,,joined"`
}
....

query := db.Query("items_with_join").Join(
	db.Query("actors").
		WhereBool("is_visible", reindexer.EQ, true),
	"actors"
).On("actors_ids", reindexer.SET, "id")

it := query.Exec()
```

In this example, Reindexer uses reflection under the hood to create Actor slice and copy Actor struct.

Join query may have from one to several `On` conditions connected with `And` (by default), `Or` or `Not` operators:

```go
query := db.Query("items_with_join").
	Join(
		db.Query("actors").
			WhereBool("is_visible", reindexer.EQ, true),
		"actors").
	On("actors_ids", reindexer.SET, "id").
	Or().
	On("actors_names", reindexer.SET, "name")
```

An `InnerJoin` combines data from two namespaces where there is a match on the joining fields in both namespaces. A `LeftJoin` returns all valid items from the namespaces on the left side of the `LeftJoin` keyword, along with the values from the table on the right side, or nothing if a matching item doesn't exist. `Join` is an alias for `LeftJoin`.

`InnerJoins` can be used as a condition in `Where` clause:

```go
query1 := db.Query("items_with_join").
	WhereInt("id", reindexer.RANGE, []int{0, 100}).
	Or().
	InnerJoin(db.Query("actors").WhereString("name", reindexer.EQ, "ActorName"), "actors").
	On("actors_ids", reindexer.SET, "id").
	Or().
	InnerJoin(db.Query("actors").WhereInt("id", reindexer.RANGE, []int{100, 200}), "actors").
	On("actors_ids", reindexer.SET, "id")

query2 := db.Query("items_with_join").
	WhereInt("id", reindexer.RANGE, []int{0, 100}).
	Or().
	OpenBracket().
		InnerJoin(db.Query("actors").WhereString("name", reindexer.EQ, "ActorName"), "actors").
		On("actors_ids", reindexer.SET, "id").
		InnerJoin(db.Query("actors").WhereInt("id", reindexer.RANGE, []int{100, 200}), "actors").
		On("actors_ids", reindexer.SET, "id").
	CloseBracket()

query3 := db.Query("items_with_join").
	WhereInt("id", reindexer.RANGE, []int{0, 100}).
	Or().
	InnerJoin(db.Query("actors").WhereInt("id", reindexer.RANGE, []int{100, 200}), "actors").
	On("actors_ids", reindexer.SET, "id").
	Limit(0)
```

Note that usually `Or` operator implements short-circuiting for `Where` conditions: if the previous condition is true the next one is not evaluated. But in case of `InnerJoin` it works differently: in `query1` (from the example above) both `InnerJoin` conditions are evaluated despite the result of `WhereInt`.
`Limit(0)` as part of `InnerJoin` (`query3` from the example above) does not join any data - it works like a filter only to verify conditions.

#### Anti-join

Reindexer does not support `ANTI JOIN` SQL construction, hovewer, it supports logical operations with JOINs. In fact `NOT (INNER JOIN ...)` is totally equivalent to the `ANTI JOIN`:
```go
query := db.Query("items_with_join").
	Not().
	OpenBracket(). // Brackets are essential here for NOT to work
		InnerJoin(
		db.Query("actors").
			WhereBool("is_visible", reindexer.EQ, true),
		"actors").
		On("id", reindexer.EQ, "id")
	CloseBracket()
```
```SQL
SELECT * FROM items_with_join
WHERE
	NOT (
		INNER JOIN (
			SELECT * FROM actors WHERE is_visible = true
		) ON items_with_join.id = actors.id
	)
```

#### Joinable interface

To avoid using reflection, `Item` can implement `Joinable` interface. If that implemented, Reindexer uses this instead of the slow reflection-based implementation. This increases overall performance by 10-20%, and reduces the amount of allocations.

```go
// Joinable interface implementation.
// Join adds items from the joined namespace to the `ItemWithJoin` object.
// When calling Joinable interface, additional context variable can be passed to implement extra logic in Join.
func (item *ItemWithJoin) Join(field string, subitems []interface{}, context interface{}) {

	switch field {
	case "actors":
		for _, joinItem := range subitems {
			item.Actors = append(item.Actors, joinItem.(*Actor))
		}
	}
}
```

### Subqueries (nested queries)

A condition could be applied to result of another query (subquery) included into the current query.
The condition may either be on resulting rows of the subquery:

```go
query := db.Query("main_ns").
	WhereQuery(db.Query("second_ns").Select("id").Where("age", reindexer.GE, 18), reindexer.GE, 100)
```
or between a field of main query's namespace and result of the subquery:
```go
query := db.Query("main_ns").
	Where("id", reindexer.EQ, db.Query("second_ns").Select("id").Where("age", reindexer.GE, 18))
```
Result of the subquery may either be a certain field pointed by `Select` method (in this case it must set the single field filter):
```go
query1 := db.Query("main_ns").
	WhereQuery(db.Query("second_ns").Select("id").Where("age", reindexer.GE, 18), reindexer.GE, 100)
query2 := db.Query("main_ns").
	Where("id", reindexer.EQ, db.Query("second_ns").Select("id").Where("age", reindexer.GE, 18))
```
or count of items satisfying to the subquery required by `ReqTotal` or `CachedTotal` methods:
```go
query1 := db.Query("main_ns").
	WhereQuery(db.Query("second_ns").Where("age", reindexer.GE, 18).ReqTotal(), reindexer.GE, 100)
query2 := db.Query("main_ns").
	Where("id", reindexer.EQ, db.Query("second_ns").Where("age", reindexer.GE, 18).CachedTotal())
```
or aggregation:
```go
query1 := db.Query("main_ns").
	WhereQuery(db.Query("second_ns").Where("age", reindexer.GE, 18).AggregateMax("age"), reindexer.GE, 33)
query2 := db.Query("main_ns").
	Where("age", reindexer.GE, db.Query("second_ns").Where("age", reindexer.GE, 18).AggregateAvg("age"))
```
`Min`, `Max`, `Avg`, `Sum`, `Count` and `CountCached` aggregations are allowed only. Subquery can not contain multiple aggregations at the same time.

Subquery can be applied to the same namespace or to the another one.

Subquery can not contain another subquery, join or merge.

If you want to check if at least one of the items is satisfying to the subqueries, you may use `ANY` or `EMPTY` condition:
```go
query1 := db.Query("main_ns").
	WhereQuery(db.Query("second_ns").Where("age", reindexer.GE, 18), reindexer.ANY, nil)
query2 := db.Query("main_ns").
		WhereQuery(db.Query("second_ns").Where("age", reindexer.LE, 18), reindexer.EMPTY, nil)
```

### Complex Primary Keys and Composite Indexes

A Document can have multiple fields as a primary key. To enable this feature add composite index to struct.
Composite index is an index that involves multiple fields, it can be used instead of several separate indexes.

```go
type Item struct {
	ID    int64 `reindex:"id"`     // 'id' is a part of a primary key
	SubID int   `reindex:"sub_id"` // 'sub_id' is a part of a primary key
	// Fields
	//	....
	// Composite index
	_ struct{} `reindex:"id+sub_id,,composite,pk"`
}
```

OR

```go
type Item struct {
	ID       int64 `reindex:"id,-"`         // 'id' is a part of primary key, WITHOUT personal searchable index
	SubID    int   `reindex:"sub_id,-"`     // 'sub_id' is a part of a primary key, WITHOUT a personal searchable index
	SubSubID int   `reindex:"sub_sub_id,-"` // 'sub_sub_id' is a part of a primary key WITHOUT a personal searchable index

	// Fields
	// ....

	// Composite index
	_ struct{} `reindex:"id+sub_id+sub_sub_id,,composite,pk"`
}

```

Also composite indexes are useful for sorting results by multiple fields:

```go
type Item struct {
	ID     int64 `reindex:"id,,pk"`
	Rating int   `reindex:"rating"`
	Year   int   `reindex:"year"`

	// Composite index
	_ struct{} `reindex:"rating+year,tree,composite"`
}

...
	// Sort query results by rating first, then by year
	query := db.Query("items").Sort("rating+year", true)

	// Sort query results by rating first, then by year, and put item where rating == 5 and year == 2010 first
	query := db.Query("items").Sort("rating+year", true,[]interface{}{5,2010})
```

For make query to the composite index, pass []interface{} to `.WhereComposite` function of Query builder:

```go
	// Get results where rating == 5 and year == 2010
	query := db.Query("items").WhereComposite("rating+year", reindexer.EQ,[]interface{}{5,2010})
```

All the fields in regular (non-fulltext) composite index must be indexed. I.e. to be able to create composite index `rating+year`, it is nessesary to create some kind of indexes for both `raiting` and `year` first:

```go
type Item struct {
	ID     int64 `reindex:"id,,pk"`
	Rating int   `reindex:"rating,-"` // this field must be indexed (using index type '-' in this example)
	Year   int   `reindex:"year"`     // this field must be indexed (using index type 'hash' in this example)
	_ struct{} `reindex:"rating+year,tree,composite"`
}
```

### Aggregations

Reindexer allows to retrieve aggregated results. Currently Count, CountCached, Average, Sum, Minimum, Maximum, Facet and
Distinct
aggregations are supported.

- `Count` - get total number of documents that meet the querie's conditions
- `CountCached` - get total number of documents that meet the querie's conditions. Result value will be cached and may
  be reused by the other queries with CountCached aggregation
- `AggregateMax` - get maximum field value
- `AggregateMin` - get minimum field value
- `AggregateSum` - get sum field value
- `AggregateAvg` - get average field value
- `AggregateFacet` - get fields facet value
- `Distinct` - get list of unique values of the field

In order to support aggregation, `Query` has methods `AggregateAvg`, `AggregateSum`, `AggregateMin`, `AggregateMax`
, `AggregateFacet` and `Distinct` those should be called before the `Query` execution: this will ask reindexer to
calculate data aggregations.
Aggregation Facet is applicable to multiple data columns and the result of that could be sorted by any data column or '
count' and cutted off by offset and limit.
In order to support this functionality method `AggregateFacet` returns `AggregationFacetRequest` which has
methods `Sort`, `Limit` and `Offset`.

Queries with MERGE will apply aggregations from the main query to all the merged subqueries. Subqueries can not have
their own aggregations. Available aggregations for MERGE-queries are: Count, CountCached, Sum, Min and Max.

To get aggregation results, `Iterator` has method `AggResults`: it is available after query execution and returns slice
of results.

Example code for aggregate `items` by `price` and `name`

```go

	query := db.Query("items")
	query.AggregateMax("price")
	query.AggregateFacet("name", "price").Sort("name", true).Sort("count", false).Offset(10).Limit(100)
	iterator := query.Exec()
	// Check the error
	if err := iterator.Error(); err != nil {
		panic(err)
	}
	defere iterator.Close()

	aggMaxRes := iterator.AggResults()[0]

	fmt.Printf ("max price = %d", aggMaxRes.Value)

	aggFacetRes := iterator.AggResults()[1]

	fmt.Printf ("'name' 'price' -> count")
	for _, facet := range aggFacetRes.Facets {
		fmt.Printf ("'%s' '%s' -> %d", facet.Values[0], facet.Values[1], facet.Count)
	}

```

```go

	query := db.Query("items")
	query.Distinct("name").Distinct("price")
	iterator := query.Exec()
	// Check the error
	if err := iterator.Error(); err != nil {
		panic(err)
	}
	defere iterator.Close()

	aggResults := iterator.aggResults()

	distNames := aggResults[0]
	fmt.Println ("names:")
	for _, name := range distNames.Distincts {
		fmt.Println(name)
	}

	distPrices := aggResults[1]
	fmt.Println ("prices:")
	for _, price := range distPrices.Distincts {
		fmt.Println(price)
	}
```

### Search in array fields with matching array indexes

Reindexer allows to search data in array fields when matching values have same indexes positions.
For instance, we've got an array of structures:

```go
type Elem struct {
   F1 int `reindex:"f1"`
   F2 int `reindex:"f2"`
}

type A struct {
   Elems []Elem
}
```

Common attempt to search values in this array

```go
db.Query("Namespace").Where("f1",EQ,1).Where("f2",EQ,2)
```

finds all items of array `Elem[]` where `f1` is equal to 1 and `f2` is equal to 2.

`EqualPosition` function allows to search in array fields with equal indexes.
Queries like this:

```go
db.Query("Namespace").Where("f1", reindexer.GE, 5).Where("f2", reindexer.EQ, 100).EqualPosition("f1", "f2")
```

or

```sql
SELECT * FROM Namespace WHERE f1 >= 5 AND f2 = 100 EQUAL_POSITION(f1,f2);
```

will find all the items of array `Elem[]` with `equal` array indexes where `f1` is greater or equal to 5 and `f2` is equal to 100 (for instance, query returned 5 items where only 3rd elements of both arrays have appropriate values).

With complex expressions (expressions with brackets) equal_position() could be within a bracket:

```sql
SELECT * FROM Namespace WHERE (f1 >= 5 AND f2 = 100 EQUAL_POSITION(f1,f2)) OR (f3 = 3 AND f4 < 4 AND f5 = 7 EQUAL_POSITION(f3,f4,f5));
SELECT * FROM Namespace WHERE (f1 >= 5 AND f2 = 100 AND f3 = 3 AND f4 < 4 EQUAL_POSITION(f1,f3) EQUAL_POSITION(f2,f4)) OR (f5 = 3 AND f6 < 4 AND f7 = 7 EQUAL_POSITION(f5,f7));
SELECT * FROM Namespace WHERE f1 >= 5 AND (f2 = 100 AND f3 = 3 AND f4 < 4 EQUAL_POSITION(f2,f3)) AND f5 = 3 AND f6 < 4 EQUAL_POSITION(f1,f5,f6);
```

`equal_position` doesn't work with the following conditions: IS NULL, IS EMPTY and IN(with empty parameter list).

### Atomic on update functions

There are atomic functions, which executes under namespace lock, and therefore guarantees data consistency:

- serial() - sequence of integer, useful for auto-increment keys
- now() - current time stamp, useful for data synchronization. It may have one of the following arguments:  msec, usec, nsec and sec. The “sec” argument is used by default.

These functions can be passed to Upsert/Insert/Update in 3-rd and next arguments.

If these functions are provided, the passed by reference item will be changed to updated value

```go
   // set ID field from serial generator
   db.Insert ("items",&item,"id=serial()")

   // set current timestamp in nanoseconds to updated_at field
   db.Update ("items",&item,"updated_at=now(NSEC)")

   // set current timestamp and ID
   db.Upsert ("items",&item,"updated_at=now(NSEC)","id=serial()")

```

### Expire Data from Namespace by Setting TTL

Data expiration is useful for some classes of information, including machine generated event data, logs, and session information that only need to persist for a limited period of time.

Reindexer makes it possible to set TTL (time to live) for Namespace items. Adding TtlIndex to Namespace automatically removes items after a specified number of seconds.

Ttl indexes work only with int64 fields and store UNIX timestamp data. Items containing ttl index expire after `expire_after` seconds. Example of declaring TtlIndex in Golang:

```go
            type NamespaceExample struct {
                ID   int    `reindex:"id,,pk" json:"id"`
                Date int64  `reindex:"date,ttl,,expire_after=3600" json:"date"`
            }
            ...
            ns.Date = time.Now().Unix()
```

In this case items of namespace NamespaceExample expire in 3600 seconds after NamespaceExample.Date field value (which is UNIX timestamp).

A TTL index supports queries in the same way non-TTL indexes do.

### Direct JSON operations

#### Upsert data in JSON format

If source data is available in JSON format, then it is possible to improve performance of Upsert/Delete operations by directly passing JSON to reindexer. JSON deserialization will be done by C++ code, without extra allocs/deserialization in Go code.

Upsert or Delete functions can process JSON just by passing []byte argument with json

```go
	json := []byte (`{"id":1,"name":"test"}`)
	db.Upsert  ("items",json)
```

It is just faster equivalent of:

```go
	item := &Item{}
	json.Unmarshal ([]byte (`{"id":1,"name":"test"}`),item)
	db.Upsert ("items",item)
```

#### Get Query results in JSON format

In case of requirement to serialize results of Query in JSON format, then it is possible to improve performance by directly obtaining results in JSON format from reindexer. JSON serialization will be done by C++ code, without extra allocs/serialization in Go code.

```go
...
	iterator := db.Query("items").
		Select ("id","name").        // Filter output JSON: Select only "id" and "name" fields of items, another fields will be omitted
		Limit (1).
		ExecToJson ("root_object")   // Name of root object of output JSON

	json,err := iterator.FetchAll()
	// Check the error
	if err != nil {
		panic(err)
	}
	fmt.Printf ("%s\n",string (json))
...
```

This code will print something like:

```json
{ "root_object": [{ "id": 1, "name": "test" }] }
```

### Using object cache

To avoid race conditions, by default object cache is turned off and all objects are allocated and deserialized from reindexer internal format (called `CJSON`) per each query.
The deserialization is uses reflection, so its speed is not optimal (in fact `CJSON` deserialization is ~3-10x faster than `JSON`, and ~1.2x faster than `GOB`), but performance is still seriously limited by reflection overhead.

There are 2 ways to enable object cache:

- Provide DeepCopy interface
- Ask query return shared objects from cache

#### DeepCopy interface

If object is implements DeepCopy interface, then reindexer will turn on object cache and use DeepCopy interface to copy objects from cache to query results. The DeepCopy interface is responsible to
make deep copy of source object.

Here is sample of DeepCopy interface implementation

```go
func (item *Item) DeepCopy () interface {} {
	copyItem := &Item{
		ID: item.ID,
		Name: item.Name,
		Articles: make ([]int,cap (item.Articles),len (item.Articles)),
		Year: item.Year,
	}
	copy (copyItem.Articles,item.Articles)
	return copyItem
}
```

#### Get shared objects from object cache (USE WITH CAUTION)

To speed up queries and do not allocate new objects per each query it is possible ask query return objects directly from object cache. For enable this behavior, call `AllowUnsafe(true)` on `Iterator`.

WARNING: when used `AllowUnsafe(true)` queries returns shared pointers to structs in object cache. Therefore application MUST NOT modify returned objects.

```go
	res, err := db.Query("items").WhereInt ("id",reindexer.EQ,1).Exec().AllowUnsafe(true).FetchAll()
	if err != nil {
		panic (err)
	}

	if len (res) > 1 {
		// item is SHARED pointer to struct in object cache
		item = res[0].(*Item)

		// It's OK - fmt.Printf will not modify item
		fmt.Printf ("%v",item)

		// It's WRONG - can race, and will corrupt data in object cache
		item.Name = "new name"
	}
```

#### Limit size of object cache

By default maximum size of object cache is 256000 items for each namespace. To change maximum size use `ObjCacheSize` method of `NameapaceOptions`, passed
to OpenNamespace. e.g.

```go
	// Set object cache limit to 4096 items
	db.OpenNamespace("items_with_huge_cache", reindexer.DefaultNamespaceOptions().ObjCacheSize(4096), Item{})
```

!This cache should not be used for the namespaces, which were replicated from the other nodes: it may be inconsistant for those replica's namespaces.

### Geometry

The only supported geometry data type is 2D point, which implemented in Golang as `[2]float64` (`reindexer.Point`).

In SQL, a point can be created as `ST_GeomFromText('point(1 -3)')`.

The only supported request for geometry field is to find all points within a distance from a point.
`DWithin(field_name, point, distance)` as on example below.

Corresponding SQL function is `ST_DWithin(field_name, point, distance)`.

RTree index can be created for points. To do so, `rtree` and `linear`, `quadratic`, `greene` or `rstar` tags should be declared. `linear`, `quadratic`, `greene` or `rstar` means which algorithm of RTree construction would be used. Here algorithms are listed in order from optimized for insertion to optimized for search. But it depends on data. Test which is more appropriate for you. Default algorithm is `rstar`.

```go
type Item struct {
	id              int               `reindex:"id,,pk"`
	pointIndexed    reindexer.Point `reindex:"point_indexed,rtree,linear"`
	pointNonIndexed reindexer.Point `json:"point_non_indexed"`
}

query1 := db.Query("items").DWithin("point_indexed", reindexer.Point{-1.0, 1.0}, 4.0)
```

```SQL
SELECT * FROM items WHERE ST_DWithin(point_non_indexed, ST_GeomFromText('point(1 -3.5)'), 5.0);
```

## Logging, debug,  profiling and tracing

### Turn on logger

Reindexer logger can be turned on by `db.SetLogger()` method, just like in this snippet of code:

```go
type Logger struct {
}
func (Logger) Printf(level int, format string, msg ...interface{}) {
	log.Printf(format, msg...)
}
...
	db.SetLogger (Logger{})
```

### Slow actions logging

Reindexer supports logging of slow actions. It can be configured via `profiling.long_queries_logging` section of the `#config` system namespace. The logging of next actions can be configured:

- SELECT queries:
  - `threshold_us (integer)`: The threshold value (in microseconds) for execution of SELECT query. If exceeded, a core-log entry will be made, if `threshold_us` is -1 logging is disabled.
  - `normalized (boolean)`: Output the query in a normalized form.

- UPDATE and  DELETE queries:
  - `threshold_us (integer)`: The threshold value (in microseconds) for execution of UPDATE or DELETE query. If exceeded, a core-log entry will be made, if `threshold_us` is -1 logging is disabled.
  - `normalized (boolean)`: Output the query in a normalized form.

- Transactions:
  - `threshold_us (integer)`: Threshold value (in microseconds) for total transaction commit time, if `threshold_us` is -1 logging by total transaction commit time is disabled.
  - `avg_step_threshold_us (integer)`: Threshold value (in microseconds) for the average step duration time in the transaction. If `avg_step_threshold_us` is -1 logging by average transaction's step duraction is disabled.

### Debug queries

Another useful feature is debug print of processed Queries. To debug print queries details there are 2 methods:

- `db.SetDefaultQueryDebug(namespace string,level int)` - it globally enables print details of all queries by namespace
- `query.Debug(level int)` - print details of query execution
  `level` is level of verbosity:
- `reindexer.INFO` - will print only query conditions
- `reindexer.TRACE` - will print query conditions and execution details with timings

- `query.Explain ()` - calculate and store query execution details.
- `iterator.GetExplainResults ()` - return query execution details

### Custom allocators support

Reindexer has support for [TCMalloc](https://github.com/google/tcmalloc) (which is also a part of [GPerfTools](https://github.com/gperftools/gperftools)) and [JEMalloc](https://github.com/jemalloc/jemalloc) allocators (check `ENABLE_TCMALLOC` and `ENABLE_JEMALLOC` in [CMakeLists.txt](cpp_src/CMakeLists.txt)).

If you have built standalone server from sources available allocators will be detected and used automatically.

In `go:generate` builds and prebuilt packages reindexer has TCMalloc support, however none of TCMalloc libraries will be linked automatically. To force allocator's libraries linkage `LD_PRELOAD` with required library has to be used:

```
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtcmalloc_and_profiler.so ./my_executable
```

Custom allocator may be handy to track memory consumation, profile heap/CPU or to improve general perforamnce.

### Profiling

#### Heap profiling

Because reindexer core is written in C++ all calls to reindexer and their memory consumption are not visible for go profiler. To profile reindexer core there are cgo profiler available. cgo profiler now is part of reindexer, but it can be used with any another cgo code.

Usage of cgo profiler is very similar with usage of [go profiler](https://golang.org/pkg/net/http/pprof/).

1. Add import:

```go
import _ "github.com/restream/reindexer/v4/pprof"
```

2. If your application is not already running an http server, you need to start one. Add "net/http" and "log" to your imports and the following code to your main function:

```go
go func() {
	log.Println(http.ListenAndServe("localhost:6060", nil))
}()
```

3. Run application with environment variable `HEAPPROFILE=/tmp/pprof`
4. Then use the pprof tool to look at the heap profile:

```bash
pprof -symbolize remote http://localhost:6060/debug/cgo/pprof/heap
```

#### CPU profiling

Internal reindexer's profiler is based on gperf_tools library and unable to get CPU profile via Go runtime. However, [go profiler](https://golang.org/pkg/net/http/pprof/) may be used with symbolizer to retrieve C++ CPU usage.

1. Add import:

```go
import _ "net/http/pprof"
```

2. If your application is not already running an http server, you need to start one. Add "net/http" and "log" to your imports and the following code to your main function:

```go
go func() {
	log.Println(http.ListenAndServe("localhost:6060", nil))
}()
```

3. Run application with environment variable `REINDEXER_CGOBACKTRACE=1`
4. Then use the pprof tool to get CPU profile:

```bash
pprof -symbolize remote http://localhost:6060/debug/pprof/profile?seconds=10
```

#### Known profiling issues

Due to internal Golang's specific it's not recommended to try to get CPU and heap profiles simultaneously, because it may cause deadlock inside the profiler.

### Tracing

Go binding for Reindexer comes with optional support for [OpenTelemetry](https://opentelemetry.io/) integration.

To enable generation of OpenTelemetry tracing spans for all exported client side calls (`OpenNamespace`, `Upsert`, etc),
pass `reindexer.WithOpenTelemetry()` option when creating a Reindexer DB instance:

```go
db := reindexer.NewReindex("cproto://user:pass@127.0.0.1:6534/testdb", reindexer.WithOpenTelemetry())
```

All client side calls on the `db` instance will generate OpenTelemetry spans with the name of the performed
operation and information about Reindexer DSN, namespace name (if applicable), etc.

For example, a call like this on the `db` instance above:

```go
db.OpenNamespace("items", reindexer.DefaultNamespaceOptions(), Item{})
```

will generate an OpenTelemetry span with the span name of `Reindexer.OpenNamespace` and with span attributes like this:

- `rx.dsn`: `cproto://user:pass@127.0.0.1:6534/testdb`
- `rx.ns`: `items`

Use [opentelemetry-go](https://opentelemetry.io/docs/instrumentation/go/getting-started/) in your client go application
to export the information externally. For example, as a minumum, you will need to configure OpenTelemetry SDK exporter
to expose the generated spans externally (see the `Getting Started` guide for more information).

## Integration with other program languages

A list of connectors for work with Reindexer via other program languages (TBC later):

### Pyreindexer for Python

Pyreindexer is official connector, and maintained by Reindexer's team. It supports both builtin and standalone modes.
Before installation reindexer-dev (version >= 2.10) should be installed. See [installation instructions](cpp_src/readme.md#Installation) for details.

- *Support modes*: standalone, builtin
- *API Used:* binary ABI, cproto
- *Dependency on reindexer library (reindexer-dev package):* yes

For install run:

```bash
pip3 install pyreindexer
```

URLs:
- https://github.com/Restream/reindexer-py
- https://pypi.org/project/pyreindexer/

Python version >=3.6 is required.

### Reindexer for Java

- *Support modes*: standalone, builtin, builtinserver
- *API Used*: binary ABI, cproto
- *Dependency on reindexer library (reindexer-dev package):* yes, for builtin & builtinserver

Reindexer for java is official connector, and maintained by Reindexer's team. It supports both builtin and standalone modes.
For enable builtin mode support reindexer-dev (version >= 3.1.0) should be installed. See [installation instructions](cpp_src/readme.md#Installation) for details.

For install reindexer to Java or Kotlin project add the following lines to maven project file
```
<dependency>
    <groupId>com.github.restream</groupId>
    <artifactId>rx-connector</artifactId>
    <version>[LATEST_VERSION]</version>
</dependency>
```
URL: https://github.com/Restream/reindexer-java

Note: Java version >= 1.8 is required.

#### Spring wrapper
Spring wrapper for Java-connector: https://github.com/evgeniycheban/spring-data-reindexer

### 3rd party open source connectors
#### PHP
URL: https://github.com/Smolevich/reindexer-client
- *Support modes:* standalone only
- *API Used:* HTTP REST API
- *Dependency on reindexer library (reindexer-dev package):* no
#### Rust
URL: https://github.com/coinrust/reindexer-rs
- *Support modes:* standalone, builtin
- *API Used:* binary ABI, cproto
- *Dependency on reindexer library (reindexer-dev package):* yes
#### .NET
URL: https://github.com/oruchreis/ReindexerNet
- *Support modes:* builtin
- *API Used:* binary ABI
- *Dependency on reindexer library (reindexer-dev package):* yes

## Limitations and known issues

Currently Reindexer is stable and production ready, but it is still a work in progress, so there are some limitations and issues:

- Internal C++ API is not stabilized and is subject to change.

## Getting help

You can get help in several ways:

1. Join Reindexer [Telegram group](https://t.me/reindexer)
2. Write [an issue](https://github.com/restream/reindexer/issues/new)

## References

Landing: https://reindexer.io/

Packages repo: https://repo.reindexer.io/

More documentation (RU): https://reindexer.io/reindexer-docs/

