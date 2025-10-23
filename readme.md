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

# Table of contents:

- [Features](#features)
  - [Performance](#performance)
  - [Memory Consumption](#memory-consumption)
  - [Full text search](#full-text-search)
  - [Vector indexes (ANN/KNN)](#vector-indexes-annknn)
  - [Hybrid search](#hybrid-search)
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
    - [NULL-values filtration](#null-values-filtration)
  - [Nested Structs](#nested-structs)
  - [Sort](#sort)
    - [Forced sort](#forced-sort)
  - [Counting](#counting)
  - [Text pattern search with LIKE condition](#text-pattern-search-with-like-condition)
  - [Update queries](#update-queries)
    - [Update queries with inner joins and subqueries](#update-queries-with-inner-joins-and-subqueries)
    - [Update field with object](#update-field-with-object)
    - [Remove field via update-query](#remove-field-via-update-query)
    - [Update array elements by indexes](#update-array-elements-by-indexes)
    - [Concatenate arrays](#concatenate-arrays)
    - [Remove array elements by values](#remove-array-elements-by-values)
  - [Delete queries](#delete-queries)
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
  - [Search in array fields](#search-in-array-fields)
    - [Search in array fields with matching indexes](#search-in-array-fields-with-matching-indexes)
    - [Search in array fields with matching indexes using grouping](#search-in-array-fields-with-matching-indexes-using-grouping)
      - [Query Execution Examples](#query-execution-examples)
    - [Grouped values extraction examples for more complex cases](#grouped-values-extraction-examples-for-more-complex-cases)
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
- [Events subscription](#events-subscription)
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

Reindexer aims to consume as little memory as possible; most queries are processed without any memory allocation at all.

To achieve that, several optimizations are employed, both on the C++ and Go level:

- Documents and indices are stored in dense binary C++ structs, so they don't impose any load on Go's garbage collector.

- String duplicates are merged.

- Memory overhead is about 32 bytes per document + ≈4-16 bytes per each search index.

- There is an object cache on the Go level for deserialized documents produced after query execution. Future queries use pre-deserialized documents, which cuts repeated deserialization and allocation costs

- The Query interface uses `sync.Pool` for reusing internal structures and buffers.
  The combination of these technologies allows Reindexer to handle most queries without any allocations.

### Full text search

Reindexer has internal full text search engine. Full text search usage documentation and examples are [here](fulltext.md)

### Vector indexes (ANN/KNN)

Reindexer has internal k-nearest neighbors search engine. k-nearest neighbors search usage documentation and examples are [here](float_vector.md)

### Hybrid search

Reindexer has internal hybrid full text and k-nearest neighbors search engine. Its usage documentation and examples are [here](hybrid.md)

### Disk Storage

Reindexer can store documents to and load documents from disk via LevelDB. Documents are written to the storage backend asynchronously by large batches automatically in background.

When a namespace is created, all its documents are stored into RAM, so the queries on these documents run entirely in in-memory mode.

### Replication

Reindexer supports synchronous and asynchronous replication. Check replication documentation [here](replication.md)

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

	"github.com/restream/reindexer/v5"
	// choose how the Reindexer binds to the app (in this case "builtin," which means link Reindexer as a static library)
	_ "github.com/restream/reindexer/v5/bindings/builtin"

	// OR use Reindexer as standalone server and connect to it via TCP or unix domain socket (if available).
	// _ "github.com/restream/reindexer/v5/bindings/cproto"

	// OR link Reindexer as static library with bundled server.
	// _ "github.com/restream/reindexer/v5/bindings/builtinserver"
	// "github.com/restream/reindexer/v5/bindings/builtinserver/config"

)

// Define struct with reindex tags. Fields must be exported - private fields can not be written into reindexer
type Item struct {
	ID       int64  `reindex:"id,,pk"`    // 'id' is primary key
	Name     string `reindex:"name"`      // add index by 'name' field
	Articles []int  `reindex:"articles"`  // add index by articles 'articles' array
	Year     int    `reindex:"year,tree"` // add sortable index by 'year' field
}

func main() {
	// Init a database instance and choose the binding (builtin)
	db, err := reindexer.NewReindex("builtin:///tmp/reindex/testdb")

	// OR - Init a database instance and choose the binding (connect to server via TCP sockets)
	// Database should be created explicitly via reindexer_tool or via WithCreateDBIfMissing option:
	// If server security mode is enabled, then username and password are mandatory
	// db, err := reindexer.NewReindex("cproto://user:pass@127.0.0.1:6534/testdb", reindexer.WithCreateDBIfMissing())

	// OR - Init a database instance and choose the binding (connect to server via TCP sockets with TLS support
	// using cprotos-protocol and a package tls from the standard GO library)
	// Database should be created explicitly via reindexer_tool or via WithCreateDBIfMissing option:
	// If server security mode is enabled, then username and password are mandatory
	// It is assumed that reindexer RPC-server with TLS support is running at this address. 6535 is the default port for it.
	// tlsConfig := tls.Config{
	// 	/*required options*/
	// }
	// db, err := reindexer.NewReindex("cprotos://user:pass@127.0.0.1:6535/testdb", reindexer.WithCreateDBIfMissing(), reindexer.WithTLSConfig(&tlsConfig))

	// OR - Init a database instance and choose the binding (connect to server via unix domain sockets)
	// Unix domain sockets are available on the unix systems only (socket file has to be explicitly set on the server's side with '--urpcaddr' option)
	// Database should be created explicitly via reindexer_tool or via WithCreateDBIfMissing option:
	// If server security mode is enabled, then username and password are mandatory
	// db, err := reindexer.NewReindex("ucproto://user:pass@/tmp/reindexer.socket:/testdb", reindexer.WithCreateDBIfMissing())

	// OR - Init a database instance and choose the binding (builtin, with bundled server)
	// serverConfig := config.DefaultServerConfig ()
	// If server security mode is enabled, then username and password are mandatory
	// db, err := reindexer.NewReindex("builtinserver://user:pass@testdb",reindexer.WithServerConfig(100*time.Second, serverConfig))

	// Check the error after rx instance init
	if err != nil {
		panic(err)
	}

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
	if err = iterator.Error(); err != nil {
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

Please note, that the Query Builder interface is preferred: it has more features and is faster than the SQL interface

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

- `embedded (builtin)` Reindexer is embedded into application as static library, and does not require separate server process.
- `embedded with server (builtinserver)` Reindexer is embedded into application as static library, and start server. In this mode other
  clients can connect to application via cproto, cprotos, ucproto, http or https.
- `standalone` Reindexer run as standalone server, application connects to Reindexer via network or unix domain sockets.

### Installation for server mode

In this mode Reindexer's Go-binding does not depend on reindexer's static library.

1.  [Install Reindexer Server](cpp_src/readme.md#installation)
2.  go get -a github.com/restream/reindexer/v5

#### Official docker image

The simplest way to get reindexer server, is pulling & run docker image from [dockerhub](https://hub.docker.com/r/reindexer/reindexer/).

```bash
docker run -p9088:9088 -p6534:6534 -it reindexer/reindexer
```

[Dockerfile](cpp_src/cmd/reindexer_server/contrib/Dockerfile)

### Installation for embedded mode

#### Prerequirements

Reindexer's core is written with C++20 and uses LevelDB as the storage backend, so Cmake, C++20 toolchain and LevelDB must be installed before installing Reindexer. Also, OpenMP and BLAS/LAPACK libraries may be required for full vector indexes support.

To build Reindexer, g++ 10+, clang 15+ or [mingw64](https://sourceforge.net/projects/mingw-w64/) is required.

In embedded modes Reindexer's Go-binding depends on reindexer's static libraries (core, server and resource).

#### Get Reindexer using go.mod

This way is recommended and will fit for the most scenarios.

Go modules with go.mod do not allow to build C++ libraries in modules' directories. Go-binding will use [pkg-config](https://pkg.go.dev/github.com/rjeczalik/pkgconfig/cmd/pkg-config) to detect libraries' directories.

Reindexer's libraries must be either installed from [sources](cpp_src/readme.md#installation-from-sources) or from [prebuilt package via package manager](cpp_src/readme.md#linux).

Then get the module:

```bash
go get -a github.com/restream/reindexer/v5
```

#### Get Reindexer using go.mod and replace

If you need modified Reindexer's sources, you can use `replace` like that.

1. Download and build reindexer:
```bash
# Clone reindexer via git. It's also possible to use 'go get -a github.com/restream/reindexer/v5', but it's behavior may vary depending on Go's version
git clone https://github.com/restream/reindexer.git $GOPATH/src/reindexer
bash $GOPATH/src/reindexer/dependencies.sh
# Generate builtin binding
cd $GOPATH/src/reindexer
go generate ./bindings/builtin
# Optional (build builtin server binding)
go generate ./bindings/builtinserver
```

2. Add Reindexer's module to your application's go.mod and replace it with the local package:
```bash
# Go to your app's directory
cd /your/app/path
go get -a github.com/restream/reindexer/v5
go mod edit -replace github.com/restream/reindexer/v5=$GOPATH/src/reindexer
```

In this case, Go-binding will generate explicit libraries' and paths' list and will not use pkg-config.

#### Get Reindexer for apps without go.mod (vendoring)

If you're not using go.mod it's possible to get and build reindexer from sources this way:

```bash
export GO111MODULE=off # Disable go1.11 modules
# Go to your app's directory
cd /your/app/path
# Clone reindexer via git. It's also possible to use 'go get -a github.com/restream/reindexer', but it's behavior may vary depending on Go's version
git clone https://github.com/restream/reindexer.git vendor/github.com/restream/reindexer/v5
# Generate builtin binding
go generate -x ./vendor/github.com/restream/reindexer/v5/bindings/builtin
# Optional (build builtin server binding)
go generate -x ./vendor/github.com/restream/reindexer/v5/bindings/builtinserver
```

#### Get Reindexer using go.mod (vendoring)

Go does not support proper vendoring for CGO code (https://github.com/golang/go/issues/26366), however, it's possible to use [vend](https://github.com/nomad-software/vend) to copy Reindexer's sources into vendor-directory.

With `vend` you'll be able to call `go generate -mod=vendor` for `builtin` and `builtinserver`, placed in your vendor-directory.

It's also possible to copy simply copy reindexer's sources into youth project, using `git clone`.

In these cases all the dependencies from Reindexer's [go.mod](go.mod) must be installed manually with proper versions.

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
  - `dense` - reduce index size. For `hash` and `tree` it will save 8-16 bytes per unique key value. For `-` it will save 4-16 bytes per each element. Useful for indexes with high selectivity, but for `tree` and `hash` indexes with low selectivity can seriously decrease update performance. Also `dense` will slow down wide fullscan queries on `-` indexes, due to lack of CPU cache optimization. For the `text` index does not reduce index' size, but switches index' rebuild to the single-thread mode.
  - `sparse` - Row (document) contains a value of Sparse index only in case if it's set on purpose - there are no empty (or default) records of this type of indexes in the row (document). It allows to save RAM, but it will cost you performance - it works a bit slower than regular indexes.
  - `collate_numeric` - create string index that provides values order in numeric sequence. The field type must be a string.
  - `collate_ascii` - create case-insensitive string index works with ASCII. The field type must be a string.
  - `collate_utf8` - create case-insensitive string index works with UTF8. The field type must be a string.
  - `collate_custom=<ORDER>` - create custom order string index. The field type must be a string. `<ORDER>` is sequence of letters, which defines sort order.
  - `linear`, `quadratic`, `greene` or `rstar` - specify algorithm for construction of `rtree` index (by default `rstar`). For details see [geometry subsection](#geometry).
  - `uuid` - store this value as UUID. This is much more effective from the RAM/network consummation standpoint for UUIDs, than strings. Only `hash` and `-` index types are supported for UUIDs. Can be used with any UUID variant, except variant 0

Fields with regular indexes are not nullable. Condition `is NULL` is supported only by `sparse` and `array` indexes.

#### NULL-values filtration

Conditions `IS NULL`/`IS NOT NULL` may have different behavior depending on target index type.
In the tables below you can find results of `IS NULL` for different values/indexes/jsonpaths (`IS NOT NULL` will have the opposite value).

Shortcuts:
- `+` - document will be in the results;
- `-` - document will not be in the results;
- `n` - document is not valid.

Non-nested fields. Index has `json_paths: ["f"]` or `json_paths: ["obj"]` depending on context:
| Documents                      | Non-indexed | Indexed array  | Sparse | Sparse array  |
|--------------------------------|-------------|----------------|--------|---------------|
| `{}`                           | +           | +              | +      | +             |
| `{"f": null}`                  | +           | +              | +      | +             |
| `{"f": []}`                    | +           | +              | +      | +             |
| `{"f": [null]}`                | +           | -              | +      | +             |
| `{"f": [1, null]}`             | +           | -              | +      | +             |
| `{"f": 0}`                     | -           | -              | -      | -             |
| `{"f": [0]}`                   | -           | -              | -      | -             |
| `{"obj": {}}`                  | -           | n              | -      | -             |
| `{"obj": {"f": null}}`         | -           | n              | -      | -             |
| `{"obj": {"f": null, "a": 0}}` | -           | n              | -      | -             |

Non-nested fields. Index has `json_paths: ["f1", "f2"]` or `json_paths: ["obj1", "obj2"]` depending on context:
| Documents                                                        | Array with multiple jsonpaths |
|------------------------------------------------------------------|-------------------------------|
| `{}`, `{}`                                                       | +                             |
| `{"f1": 1}`, `{}`                                                | -                             |
| `{"f1": null}`, `{"f2": null}`                                   | +                             |
| `{"f1": 1}`, `{"f2": null}`                                      | -                             |
| `{"f1": []}`, `{"f2": []}`                                       | +                             |
| `{"f1": [1]}`, `{"f2": []}`                                      | -                             |
| `{"f1": [null]}`, `{"f2": [null]}`                               | -                             |
| `{"f1": [1, null]}`, `{"f2": [1, null]}`                         | -                             |
| `{"f1": 0}`, `{"f2": 0}`                                         | -                             |
| `{"f1": [0]}`, `{"f2": [0]}`                                     | -                             |
| `{"obj1": {}}`, `{"obj2": {}}`                                   | n                             |
| `{"obj1": {"f": null}}`, `{"obj2": {"f": null}}`                 | n                             |
| `{"obj1": {"f": null, "a": 0}}`, `{"obj2": {"f": null, "a": 0}}` | n                             |

Nested fields. Index has `json_paths: ["obj.f"]`:
| Documents                      | Indexed array | Sparse | Sparse array |
|--------------------------------|---------------|--------|--------------|
| `{"obj": {}}`                  | +             | +      | +            |
| `{"obj": {"f": null}}`         | +             | +      | +            |
| `{"obj": {"f": null, "a": 0}}` | +             | +      | +            |
| `{"obj": {"f": [null]}}`       | -             | n      | +            |
| `{"obj": {"f": [1, null]}}`    | -             | n      | +            |

Nested fields. Index has `json_paths: ["obj1.f", "obj2.f"]`:
| Documents                                                        | Array with multiple jsonpaths |
|------------------------------------------------------------------|-------------------------------|
| `{"obj1": {}}`, `{"obj2": {}}`                                   | +                             |
| `{"obj1": {"f": 1}}`, `{"obj2": {}}`                             | -                             |
| `{"obj1": {"f": null}}`, `{"obj2": {"f": null}}`                 | +                             |
| `{"obj1": {"f": 1}}`, `{"obj2": {"f": null}}`                    | -                             |
| `{"obj1": {"f": null, "a": 0}}`, `{"obj2": {"f": null, "a": 0}}` | +                             |
| `{"obj1": {"f": 1, "a": 0}}`, `{"obj2": {"f": null, "a": 0}}`    | -                             |
| `{"obj1": {"f": [null]}}`, `{"obj2": {"f": [null]}}`             | -                             |
| `{"obj1": {"f": [1, null]}}`, `{"obj2": {"f": [1, null]}}`       | -                             |

### Nested Structs

By default, Reindexer scans all nested structs and adds their fields to the namespace (as well as indexes specified).
During indexes scan private (non-exported fields), fields tagged with `reindex:"-"` and fields tagged with `json:"-"` will be skipped.

```go
type Actor struct {
	Name string `reindex:"actor_name"`
	Age  int    `reindex:"age"`
}

type BaseItem struct {
	ID        int64  `reindex:"id,hash,pk"`
	UUIDValue string `reindex:"uuid_value,hash,uuid"`
}

type ComplexItem struct {
	BaseItem              // Index fields of BaseItem will be added to reindex
	Actor         []Actor // Index fields ("name" and "age") of Actor will be added to reindex as array-indexes
	Name          string  `reindex:"name"`      // Hash-index for "name"
	Year          int     `reindex:"year,tree"` // Tree-index for "year"
	Value         int     `reindex:"value,-"`   // Store(column)-index for "value"
	Metainfo      int     `json:"-"`            // Field "MetaInfo" will not be stored in reindexer
	Parent        *Item   `reindex:"-"`         // Index fields of "Parent" will NOT be added to reindex and all of the "Parent" exported content will be stored as non-indexed data
	ParentHidden  *Item   `json:"-"`            // Indexes and fields of "ParentHidden" will NOT be added to reindexer
	privateParent *Item                         // Indexes and fields of "ParentHidden" will NOT be added to reindexer (same as with `json:"-"`)
	AnotherActor  Actor   `reindex:"actor"`     // Index fields of "AnotherActor" will be added to reindexer with prefix "actor." (in this example two indexes will be created: "actor.actor_name" and "actor.age")
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

`Rank()` means fulltext or knn rank of match and is applicable only in fulltext and knn query.

`ST_Distance()` means distance between geometry points (see [geometry subsection](#geometry)). The points could be columns in current or joined namespaces or fixed point in format `ST_GeomFromText('point(1 -3)')`

`hash()` or `hash(seed)` means get hash (uint32) from record id. If `seed` is not specified, it is generated randomly. This function is usefully to randomize sorting order.

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
    Sort("rank() + id / 100", true)   // Sort with rank
....
// Sort by geometry distance
query = db.Query("actors").
    Join(db.Query("cities")).On("birth_place_id", reindexer.EQ, "id").
    SortStPointDistance(cities.center, reindexer.Point{1.0, -3.0}, true).
    SortStFieldDistance("location", "cities.center", true)
....
// Random sorting with stable seed:
query = db.Query("actors").Sort("hash(123123)", true)
....
// In SQL query:
iterator := db.ExecSQL ("SELECT * FROM actors ORDER BY person.name ASC")
....
iterator := db.ExecSQL ("SELECT * FROM actors WHERE description = 'ququ' ORDER BY 'rank() + id / 100' DESC")
....
iterator := db.ExecSQL ("SELECT * FROM actors ORDER BY 'ST_Distance(location, ST_GeomFromText(\'point(1 -3)\'))' ASC")
....
iterator := db.ExecSQL ("SELECT * FROM actors ORDER BY 'ST_Distance(location, cities.center)' ASC")
....
iterator := db.ExecSQL ("SELECT * FROM actors ORDER BY 'hash(123123)'")
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

#### Forced sort

It's possible to force sorting order by pulling some specific keys on the top of the selection results (works with `ASC` sort only):

```go
query := db.Query("actors").Sort("id", false, 10, 15, 17) // Documents with IDs 10, 15, 17 will be on the top (if corresponding documents exist)
```

### Counting

Reindexer supports 2 version of counting aggregation:
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

For simple searching text pattern in string fields condition `LIKE` can be used. It looks for strings matching the pattern. In the pattern `_` means any char and `%` means any sequence of chars.

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

General UPDATE SQL-Syntax:
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
UPDATE NS SET arrayfield = [] WHERE id = 100
```

and set it to null

```sql
UPDATE NS SET field = NULL WHERE id > 100
```

In case of non-indexed fields, setting its value to a value of a different type will replace it completely; in case of indexed fields, it is only possible to convert it from adjacent type (integral types and bool), numeric strings (like "123456") to integral types and back. Setting indexed field to null resets it to a default value.

It is possible to add new fields to existing items

```sql
UPDATE NS SET newField = 'Brand new!' WHERE id > 100
```

and even add a new field by a complex nested path like this

```sql
UPDATE NS SET nested.nested2.nested3.nested4.newField = 'new nested field!' WHERE id > 100
```

will create the following nested objects: nested, nested2, nested3, nested4 and newField as a member of object nested4.

Example of using Update queries in golang code:

```go
db.Query("items").Where("id", reindexer.EQ, 40).Set("field1", value1).Set("field2", value2).Update()
```
- there can be multiple `.Set` expressions - one for a field. Also, it is possible to combine several `Set...` expressions of different types in one query, like this: `Set().SetExpression().SetObject()...`

#### Update queries with inner joins and subqueries

Inner join and subquery may also be used as WHERE-condition.  Inner joins don't actually joining items, but will be applied as filtering condition.

SQL syntax:
```sql
UPDATE ns SET val = 100 WHERE INNER JOIN (SELECT * FROM join_ns WHERE id > 100) ON ns.id = join_ns.id
UPDATE ns SET val = 100 WHERE field = (SELECT id FROM subq_ns WHERE val = 200)
```
Go syntax:
```go
db.Query(items).
	OpenBracket()
		q.InnerJoin(DB.Query(joinItems).Where("id", reindexer.GT, 100)).
		On("ID", reindexer.EQ, "ID")
	q.CloseBracket().
	Set("val", 100).Update()

DB.Query(items).
	Where("field", reindexer.EQ,
		DB.Query(subq_ns).Select("id").Where("val", reindexer.EQ, 200)
	).
	Set("val", 100).Update()
```

#### Update field with object

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
UPDATE clients SET client_data = {"Name":"John Doe","Age":40,"Address":"Fifth Avenue, Manhattan","Occupation":"Bank Manager","TaxYear":1999,"TaxConsultant":"Jane Smith"} WHERE id = 100;
```

#### Remove field via update-query

UPDATE Sql-Syntax of queries that drop existing non-indexed fields:

```sql
UPDATE nsName
DROP field1, field2, ..
WHERE condition;
```

```go
db.Query("items").Where("id", reindexer.EQ, 40).Drop("field1").Drop("field2").Update()
```

#### Update array elements by indexes

Reindexer update mechanism enables to modify array fields: to modify a certain item of an existing array or even to replace an entire field.

To update an item subscription operator syntax is used:

```sql
UPDATE NS SET array[*].prices[0] = 9999 WHERE id = 5
```

where `*` means all items.

To update entire array the following is used:

```sql
UPDATE NS SET prices = [999, 1999, 2999] WHERE id = 9
```

any non-indexed field can be easily converted to array using this syntax.

Reindexer also allows to update items of object arrays:

```sql
UPDATE NS SET extra.objects[0] = {"Id":0,"Description":"Updated!"} WHERE id = 9
```

also like this

```golang
db.Query("clients").Where("id", reindexer.EQ, 100).SetObject("extra.objects[0]", updatedValue).Update()
```

Reindexer supports heterogeneous arrays:

```sql
UPDATE NS SET info = ["hi", "bro", 111, 2.71] WHERE id = 9
```

```golang
	q := DB.Query(ns).Where("id", reindexer.EQ, 1).Set("array", []interface{}{"whatsup", 777, "bro"})
	res, err := q.Update().FetchAll()
```

Index array-fields support values that can be converted to an index type only. When saved, such values may change precision due to conversion.

```sql
UPDATE NS SET prices_idx = [11, '2', 3]
```

To remove item by index you should do the following:

```sql
UPDATE NS DROP array[5]
```

#### Concatenate arrays

To add items to an existing array the following syntax is supported:

```sql
UPDATE NS SET integer_array = integer_array || [5,6,7,8]
```

and

```sql
UPDATE NS SET integer_array = [1,2,3,4,5] || integer_array
```

The first one adds elements to the end of `integer_array`, the second one adds 5 items to the front of it. To make this code work in Golang `SetExpression()` should be used instead of `Set()`.

#### Remove array elements by values

To remove items by value into an existing array the following syntax is supported:

```sql
UPDATE NS SET integer_array = array_remove(integer_array, [5,6,7,8])
```

and

```sql
UPDATE NS SET integer_array = array_remove_once(integer_array, [5,6,7,8,6])
```

The first one removes all occurrences of the listed values in `integer_array`, the second one deletes only the first occurrence found. To make this code work in Golang `SetExpression()` should be used instead of `Set()`.
If you need to remove one value, you can use square brackets `[5]` or simple value `5`.

```sql
UPDATE NS SET integer_array = array_remove(integer_array, [5])
```
```sql
update ns set integer_array = array_remove(integer_array, 5)
```

Remove command can be combined with array concatenate:

```sql
UPDATE NS SET integer_array = array_remove_once(integer_array, [5,6,7,8]) || [1,2,3]
```

also like this

```golang
db.Query("main_ns").SetExpression("integer_array", "array_remove(integer_array, [5,6,7,8]) || [1,2,3]").Update()
```

It is possible to remove the values of the second field from the values of the first field. And also add new values etc.
Note: The first parameter in commands is expected to be an array/field-array, the second parameter can be an array/scalar/field-array/field-scalar. For values compatibility/convertibility required

```sql
UPDATE NS SET integer_array = [3] || array_remove(integer_array, integer_array2) || integer_array3 || array_remove_once(integer_array, [8,1]) || [2,4]
```
```sql
UPDATE NS SET integer_array = array_remove(integer_array, integer_array2) || array_remove(integer_array, integer_array3) || array_remove_once(integer_array, [33,777])
```

```golang
db.Query("main_ns").SetExpression("integer_array", "[3] || array_remove(integer_array, integer_array2) || integer_array3 || array_remove(integer_array, [8,1]) || [2,4]").Update()
```

### Delete queries

Reindexer also supports `delete`-queries. Those queries may contain conditions, inner joins and subqueries. Inner joins don't actually joining items, but will be applied as filtering condition.

SQL syntax for delete queries:
```sql
DELETE FROM ns WHERE field = 111
DELETE FROM ns WHERE id < 5 OR INNER JOIN ns_join ON ns.id = ns_join.id
DELETE FROM ns WHERE id < 5 OR INNER JOIN (SELECT * FROM ns_join WHERE age > 18) ON ns.id = ns_join.id
DELETE FROM ns WHERE id = (SELECT id FROM sub_ns WHERE age < 13)
```

Go syntax for delete queries:
```go
// Delete query with INNER JOIN
DB.Query(ns).Where("id", reindexer.EQ, 1).
	OpenBracket().
		InnerJoin(
			DB.Query(join_ns).Where("DEVICE", reindexer.EQ, "android"), 
			"some random name").
		On("PRICE_ID", reindexer.SET, "ID")
	CloseBracket().
	Delete()

// Delete query with subquery
DB.Query(ns).
	Where("ID", reindexer.EQ,
		DB.Query(sub_ns).Select("id").Where("age", reindexer.LT, 13)
	).
	Delete()
```

### Transactions and batch update

Reindexer supports transactions. Transaction are performs atomic namespace update. There are synchronous and async transaction available. To start transaction method `db.BeginTx()` is used. This method creates transaction object, which provides usual Update/Upsert/Insert/Delete interface for application.
For RPC clients there is transactions count limitation - each connection can't have more than 1024 open transactions at the same time.

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

The amount of data for selecting a Commit strategy can be selected in the namespace configuration. Check fields `StartCopyPolicyTxSize`, `CopyPolicyMultiplier` and `TxSizeToAlwaysCopy` in `struct DBNamespacesConfig`([describer.go](describer.go))

If transaction size is less than `TxSizeToAlwaysCopy`, reindexer uses extra heuristic and trying to avoid namespace copying, if there were no selecting queries seen for this namespace.
In some cases this heuristic may increase selects latency, so it may be disabled by setting `REINDEXER_NOTXHEURISTIC` env variable to any non-empty value.

#### Implementation notes

1. Transaction object is not thread safe and can't be used from different goroutines;
2. Transaction object holds Reindexer's resources, therefore application should explicitly call Rollback or Commit, otherwise resources will leak;
3. It is safe to call Rollback after Commit;
4. It is possible to call Query from transaction by call `tx.Query("ns").Exec() ...`;
5. Only serializable isolation is available, i.e. each transaction takes exclusive lock over the target namespace until all the steps of the transaction committed.

### Join

Reindexer can join documents from multiple namespaces into a single result:

```go
type Actor struct {
	ID        int    `reindex:"id"`
	Name      string `reindex:"name"`
	IsVisible bool   `reindex:"is_visible"`
}

// Fields, marked with 'joined' must also be exported - otherwise reindexer's binding will not be able to put data in those fields
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
defer it.Close()
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

Reindexer does not support `ANTI JOIN` SQL construction, however, it supports logical operations with JOINs. In fact `NOT INNER JOIN ...` is totally equivalent to the `ANTI JOIN`:
```go
query := db.Query("items_with_join").
	Not().
	InnerJoin(
	db.Query("actors").
		WhereBool("is_visible", reindexer.EQ, true),
	"actors").
	On("id", reindexer.EQ, "id")
```
```SQL
SELECT * FROM items_with_join WHERE NOT INNER JOIN (SELECT * FROM actors WHERE is_visible = true) ON items_with_join.id = actors.id
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
Composite index is an index that involves multiple fields, it can be used instead of multiple separate indexes.

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
	query := db.Query("items").Sort("rating+year", true, []interface{}{5,2010})
```

For make query to the composite index, pass []interface{} to `.WhereComposite` function of Query builder:

```go
	// Get results where rating == 5 and year == 2010
	query := db.Query("items").WhereComposite("rating+year", reindexer.EQ,[]interface{}{5,2010})
```

All the fields in regular (non-fulltext) composite index must be indexed. I.e. to be able to create composite index `rating+year`, it is necessary to create some kind of indexes for both `raiting` and `year` first:

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
count' and cut off by offset and limit.
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
	defer iterator.Close()

	aggMaxRes := iterator.AggResults()[0]

	if aggMaxRes.Value != nil {
		fmt.Printf ("max price = %d\n", *aggMaxRes.Value)
	} else {
		fmt.Println ("no data to aggregate")
	}

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
	defer iterator.Close()

	aggResults := iterator.AggResults()

	distNames := aggResults[0]
	fmt.Println ("names:")
	for _, name := range distNames.Distincts {
		fmt.Println(name[0])
	}

	distPrices := aggResults[1]
	fmt.Println ("prices:")
	for _, price := range distPrices.Distincts {
		fmt.Println(price[0])
	}
```

Sorting by aggregated`FACET`'s fields has distinct syntax in its SQL version:
```sql
SELECT FACET(name, price ORDER BY "name" ASC, "count" DESC) FROM items
```

`Distinct(field1,field2,...)`  works as follows
1. If a single field is specified (Scalar fields, arrays, and composite indexes are supported)
A namespace row is considered unique if
- The scalar is unique
- One of the array values is unique
- Composite value `v1+v2+..` is unique
All unique values (including all array values) are added to the distinct aggregation result
2. If multiple fields are specified (Scalar fields and arrays are supported.)
A table row is considered unique if
- Scalar `v1+v2+..` is unique
- Arrays are padded with empty values up to the same length. Scalars are represented as arrays of maximum length with the same value. It turns out to be a rectangular table. Each row of the `v1[i]+v2[i]+...` table is taken, and it checks whether there is such a value in the list of unique values.
 If at least one of these values is unique, then the string is considered unique.
The aggregation result adds up all the unique values `v1[i]+v2[i]+...`. 

### Search in array fields

Let's consider the case of filtering data by multiple arrays.
Suppose we have the following data structure:

```go
type Element struct {
    Project   string   `reindex:"project"`
    Countries []string `reindex:"countries"`
}

type Filters struct {
    Filters []Element
    Array   []int `reindex:"array"`
}
```

with the following data:

```json
{
  "Filters": [
    {"Project": "wink", "Countries": ["ru", "am"]},
    {"Project": "dns", "Countries": ["ru"]}
  ],
  "Array": [10, 20]
}
```

When filtering with the query:
```sql
SELECT * FROM Namespace WHERE Array = 10 AND Filters.Countries = 'ru'
```
or
```sql
SELECT * FROM Namespace WHERE array = 10 AND countries = 'ru'
```

the condition `Array = 10` checks whether the value `10` is present in the array `[10, 20]`,
the condition `Filters.Countries = 'ru'` checks whether the value `'ru'` is present in the array `["ru", "am"]`.
It does not matter at which position in the array the value was found.

#### Search in array fields with matching indexes

In some tasks, it is necessary to require that matches occur at the same indexes. To solve this problem, there is the function
`EQUAL_POSITION(FieldName1, FieldName2, ...)`, where the `FieldNameN` parameter can be a JSON path or an index name:

```sql
SELECT * FROM Namespace WHERE Filters.Project = 'wink' AND Array = 20 EQUAL_POSITION(Filters.Project, Array)
```

The query above will be processed as follows:

1. Values are extracted:
```
Filters.Project -> ["wink", "dns"]
Array           -> [10, 20]
```
2. The query condition is checked for array index 0:
```
Filters.Project[0]('wink') = 'wink' AND Array[0](10) = 20
```
3. The query condition is checked for array index 1:
```
Filters.Project[1]('dns') = 'wink' AND Array[1](20) = 20
```
4. Since the condition is not met for any of the array indexes, this document does not satisfy the query condition.

Consider another query:
```sql
SELECT * FROM Namespace WHERE Filters.Project = 'wink' AND Array = 10 EQUAL_POSITION(Filters.Project, Array)
```
1. Values are extracted:
```
Filters.Project -> ["wink", "dns"]
Array           -> [10, 20]
```
2. The query condition is checked for array index 0:
```
Filters.Project[0]('wink') = 'wink' AND Array[0](10) = 10
```
3. The query condition is checked for array index 1:
```
Filters.Project[1]('dns') = 'wink' AND Array[1](20) = 10
```
4. Since the condition is met for index 0, this document satisfies the query condition.

For conditions with parentheses, `EQUAL_POSITION(...)` applies to the conditions inside the parentheses. Multiple `EQUAL_POSITION` functions can be specified for a single set of parentheses.
`EQUAL_POSITION` does not work for the following conditions: `IS NULL`, `IS EMPTY`, and `IN` (with an empty list of values).
Query examples:
```sql
SELECT * FROM Namespace WHERE (f1 >= 5 AND f2 = 100 EQUAL_POSITION(f1,f2)) OR (f3 = 3 AND f4 < 4 AND f5 = 7 EQUAL_POSITION(f3,f4,f5));

SELECT * FROM Namespace WHERE (f1 >= 5 AND f2 = 100 AND f3 = 3 AND f4 < 4 EQUAL_POSITION(f1,f3) EQUAL_POSITION(f2,f4)) OR (f5 = 3 AND f6 < 4 AND f7 = 7 EQUAL_POSITION(f5,f7));

SELECT * FROM Namespace WHERE f1 >= 5 AND (f2 = 100 AND f3 = 3 AND f4 < 4 EQUAL_POSITION(f2,f3)) AND f5 = 3 AND f6 < 4 EQUAL_POSITION(f1,f5,f6);
```
Both index names and JSON paths can be used in conditions and `EQUAL_POSITION`.
The following combinations are allowed:

Index (index) built on a single JSON path (json_path) 

| Condition | Equal_position | Allowed |
|-----------|----------------|---------|
| index     | index          | +       |
| index     | json_path      | +       |
| json_path | json_path      | +       |
| json_path | index          | +       |

Index (index) built on two JSON paths (json_path1, json_path2)

| Condition  | Equal_position | Allowed |
|------------|----------------|---------|
| index      | index          | +       |
| index      | json_path1     | +       |
| index      | json_path2     | +       |
| json_path1 | json_path1     | +       |
| json_path1 | json_path2     | -       |
| json_path2 | json_path1     | -       |
| json_path2 | json_path2     | +       |
| json_path1 | index          | -       |
| json_path2 | index          | -       |

#### Search in array fields with matching indexes using grouping

For nested arrays, grouping logic is supported.
To use it, add the `[#]` label after the field name. 
It should be noted that in this case, only the JSON path is used because the index name does not reflect the document's structure.
In this case, during processing, a table will be formed: each row contains all values
for one index of the marked array, and the row number corresponds to this index.

For example, if we need to group the values of the `Filters.Countries` field by the indexes of the `Filters` array,
the notation would look like this: `Filters[#].Countries`.
The table created as a result of grouping will be as follows:

| N |      |     |
|---|------|-----|
| 0 | "ru" | "am"|
| 1 | "ru" |     |

If we need to group the values of the `Filters.Project` field by the indices of the `Filters` array,
the notation would look like this: `Filters[#].Project`.
The table created as a result of grouping will be as follows:

| N |      |
|---|------|
| 0 |"wink"|
| 1 |"dns" |

To check the condition, rows with the same indices are selected.

##### Query Execution Examples

Field values are extracted according to the tables above.

**Example 1:**
```go
db.Query("Namespace").
    Where("Filters.Project", reindexer.EQ, "dns").
    Where("Filters.Countries", reindexer.EQ, "ru").
    EqualPosition("Filters[#].Project", "Filters[#].Countries")
```
```sql
SELECT * FROM ns WHERE Filters.Project = 'dns' AND Filters.Countries = 'ru'
  EQUAL_POSITION(Filters[#].Project, Filters[#].Countries)
```

1. The query condition is checked for array index 0:
```
Filters[0].Countries[0]('ru') = 'ru' OR Filters[0].Countries[1]('am') = 'ru'
AND
Filters[0].Project[0]('wink') = 'dns'
```
2. The query condition is checked for array index 1:
```
Filters[1].Countries[0]('ru') = 'ru'
AND
Filters[1].Project[0]('dns') = 'dns'
```
3. Since the condition is met for index 1, this document satisfies the query condition.

**Example 2:**
```go
db.Query("Namespace").
    Where("Filters.Project", reindexer.EQ, "dns").
    Where("Filters.Countries", reindexer.EQ, "am").
    EqualPosition("Filters[#].Project", "Filters[#].Countries")
```

1. The query condition is checked for array index 0:
```
Filters[0].Countries[0]('ru') = 'am' OR Filters[0].Countries[1]('am') = 'am'
AND
filters[0].Project[0]('wink') = 'dns'
```
2. The query condition is checked for array index 1:
```
Filters[1].Countries[0]('ru') = 'am'
AND
Filters[1].Project[0]('dns') = 'dns'
```
3. Since the condition is not met for any index, this document does not satisfy the query condition.

**Example 3:**
```go
db.Query("Namespace").
    Where("Filters.Project", reindexer.EQ, "wink").
    Where("Filters.Countries", reindexer.EQ, "am").
    EqualPosition("Filters[#].Project", "Filters[#].Countries")
```

1. The query condition is checked for array index 0:
```
Filters[0].Countries[0]('ru') = 'am' OR Filters[0].Countries[1]('am') = 'am'
AND
Filters[0].Project[0]('wink') = 'wink'
```
2. The query condition is checked for array index 1:
```
Filters[1].Countries[0]('ru') = 'am'
AND
Filters[1].Project[0]('dns') = 'wink'
```
3. Since the condition is met for index 0, this document satisfies the query condition.

#### Grouped values extraction examples for more complex cases

Consider the document:

```json
"arr_root": [
  {
    "obj_nested": {
      "arr_nested": [
        {
          "field": 1
        },
        {
          "field": [5, 3]
        }
      ]
    }
  },
  {
  },
  {
    "obj_nested": {
      "arr_nested": [
        {
          "field": null
        },
        {
          "field": [4, 7]
        }
      ]
    }
  }
]
```

Grouping by `arr_root.obj_nested.arr_nested.field[#]`:

| N |   |   |     |   |
|---|---|---|-----|---|
| 0 | 1 | 5 | null| 4 |
| 1 | 3 | 7 |     |   |

Grouping by `arr_root.obj_nested.arr_nested[#].field`:

| N |   |      |   |   |
|---|---|------|---|---|
| 0 | 1 | null |   |   |
| 1 | 5 | 3    | 4 | 7 |

Grouping by `arr_root.obj_nested[#].arr_nested.field`:

| N |   |   |   |     |   |   |
|---|---|---|---|-----|---|---|
| 0 | 1 | 5 | 3 | null| 4 | 7 |

Grouping by `arr_root[#].obj_nested.arr_nested.field`:

| N |      |   |   |
|---|------|---|---|
| 0 | 1    | 5 | 3 |
| 1 |      |   |   |
| 2 | null | 4 | 7 |

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
		Select ("id","name").        // Filter output JSON: Select only "id" and "name" fields of items, another fields will be omitted. This fields should be specified in the same case as the jsonpaths corresponding to them.
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

WARNING: when used `AllowUnsafe(true)` queries returns shared pointers to structs in object cache. Therefore, application MUST NOT modify returned objects.

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

By default, maximum size of object cache is 256000 items for each namespace. To change maximum size use `ObjCacheSize` method of `NamespaceOptions`, passed
to OpenNamespace. e.g.

```go
	// Set object cache limit to 4096 items
	db.OpenNamespace("items_with_huge_cache", reindexer.DefaultNamespaceOptions().ObjCacheSize(4096), Item{})
```

!This cache should not be used for the namespaces, which were replicated from the other nodes: it may be inconsistent for those replica's namespaces.

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

## Events subscription

When reindexer used as caching DB by multiple applications, it may be helpful to subscribe on the database updates (for example, if you need to invalidate internal application cache on some database event):

```go

import events "github.com/restream/reindexer/v5/events"

// Create subscription options for the specific event types (any documents modifications + transactions commits)
opts := events.DefaultEventsStreamOptions().WithDocModifyEvents().WithTransactionCommitEvents()
// Create events stream
stream := db.Subscribe(opts)
if err := stream.Error(); err != nil {
	panic(err)
}
// Stream must be finalized. Finalization here basicly means 'unsubscribe'
defer stream.Close(context.Background())

for {
	// Reading events
	event, ok := <- stream.Chan()
	if !ok {
		// Events channel is closed
		fmt.Printf("Reindexer events stream was invalidated: %v\n", stream.Error())
		break
	}
	// Event handling logic
	...
}
```

Single Go binding supports up to 32 simultaneous streams. All the streams are handled together via separated connection (for `cproto`) or separated goroutine (for `builtin`/`builtinserver`).

In case of critical errors `Chan()` will be closed automatically. New stream must be created to renew events subscription in this case.

To configure subscription `EventsStreamOptions`-object should be used. It is possible to subscribe for the specific operations types and namespaces.
There are predefined events collections in [streamopts.go](#events/streamopts.go), but any required types may also be specified via `WithEvents`-option. With default options events stream will receive all the events from all the namespaces (except `#config`-namespace).

`EventsStreamOptions` also allow to configure events contents: LSN, database name, timestamps, etc. Currently events do not contain related documents' JSON/CJSON.

Note: Reindexer's core is using internal queue to collect and send events to the subscribers. Events objects are shared between events queue and replication queue. Max size of those queues may be configured via `maxupdatessize`(standalone)/`MaxUpdatesSizeBytes`(builtinserver)/`WithMaxUpdatesSize()`(builtin) DB parameter (default size is 1 GB).
In case of queue overflow some events may be dropped and `EventTypeUpdatesDrop` will be generated instead of them.

## Logging, debug, profiling and tracing

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
  - `avg_step_threshold_us (integer)`: Threshold value (in microseconds) for the average step duration time in the transaction. If `avg_step_threshold_us` is -1 logging by average transaction's step duration is disabled.

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

Custom allocator may be handy to track memory consummation, profile heap/CPU or to improve general performance.

### Profiling

#### Heap profiling

Because reindexer core is written in C++ all calls to reindexer and their memory consumption are not visible for go profiler. To profile reindexer core there are cgo profiler available. cgo profiler now is part of reindexer, but it can be used with any another cgo code.

Usage of cgo profiler is very similar with usage of [go profiler](https://golang.org/pkg/net/http/pprof/).

1. Add import:

```go
import _ "github.com/restream/reindexer/v5/pprof"
```

2. If your application is not already running a http server, you need to start one. Add "net/http" and "log" to your imports and the following code to your main function:

```go
go func() {
	log.Println(http.ListenAndServe("localhost:6060", nil))
}()
```

3. Run application with environment variable `HEAPPROFILE=/tmp/pprof` or `TCMALLOC_SAMPLE_PARAMETER=512000`
4. Then use the pprof tool to look at the heap profile:

```bash
pprof -symbolize remote http://localhost:6060/debug/cgo/pprof/heap
```

In some cases remote symbolizer unable to resolve the symbols, so you may try the default:
```bash
pprof http://localhost:6060/debug/cgo/pprof/heap
```

#### CPU profiling

Internal Reindexer's profiler is based on gperf_tools library and unable to get CPU profile via Go runtime. However, [go profiler](https://golang.org/pkg/net/http/pprof/) may be used with symbolizer to retrieve C++ CPU usage.

1. Add import:

```go
import _ "net/http/pprof"
```

2. If your application is not already running a http server, you need to start one. Add "net/http" and "log" to your imports and the following code to your main function:

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
db, err := reindexer.NewReindex("cproto://user:pass@127.0.0.1:6534/testdb", reindexer.WithOpenTelemetry())
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
to export the information externally. For example, as a minimum, you will need to configure OpenTelemetry SDK exporter
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

- *Support modes*: standalone, builtin, builtin-server
- *API Used*: binary ABI, cproto
- *Dependency on reindexer library (reindexer-dev package):* yes, for builtin & builtin-server

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

Currently, Reindexer is stable and production ready, but it is still a work in progress, so there are some limitations and issues:

- Internal C++ API is not stabilized and is subject to change.

## Getting help

You can get help in several ways:

1. Join Reindexer [Telegram group](https://t.me/reindexer)
2. Write [an issue](https://github.com/restream/reindexer/issues/new)

## References

Landing: https://reindexer.io/

Packages repo: https://repo.reindexer.io/

More documentation (RU): https://reindexer.io/reindexer-docs/
