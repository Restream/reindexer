# Reindexer

[![GoDoc](https://godoc.org/github.com/Restream/reindexer?status.svg)](https://godoc.org/github.com/Restream/reindexer)
[![Build Status](https://travis-ci.org/Restream/reindexer.svg?branch=master)](https://travis-ci.org/Restream/reindexer)
[![Build Status](https://ci.appveyor.com/api/projects/status/yonpih8vx3acaj86?svg=true)](https://ci.appveyor.com/project/olegator77/reindexer)

**Reindexer** is an embeddable, in-memory, document-oriented database with a high-level Query builder interface.

Reindexer's goal is to provide fast search with complex queries. We at Restream weren't happy with Elasticsearch and created Reindexer as a more performant alternative.

The core is written in C++ and the application level API is in Go.

This document describes Go connector and it's API. To get information
about reindexer server and HTTP API refer to
[reindexer documentation](cpp_src/readme.md)

# Table of contents: 

- [Features](#features)
  - [Performance](#performance)
  - [Memory Consumption](#memory-consumption)
  - [Full text search](#full-text-search)
  - [Disk Storage](#disk-storage)
- [Usage](#usage)
  - [SQL compatible interface](#sql-compatible-interface)
- [Installation](#installation)
  - [Installation for server mode](#installation-for-server-mode)
    - [Official docker image](#official-docker-image)
  - [Installation for embeded mode](#installation-for-embeded-mode)
    - [Prerequirements](#prerequirements)
    - [Get Reindexer](#get-reindexer)
- [Advanced Usage](#advanced-usage)
  - [Index Types and Their Capabilites](#index-types-and-their-capabilites)
  - [Nested Structs](#nested-structs)
  - [Sort](#sort)
  - [Join](#join)
    - [Joinable interface](#joinable-interface)
    - [Update queries](#update-queries)
    - [Transactions and batch update](#transactions-and-batch-update)
      - [Synchronous mode](#synchronous-mode)
      - [Async batch mode](#async-batch-mode)
      - [Transactions commit strategies](#transactions-commit-strategies)
      - [Implementation notes](#implementation-notes)
  - [Complex Primary Keys and Composite Indices](#complex-primary-keys-and-composite-indices)
  - [Atomic on update functions](#atomic-on-update-functions)
  - [Aggregations](#aggregations)
  - [Expire Data from Namespace by Setting TTL](#ttl-indexes)
  - [Direct JSON operations](#direct-json-operations)
    - [Upsert data in JSON format](#upsert-data-in-json-format)
    - [Get Query results in JSON format](#get-query-results-in-json-format)
  - [Using object cache](#using-object-cache)
    - [DeepCopy interface](#deepcopy-interface)
    - [Get shared objects from object cache (USE WITH CAUTION)](#get-shared-objects-from-object-cache-use-with-caution)
    - [Limit size of object cache](#limit-size-of-object-cache)
    - [Geometry](#geometry)
- [Logging, debug and profiling](#logging-debug-and-profiling)
  - [Turn on logger](#turn-on-logger)
  - [Debug queries](#debug-queries)
  - [Profiling](#profiling)
- [Integration with other program languages](#integration-with-other-program-languages)
  - [Pyreindexer](#pyreindexer)
- [Limitations and known issues](#limitations-and-known-issues)
- [Getting help](#getting-help)

## Features

Key features:

- Sortable indices
- Aggregation queries
- Indices on array fields
- Complex primary keys
- Composite indices
- Join operations
- Full-text search
- Up to 64 indices for one namespace
- ORM-like query interface
- SQL queries

### Performance

Performance has been our top priority from the start, and we think we managed to get it pretty good. Benchmarks show that Reindexer's performance is on par with a typical key-value database. On a single CPU core, we get:

- up to 500K queries/sec for queries `SELECT * FROM items WHERE id='?'`
- up to 50K queries/sec for queries `SELECT * FROM items WHERE year > 2010 AND name = 'string' AND id IN (....)`
- up to 20K queries/sec for queries `SELECT * FROM items WHERE year > 2010 AND name = 'string' JOIN subitems ON ...`

See benchmarking results and more details in [benchmarking section](benchmarks)

### Memory Consumption

Reindexer aims to consume as little memory as possible; most queries are processed without memory allocs at all.

To achieve that, several optimizations are employed, both on the C++ and Go level:

- Documents and indices are stored in dense binary C++ structs, so they don't impose any load on Go's garbage collector.

- String duplicates are merged.

- Memory overhead is about 32 bytes per document + ≈4-16 bytes per each search index.

- There is an object cache on the Go level for deserialized documents produced after query execution. Future queries use pre-deserialized documents, which cuts repeated deserialization and allocation costs

- The Query interface uses `sync.Pool` for reusing internal structures and buffers.
  Combining of these techings let's Reindexer execute most of queries without any allocations.

### Full text search

Reindexer has internal full text search engine. Full text search usage documentation and examples are [here](fulltext.md)

### Disk Storage

Reindexer can store documents to and load documents from disk via LevelDB. Documents are written to the storage backend asynchronously by large batches automatically in background.

When a namespace is created, all its documents are stored into RAM, so the queries on these documents run entirely in in-memory mode.

## Usage

Here is complete example of basic Reindexer usage:

```go
package main

// Import package
import (
	"fmt"
	"math/rand"

	"github.com/restream/reindexer"
	// choose how the Reindexer binds to the app (in this case "builtin," which means link Reindexer as a static library)
	_ "github.com/restream/reindexer/bindings/builtin"

	// OR link Reindexer as static library with bundled server.
	// _ "github.com/restream/reindexer/bindings/builtinserver"
	// "github.com/restream/reindexer/bindings/builtinserver/config"

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

	// OR - Init a database instance and choose the binding (connect to server)
	// Database should be created explicitly via reindexer_tool or via WithCreateDBIfMissing option:
	// If server security mode is enabled, then username and password are mandatory
	// db := reindexer.NewReindex("cproto://user:pass@127.0.0.1:6534/testdb", reindexer.WithCreateDBIfMissing())

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

### SQL compatible interface

As alternative to Query builder Reindexer provides SQL compatible query interface. Here is sample of SQL interface usage:

```go
    ...
	iterator := db.ExecSQL ("SELECT * FROM items WHERE name='Vasya' AND year > 2020 AND articles IN (6,1,8) ORDER BY year LIMIT 10")
    ...
```

Please note, that Query builder interface is preferable way: It have more features, and faster than SQL interface

## Installation

Reindexer can run in 3 different modes:

- `embeded (builtin)` Reindexer is embeded into application as static library, and does not reuqire separate server proccess.
- `embeded with server (builtinserver)` Reindexer is embeded into application as static library, and start server. In this mode other
  clients can connect to application via cproto or http.
- `standalone` Reindexer run as standalone server, application connects to Reindexer via network

### Installation for server mode

1.  [Install Reindexer Server](cpp_src/readme.md#installation)
2.  go get -a github.com/restream/reindexer

#### Official docker image

The simplest way to get reindexer server, is pulling & run docker image from [dockerhub](https://hub.docker.com/r/reindexer/reindexer/).

```bash
docker run -p9088:9088 -p6534:6534 -it reindexer/reindexer
```

[Dockerfile](cpp_src/cmd/reindexer_server/contrib/Dockerfile)

### Installation for embeded mode

#### Prerequirements

Reindexer's core is written in C++11 and uses LevelDB as the storage backend, so the Cmake, C++11 toolchain and LevelDB must be installed before installing Reindexer.

To build Reindexer, g++ 4.8+, clang 3.3+ or [mingw64](https://sourceforge.net/projects/mingw-w64/) is required.

#### Get Reindexer

```bash
go get -a github.com/restream/reindexer
bash $GOPATH/src/github.com/restream/reindexer/dependencies.sh
go generate github.com/restream/reindexer/bindings/builtin
# Optional (build builtin server binding)
go generate github.com/restream/reindexer/bindings/builtinserver

```

## Advanced Usage

### Index Types and Their Capabilites

Internally, structs are split into two parts:

- indexed fields, marked with `reindex` struct tag
- tuple of non-indexed fields

Queries are possible only on the indexed fields, marked with `reindex` tag. The `reindex` tag contains the index name, type, and additional options:

`reindex:"<name>[[,<type>],<opts>]"`

- `name` – index name.
- `type` – index type:
  - `hash` – fast select by EQ and SET match. Used by default. Allows _slow_ and ineffecient sorting by field.
  - `tree` – fast select by RANGE, GT, and LT matches. A bit slower for EQ and SET matches than `hash` index. Allows fast sorting results by field.
  - `text` – full text search index. Usage details of full text search is described [here](fulltext.md)
  - `-` – column index. Can't perform fast select because it's implemented with full-scan technic. Has the smallest memory overhead.
  - `ttl` - TTL index that works only with int64 fields. These indexes are quite convenient for representation of date fields (stored as UNIX timestamps) that expire after specified amount of seconds.
  - `rtree` - available only DWITHIN match. Acceptable only for `[2]float64` field type. For details see [geometry subsection](#geometry).
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

Fields with regular indexes are not nullable. Condition `is NULL` is supported only by `sparse` and `array` indexes.

### Nested Structs

By default Reindexer scans all nested structs and adds their fields to the namespace (as well as indexes specified).

```go
type Actor struct {
	Name string `reindex:"actor_name"`
}

type BaseItem struct {
	ID int64 `reindex:"id,hash,pk"`
}

type ComplexItem struct {
	BaseItem         // Index fields of BaseItem will be added to reindex
	actor    []Actor // Index fields of Actor will be added to reindex as arrays
	Name     string  `reindex:"name"`
	Year     int     `reindex:"year,tree"`
	parent   *Item   `reindex:"-"` // Index fields of parent will NOT be added to reindex
}
```

### Sort

Reindexer can sort documents by fields (including nested and fields of joined `namespaces`) or by expressions in ascending or descending order.

Sort expressions can contain fields names (including nested and fields of joined `namespaces`) of int, float or bool type, numbers, functions rank(), abs() and ST_Distance(), parenthesis and arithmetic operations: +, - (unary and binary), \* and /.
If field name followed by '+' they must be separated by space (to distinguish composite index name).
Fields of joined namespaces writes in form `joined_namespace.field`.

Abs() means absolute value of an argument.

Rank() means fulltext rank of match and is applicable only in fulltext query.

ST_Distance() means distance between geometry points (see [geometry subsection](#geometry)). The points could be columns in current or joined namespaces or fixed point in format `ST_GeomFromText("point(1 -3)")`

In SQL query sort expression must be quoted.

```go
type Person struct {
	Name string `reindex:"name"`
	Age  int    `reindex:"age"`
}

type City struct {
	Id                 int        `reindex:"id"`
	NumberOfPopulation int        `reindex:"population"`
	Center             [2]float64 `reindex:"center,rtree,linear"`
}

type Actor struct {
	ID          int        `reindex:"id"`
	PersonData  Person     `reindex:"person"`
	Price       int        `reindex:"price"`
	Description string     `reindex:"description,text"`
	BirthPlace  int        `reindex:"birth_place_id"`
	Location    [2]float64 `reindex:"location,rtree,greene"`
}
....

query := db.Query("actors").Sort("id", true)           // Sort by field
....
query = db.Query("actors").Sort("person.age", true)   // Sort by nested field
....
// Sort by joined field
query = db.Query("actors").
	Join(db.Query("cities")).On("birth_place_id", reindexer.EQ, "id").
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
    Sort("ST_Distance(cities.center, ST_GeomFromText(\"point(1 -3)\"))", true).
    Sort("ST_Distance(location, cities.center)", true)
....
// In SQL query:
iterator := db.ExecSQL ("SELECT * FROM actors ORDER BY person.name ASC")
....
iterator := db.ExecSQL ("SELECT * FROM actors WHERE description = 'ququ' ORDER BY 'rank() + id / 100' DESC")
....
iterator := db.ExecSQL ("SELECT * FROM actors ORDER BY 'ST_Distance(location, ST_GeomFromText(\"point(1 -3)\"))' ASC")
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

In case of non-indexed fields, setting it's value to a value of a different type will replace it completely; in case of indexed fields, it is only possible to convert it from adjacent type (integral types and bool), numeric strings (like "123456") to integral types and back. Setting indexed field to null resets it to a default value.

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
db.Query("items").Where("id", reindexer.EQ, 40).Set("field1", values).Update()
```

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
db.Query("items").Where("id", reindexer.EQ, 40).Drop("field1").Update()
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

To remove and item you should do the following:

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

Depends on amount changes in transaction there are 2 possible Commit strategies:

- Locked atomic update. Reindexer locks namespace and applying all changes under common lock. This mode is used with small amounts of changes.
- Copy & atomic replace. In this mode Reindexer makes namespace's snapshot, applying all changes to this snapshot, and atomically replaces namespace without lock

Data amount for choosing Commit strategy can be choose in namespaces config. pls refer [DBNamespacesConfig](describer.go#L277)

#### Implementation notes

1. Transaction object is not thread safe and can't be used from different goroutines.
2. Transaction object holds Reindexer's resources, therefore application should explicitly call Rollback or Commit, otherwise resources will leak
3. It is safe to call Rollback after Commit
4. It is possible ro call Query from transaction by call `tx.Query("ns").Exec() ...`. Only read-committed isolation is available. Changes made in active transaction is invisible to current and another transactions.

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

query.Exec ()
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
	// Sort query resuls by rating first, then by year
	query := db.Query("items").Sort("rating+year", true)

	// Sort query resuls by rating first, then by year, and put item where rating == 5 and year == 2010 first
	query := db.Query("items").Sort("rating+year", true,[]interface{}{5,2010})
```

For make query to the composite index, pass []interface{} to `.WhereComposite` function of Query builder:

```go
	// Get results where rating == 5 and year == 2010
	query := db.Query("items").WhereComposite("rating+year", reindexer.EQ,[]interface{}{5,2010})
```

### Aggregations

Reindexer allows to retrive aggregated results. Currently Average, Sum, Minimum, Maximum Facet and Distinct aggregations are supported.

- `AggregateMax` - get maximum field value
- `AggregateMin` - get manimum field value
- `AggregateSum` - get sum field value
- `AggregateAvg` - get averatge field value
- `AggregateFacet` - get fields facet value
- `Distinct` - get list of unique values of the field

In order to support aggregation, `Query` has methods `AggregateAvg`, `AggregateSum`, `AggregateMin`, `AggregateMax`, `AggregateFacet` and `Distinct` those should be called before the `Query` execution: this will ask reindexer to calculate data aggregations.
Aggregation Facet is applicable to multiple data columns and the result of that could be sorted by any data column or 'count' and cutted off by offset and limit.
In order to support this functionality method `AggregateFacet` returns `AggregationFacetRequest` which has methods `Sort`, `Limit` and `Offset`.

To get aggregation results, `Iterator` has method `AggResults`: it is available after query execution and returns slice of results.

Example code for aggregate `items` by `price` and `name`

```go

	query := db.Query("items")
	query.AggregateMax("price")
	query.AggregateFacet("name", "price").Sort("name", true).Sort("count", false).Offset(10).Limit(100)
	iterator := query.Exec()

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

### Searching in array fields with matching array indexes

Reindexer allows to search data in array fields when matching values have same indixes positions.
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
Where ("f1",EQ,1).Where ("f2",EQ,2)
```

finds all items of array `Elem[]` where `f1` is equal to 1 and `f2` is equal to 2.

`EqualPosition` function allows to search in array fields with equal indices.
Search like this:

```go
Where("f1", reindexer.GE, 5).Where("f2", reindexer.EQ, 100).EqualPosition("f1", "f2")
```

or

```sql
SELECT * FROM Namespace WHERE f1 >= 5 AND f2 = 100 EQUAL_POSITION(f1,f2);
```

finds all items of array `Elem[]` where `f1` is greater or equal to 5 and `f2` is equal to 100 `and` their indices are always equal (for instance, query returned 5 items where only 3rd element of array has appropriate values).

With complex expressions (expressions with brackets) equal_position() works only within a bracket:

```sql
SELECT * FROM Namespace WHERE (f1 >= 5 AND f2 = 100 EQUAL_POSITION(f1,f2)) OR (f3 = 3 AND f4 < 4 EQUAL_POSITION(f3,f4));
```

equal_position doesn't work with the following conditions: IS NULL, IS EMPTY and IN(with empty parameter list).

### Atomic on update functions

There are atomic functions, which executes under namespace lock, and therefore guarantes data consistency:

- serial - sequence of integer, useful for uniq ID generation
- timestamp - current time stamp of operation, useful for data syncronisation

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

Ttl indexes work only with int64 fields and store UNIX timestamp data. Items containig ttl index expire after `expire_after` seconds. Example of decalring TtlIndex in Golang:

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

It is just faster equalent of:

```go
	item := &Item{}
	json.Unmarshal ([]byte (`{"id":1,"name":"test"}`),item)
	db.Upsert ("items",item)
```

#### Get Query results in JSON format

In case of requiment to serialize results of Query in JSON format, then it is possible to improve performance by directly obtaining results in JSON format from reindexer. JSON serialization will be done by C++ code, without extra allocs/serialization in Go code.

```go
...
	iterator := db.Query("items").
		Select ("id","name").        // Filter output JSON: Select only "id" and "name" fields of items, another fields will be ommited
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
The deserialization is uses reflection, so it's speed is not optimal (in fact `CJSON` deserialization is ~3-10x faster than `JSON`, and ~1.2x faster than `GOB`), but perfrormance is still seriously limited by reflection overhead.

There are 2 ways to enable object cache:

- Provide DeepCopy interface
- Ask query return shared objects from cache

#### DeepCopy interface

If object is implements DeepCopy intreface, then reindexer will turn on object cache and use DeepCopy interface to copy objects from cache to query results. The DeepCopy interface is responsible to
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

There are availbale code generation tool [gencopy](../gencopy), which can automatically generate DeepCopy interface for structs.

#### Get shared objects from object cache (USE WITH CAUTION)

To speed up queries and do not allocate new objects per each query it is possible ask query return objects directly from object cache. For enable this behaviour, call `AllowUnsafe(true)` on `Iterator`.

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

By default maximum size of object cache is 256000 items for each namespace. To change maximim size use `ObjCacheSize` method of `NameapaceOptions`, passed
to OpenNamespace. e.g.

```go
	// Set object cache limit to 4096 items
	db.OpenNamespace("items_with_huge_cache", reindexer.DefaultNamespaceOptions().ObjCacheSize(4096), Item{})
```

### Geometry

The only supported geometry data type is 2D point, which implemented in Golang as `[2]float64`.

In SQL, a point can be created as `ST_GeomFromText("point(1 -3)")`.

The only supported request for geometry field is to find all points within a distance from a point.
`DWithin(field_name, point, distance)` as on example below.

Corresponding SQL function is `ST_DWithin(field_name, point, distance)`.

RTree index can be created for points. To do so, `rtree` and `linear`, `quadratic`, `greene` or `rstar` tags should be declared. `linear`, `quadratic`, `greene` or `rstar` means which algorithm of RTree construction would be used. Here algorithms are listed in order from optimized for insertion to optimized for search. But it depends on data. Test which is more appropriate for you. Default algorithm is `rstar`.

```go
type Item struct {
	id              int        `reindexer:"id,,pk"`
	pointIndexed    [2]float64 `reindexer:"point_indexed,rtree,linear"`
	pointNonIndexed [2]float64 `json:"point_non_indexed"`
}

query1 := db.Query("items").DWithin("point_indexed", [2]float64{-1.0, 1.0}, 4.0)
```

```SQL
SELECT * FROM items WHERE ST_DWithin(point_non_indexed, ST_GeomFromText("point(1 -3.5)"), 5.0);
```

## Logging, debug and profiling

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

### Debug queries

Another useful feature is debug print of processed Queries. To debug print queries details there are 2 methods:

- `db.SetDefaultQueryDebug(namespace string,level int)` - it globally enables print details of all queries by namespace
- `query.Debug(level int)` - print details of query execution
  `level` is level of verbosity:
- `reindexer.INFO` - will print only query conditions
- `reindexer.TRACE` - will print query conditions and execution details with timings

- `query.Explain ()` - calculate and store query execution details.
- `iterator.GetExplainResults ()` - return query execution details

### Profiling

Because reindexer core is written in C++ all calls to reindexer and their memory consumption are not visible for go profiler. To profile reindexer core there are cgo profiler available. cgo profiler now is part of reindexer, but it can be used with any another cgo code.

Usage of cgo profiler is very similar with usage of [go profiler](https://golang.org/pkg/net/http/pprof/).

1. Add import:

```go
import _ "github.com/restream/reindexer/pprof"
```

2. If your application is not already running an http server, you need to start one. Add "net/http" and "log" to your imports and the following code to your main function:

```go
go func() {
	log.Println(http.ListenAndServe("localhost:6060", nil))
}()
```

3. Run application with envirnoment variable `HEAPPROFILE=/tmp/pprof`
4. Then use the pprof tool to look at the heap profile:

```bash
pprof -symbolize remote http://localhost:6060/debug/cgo/pprof/heap
```

## Integration with other program languages

A list of connectors for work with Reindexer via other program languages (TBC later):

### Pyreindexer

1. Pyreindexer for Python (version >=3.6 is required).

Before installation reindexer-dev (version >= 2.10) should be installed. See [installation instructions](cpp_src/readme.md#Installation) for details.
For install run:

```bash
pip3 install pyreindexer
```

https://github.com/Restream/reindexer-py
https://pypi.org/project/pyreindexer/

## Limitations and known issues

Currently Reindexer is stable and production ready, but it is still a work in progress, so there are some limitations and issues:

- Internal C++ API is not stabilized and is subject to change.

## Getting help

You can get help in several ways:

1. Join Reindexer [Telegram group](https://t.me/reindexer)
2. Write [an issue](https://github.com/restream/reindexer/issues/new)
