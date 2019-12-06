
# Reindexer

[![GoDoc](https://godoc.org/github.com/Restream/reindexer?status.svg)](https://godoc.org/github.com/Restream/reindexer)
 [![Build Status](https://travis-ci.org/Restream/reindexer.svg?branch=master)](https://travis-ci.org/Restream/reindexer)
 [![Build Status](https://ci.appveyor.com/api/projects/status/yonpih8vx3acaj86?svg=true)](https://ci.appveyor.com/project/olegator77/reindexer)

**Reindexer** is an embeddable, in-memory, document-oriented database with a high-level Query builder interface.

Reindexer's goal is to provide fast search with complex queries. We at Restream weren't happy with Elasticsearch and created Reindexer as a more performant alternative.

The core is written in C++ and the application level API is in Go.

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
	- [Complex Primary Keys and Composite Indexes](#complex-primary-keys-and-composite-indexes)
	- [Sort](#sort)
	- [Join](#join)
		- [Joinable interface](#joinable-interface)
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
- [Logging, debug and profiling](#logging-debug-and-profiling)
	- [Turn on logger](#turn-on-logger)
	- [Debug queries](#debug-queries)
	- [Profiling](#profiling)
	- [Prometheus](#prometheus)
- [Maintenance](#maintenance)
    - [Web interface](#web-interface)
    - [Command line tool](#command-line-tool)
    - [Dump and restore database](#dump-and-restore-database)
    - [Replication](#replication)
- [Security](#security)
- [Alternative storages](#alternative-storages)
    - [RockDB](#rocksdb)
- [Integration with other program languages](#integration-with-other-program-languages)
	- [Pyreindexer](#pyreindexer)
	- [HTTP REST API](#http-rest-api)
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

-	Documents and indices are stored in dense binary C++ structs, so they don't impose any load on Go's garbage collector.

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
	// db := reindexer.NewReindex("cproto://127.0.0.1:6534/testdb", reindexer.WithCreateDBIfMissing())

	// OR - Init a database instance and choose the binding (builtin, with bundled server)
	// serverConfig := config.DefaultServerConfig ()
	// db := reindexer.NewReindex("builtinserver://testdb",reindexer.WithServerConfig(100*time.Second, serverConfig))

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
Please note, that Query builder interface is prefferable way: It have more features, and faster than SQL interface

## Installation

Reindexer can run in 3 different modes:
 - `embeded (builtin)` Reindexer is embeded into application as static library, and does not reuqire separate server proccess.
 - `embeded with server (builtinserver)` Reindexer is embeded into application as static library, and start server. In this mode other
 clients can connect to application via cproto or http.
 - `standalone` Reindexer run as standalone server,  application connects to Reindexer via network

### Installation for server mode

 1. [Install Reindexer Server](cpp_src/readme.md#installation)
 2. go get -a github.com/restream/reindexer

#### Official docker image

The simplest way to get reindexer server, is pulling & run docker image from [dockerhub](https://hub.docker.com/r/reindexer/reindexer/).

````bash
docker run -p9088:9088 -p6534:6534 -it reindexer/reindexer
````

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
    - `hash` – fast select by EQ and SET match. Does not allow sorting results by field. Used by default. Allows *slow* and uneffecient sorting by field
    - `tree` – fast select by RANGE, GT, and LT matches. A bit slower for EQ and SET matches than `hash` index. Allows fast sorting results by field.
    - `text` – full text search index. Usage details of full text search is described [here](fulltext.md)
    - `-` – column index. Can't perform fast select because it's implemented with full-scan technic. Has the smallest memory overhead.
    - `ttl` - TTL index that works only with int64 fields. These indexes are quite convenient for representation of date fields (stored as UNIX timestamps) that expire after specified amount of seconds.
- `opts` – additional index options:
    - `pk` – field is part of a primary key. Struct must have at least 1 field tagged with `pk`
    - `composite` – create composite index. The field type must be an empty struct: `struct{}`.
    - `joined` – field is a recipient for join. The field type must be `[]*SubitemType`.
	- `dense` - reduce index size. For `hash` and `tree` it will save 8 bytes per unique key value. For `-` it will save 4-8 bytes per each element. Useful for indexes with high sectivity, but for `tree` and `hash` indexes with low selectivity can seriously decrease update performance. Also `dense` will slow down wide fullscan queries on `-` indexes, due to lack of CPU cache optimization.
	- `sparse` - Row (document) contains a value of Sparse index only in case if it's set on purpose - there are no empty (or default) records of this type of indexes in the row (document). It allows to save RAM but it will cost you performance - it works a bit slower than regular indexes.
	- `collate_numeric` - create string index that provides values order in numeric sequence. The field type must be a string.
	- `collate_ascii` - create case-insensitive string index works with ASCII. The field type must be a string.
	- `collate_utf8` - create case-insensitive string index works with UTF8. The field type must be a string.
	- `collate_custom=<ORDER>` - create custom order string index. The field type must be a string. `<ORDER>` is sequence of letters, which defines sort order.

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

Reindexer can sort documents by fields (including nested) or by expressions in asceding or descending order.

Sort expressions can contain fields names (including nested) of int, float or bool type, numbers, function rank(), parenthesis and arithmetic operations: +, - (unary and binary), * and /.
Fields names must end by space (before close parenthesis space is optional).
Rank() means fulltext rank of match and is applicable only in fulltext query.
In SQL query sort expression must be quoted.

```go
type Person struct {
	Name string `reindex:"name"`
	Age  int    `reindex:"age"`
}

type Actor struct {
	ID          int    `reindex:"id"`
	PersonData  Person `reindex:"person"`
	Price       int    `reindex:"price"`
	Description string `reindex:"description,text"`
}
....

query := db.Query("actors").Sort("id", true)           // Sort by field
....
query = db.Query("actors").Sort("person.age", true)   // Sort by nested field
....
// Sort by expression:
query = db.Query("actors").Sort("person.age / -10 + price / 1000 * (id - 5)", true)
....
query = db.Query("actors").Where("description", reindexer.EQ, "ququ").
    Sort("rank() + id / 100", true)   // Sort with fulltext rank
....
// In SQL query:
iterator := db.ExecSQL ("SELECT * FROM actors ORDER BY person.name ASC")
....
iterator := db.ExecSQL ("SELECT * FROM actors WHERE description = 'ququ' ORDER BY 'rank() + id / 100' DESC")
```

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

Reindexer allows to retrive aggregated results. Currently Average, Sum, Minimum, Maximum and Facet aggregations are supported.
- `AggregateMax` - get maximum field value
- `AggregateMin` - get manimum field value
- `AggregateSum` - get sum field value
- `AggregateAvg` - get averatge field value
- `AggregateFacet` - get fields facet value

In order to support aggregation, `Query` has methods `AggregateAvg`, `AggregateSum`, `AggregateMin`, `AggregateMax` and `AggregateFacet` those should be called before the `Query` execution: this will ask reindexer to calculate data aggregations.
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
{"root_object":[{"id":1,"name":"test"}]}
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

### Prometheus

Reindexer has a banch prometheus metrics available via http-URL `/metrics`. This metrics may be enabled by passing `--prometheus` as reindexer_server command line argument or by setting `metrics:prometheus` flag in server yaml-config file. Some of the metrics also require
`perfstats` to be enabled in `profiling`-config

`reindexer_qps_total` - total queries per second for each database, namespace and query type
`reindexer_avg_latency` - average queryies latency for each database, namespace and query type
`reindexer_caches_size_bytes`, `reindexer_indexes_size_bytes`, `reindexer_data_size_bytes` - caches, indexes and data size for each namespace
`reindexer_items_count` - items count in each namespace
`reindexer_memory_allocated_bytes` - current amount of dynamicly allocated memory according to tcmalloc/jemalloc
`reindexer_rpc_clients_count` - current number of RPC clients for each database
`reindexer_input_traffic_total_bytes`, `reindexer_output_traffic_total_bytes` - total input/output RPC/http traffic for each database

## Maintenance

For maintenance and work with data, stored in reindexer database there are 2 methods available:

- Web interface
- Command line tool

### Web interface

Reindexer server and `builtinserver` binding mode are coming with Web UI out-of-the box. To open web UI just start reindexer server
or application with `builtinserver` mode, and open http://server-ip:9088/face in browser

### Command line tool

To work with database from command line you can use reindexer [command line tool](cpp_src/cmd/reindexer_tool/readme.md)
Command line tool have the following functions

- Backup whole database into text file or console.
- Make queries to database
- Modify documents and DB metadata

Command line tool can run in 2 modes. With server via network, and in server-less mode, directly with storage.

### Dump and restore database

Database creation via reindexer_tool:
```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/mydb --command '\databases create mydb'
```

To dump and restore database in normal way there reindexer command line tool is used

Backup whole database into single backup file:
```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/mydb --command '\dump' --output mydb.rxdump
```

Restore database from backup file:
```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/mydb --filename mydb.rxdump
```
### Replication

Reindexer supports master slave replication. To create slave DB the following command can be used:

```sh
reindexer_tool --dsn cproto://127.0.0.1:6534/slavedb --command '\upsert #config {"type":"replication","replication":{"role":"slave","master_dsn":"cproto://127.0.0.1:6534/masterdb","cluster_id":2}}'
```

More details about replication is [here](replication.md)

## Security

Reindexer server supports login/password authorization for http/rpc client with different access levels for each user/database. To enable this feature `security` flag should be set in server.yml.
If security option is active reindexer will try to load users list from `users.yml` or `users.json`(deprecate) found in database path. If users-file was not found the default one
will be created automaticly (default login/password are `reindexer`/`reindexer`)

## Alternative storages

A list of storages, which may be used by reindexer as an alternative for LevelDB.

Storage type may be selected by passing command line option to reindexer_server like this:

```sh
reindexer_server --db /tmp/rx --engine rocksdb
```

Also storage type may be set via server's `config.yml`:

```yaml
storage:
  engine: leveldb
```

To configure storage type for Go bindings either `bindings.ConnectOptions` (for builtin) or `confg.ServerConfig` (for builtinserver) structs may be used.

### RocksDB

Reindexer will try to autodetect RocksDB library and it's dependencies at compile time if CMake flag `ENABLE_ROCKSDB` was passed (enabled by default).
If reindexer library was built with rocksdb, it requires Go build tag `rocksdb` in order to link with go-applications and go-bindinds.

## Integration with other program languages

A list of connectors for work with Reindexer via other program languages (TBC later):

### Pyreindexer

1. [Pyreindexer](connectors/py_reindexer) for Python (version >=3.6 is required). For setup run:


```bash
pip3 install git+https://github.com/Restream/reindexer.git

```

### HTTP REST API

The simplest way to use reindexer with any program language - is using REST API. The
[complete REST API documentation is here](cpp_src/server/contrib/server.md).  
[Or explore interactive version of Reindexer's swagger documentation](https://editor.swagger.io/?url=https://raw.githubusercontent.com/Restream/reindexer/master/cpp_src/server/contrib/server.yml)


## Limitations and known issues

Currently Reindexer is stable and production ready, but it is still a work in progress, so there are some limitations and issues:

- Internal C++ API is not stabilized and is subject to change.

## Getting help

You can get help in several ways:

1. Join Reindexer [Telegram group](https://t.me/reindexer)
2. Write [an issue](https://github.com/restream/reindexer/issues/new)
