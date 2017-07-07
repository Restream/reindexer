# Reindexer

**Reindexer** is an embeddable, in-memory, document-oriented database with a high-level Query builder interface.

Reindexer's goal is to provide fast search with complex queries. We at Restream weren't happy with Elasticsearch and created Reindexer as a more performant alternative.

The core is written in C++ and the application level API is in Go. 

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

- There is an object cache on the Go level for deserialized documents produced after query execution. Future queries use pre-deserialized documents, which cuts repeated deserialization and allocation costs.

- The Query interface uses `sync.Pool` for reusing internal structures and buffers. 
Combining of these techings let's Reindexer execute most of queries without any allocations.


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
)

// Define struct with reindex tags
type Item struct {
	ID       int64  `reindex:"id,,pk"`    // 'id' is primary key
	Name     string `reindex:"name"`      // add index by 'name' field
	Articles []int  `reindex:"articles"`  // add index by articles 'articles' array
	Year     int    `reindex:"year,tree"` // add sortable index by 'year' field
}

func main() {
	// Init a database instance and choose the binding
	db := reindexer.NewReindex("builtin")

	// Enable persistent storage (optional)
	db.EnableStorage("/tmp/reindex/")

	// Create new namespace with name 'items', which will store structs of type 'Item'
	db.NewNamespace("items", reindexer.DefaultNamespaceOptions(), Item{})

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

	// `defer query.Close` is optional but lets Reindexer reuse internal buffers, and avoid extra allocs
	defer query.Close()
	// Execute the query and return an iterator
	iterator := query.Exec()

	// Check the error
	if err := iterator.Error(); err != nil {
		panic(err)
	}

	fmt.Println("Found", query.GetTotal(), "total documents, first", iterator.Len(), "documents:")

	// Iterate over results
	for iterator.Next() {
		// Get the next document and cast it to a pointer
		elem := iterator.Ptr().(*Item)
		fmt.Println(*elem)
	}
}
``` 

## Installation
### Prerequirements

Reindexer's core is written in C++11 and uses LevelDB as the storage backend, so the C++11 toolchain and LevelDB must be installed before installing Reindexer. 

To build Reindexer, g++ 4.7+ or clang 3.3+ is required.


#### OSX

Assuming [homebrew](https://brew.sh/index.html) is installed:

```bash
brew install leveldb snappy
```


#### Ubuntu 14.04+, Debian 7+

```bash
sudo apt-get install -y build-essential g++ libsnappy-dev make curl
curl -L https://github.com/google/leveldb/archive/v1.20.tar.gz | tar xzv 
cd leveldb-1.20 && make -j4 && sudo mv out-static/libleveldb.* /usr/local/lib
cd include && sudo cp -R leveldb /usr/local/include
sudo ldconfig
```

#### Fedora, RHEL, Centos 7+
```bash
sudo yum install -y gcc-c++ make snappy-devel findutils curl
curl -L https://github.com/google/leveldb/archive/v1.20.tar.gz | tar xzv 
cd leveldb-1.20 && make -j4 && sudo mv out-static/libleveldb.* /usr/local/lib
cd include && sudo cp -R leveldb /usr/local/include
sudo ldconfig
```

#### Centos 6
```bash
sudo yum -y install centos-release-scl
sudo yum -y install devtoolset-4-gcc devtoolset-4-gcc-c++ make snappy-devel findutils curl
source scl_source enable devtoolset-4
curl -L https://github.com/google/leveldb/archive/v1.20.tar.gz | tar xzv 
cd leveldb-1.20 && make -j4 && sudo mv out-static/libleveldb.* /usr/local/lib
cd include && sudo cp -R leveldb /usr/local/include
sudo ldconfig
```

### Get Reindexer

```bash
go get -a github.com/restream/reindexer
go generate github.com/restream/reindexer
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

    - `hash` – fast select by EQ and SET match. Does not allow sorting results by field. Used by default.
    - `tree` – fast select by RANGE, GT, and LT matches. A bit slower for EQ and SET matches than `hash` index. Allows sorting results by field.
    - `fulltext` – full text search index. 
    - `text` – simple and fast full text search index.
    - `-` – column index. Can't perform fast select because it's implemented with full-scan technic. Has the smallest memory overhead.
- `opts` – additional index options:
    - `pk` – field is part of a primary key.
    - `composite` – create composite index. The field type must be an empty struct: `struct{}`.
    - `joined` – field is a recipient for join. The field type must be `[]*SubitemType`.


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

### Join

Reindexer can join documents from multiple namespaces into a single result:

```go
type Actor struct {
	ID        int    `reindex:"id"`
	Name      string `reindex:"name"`
	IsVisible bool   `reindex:"is_visible"`
}

type ItemWithJoin struct {
	ID        int      `reindex:"id"`
	Name      string   `reindex:"name"`
	ActorsIDs []int    `reindex:"actors_ids"`
	Actors    []*Actor `reindex:"actors,,joined"`
}
....
    
	query := db.Query("items_with_join").Join(
		db.Query("actors").
			WhereBool("is_visible", reindexer.EQ, true),
		"actors",
	).On("id", reindexer.SET, "actors_ids")

	query.Exec ()
```

In this example, Reindexer uses reflection under the hood to create Actor slice and copy Actor struct. 


#### Joinable and Clonable inerfaces

To avoid using reflection, `Item` can implement `Joinable` and `Clonable` interfaces. If they are implemented, Reindexer uses them instead of the slow reflection-based implementation. This increases overall performance by 10-20%, and reduces the amount of allocations.


```go
// Clonable interface implementation.
// ClonePtrSlice must create slice of pointers to object copies.
// WARNING: ClonePtrSlice is called under the Reindexer namespace lock. Therefore calls from ClonePtrSlice to any Reindexer function can produce deadlock.
func (item *ItemWithJoin) ClonePtrSlice(src []interface{}) []interface{} {
	// Create a single slice of objects to avoid separate allocation of each object
	objSlice := make([]ItemWithJoin, 0, len(src))
	for i := 0; i < len(src); i++ {
		objSlice = append(objSlice, *src[i].(*ItemWithJoin))
	}

	// Fill the slice with pointers to elements.
	// It is possible to reuse original src slice.
	for i := 0; i < len(src); i++ {
		src[i] = &objSlice[i]
	}
	return src
}

// Joinable interface implementation.
// Join adds items from the joined namespace to the `ItemWithJoin` object.
// Join is called from lock-free context, so calling Reindexer functions from Join is safe.
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


### Complex Primary Keys and Composite Indices

A Document can have multiple fields as its primary key. Reindexer checks unique composition of all pk fields during upserts:

```go
type Item struct {
	ID    int64 `reindex:"id,,pk"`     // 'id' is a part of primary key
	SubID int   `reindex:"sub_id,,pk"` // 'sub_id' is a part of primary key
	// Fields
	//	....
}
```

Too complex primary key (>2 fields) can slow down upsert and select operations, because Reindexer has to do separate selects to each index, and intersect results.

Composite index feature exists to speed things up. Reindexer can create composite index using multiple fields and use it instead of several separate indices.

```go
type Item struct {
	ID       int64 `reindex:"id,-,pk"`         // 'id' is part of primary key, WITHOUT personal searchable index
	SubID    int   `reindex:"sub_id,-,pk"`     // 'sub_id' is part of primary key, WITHOUT personal searchable index
	SubSubID int   `reindex:"sub_sub_id,-,pk"` // 'sub_sub_id' is part of primary key WITHOUT personal searchable index

	// Fields
	// ....

	// Composite index
	_ struct{} `reindex:"id+sub_id+sub_sub_id,,composite"`
}

```

Also composite indices are useful for sorting results by multiple fields: 

```go
type Item struct {
	ID     int64 `reindex:"id,-,pk"`
	Rating int   `reindex:"rating"`
	Year   int   `reindex:"year"`

	// Composite index
	_ struct{} `reindex:"rating+year,tree,composite"`
}

...
	// Sort query resuls by rating first, then by year
	query := db.Query("items").Sort("rating+year", true)

```


## Limitations and known issues

Currently Reindexer is stable and production ready, but it is still a work in progress, so there are some limitations and issues:

- Current `Iterator` implementation is not optimal and always deserializes all documents returned by Query. Therefore queries which return big datasets without `.Limit ()` can be pretty slow.
- Slow switching between write and read modes. For example, a loop with a single upsert followed by a select will run slowly. 
- There is no standalone server mode. Only embeded (`builtin`) binding is supported for now.
- Internal C++ API is not stabilized and is subject to change.
- It's impossible to change index structure if it already contains data. If index structure changes, the storage is dropped or an error is returned (depends on the namespace options; default behavior is drop the storage).
