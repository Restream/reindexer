# Reindexer benchmarks 


## Data
We are using dataset with 100K documents for this benchmarks. The documents containts 3 field, and have the following go struct representation:

```go
type Item struct {
	ID   int64  `reindex:"id,,pk" json:"id" db:"id"`
	Name string `reindex:"name" json:"name"  db:"name"`
	Year int    `reindex:"year,tree" json:"year" db:"year"`
}
```

The `ID` field is the primary key, and each document has an uniq ID in range from 0 to documents count. `Name` and `Year` fields are filled with some random data.
The easyjson library is used in benchmarks for unmarshaling documents to golang objects, except sqlite. For sqlite sqlx package was used.

## Results with Go 1.8.1 and clang-802.0.42 on a 2.7 GHz Intel Core i7 (MacBook Pro Retina 15-inch, Mid 2016):

The results of beches are compared with the following databases:
- Tarantool 1.7.4.18
- Elasticsearch 5.4.3
- Mongo 3.4.5
- Sqlite 3.17.0 from github.com/mattn/go-sqlite3/
- Boltdb from github.com/boltdb/bolt

## Get document by primary key (id).

In this benchmark documents are fetched from DB by ID and unmarshaling it to golang object.

```
BenchmarkElasticGetByID                     3000            369109 ns/op           91226 B/op        196 allocs/op
BenchmarkMongoGetByID                      10000            115321 ns/op            3004 B/op         93 allocs/op
BenchmarkTarantoolGetByID                  30000             56921 ns/op             688 B/op         19 allocs/op
BenchmarkRedisGetByID                      50000             36203 ns/op             662 B/op         15 allocs/op
BenchmarkSqliteGetByID                    100000             20795 ns/op            1330 B/op         42 allocs/op
BenchmarkReindexGetByID                   300000              4443 ns/op             351 B/op          5 allocs/op
BenchmarkBolt                             500000              3142 ns/op             935 B/op         14 allocs/op
```

## Get document by primary key (id) using object cache

Reindexer have an deserialized objects cache. The following benchmark exposes how it influences to performace. 
For other databases unmarshaler was not called after fetching documents from DB, and so there are no objects was returned.

```
BenchmarkRedisGetByIDNoObject              50000             32026 ns/op             263 B/op          9 allocs/op
BenchmarkSqliteGetByIDNoObject            100000             15916 ns/op             859 B/op         26 allocs/op
BenchmarkReindexGetByIDObjCache          1000000              2176 ns/op               0 B/op          0 allocs/op
BenchmarkBoltNoObject                    1000000              1641 ns/op             584 B/op          9 allocs/op
```

## Query documents with 1 condition

In this benchmark the query `SELECT ... FROM ... WHERE year > ? LIMIT 10` is executing, and fetched documents are marshaled to golang objects.


```
BenchmarkElastic1CondQuery                  3000            364084 ns/op           90152 B/op        174 allocs/op
BenchmarkMongo1CondQuery                   10000            173854 ns/op            7507 B/op        374 allocs/op
BenchmarkSqliteQuery1Cond                  30000             39794 ns/op            2512 B/op        113 allocs/op
BenchmarkReindexQuery1Cond                100000             20011 ns/op            3512 B/op         50 allocs/op
```

## Query documents with 1 condition, using object cache

The following benchmark exposes how object cache is influences to perormace.
For other databases unmarshaler was not called after fetching documents from DB, and so there are no objects was returned.

```
BenchmarkElasticQuery1CondNoObjects         5000            355908 ns/op           90121 B/op        173 allocs/op
BenchmarkMongoQuery1CondNoObjects          10000            155489 ns/op            6646 B/op        252 allocs/op
BenchmarkSqliteQuery1CondNoObjects         50000             31712 ns/op            1863 B/op         80 allocs/op
BenchmarkReindexQuery1CondObjCache        300000              3650 ns/op               0 B/op          0 allocs/op
```

## Query documents with 2 conditions

In this benchmark the query  `SELECT ... FROM ... WHERE year > ? AND name = ? LIMIT 10` is executing, and fetched documents are marshaled to golang objects.

```
BenchmarkElasticQuery2Cond                  3000            356729 ns/op           90152 B/op        174 allocs/op
BenchmarkMongoQuery2Cond                   10000            131600 ns/op            2878 B/op         63 allocs/op
BenchmarkSqliteQuery2Cond                  30000             49771 ns/op            2451 B/op        107 allocs/op
BenchmarkReindexQuery2Cond                 50000             25047 ns/op            3158 B/op         45 allocs/op
```

The following benchmark exposes how object cache is influences to perormace.
For other databases unmarshaler was not called after fetching documents from DB, and so there are no objects was returned.
```
BenchmarkElasticQuery2CondNoObjects         5000            356544 ns/op           90120 B/op        173 allocs/op
BenchmarkMongoQuery2CondNoObjects          10000            133504 ns/op            2878 B/op         63 allocs/op
BenchmarkSqliteQuery2CondNoObjects         30000             40768 ns/op            1875 B/op         78 allocs/op
BenchmarkReindexQuery2CondObjCache        200000             10004 ns/op              15 B/op          1 allocs/op
```
## Internal Reindexer benchmarks

Reindexer test suite contains additional benchmarks with various queries conditions, and complex document structures. This suite run on dataset of 500K documents with the following structure:

```golang
type TestItemBench struct {
	Prices  []*TestJoinItem `reindex:"prices,,joined"`
	Pricesx []*TestJoinItem `reindex:"pricesx,,joined"`
	//	TestItemBase
	ID         int      `reindex:"id,,pk"`
	Genre      int64    `reindex:"genre,tree"`
	Year       int      `reindex:"year,tree"`
	Packages   []int    `reindex:"packages,hash"`
	Countries  []string `reindex:"countries,tree"`
	Age        int      `reindex:"age,hash"`
	PricesIDs  []int    `reindex:"price_id"`
	LocationID string   `reindex:"location"`
	EndTime    int      `reindex:"end_time,-"`
	StartTime  int      `reindex:"start_time,tree"`
}
```
The obect cache is used by default in all tests. For more details about this benchmark see the reindexer_bench_test.go source file.


### Results of internal Reindexer benchmarks


```
BenchmarkSimpleInsert-8                   200000              5506 ns/op              55 B/op          3 allocs/op
BenchmarkSimpleUpdate-8                   200000              5794 ns/op              55 B/op          3 allocs/op
BenchmarkSimpleCmplxPKUpsert-8            200000              7110 ns/op              86 B/op          4 allocs/op
BenchmarkInsert-8                          50000             26300 ns/op             679 B/op          9 allocs/op
BenchmarkUpdate-8                          50000             31131 ns/op             597 B/op          9 allocs/op
BenchmarkDeleteAndUpdate-8                 30000             43956 ns/op             917 B/op         13 allocs/op
Benchmark4CondQuery-8                      50000             25552 ns/op               0 B/op          0 allocs/op
Benchmark4CondQueryTotal-8                 10000            108447 ns/op               0 B/op          0 allocs/op
Benchmark4CondRangeQuery-8                 50000             32038 ns/op               0 B/op          0 allocs/op
Benchmark4CondRangeQueryTotal-8            10000            171957 ns/op               0 B/op          0 allocs/op
Benchmark3CondQuery-8                     100000             11706 ns/op               0 B/op          0 allocs/op
Benchmark3CondQueryTotal-8                 10000            150312 ns/op               0 B/op          0 allocs/op
Benchmark3CondQueryKillIdsCache-8          10000            160523 ns/op             160 B/op          1 allocs/op
Benchmark3CondQueryRestoreIdsCache-8       50000             32632 ns/op               0 B/op          0 allocs/op
Benchmark2CondQuery-8                     200000              6582 ns/op               0 B/op          0 allocs/op
Benchmark2CondQueryTotal-8                 50000             34520 ns/op               0 B/op          0 allocs/op
Benchmark2CondQueryLeftJoin-8              20000             76520 ns/op            5888 B/op          2 allocs/op
Benchmark2CondQueryLeftJoinTotal-8         20000             89817 ns/op            5888 B/op          2 allocs/op
Benchmark2CondQueryInnerJoin-8             20000             74647 ns/op            5888 B/op          2 allocs/op
Benchmark2CondQueryInnerJoinTotal-8          500           2406532 ns/op            5899 B/op          2 allocs/op
Benchmark1CondQuery-8                     300000              4740 ns/op               0 B/op          0 allocs/op
Benchmark1CondQueryTotal-8                300000              4724 ns/op               0 B/op          0 allocs/op
BenchmarkByIdQuery-8                      500000              2173 ns/op               0 B/op          0 allocs/op
BenchmarkFullScan-8                          100          10094076 ns/op              38 B/op          0 allocs/op
```
