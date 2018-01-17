# Reindexer benchmarks 


## Data
We are using data set with 100K documents for this benchmarks. The documents containts 4 fields, and uses the following go struct representation:

```go
type Item struct {
	ID   int64  `reindex:"id,,pk" json:"id" db:"id"`
	Name string `reindex:"name" json:"name"  db:"name"`
	Year int    `reindex:"year,tree" json:"year" db:"year"`
	Description string `reindex:"description,text" json:"description"  db:"description"`
}
```

The `ID` field is the primary key. Each document in data set has an uniq `ID` in the range from 0 to documents count. `Name` and `Year` fields are filled with some random data, `Description` field is contains random text generated from 100k english words vocabulary.

## Results with Go 1.9.3 and clang-802.0.42 on a 2.7 GHz Intel Core i7 (MacBook Pro Retina 15-inch, Mid 2016):

The results of benchmarks are compared with the following databases:
- Tarantool 1.7.6.12
- Elasticsearch 5.4.3
- Mongo 3.4.5
- Sqlite 3.17.0 from github.com/mattn/go-sqlite3
- Sphinx 2.2.11
- Redis 2.3.9
- Mysql 5.7.19

All benchmarks are run in single CPU core

## Get document by primary key (id). Object cache is disabled

In this benchmark the documents are fetched from DB by ID and unmarshaled to golang objects.

```
benchmark                           iter               time/iter          bytes alloc               allocs
---------                           ----               ---------          -----------               ------
BenchmarkReindexGetByID           300000              4224 ns/op             567 B/op          3 allocs/op
BenchmarkSqliteGetByID             50000             23735 ns/op            2885 B/op         48 allocs/op
BenchmarkRedisGetByID              30000             47492 ns/op            2230 B/op         15 allocs/op
BenchmarkTarantoolGetByID          20000             64870 ns/op            2715 B/op         22 allocs/op
BenchmarkMysqlGetByID              20000             85500 ns/op            1664 B/op         32 allocs/op
BenchmarkMongoGetByID              10000            124410 ns/op            4623 B/op         99 allocs/op
BenchmarkElasticGetByID             2000            521843 ns/op           92979 B/op        191 allocs/op
```

## Get document by primary key (id). Object cache is enabled

Reindexer have an deserialized objects cache. The following benchmark exposes how it influences to performace.
For other databases unmarshaler was not called after fetching documents from DB, and so there are no objects was returned.

```
benchmark                           iter               time/iter          bytes alloc               allocs
---------                           ----               ---------          -----------               ------
BenchmarkReindexGetByIDUnsafe     500000              2530 ns/op               0 B/op          0 allocs/op
BenchmarkSqliteGetByIDNoObject    100000             20615 ns/op            2198 B/op         45 allocs/op
BenchmarkRedisGetByIDNoObject      50000             36324 ns/op             778 B/op          7 allocs/op
BenchmarkMysqlGetByIDNoObject      20000             80989 ns/op             992 B/op         29 allocs/op
```

## Query documents with 1 condition. Object cache is disbaled

In this benchmark the query `SELECT ... FROM ... WHERE year > ? LIMIT 10` is executing, and fetched documents are marshaled to golang objects.


```
benchmark                           iter               time/iter          bytes alloc               allocs
---------                           ----               ---------          -----------               ------
BenchmarkReindex1Cond             100000             17403 ns/op            5728 B/op         30 allocs/op
BenchmarkSqlite1Cond               20000             60749 ns/op           18256 B/op        155 allocs/op
BenchmarkTarantool1Cond            20000             78152 ns/op           14168 B/op        102 allocs/op
BenchmarkRedis1Cond                10000            126484 ns/op           20880 B/op        112 allocs/op
BenchmarkMysql1Cond                10000            135248 ns/op            7856 B/op        104 allocs/op
BenchmarkMongo1Cond                10000            174690 ns/op           24721 B/op        465 allocs/op
BenchmarkElastic1Cond               1000           1187006 ns/op          129663 B/op        379 allocs/op
```

## Query documents with 1 condition. Object cache is enabled

The following benchmark exposes how object cache is influences to perormace.
For other databases unmarshaler was not called after fetching documents from DB, and so there are no objects was returned.

```
benchmark                           iter               time/iter          bytes alloc               allocs
---------                           ----               ---------          -----------               ------
BenchmarkReindex1CondObjCache     300000              3738 ns/op               0 B/op          0 allocs/op
BenchmarkSqlite1CondNoObj          30000             47023 ns/op           12287 B/op        113 allocs/op
BenchmarkRedis1CondNoObject        30000             53056 ns/op            6400 B/op         32 allocs/op
BenchmarkTarantool1CondNoObj       20000             75840 ns/op           13608 B/op         92 allocs/op
BenchmarkMysql1CondNoObj           10000            118977 ns/op            1728 B/op         62 allocs/op
BenchmarkMongo1CondNoObj           10000            157125 ns/op           18528 B/op        303 allocs/op
BenchmarkElastic1CondNoObj          2000           1095203 ns/op          119107 B/op        283 allocs/op
```

## Query documents with 2 conditions. Object cache is disabled

In this benchmark the query  `SELECT ... FROM ... WHERE year > ? AND name = ? LIMIT 10` is executing, and fetched documents are marshaled to golang objects.

```
benchmark                           iter               time/iter          bytes alloc               allocs
---------                           ----               ---------          -----------               ------
BenchmarkReindex2Cond             100000             22531 ns/op            5193 B/op         27 allocs/op
BenchmarkSqlite2CondQuery          20000             61354 ns/op           18544 B/op        156 allocs/op
BenchmarkTarantool2Cond            20000             80595 ns/op           14884 B/op        105 allocs/op
BenchmarkMysql2CondQuery           10000            163772 ns/op            7008 B/op         96 allocs/op
BenchmarkMongo2Cond                 5000            230338 ns/op           24764 B/op        469 allocs/op
BenchmarkElastic2Cond               1000           1247815 ns/op          133573 B/op        426 allocs/op
```
## Query documents with 2 conditions. Object cache is enabled

The following benchmark exposes how object cache is influences to perormace.
For other databases unmarshaler was not called after fetching documents from DB, and so there are no objects was returned.
```
benchmark                           iter               time/iter          bytes alloc               allocs
---------                           ----               ---------          -----------               ------
BenchmarkReindex2CondObjCache     200000              7591 ns/op              15 B/op          1 allocs/op
BenchmarkSqlite2CondNoObj          30000             49181 ns/op           12224 B/op        114 allocs/op
BenchmarkTarantool2CondNoObj       20000             82000 ns/op           14329 B/op         95 allocs/op
BenchmarkMongo2CondNoObj            5000            215070 ns/op           18584 B/op        307 allocs/op
BenchmarkMysql2CondNoObj           10000            147743 ns/op            1792 B/op         62 allocs/op
BenchmarkElastic2CondNoObj          1000           1240421 ns/op          133578 B/op        426 allocs/op

```
## Full text search benchmarks

### Exact match test
BenchmarkReindexFullText           30000             39722 ns/op             109 B/op          1 allocs/op
BenchmarkMysqlFullText              5000            294697 ns/op            7715 B/op        106 allocs/op
BenchmarkSphinxFullText             2000            529136 ns/op            2015 B/op         21 allocs/op
BenchmarkMongoFullText              2000            586470 ns/op           24613 B/op        467 allocs/op
BenchmarkElasticFullText            1000           1245806 ns/op          129806 B/op        372 allocs/op
BenchmarkSqliteFullText             1000           1274902 ns/op           11528 B/op         54 allocs/op

### Fuzzy match test

benchmark                           iter               time/iter          bytes alloc               allocs
---------                           ----               ---------          -----------               ------
BenchmarkReindexFullText3Fuzzy      5000            425837 ns/op             142 B/op          1 allocs/op
BenchmarkMysqlFullText3Fuzzy        3000            481149 ns/op            7727 B/op        106 allocs/op
BenchmarkElasticFullText3Fuzzy      1000           1565921 ns/op          129932 B/op        373 allocs/op
BenchmarkSphinxFullText3Fuzzy       1000           1693271 ns/op            2190 B/op         22 allocs/op
BenchmarkSqliteFullText3Fuzzy       1000           2505292 ns/op           11647 B/op         55 allocs/op

