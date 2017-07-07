package benchmarks

import (
	"context"
	"encoding/json"
	"log"
	"reflect"
	"strconv"
	"testing"

	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/bindings/builtin"
	"github.com/boltdb/bolt"
	_ "github.com/mattn/go-sqlite3"
	tarantool "github.com/tarantool/go-tarantool"
	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v5"
)

func BenchmarkElasticGetByID(b *testing.B) {
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		if searchResult, err := elasticDB.Search().Index("items").
			Query(elastic.NewTermQuery("id", strconv.Itoa(i%itemsInDataSet))).
			From(0).Size(1).Do(ctx); err != nil {
			panic(err)
		} else {
			for _, item := range searchResult.Each(reflect.TypeOf(Item{})) {
				_ = item.(Item)
				break
			}
		}
	}
}

func BenchmarkMongoGetByID(b *testing.B) {
	c := mgoDb.C("items")
	for i := 0; i < b.N; i++ {
		it := &Item{}
		if err := c.Find(bson.M{"id": i}).One(it); err != nil {
			panic(err)
		}
	}
}

func BenchmarkTarantoolGetByID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := tarantulDb.Select("items", "primary", 0, 1, tarantool.IterEq, []interface{}{uint(i % itemsInDataSet)}); err != nil {
			panic(err)
		}
	}
}

func BenchmarkSqliteGetByID(b *testing.B) {
	stmt, err := sqliteDB.Preparex("select id,name,year from items where id = ?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < b.N; i++ {
		it := &Item{}
		if err = stmt.QueryRowx(i % itemsInDataSet).StructScan(it); err != nil {
			panic(err)
		}
	}
}

func BenchmarkSqliteGetByIDNoObject(b *testing.B) {
	stmt, err := sqliteDB.Preparex("select id from items where id = ?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < b.N; i++ {
		id := 0
		stmt.QueryRow(i % itemsInDataSet).Scan(&id)
	}
}

func BenchmarkReindexGetByID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, found := reindexDB.Query("items").WhereInt("id", reindexer.EQ, i%itemsInDataSet).NoObjCache().Get(); !found {
			panic("Item not found")
		}
	}
}

func BenchmarkReindexGetByIDObjCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, found := reindexDB.Query("items").WhereInt("id", reindexer.EQ, i%10000).Get(); !found {
			panic("Item not found")
		}
	}
}

func BenchmarkRedisGetByID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if res, err := redisDb.Get(strconv.Itoa(i)).Result(); err != nil {
			panic(err)
		} else {
			item := &Item{}
			err := json.Unmarshal([]byte(res), item)
			if err != nil {
				panic(err)
			}
		}
	}
}

func BenchmarkRedisGetByIDNoObject(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := redisDb.Get(strconv.Itoa(i)).Result(); err != nil {
			panic(err)
		}
	}
}

func BenchmarkBolt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		boltDb.View(func(tx *bolt.Tx) error {
			it := tx.Bucket([]byte("items")).Get(itob(i % itemsInDataSet))
			item := &Item{}
			err := json.Unmarshal(it, item)
			return err
		})
	}
}

func BenchmarkBoltNoObject(b *testing.B) {
	for i := 0; i < b.N; i++ {
		boltDb.View(func(tx *bolt.Tx) error {
			_ = tx.Bucket([]byte("items")).Get(itob(i % itemsInDataSet))
			return nil
		})
	}
}
