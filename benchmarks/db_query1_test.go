package benchmarks

import (
	"context"
	"encoding/json"
	"log"
	"reflect"
	"testing"

	"github.com/boltdb/bolt"

	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/restream/reindexer"
)

func BenchmarkElastic1CondQuery(b *testing.B) {
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		if searchResult, err := elasticDB.Search().Index("items").
			Query(elastic.NewTermQuery("name", randString())).
			From(0).Size(10).Do(ctx); err != nil {
			panic(err)
		} else {
			for _, item := range searchResult.Each(reflect.TypeOf(Item{})) {
				_ = item.(Item)
			}
		}
	}
}

func BenchmarkElasticQuery1CondNoObjects(b *testing.B) {
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		if _, err := elasticDB.Search().Index("items").
			Query(elastic.NewTermQuery("name", randString())).
			From(0).Size(10).Do(ctx); err != nil {
			panic(err)
		} else {
		}
	}
}

func BenchmarkMongo1CondQuery(b *testing.B) {
	c := mgoDb.C("items")
	for i := 0; i < b.N; i++ {
		it := c.Find(bson.M{"name": randString()}).Limit(10).Iter()
		item := &Item{}
		for it.Next(item) {
		}
	}
}

func BenchmarkMongoQuery1CondNoObjects(b *testing.B) {
	c := mgoDb.C("items")
	for i := 0; i < b.N; i++ {
		it := c.Find(bson.M{"name": randString()}).Limit(10).Iter()
		item := &Item{}
		it.Next(item)
		it.Close()
	}
}

func BenchmarkSqliteQuery1Cond(b *testing.B) {
	stmt, err := sqliteDB.Preparex("select id,name,year from items where year > ? limit 10")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < b.N; i++ {
		rows, err := stmt.Queryx(2010)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			it := &Item{}
			if err = rows.StructScan(it); err != nil {
				panic(err)
			}
		}
		rows.Close()
	}
}
func BenchmarkSqliteQuery1CondNoObjects(b *testing.B) {
	stmt, err := sqliteDB.Preparex("select id,name,year from items where year > ? limit 10")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < b.N; i++ {
		rows, err := stmt.Queryx(2010)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
		}
		rows.Close()
	}
}

func BenchmarkReindexQuery1Cond(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := reindexDB.Query("items").
			WhereInt("year", reindexer.GT, 2010).
			NoObjCache().
			Limit(10)
		it := q.Exec()
		if it.Error() != nil {
			panic(it.Error())
		}
		for it.Next() {
			obj := it.Ptr().(*Item)
			_ = obj
		}
		q.Close()
	}
}

func BenchmarkReindexQuery1CondObjCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := reindexDB.Query("items").
			WhereInt("year", reindexer.GT, 2010).
			Limit(10)
		it := q.Exec()
		if it.Error() != nil {
			panic(it.Error())
		}
		for it.Next() {
			obj := it.Ptr().(*Item)
			_ = obj
		}
		q.Close()
	}
}
func BenchmarkBoltIterateKeys(b *testing.B) {
	for i := 0; i < b.N; i++ {
		boltDb.View(func(tx *bolt.Tx) error {

			c := tx.Bucket([]byte("items")).Cursor()

			c.Seek(itob(i * 10 % 100000))
			count := 0

			for k, v := c.First(); k != nil && count < 10; k, v = c.Next() {
				item := &Item{}
				if err := json.Unmarshal(v, item); err != nil {
					panic(err)
				}
				count++
			}
			return nil
		})
	}
}

func BenchmarkBoltIterateKeysNoObjects(b *testing.B) {
	for i := 0; i < b.N; i++ {
		boltDb.View(func(tx *bolt.Tx) error {

			c := tx.Bucket([]byte("items")).Cursor()

			c.Seek(itob(i * 10 % 100000))
			count := 0

			for k, v := c.First(); k != nil && count < 10; k, v = c.Next() {
				_ = v
				count++
			}
			return nil
		})
	}
}
