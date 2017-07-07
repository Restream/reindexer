package benchmarks

import (
	"context"
	"log"
	"reflect"
	"testing"

	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/restream/reindexer"
)

func BenchmarkElasticQuery2Cond(b *testing.B) {
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

func BenchmarkElasticQuery2CondNoObjects(b *testing.B) {
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

func BenchmarkMongoQuery2Cond(b *testing.B) {
	c := mgoDb.C("items")
	for i := 0; i < b.N; i++ {
		it := c.Find(bson.M{"name": randString(), "year": "2010"}).Limit(10).Iter()
		item := &Item{}
		for it.Next(item) {
		}
	}
}

func BenchmarkMongoQuery2CondNoObjects(b *testing.B) {
	c := mgoDb.C("items")
	for i := 0; i < b.N; i++ {
		it := c.Find(bson.M{"name": randString(), "year": "2010"}).Limit(10).Iter()
		item := &Item{}
		it.Next(item)
		it.Close()
	}
}

func BenchmarkSqliteQuery2Cond(b *testing.B) {
	stmt, err := sqliteDB.Preparex("select id,name,year from items where year > ? and name = ? limit 10")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < b.N; i++ {
		rows, err := stmt.Queryx(2010, randString())
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
func BenchmarkSqliteQuery2CondNoObjects(b *testing.B) {
	stmt, err := sqliteDB.Preparex("select id,name,year from items where year > ? and name = ? limit 10")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < b.N; i++ {
		rows, err := stmt.Queryx(2010, randString())
		if err != nil {
			panic(err)
		}
		for rows.Next() {
		}
		rows.Close()
	}
}

func BenchmarkReindexQuery2Cond(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := reindexDB.Query("items").
			WhereInt("year", reindexer.GT, 2010).
			WhereString("name", reindexer.EQ, randString()).
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

func BenchmarkReindexQuery2CondObjCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := reindexDB.Query("items").
			WhereInt("year", reindexer.GT, 2010).
			WhereString("name", reindexer.EQ, randString()).
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
