package benchmarks

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"strconv"

	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/bindings/builtin"
	"github.com/boltdb/bolt"
	"github.com/go-redis/redis"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	tarantool "github.com/tarantool/go-tarantool"
	mgo "gopkg.in/mgo.v2"
	elastic "gopkg.in/olivere/elastic.v5"
)

const itemsInDataSet = 100000

var tarantulDb *tarantool.Connection
var reindexDB *reindexer.Reindexer
var elasticDB *elastic.Client
var sqliteDB *sqlx.DB
var redisDb *redis.Client
var mgoDb *mgo.Database
var boltDb *bolt.DB

func initElastic() {
	var err error
	if elasticDB, err = elastic.NewClient(); err != nil {
		panic(err)
	}
}

func seedElastic() {
	log.Printf("Seeding data to Elastic")
	ctx := context.Background()
	elasticDB.DeleteIndex("items").Do(ctx)
	if _, err := elasticDB.CreateIndex("items").Do(ctx); err != nil {
		panic(err)
	}

	bulkRequest := elasticDB.Bulk()
	for i := 0; i < itemsInDataSet; i++ {
		bulkRequest = bulkRequest.Add(elastic.NewBulkIndexRequest().Index("items").Type("items").Id(strconv.Itoa(i)).Doc(newItem(i)))
	}
	if _, err := bulkRequest.Do(ctx); err != nil {
		panic(err)
	}
}

func initSqlite() {
	os.Remove("/tmp/foo.db")

	var err error
	if sqliteDB, err = sqlx.Connect("sqlite3", "/tmp/foo.db?mode=memory&cache=shared"); err != nil {
		panic(err)
	}

	sqlStmt := `
	CREATE TABLE items (
		id INTEGER NOT NULL PRIMARY KEY, 
		name TEXT,
		year INTEGER
		articles TEXT
	);
	CREATE INDEX index_name ON items(name);
	CREATE INDEX index_year ON items(year);
	`
	if _, err = sqliteDB.Exec(sqlStmt); err != nil {
		panic(err)
	}
}

func seedSqlite() {
	log.Printf("Seeding data to Sqlite")
	tx, err := sqliteDB.Begin()
	if err != nil {
		panic(err)
	}
	stmt, err := tx.Prepare("INSERT INTO items(id, name,year) VALUES (?,?,?)")
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	for i := 0; i < itemsInDataSet; i++ {
		it := newItem(i)
		if _, err = stmt.Exec(i, it.Name, it.Year); err != nil {
			panic(err)
		}
	}
	tx.Commit()
}

func initRedis() {
	redisDb = redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "", DB: 0})
	if _, err := redisDb.Ping().Result(); err != nil {
		log.Fatal(err)
	}
}

func seedRedis() {
	log.Printf("Seeding data to Redis")
	bulk := make([]interface{}, 0)
	for i := 0; i < itemsInDataSet; i++ {
		item := newItem(i)
		bulk = append(bulk, strconv.Itoa(i))
		json, err := json.Marshal(item)
		if err != nil {
			panic(err)
		}
		bulk = append(bulk, string(json))
	}
	if err := redisDb.MSet(bulk...).Err(); err != nil {
		panic(err)
	}
}

func initMongo() {
	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		panic(err)
	}
	session.SetMode(mgo.Monotonic, true)
	session.DB("test").DropDatabase()
	mgoDb = session.DB("test")
}

func seedMongo() {
	c := mgoDb.C("items")

	c.EnsureIndexKey("id")
	c.EnsureIndexKey("name")
	c.EnsureIndexKey("year")

	log.Printf("Seeding data to mongo")
	bulk := c.Bulk()

	for i := 0; i < itemsInDataSet; i++ {
		bulk.Insert(newItem(i))
	}
	if _, err := bulk.Run(); err != nil {
		panic(err)
	}
}

func initReindex() {

	os.RemoveAll("/tmp/reindex")

	reindexDB = reindexer.NewReindex("builtin")

	reindexDB.EnableStorage("/tmp/reindex/")
	reindexDB.NewNamespace("items", reindexer.DefaultNamespaceOptions(), Item{})
}

func seedReindex() {
	log.Printf("Seeding data to Reindex")
	for i := 0; i < itemsInDataSet; i++ {
		if err := reindexDB.Upsert("items", newItem(i)); err != nil {
			panic(err)
		}
	}
}

func initTarantool() {
	var err error
	if tarantulDb, err = tarantool.Connect("localhost:3301", tarantool.Opts{User: "guest"}); err != nil {
		panic(err)
	}
}

func seedTarantool() {
	log.Printf("Seeding data to Tarantool")
	for i := 0; i < itemsInDataSet; i++ {
		it := newItem(i)
		if _, err := tarantulDb.Insert("items", []interface{}{int(it.ID), it.Name, it.Year}); err != nil {
			panic(err)
		}
	}
}

func initBolt() {
	var err error

	os.RemoveAll("/tmp/bolt")
	if boltDb, err = bolt.Open("/tmp/bolt", 0600, nil); err != nil {
		panic(err)
	}

	boltDb.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket([]byte("items")); err != nil {
			panic(err)
		}
		return nil
	})
}

func seedBolt() {
	log.Printf("Seeding data to Bolt")
	boltDb.Update(func(tx *bolt.Tx) error {
		for i := 0; i < itemsInDataSet; i++ {
			b := tx.Bucket([]byte("items"))
			buf, err := json.Marshal(newItem(i))
			if err != nil {
				return err
			}

			if err = b.Put(itob(int(i)), buf); err != nil {
				return err
			}
		}
		return nil
	})
}

func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func init() {
	initMongo()
	initTarantool()
	initBolt()
	initReindex()
	initRedis()
	initElastic()
	initSqlite()

	seedMongo()
	//	seedTarantool()
	seedBolt()
	seedReindex()
	seedRedis()
	seedElastic()
	seedSqlite()

}
