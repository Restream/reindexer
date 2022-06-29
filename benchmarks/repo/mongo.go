package repo

import (
	"log"
	"math/rand"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type MongoDBRepo struct {
	db      *mgo.Database
	session *mgo.Session
}

func (repo *MongoDBRepo) Init() bool {

	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		panic(err)
	}
	session.SetMode(mgo.Monotonic, true)
	repo.db = session.DB("test")
	return true
}

func (repo *MongoDBRepo) Seed(itemsInDataSet int) bool {

	c := repo.db.C("items")

	c.EnsureIndexKey("id")
	c.EnsureIndexKey("name")
	c.EnsureIndexKey("year")
	c.EnsureIndex(mgo.Index{
		Key: []string{"$text:description"}})

	log.Printf("Seeding data to mongo")
	bulk := c.Bulk()

	for i := 0; i < itemsInDataSet; i++ {
		bulk.Insert(newItem(i))
	}
	if _, err := bulk.Run(); err != nil {
		panic(err)
	}
	return true
}

func (repo *MongoDBRepo) QueryFullText(textQuery func() string, N int, limit int) (ret []*Item) {
	c := repo.db.C("items")

	ret = make([]*Item, 0, limit)
	for i := 0; i < N; i++ {
		it := c.Find(bson.M{"$text": bson.M{"$search": textQuery()}}).Limit(limit).Iter()
		item := &Item{}
		for it.Next(item) {
			ret = append(ret, item)
		}
		it.Close()
	}
	return ret
}

func (repo *MongoDBRepo) QueryByID(N int, onlyQuery bool) (it *Item) {
	c := repo.db.C("items")
	for i := 0; i < N; i++ {
		it = &Item{}
		if err := c.Find(bson.M{"id": rand.Int() % itemsInDataSet}).One(it); err != nil {
			panic(err)
		}
	}
	return it
}

func (repo *MongoDBRepo) Query2Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	c := repo.db.C("items")
	if !onlyQuery {
		ret = make([]*Item, 0, limit)
	}

	for i := 0; i < N; i++ {
		it := c.Find(bson.M{"name": randString(), "year": bson.M{"$gt": 2010}}).Limit(limit).Iter()
		item := &Item{}
		if !onlyQuery {
			for it.Next(item) {
				ret = append(ret, item)
			}
		} else {
			it.Next(item)
		}
	}
	return ret
}

func (repo *MongoDBRepo) Query1Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	c := repo.db.C("items")
	if !onlyQuery {
		ret = make([]*Item, 0, limit)
	}

	for i := 0; i < N; i++ {
		it := c.Find(bson.M{"year": bson.M{"$gt": 2010}}).Limit(limit).Iter()
		item := &Item{}
		if !onlyQuery {
			for it.Next(item) {
				ret = append(ret, item)
			}
		} else {
			it.Next(item)
		}
	}
	return ret
}

func (repo *MongoDBRepo) QueryJoin(N int, limit int, filtersSet [10]interface{}) (ret []*Item) {
	return
}

func (repo *MongoDBRepo) Update(N int) {
	c := repo.db.C("items")

	for i := 0; i < N; i++ {
		id := rand.Int() % itemsInDataSet
		item := newItem(id)
		_, err := c.Upsert(bson.M{"id": id}, item)
		if err != nil {
			panic(err)
		}
	}

}

func init() {
	registerRepo("mongo", &MongoDBRepo{})
}
