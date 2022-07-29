package repo

import (
	"log"
	"math/rand"

	driver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
)

type ArangoDBRepo struct {
	db     driver.Database
	client driver.Client
	col    driver.Collection
}

func (repo *ArangoDBRepo) Init() bool {

	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{"http://localhost:8529"},
	})
	if err != nil {
		panic(err)
	}
	c, err := driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication("root", ""),
	})
	if err != nil {
		panic(err)
	}

	c.CreateDatabase(nil, "test", nil)
	db, err := c.Database(nil, "test")
	if err != nil {
		panic(0)
	}
	repo.db = db
	repo.client = c
	repo.col, _ = repo.db.Collection(nil, "items")

	return true
}

func (repo *ArangoDBRepo) Seed(itemsInDataSet int) bool {

	col, err := repo.db.Collection(nil, "items")
	if err == nil {
		col.Remove(nil)
	}

	col, err = repo.db.CreateCollection(nil, "items", nil)
	if err != nil {
		panic(err)
	}

	col.EnsureHashIndex(nil, []string{"id"}, &driver.EnsureHashIndexOptions{Unique: true})
	col.EnsureHashIndex(nil, []string{"name"}, &driver.EnsureHashIndexOptions{Unique: false})
	col.EnsureHashIndex(nil, []string{"year"}, &driver.EnsureHashIndexOptions{Unique: false})
	col.EnsureFullTextIndex(nil, []string{"description"}, &driver.EnsureFullTextIndexOptions{})

	log.Printf("Seeding data to arango")

	for i := 0; i < itemsInDataSet; i++ {
		_, err := col.CreateDocument(nil, newItem(i))
		if err != nil {
			panic(0)
		}
	}
	repo.col = col
	return true
}

func (repo *ArangoDBRepo) QueryFullText(textQuery func() string, N int, limit int) (ret []*Item) {
	ret = make([]*Item, 0, limit)

	for i := 0; i < N; i++ {
		cursor, err := repo.db.Query(nil, "FOR d IN FULLTEXT(items, 'description', @text,@limit ) RETURN d", map[string]interface{}{
			"text":  textQuery(),
			"limit": limit,
		})

		if err != nil {
			panic(err)
		}
		defer cursor.Close()

		for cursor.HasMore() {
			it := &Item{}
			cursor.ReadDocument(nil, it)
			ret = append(ret, it)
		}
	}
	return ret
}

func (repo *ArangoDBRepo) QueryByID(N int, onlyQuery bool) (it *Item) {
	for i := 0; i < N; i++ {
		cursor, err := repo.db.Query(nil, "FOR d IN items FILTER d.id == @id RETURN d", map[string]interface{}{
			"id": rand.Int() % itemsInDataSet,
		})

		if err != nil {
			panic(err)
		}
		defer cursor.Close()

		it = &Item{}
		if !onlyQuery {
			cursor.ReadDocument(nil, it)
		}
	}
	return it
}

func (repo *ArangoDBRepo) Query2Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	if !onlyQuery {
		ret = make([]*Item, 0, limit)
	}

	for i := 0; i < N; i++ {
		cursor, err := repo.db.Query(nil, "FOR d IN items FILTER d.name == @name && d.year > @year LIMIT @limit RETURN d", map[string]interface{}{
			"name":  randString(),
			"year":  2010,
			"limit": limit,
		})

		if err != nil {
			panic(err)
		}
		defer cursor.Close()

		if !onlyQuery {
			for cursor.HasMore() {
				it := &Item{}
				cursor.ReadDocument(nil, it)
				ret = append(ret, it)
			}
		}
	}
	return ret
}

func (repo *ArangoDBRepo) Query1Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	if !onlyQuery {
		ret = make([]*Item, 0, limit)
	}

	for i := 0; i < N; i++ {
		cursor, err := repo.db.Query(nil, "FOR d IN items FILTER d.year == @year LIMIT @limit RETURN d", map[string]interface{}{
			"year":  2010,
			"limit": limit,
		})

		if err != nil {
			panic(err)
		}
		defer cursor.Close()

		if !onlyQuery {
			for cursor.HasMore() {
				it := &Item{}
				cursor.ReadDocument(nil, it)
				ret = append(ret, it)
			}
		}
	}
	return ret
}

func (repo *ArangoDBRepo) QueryJoin(N int, limit int, filtersSet [10]interface{}) (ret []*Item) {
	return
}

func (repo *ArangoDBRepo) Update(N int) {

	for i := 0; i < N; i++ {
		id := rand.Int() % itemsInDataSet

		cursor, err := repo.db.Query(nil, "FOR d IN items FILTER d.id == @id RETURN d", map[string]interface{}{"id": id})
		if err != nil {
			panic(err)
		}
		defer cursor.Close()

		it := &Item{}
		meta, err := cursor.ReadDocument(nil, it)
		if err != nil {
			panic(err)
		}

		item := newItem(id)
		_, err = repo.col.ReplaceDocument(nil, meta.Key, item)
		if err != nil {
			panic(err)
		}
	}

}

func init() {
	registerRepo("arango", &ArangoDBRepo{})
}
