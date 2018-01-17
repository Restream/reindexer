package repo

import (
	"log"
	"math/rand"

	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/bindings/builtin"
)

type ReindexRepo struct {
	db            *reindexer.Reindexer
	forceObjCache bool
}

type TestLogger struct {
}

func (TestLogger) Printf(level int, format string, msg ...interface{}) {
	log.Printf(format, msg...)
}

func (repo *ReindexRepo) Init() bool {

	repo.db = reindexer.NewReindex("builtin")

	repo.db.EnableStorage("/tmp/reindex/")
	if err := repo.db.OpenNamespace("items", reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), Item{}); err != nil {
		panic(err)
	}

	//	ftcfg := reindexer.DefaultFT1Config()
	// ftcfg.Stemmers = nil
	// ftcfg.EnableTranslit = false
	// ftcfg.EnableKbLayout = false
	// ftcfg.MaxTyposInWord = 0

	//	repo.db.ConfigureIndex("items", "description", ftcfg)

	repo.db.Query("items").Sort("year", false).Exec().FetchAll()
	repo.db.Query("items").Match("description", "").Limit(1).Exec()
	repo.db.SetLogger(TestLogger{})
	return true
}

func (repo *ReindexRepo) Seed(itemsInDataSet int) bool {
	log.Printf("Seeding data to Reindex")
	for i := 0; i < itemsInDataSet; i++ {
		if err := repo.db.Upsert("items", newItem(i)); err != nil {
			panic(err)
		}
	}
	repo.db.Query("items").Sort("year", false).Exec().FetchAll()
	repo.db.Query("items").Match("description", "").Limit(1).Exec()
	repo.db.SetLogger(nil)
	return true
}

func (repo *ReindexRepo) QueryFullText(textQuery func() string, N int, limit int) (ret []*Item) {

	ret = make([]*Item, 0, N*limit)

	for i := 0; i < N; i++ {
		iter := repo.db.Query("items").Match("description", textQuery()).Limit(limit).Exec()
		iter.AllowUnsafe(true)

		if iter.Error() != nil {
			panic(iter.Error())
		}
		for iter.Next() {
			obj := iter.Object().(*Item)
			ret = append(ret, obj)
		}
		iter.Close()
	}
	return ret
}

func (repo *ReindexRepo) QueryByID(N int, onlyQuery bool) (it *Item) {
	for i := 0; i < N; i++ {
		iter := repo.db.Query("items").WhereInt("id", reindexer.EQ, rand.Int()%itemsInDataSet).Exec()
		iter.AllowUnsafe(onlyQuery || repo.forceObjCache)

		if iter.Error() != nil || !iter.Next() {
			panic("Item not found")
		}
		it = iter.Object().(*Item)
		iter.Close()
	}
	return it
}

func (repo *ReindexRepo) Query2Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	if !onlyQuery {
		ret = make([]*Item, 0, limit*N)
	}

	for i := 0; i < N; i++ {
		iter := repo.db.Query("items").WhereInt("year", reindexer.GT, 2010).WhereString("name", reindexer.EQ, randString()).Limit(limit).Exec()
		iter.AllowUnsafe(onlyQuery || repo.forceObjCache)

		if iter.Error() != nil {
			panic(iter.Error())
		}
		if !onlyQuery {
			for iter.Next() {
				obj := iter.Object().(*Item)
				ret = append(ret, obj)
			}
		}
		iter.Close()
	}
	return ret
}

func (repo *ReindexRepo) Query1Cond(N int, onlyQuery bool, limit int) (ret []*Item) {

	if !onlyQuery {
		ret = make([]*Item, 0, limit*N)
	}

	for i := 0; i < N; i++ {
		iter := repo.db.Query("items").WhereInt("year", reindexer.GT, 2010).Limit(limit).Exec()
		iter.AllowUnsafe(onlyQuery || repo.forceObjCache)

		if iter.Error() != nil {
			panic(iter.Error())
		}
		if !onlyQuery {
			for iter.Next() {
				obj := iter.Object().(*Item)
				ret = append(ret, obj)
			}
		}
		iter.Close()
	}
	return ret
}
func (repo *ReindexRepo) Update(N int) {
	for i := 0; i < N; i++ {
		id := rand.Int() % itemsInDataSet
		item := newItem(id)
		err := repo.db.Upsert("items", item)
		if err != nil {
			panic(err)
		}
	}
}

func (repo *ReindexRepo) ForceObjCache() {
	repo.forceObjCache = true
}

func init() {
	registerRepo("reindex", &ReindexRepo{})
}
