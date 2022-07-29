package repo

import (
	"log"
	"math/rand"

	tarantool "github.com/tarantool/go-tarantool"
)

type TarantoolRepo struct {
	db *tarantool.Connection
}

func (repo *TarantoolRepo) Init() bool {
	var err error
	if repo.db, err = tarantool.Connect("localhost:3301", tarantool.Opts{User: "guest"}); err != nil {
		panic(err)
	}
	return true
}

func (repo *TarantoolRepo) Seed(itemsInDataSet int) bool {
	log.Printf("Seeding data to Tarantool")
	for i := 0; i < itemsInDataSet; i++ {
		it := newItem(i)
		repo.db.Delete("items", "primary", []interface{}{uint(it.ID)})
		if _, err := repo.db.Insert("items", []interface{}{int(it.ID), it.Name, it.Description, it.Year}); err != nil {
			panic(err)
		}
	}
	for i := 0; i < itemsInDataSet; i++ {
		it := newJoinedItem(i)
		repo.db.Delete("joined_items", "primary", []interface{}{uint(it.ID)})
		if _, err := repo.db.Insert("joined_items", []interface{}{int(it.ID), it.Description}); err != nil {
			panic(err)
		}
	}
	return true
}

func (repo *TarantoolRepo) QueryFullText(textQuery func() string, N int, limit int) []*Item {
	return nil
}

func decodeTuple(tuple []interface{}) (it *Item) {
	it = &Item{}

	it.ID = int64(tuple[0].(uint64))
	it.Name = tuple[1].(string)
	it.Description = tuple[2].(string)
	it.Year = int(tuple[3].(uint64))
	return it
}

func (repo *TarantoolRepo) QueryByID(N int, onlyQuery bool) (it *Item) {
	for i := 0; i < N; i++ {
		items, err := repo.db.Select("items", "primary", 0, 1, tarantool.IterEq, []interface{}{uint(rand.Int() % itemsInDataSet)})
		if err != nil {
			panic(err)
		}
		if !onlyQuery {
			it = decodeTuple(items.Data[0].([]interface{}))
		}
	}
	return it
}

func (repo *TarantoolRepo) Query2Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	if !onlyQuery {
		ret = make([]*Item, 0, limit*N)
	}

	for i := 0; i < N; i++ {
		items, err := repo.db.Select("items", "name_year", 0, uint32(limit), tarantool.IterGt, []interface{}{randString(), uint(2010)})
		if err != nil {
			panic(err)
		}
		if !onlyQuery {
			for _, tuple := range items.Data {
				it := decodeTuple(tuple.([]interface{}))
				ret = append(ret, it)
			}
		}
	}
	return ret
}

func (repo *TarantoolRepo) Query1Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	if !onlyQuery {
		ret = make([]*Item, 0, limit*N)
	}

	for i := 0; i < N; i++ {
		items, err := repo.db.Select("items", "year", 0, uint32(limit), tarantool.IterGt, []interface{}{uint(2010)})
		if err != nil {
			panic(err)
		}
		if !onlyQuery {
			for _, tuple := range items.Data {
				it := decodeTuple(tuple.([]interface{}))
				ret = append(ret, it)
			}
		}
	}
	return ret
}

func (repo *TarantoolRepo) QueryJoin(N int, limit int, filtersSet [10]interface{}) (ret []*Item) {
	return
}

func (repo *TarantoolRepo) Update(N int) {
	for i := 0; i < N; i++ {
		id := rand.Int() % itemsInDataSet
		it := newItem(id)
		if _, err := repo.db.Update("items", "primary", []interface{}{uint(it.ID)}, []interface{}{
			[]interface{}{"=", 1, it.Name},
			[]interface{}{"=", 2, it.Year},
			[]interface{}{"=", 3, it.Description},
		}); err != nil {
			panic(err)
		}
	}
}

func init() {
	registerRepo("tarantool", &TarantoolRepo{})
}
