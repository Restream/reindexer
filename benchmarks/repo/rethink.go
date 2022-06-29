package repo

import (
	"log"
	"math/rand"

	r "gopkg.in/gorethink/gorethink.v4"
)

type RethinkDBRepo struct {
	session *r.Session
}

func (repo *RethinkDBRepo) Init() bool {
	session, err := r.Connect(r.ConnectOpts{
		Address:    "127.0.0.1",
		MaxIdle:    10,
		MaxOpen:    10,
		InitialCap: 10,
	})
	if err != nil {
		log.Fatalln(err)
	}
	r.DBCreate("test").Exec(session)

	repo.session = session

	return true
}

func (repo *RethinkDBRepo) Seed(itemsInDataSet int) bool {

	r.DB("test").TableDrop("items").Exec(repo.session)

	if err := r.DB("test").TableCreate("items", r.TableCreateOpts{PrimaryKey: "id"}).Exec(repo.session); err != nil {
		panic(err)
	}

	// if err := r.DB("test").Table("items").IndexCreate("id", r.IndexCreateOpts{}).Exec(repo.session); err != nil {
	// 	panic(err)
	// }

	if err := r.DB("test").Table("items").IndexCreate("year", r.IndexCreateOpts{}).Exec(repo.session); err != nil {
		panic(err)
	}
	if err := r.DB("test").Table("items").IndexCreate("name", r.IndexCreateOpts{}).Exec(repo.session); err != nil {
		panic(err)
	}

	if err := r.DB("test").Table("items").IndexCreateFunc(
		"year_name",
		[]interface{}{
			r.Row.Field("year"),
			r.Row.Field("name"),
		},
	).Exec(repo.session); err != nil {
		panic(err)
	}
	log.Printf("Seeding data to rethink")

	for i := 0; i < itemsInDataSet; i++ {
		err := r.DB("test").Table("items").Insert(newItem(i), r.InsertOpts{}).Exec(repo.session)
		if err != nil {
			panic(0)
		}
	}
	return true
}

func (repo *RethinkDBRepo) QueryFullText(textQuery func() string, N int, limit int) (ret []*Item) {
	return ret
}

func (repo *RethinkDBRepo) QueryByID(N int, onlyQuery bool) (it *Item) {
	for i := 0; i < N; i++ {
		res, err := r.Table("items").Get(i).Run(repo.session)
		if err != nil {
			panic(err)
		}
		if !onlyQuery {
			res.Next(it)
		}
		res.Close()
	}
	return it
}

func (repo *RethinkDBRepo) Query2Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	if !onlyQuery {
		ret = make([]*Item, 0, limit)
	}

	for i := 0; i < N; i++ {
		str := randString()
		res, err := r.Table("items").Between([]interface{}{2010, str}, []interface{}{2050, str}).Limit(limit).Run(repo.session)
		if err != nil {
			panic(err)
		}
		if !onlyQuery {
			it := Item{}
			for res.Next(&it) {
				itc := it
				ret = append(ret, &itc)
			}
		}
		res.Close()
	}
	return ret
}

func (repo *RethinkDBRepo) Query1Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	if !onlyQuery {
		ret = make([]*Item, 0, limit)
	}

	for i := 0; i < N; i++ {
		res, err := r.Table("items").GetAllByIndex("year", 2010).Limit(limit).Run(repo.session)
		if err != nil {
			panic(err)
		}
		if !onlyQuery {
			it := Item{}
			for res.Next(&it) {
				itc := it
				ret = append(ret, &itc)
			}
		}
		res.Close()
	}
	return ret
}

func (repo *RethinkDBRepo) QueryJoin(N int, limit int, filtersSet [10]interface{}) (ret []*Item) {
	return
}

func (repo *RethinkDBRepo) Update(N int) {

	for i := 0; i < N; i++ {
		id := rand.Int() % itemsInDataSet
		err := r.Table("items").Update(newItem(id)).Exec(repo.session)
		if err != nil {
			panic(err)
		}
	}
}

func init() {
	registerRepo("rethink", &RethinkDBRepo{})
}
