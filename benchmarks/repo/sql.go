package repo

import (
	"log"
	"math/rand"

	"github.com/jmoiron/sqlx"
)

func sqlFetchAll(stmt *sqlx.Stmt, N int, onlyQuery bool, limit int, qargs ...interface{}) (ret []*Item) {

	defer stmt.Close()
	if !onlyQuery {
		ret = make([]*Item, 0, limit*N)
	}

	for i := 0; i < N; i++ {
		rows, err := stmt.Queryx(qargs...)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			if !onlyQuery {
				it := &Item{}
				if err = rows.StructScan(it); err != nil {
					panic(err)
				}
				ret = append(ret, it)
			}
		}
		rows.Close()
	}
	return ret
}

func sqlFetchOne(stmt *sqlx.Stmt, N int, onlyQuery bool, qargs ...interface{}) (it *Item) {
	defer stmt.Close()
	for i := 0; i < N; i++ {

		if onlyQuery {
			id := 0
			stmt.QueryRow(qargs...).Scan(&id)
		} else {
			it = &Item{}
			if err := stmt.QueryRowx(qargs...).StructScan(it); err != nil {
				panic(err)
			}
		}
	}
	return it
}

func sqlUpdate(stmt *sqlx.Stmt, N int) {

	defer stmt.Close()
	for i := 0; i < N; i++ {
		id := rand.Int() % itemsInDataSet
		item := newItem(id)
		_, err := stmt.Exec(item.Name, item.Year, item.Description, id)
		if err != nil {
			log.Print(err.Error())
		}
	}
}
