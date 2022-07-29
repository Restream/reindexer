package repo

import (
	"log"
	"math/rand"
	"sync"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type SqliteRepo struct {
	db         *sqlx.DB
	updateLock sync.Mutex
}

func (repo *SqliteRepo) Init() bool {

	var err error
	if repo.db, err = sqlx.Connect("sqlite3", "/tmp/foo.db?mode=memory&cache=shared"); err != nil {
		panic(err)
	}
	repo.db.SetMaxOpenConns(8)
	repo.db.SetMaxIdleConns(8)
	return true
}

func (repo *SqliteRepo) Seed(itemsInDataSet int) bool {

	if _, err := repo.db.Exec(`DROP TABLE IF EXISTS items`); err != nil {
		panic(err)
	}

	if _, err := repo.db.Exec(`DROP TABLE IF EXISTS joined_items`); err != nil {
		panic(err)
	}

	if _, err := repo.db.Exec(`DROP TABLE IF EXISTS items_fts`); err != nil {
		panic(err)
	}

	log.Printf("Seeding data to Sqlite")
	sqlStmt := `
		CREATE TABLE items (
			id INTEGER NOT NULL PRIMARY KEY, 
			name TEXT,
			year INTEGER,
			description TEXT
		);
		CREATE INDEX index_name ON items(name);
		CREATE INDEX index_year ON items(year);
		CREATE VIRTUAL TABLE items_fts USING fts5 (
			id,name,year,description
		);
		CREATE TABLE joined_items (
			id INTEGER NOT NULL PRIMARY KEY, 
			item_id INTEGER,
			description TEXT,
			FOREIGN KEY (item_id) REFERENCES items(id)
		);
		CREATE INDEX index_item_id ON joined_items(item_id);
		`
	if _, err := repo.db.Exec(sqlStmt); err != nil {
		panic(err)
	}

	tx, err := repo.db.Begin()
	if err != nil {
		panic(err)
	}
	stmt, err := tx.Prepare("INSERT INTO items(id, name,year,description) VALUES (?,?,?,?)")
	stmt2, err := tx.Prepare("INSERT INTO items_fts(id, name,year,description) VALUES (?,?,?,?)")
	stmt3, err := tx.Prepare("INSERT INTO joined_items(id, item_id,description) VALUES (?,?,?)")

	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	for i := 0; i < itemsInDataSet; i++ {
		it := newItem(i)
		if _, err = stmt.Exec(i, it.Name, it.Year, it.Description); err != nil {
			panic(err)
		}
		if _, err = stmt2.Exec(i, it.Name, it.Year, it.Description); err != nil {
			panic(err)
		}
		jit := newJoinedItem(i)
		if _, err = stmt3.Exec(jit.ID, jit.ID, jit.Description); err != nil {
			panic(err)
		}
	}
	tx.Commit()
	return true
}

func (repo *SqliteRepo) QueryFullText(textQuery func() string, N int, limit int) (ret []*Item) {

	stmt, err := repo.db.Preparex("select * from items_fts where description match ? order by rank limit ?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	ret = make([]*Item, 0, limit*N)

	for i := 0; i < N; i++ {
		rows, err := stmt.Queryx(textQuery(), limit)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			it := &Item{}
			if err = rows.StructScan(it); err != nil {
				panic(err)
			}
			ret = append(ret, it)
		}

		rows.Close()
	}
	return ret
}

func (repo *SqliteRepo) QueryByID(N int, onlyQuery bool) (it *Item) {
	stmt, err := repo.db.Preparex("select id,name,year,description from items where id = ?")
	if err != nil {
		log.Fatal(err)
	}
	return sqlFetchOne(stmt, N, onlyQuery, rand.Int()%itemsInDataSet)
}

func (repo *SqliteRepo) Query2Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	stmt, err := repo.db.Preparex("select id,name,year,description from items where year > ? and name = ? limit ?")
	if err != nil {
		log.Fatal(err)
	}
	return sqlFetchAll(stmt, N, onlyQuery, limit, 2010, randString(), limit)
}

func (repo *SqliteRepo) Query1Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	stmt, err := repo.db.Preparex("select id,name,year,description from items where year > ? limit ?")
	if err != nil {
		log.Fatal(err)
	}
	return sqlFetchAll(stmt, N, onlyQuery, limit, 2010, limit)
}
func (repo *SqliteRepo) QueryJoin(N int, limit int, filtersSet [10]interface{}) (ret []*Item) {
	stmt, err := repo.db.Preparex("select * from items inner join joined_items on items.id=joined_items.item_id where items.id in (?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		log.Fatal(err)
	}
	return sqlFetchAll(stmt, N, true, limit, filtersSet[:]...)
}

func (repo *SqliteRepo) Update(N int) {
	stmt, err := repo.db.Preparex("UPDATE items SET name=?,year=?,description=? WHERE id=?")
	if err != nil {
		log.Fatal(err)
	}
	sqlUpdate(stmt, 1)
}

func init() {
	registerRepo("sqlite", &SqliteRepo{})
}
