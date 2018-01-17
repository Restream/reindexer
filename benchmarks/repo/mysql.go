package repo

import (
	"log"
	"math/rand"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type MySQLRepo struct {
	db *sqlx.DB
}

func (repo *MySQLRepo) Init() bool {

	var err error
	if repo.db, err = sqlx.Connect("mysql", "root@/test"); err != nil {
		panic(err)
	}
	repo.db.SetMaxOpenConns(8)
	repo.db.SetMaxIdleConns(8)
	return true
}

func (repo *MySQLRepo) Seed(itemsInDataSet int) bool {
	log.Printf("Seeding data to Mysql")

	if _, err := repo.db.Exec(`DROP TABLE IF EXISTS items`); err != nil {
		panic(err)
	}

	sqlStmt := `
		CREATE TABLE items (
				id INTEGER NOT NULL PRIMARY KEY, 
				name VARCHAR(255),
				year INTEGER,
				description TEXT,
				FULLTEXT (description),
				INDEX (year),
				INDEX (name)
			)
				`
	if _, err := repo.db.Exec(sqlStmt); err != nil {
		panic(err)
	}

	tx, err := repo.db.Begin()
	if err != nil {
		panic(err)
	}
	stmt, err := tx.Prepare("INSERT INTO items(id, name,year,description) VALUES (?,?,?,?)")

	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	for i := 0; i < itemsInDataSet; i++ {
		it := newItem(i)
		if _, err = stmt.Exec(i, it.Name, it.Year, it.Description); err != nil {
			panic(err)
		}
	}
	tx.Commit()
	return true
}

func (repo *MySQLRepo) QueryFullText(textQuery func() string, N int, limit int) (ret []*Item) {
	stmt, err := repo.db.Preparex("select * from items where MATCH (description) AGAINST (? IN BOOLEAN MODE) LIMIT ?")
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

func (repo *MySQLRepo) QueryByID(N int, onlyQuery bool) (it *Item) {
	stmt, err := repo.db.Preparex("select id,name,year,description from items where id = ?")
	if err != nil {
		log.Fatal(err)
	}
	return sqlFetchOne(stmt, N, onlyQuery, rand.Int()%itemsInDataSet)
}

func (repo *MySQLRepo) Query2Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	stmt, err := repo.db.Preparex("select id,name,year,description from items where year > ? and name = ? limit ?")
	if err != nil {
		log.Fatal(err)
	}
	return sqlFetchAll(stmt, N, onlyQuery, limit, 2010, randString(), limit)
}

func (repo *MySQLRepo) Query1Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	stmt, err := repo.db.Preparex("select id,name,year,description from items where year > ? limit ?")
	if err != nil {
		log.Fatal(err)
	}
	return sqlFetchAll(stmt, N, onlyQuery, limit, 2010, limit)
}
func (repo *MySQLRepo) Update(N int) {
	stmt, err := repo.db.Preparex("UPDATE items SET name=?,year=?,description=? WHERE id=?")
	if err != nil {
		log.Fatal(err)
	}
	sqlUpdate(stmt, 1)
}

func init() {
	registerRepo("mysql", &MySQLRepo{})
}
