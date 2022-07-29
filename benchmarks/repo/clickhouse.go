package repo

import (
	"log"
	"math/rand"
	"strconv"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
)

type ClickHouseRepo struct {
	db *sqlx.DB
}

func (repo *ClickHouseRepo) Init() bool {

	var err error
	if repo.db, err = sqlx.Connect("clickhouse", "tcp://127.0.0.1:9000?debug=false"); err != nil {
		panic(err)
	}
	repo.db.SetMaxOpenConns(8)
	repo.db.SetMaxIdleConns(8)
	return true
}

func (repo *ClickHouseRepo) Seed(itemsInDataSet int) bool {
	log.Printf("Seeding data to Clickhouse")
	if _, err := repo.db.Exec("DROP TABLE IF EXISTS items;"); err != nil {
		panic(err)
	}

	sqlStmt := `
	CREATE TABLE IF NOT EXISTS items (
		EventDate Date, 
		id UInt32, 
		name String,
		year UInt32,
		description String
	) engine=Memory;
	`
	if _, err := repo.db.Exec(sqlStmt); err != nil {
		panic(err)
	}

	tx, err := repo.db.Begin()
	if err != nil {
		panic(err)
	}
	stmt, err := tx.Prepare("INSERT INTO items(id, name, year, description) VALUES (?,?,?,?)")
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

func (repo *ClickHouseRepo) QueryFullText(textQuery func() string, N int, limit int) (ret []*Item) {
	return nil
}

func (repo *ClickHouseRepo) QueryByID(N int, onlyQuery bool) (it *Item) {
	stmt, err := repo.db.Preparex("select id,name,year,description from items where id = ?")
	if err != nil {
		log.Fatal(err)
	}
	return sqlFetchOne(stmt, N, onlyQuery, rand.Int()%itemsInDataSet)
}

func (repo *ClickHouseRepo) Query2Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	stmt, err := repo.db.Preparex("select id,name,year,description from items where year > ? and name = ? limit " + strconv.Itoa(limit))
	if err != nil {
		log.Fatal(err)
	}
	return sqlFetchAll(stmt, N, onlyQuery, limit, 2010, randString())
}

func (repo *ClickHouseRepo) Query1Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	stmt, err := repo.db.Preparex("select id,name,year,description from items where year > ? limit " + strconv.Itoa(limit))
	if err != nil {
		log.Fatal(err)
	}
	return sqlFetchAll(stmt, N, onlyQuery, limit, 2010)
}

func (repo *ClickHouseRepo) QueryJoin(N int, limit int, filtersSet [10]interface{}) (ret []*Item) {
	return
}

func (repo *ClickHouseRepo) Update(N int) {
	stmt, err := repo.db.Preparex("UPDATE items SET name=?,year=?,description=? WHERE id=?")
	if err != nil {
		log.Fatal(err)
	}
	sqlUpdate(stmt, 1)
}

func init() {
	registerRepo("clickhouse", &ClickHouseRepo{})
}
