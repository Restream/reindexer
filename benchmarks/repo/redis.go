package repo

import (
	"encoding/json"
	"log"
	"math/rand"
	"strconv"

	"github.com/go-redis/redis"
)

type RedisRepo struct {
	db *redis.Client
}

func (repo *RedisRepo) Init() bool {
	repo.db = redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", Password: "", DB: 0})
	if _, err := repo.db.Ping().Result(); err != nil {
		log.Fatal(err)
	}
	return true
}

func (repo *RedisRepo) Seed(itemsInDataSet int) bool {
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
		status1 := repo.db.ZAdd("years", redis.Z{Member: json, Score: float64(item.Year)})
		if status1.Err() != nil {
			panic(status1.Err())
		}
	}
	if err := repo.db.MSet(bulk...).Err(); err != nil {
		panic(err)
	}

	return true
}

func (repo *RedisRepo) QueryFullText(textQuery func() string, N int, limit int) []*Item {

	return nil
}
func (repo *RedisRepo) QueryByID(N int, onlyQuery bool) (item *Item) {
	for i := 0; i < N; i++ {
		if res, err := repo.db.Get(strconv.Itoa(rand.Int() % itemsInDataSet)).Result(); err != nil {
			panic(err)
		} else if !onlyQuery {
			item = &Item{}
			err := json.Unmarshal([]byte(res), item)
			if err != nil {
				panic(err)
			}
		}
	}
	return item
}

func (repo *RedisRepo) Query2Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	return ret
}

func (repo *RedisRepo) Query1Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	if !onlyQuery {
		ret = make([]*Item, 0, limit*N)
	}

	for i := 0; i < N; i++ {
		if res, err := repo.db.ZRangeByScore("years", redis.ZRangeBy{Min: "(2010", Max: "(2050", Count: int64(limit)}).Result(); err != nil {
			panic(err)
		} else if !onlyQuery {
			for _, r := range res {
				item := &Item{}
				err := json.Unmarshal([]byte(r), item)
				if err != nil {
					panic(err)
				}
				ret = append(ret, item)
			}
		}
	}
	return ret
}

func (repo *RedisRepo) Update(N int) {
	for i := 0; i < N; i++ {
		id := rand.Int() % itemsInDataSet
		item := newItem(id)
		json, _ := json.Marshal(item)

		status := repo.db.MSet(strconv.Itoa(id), json)
		if status.Err() != nil {
			panic(status.Err())
		}
		status1 := repo.db.ZAdd("years", redis.Z{Member: json, Score: float64(item.Year)})
		if status1.Err() != nil {
			panic(status1.Err())
		}
	}
}

func init() {
	registerRepo("redis", &RedisRepo{})
}
