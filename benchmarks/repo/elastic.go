package repo

import (
	"context"
	"log"
	"math/rand"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"time"

	elastic "gopkg.in/olivere/elastic.v5"
)

type ElasticRepo struct {
	db *elastic.Client
}

func (repo *ElasticRepo) Init() bool {
	var err error

	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	if repo.db, err = elastic.NewClient(elastic.SetURL("http://127.0.0.1:9200"), elastic.SetHttpClient(httpClient)); err != nil {
		panic(err)
	}

	return true
}

func (repo *ElasticRepo) Seed(itemsInDataSet int) bool {
	log.Printf("Seeding data to Elastic")

	ctx := context.Background()
	repo.db.DeleteIndex("items").Do(ctx)
	if _, err := repo.db.CreateIndex("items").Do(ctx); err != nil {
		panic(err)
	}

	bulkRequest := repo.db.Bulk()
	for i := 0; i < itemsInDataSet; i++ {

		bulkRequest = bulkRequest.Add(elastic.NewBulkIndexRequest().Index("items").Type("items").Id(strconv.Itoa(i)).Doc(newItem(i)))

		if (i%10000) == 0 || i == itemsInDataSet-1 {
			if _, err := bulkRequest.Do(ctx); err != nil {
				panic(err)
			}
			bulkRequest = repo.db.Bulk()
		}
	}
	return true
}

func (repo *ElasticRepo) QueryFullText(textQuery func() string, N int, limit int) (ret []*Item) {
	ctx := context.Background()

	ret = make([]*Item, 0, limit)
	for i := 0; i < N; i++ {
		was := false
		if searchResult, err := repo.db.Search().Index("items").
			Query(elastic.NewQueryStringQuery(textQuery()).DefaultField("description").DefaultOperator("OR")).
			From(0).Size(limit).Do(ctx); err != nil {
			panic(err)
		} else {
			for _, item := range searchResult.Each(reflect.TypeOf(Item{})) {
				it := item.(Item)
				ret = append(ret, &it)
				was = true
			}
			if !was {
				//		panic(0)
			}
		}
	}
	return ret
}

func (repo *ElasticRepo) QueryByID(N int, onlyQuery bool) (it *Item) {
	ctx := context.Background()

	for i := 0; i < N; i++ {
		if searchResult, err := repo.db.Search().Index("items").
			Query(elastic.NewTermQuery("id", strconv.Itoa(rand.Int()%itemsInDataSet))).
			From(0).Size(1).Do(ctx); err != nil {
			panic(err)
		} else if !onlyQuery {
			for _, item := range searchResult.Each(reflect.TypeOf(Item{})) {
				pit := item.(Item)
				it = &pit
				break
			}
		}
	}
	return it
}

func (repo *ElasticRepo) Query2Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	ctx := context.Background()
	if !onlyQuery {
		ret = make([]*Item, 0, limit)
	}

	for i := 0; i < N; i++ {
		if searchResult, err := repo.db.Search().Index("items").
			Query(
				elastic.NewBoolQuery().Filter(
					elastic.NewRangeQuery("year").Gt("2010"),
					elastic.NewMatchQuery("name", randString()),
				),
			).From(0).Size(limit).Do(ctx); err != nil {
			panic(err)
		} else {
			for _, item := range searchResult.Each(reflect.TypeOf(Item{})) {
				it := item.(Item)
				ret = append(ret, &it)
			}
		}
	}
	return ret
}

func (repo *ElasticRepo) Query1Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	ctx := context.Background()

	if !onlyQuery {
		ret = make([]*Item, 0, limit)
	}
	for i := 0; i < N; i++ {
		if searchResult, err := repo.db.Search().Index("items").
			Query(elastic.NewRangeQuery("year").Gt("2010")).
			From(0).Size(limit).Do(ctx); err != nil {
			panic(err)
		} else if !onlyQuery {
			for _, item := range searchResult.Each(reflect.TypeOf(Item{})) {
				it := item.(Item)
				ret = append(ret, &it)
			}
		}
	}
	return ret
}

func (repo *ElasticRepo) QueryJoin(N int, limit int, filtersSet [10]interface{}) (ret []*Item) {
	return
}

func (repo *ElasticRepo) Update(N int) {
	ctx := context.Background()

	for i := 0; i < N; i++ {
		id := rand.Int() % itemsInDataSet
		item := newItem(id)
		_, err := repo.db.Index().Index("items").Type("items").Id(strconv.Itoa(id)).BodyJson(*item).Do(ctx)
		if err != nil {
			panic(err)
		}
	}
}

func init() {
	registerRepo("elastic", &ElasticRepo{})
}
