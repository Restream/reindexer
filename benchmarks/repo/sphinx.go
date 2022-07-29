package repo

import (
	"sync"

	"github.com/yunge/sphinx"
)

const maxSphinxClients = 8

type sphinxClient struct {
	conn *sphinx.Client
	busy bool
}

type SphinxRepo struct {

	// sphinx.Client is not thread safe, so we have to implement our client pool
	pool []*sphinxClient
	lock sync.Mutex
	cond *sync.Cond

	// sphinx does not have storage, so we have to implement fake storage
	fakeItems []*Item
}

func (repo *SphinxRepo) Init() bool {

	repo.cond = sync.NewCond(&repo.lock)
	for i := 0; i < maxSphinxClients; i++ {
		conn := sphinx.NewClient().SetServer("127.0.0.1", 0).SetConnectTimeout(5000)
		conn.Open()
		if err := conn.Error(); err != nil {
			panic(err)
		}
		repo.pool = append(repo.pool, &sphinxClient{conn: conn})
	}

	for i := 0; i < 100; i++ {
		repo.fakeItems = append(repo.fakeItems, newItem(i))
	}

	return true
}

func (repo *SphinxRepo) Seed(itemsInDataSet int) bool {
	return true
}

func (repo *SphinxRepo) QueryFullText(textQuery func() string, N int, limit int) (ret []*Item) {

	client := repo.getClient()
	client.conn.Limit = limit

	ret = make([]*Item, 0, N*limit)

	for i := 0; i < N; i++ {
		res, err := client.conn.Query(textQuery(), "test1", "Some comment...")
		if err != nil {
			continue
			//			panic(err)
		}

		for _, r := range res.Matches {
			ret = append(ret, repo.fakeItems[int(r.DocId)%len(repo.fakeItems)])
		}
	}
	repo.freeClient(client)
	return ret
}

func (repo *SphinxRepo) QueryByID(N int, onlyQuery bool) *Item {
	return nil
}

func (repo *SphinxRepo) Query2Cond(N int, onlyQuery bool, limit int) []*Item {
	return nil
}

func (repo *SphinxRepo) Query1Cond(N int, onlyQuery bool, limit int) []*Item {
	return nil
}

func (repo *SphinxRepo) QueryJoin(N int, limit int, filtersSet [10]interface{}) (ret []*Item) {
	return
}

func (repo *SphinxRepo) Update(N int) {
}

func (repo *SphinxRepo) getClient() *sphinxClient {
	repo.lock.Lock()
	for {
		for _, client := range repo.pool {
			if !client.busy {
				client.busy = true
				repo.lock.Unlock()
				return client
			}
		}
		repo.cond.Wait()
	}
}

func (repo *SphinxRepo) freeClient(client *sphinxClient) {
	repo.lock.Lock()
	client.busy = false
	repo.cond.Signal()
	repo.lock.Unlock()
}

func init() {
	registerRepo("sphinx", &SphinxRepo{})
}
