package repo

import "math/rand"

type DummyRepo struct {
	items []*Item
}

func (repo *DummyRepo) Init() bool {
	for i := 0; i < itemsInDataSet; i++ {
		repo.items = append(repo.items, newItem(i))
	}
	return true
}

func (repo *DummyRepo) Seed(itemsInDataSet int) bool {
	return true
}

func (repo *DummyRepo) QueryFullText(textQuery func() string, N int, limit int) (ret []*Item) {

	ret = make([]*Item, 0, limit)

	for i := 0; i < limit; i++ {
		ret = append(ret, repo.items[rand.Int()%itemsInDataSet])
	}
	return ret
}
func (repo *DummyRepo) QueryByID(N int, onlyQuery bool) (it *Item) {
	return repo.items[rand.Int()%itemsInDataSet]
}

func (repo *DummyRepo) Query2Cond(N int, onlyQuery bool, limit int) (ret []*Item) {
	ret = make([]*Item, 0, limit)

	for i := 0; i < limit; i++ {
		ret = append(ret, repo.items[rand.Int()%itemsInDataSet])
	}
	return ret
}

func (repo *DummyRepo) Query1Cond(N int, onlyQuery bool, limit int) (ret []*Item) {

	ret = make([]*Item, 0, limit)

	for i := 0; i < limit; i++ {
		ret = append(ret, repo.items[rand.Int()%itemsInDataSet])
	}
	return ret
}

func (repo *DummyRepo) QueryJoin(N int, limit int, filtersSet [10]interface{}) (ret []*Item) {
	return
}

func (repo *DummyRepo) Update(N int) {
	for i := 0; i < N; i++ {
		id := rand.Int() % itemsInDataSet
		_ = newItem(id)
	}
}

func init() {
	registerRepo("dummy", &DummyRepo{})
}
