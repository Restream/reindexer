package repo

import (
	"strings"
)

type Repo interface {
	Init() bool
	Seed(itemInDataSet int) bool
	QueryFullText(textQuery func() string, N int, limit int) []*Item
	QueryByID(N int, onlyQuery bool) *Item
	Query2Cond(N int, onlyQuery bool, limit int) []*Item
	Query1Cond(N int, onlyQuery bool, limit int) []*Item
	QueryJoin(N int, limit int, filtersSet [10]interface{}) []*Item
	Update(N int)
}

var repos = make(map[string]Repo)
var itemsInDataSet int

func registerRepo(name string, repo Repo) {
	repos[name] = repo
}

func Get(name string) Repo {
	r, f := repos[name]
	if !f {
		return nil
	}
	return r
}

func Start(enableSeed bool, enabledRepos string, items int) {
	itemsInDataSet = items
	downloadWordList()
	enabledRepos = strings.ToLower(enabledRepos)
	for name, repo := range repos {
		if (strings.Index(enabledRepos, name)) < 0 {
			continue
		}

		repo.Init()
		if enableSeed {
			repo.Seed(itemsInDataSet)
		}
	}

}
