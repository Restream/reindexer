package main

import (
	"flag"

	"./repo"
)

const itemsInDataSet = 100000

func main() {
	enableSeed := false
	dbList := ""

	flag.StringVar(&dbList, "db", "reindex,redis,tarantool,dummy,mysql,sphinx,elastic,mongo", "List of enabled DB engines")
	flag.BoolVar(&enableSeed, "seed", false, "Do seed before test")
	flag.Parse()

	repo.Start(enableSeed, dbList, itemsInDataSet)
	reindexRepo := repo.Get("reindex")
	if reindexRepo != nil {
		reindexRepo.(*repo.ReindexRepo).ForceObjCache()
	}

	go StartEchoHTTP()
	StartHTTP()
}
