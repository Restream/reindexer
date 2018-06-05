package main

import (
	"flag"
	"strings"
	"testing"

	"github.com/restream/reindexer/benchmarks/repo"
)

func TestMain(t *testing.T) {
	benches := flag.CommandLine.Lookup("test.bench").Value.String()
	repo.Start(strings.Index(benches, "Seed") >= 0, benches, itemsInDataSet)
}
