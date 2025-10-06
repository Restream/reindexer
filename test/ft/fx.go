package ft

import (
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
)

type TextItem struct {
	ID        int `reindex:"id,,pk"`
	TextField string
}

// CLI flags for test configuration
var (
	dsn                = flag.String("dsn", "builtin://", "reindex db dsn")
	dsnSlave           = flag.String("dsnslave", "", "reindex slave db dsn")
	slaveCount         = flag.Int("slavecount", 1, "reindex slave db count")
	benchmarkSeedCount = flag.Int("seedcount", 500_000, "count of items for benchmark seed")
	benchmarkSeedCPU   = flag.Int("seedcpu", 1, "number threads for seeding")
	benchmarkSeed      = flag.Int64("seed", time.Now().Unix(), "seed number for random")
	legacyServerBinary = flag.String("legacyserver", "", "legacy server binary for compatibility check")
	saveTestArtifacts  = flag.Bool("testartifacts", false, "save test artifact files")
	qualityCheck       = flag.Bool("qualitycheck", false, "run quality checks")
)

func createReindexDbInstance(rx *reindexer.Reindexer, namespace string, indexType string, mergeLimit int) {
	err := rx.OpenNamespace(namespace, reindexer.DefaultNamespaceOptions(), TextItem{})
	if err != nil {
		panic(fmt.Errorf("Couldn't create namespace: "+namespace, err))
	}

	var config interface{}
	if indexType == "fuzzytext" {
		// Disable non exact searchers, disable stop word dictionat
		cfg := reindexer.DefaultFtFuzzyConfig()
		cfg.StopWords = make([]interface{}, 0)
		cfg.Stemmers = []string{}
		cfg.EnableKbLayout = false
		cfg.EnableTranslit = false
		if mergeLimit > 0 {
			cfg.MergeLimit = mergeLimit
		}
		config = cfg
	} else {
		cfg := reindexer.DefaultFtFastConfig()
		cfg.StopWords = make([]interface{}, 0)
		cfg.Stemmers = []string{}
		cfg.EnableKbLayout = false
		cfg.EnableTranslit = false
		if mergeLimit > 0 {
			cfg.MergeLimit = mergeLimit
		}
		config = cfg
	}

	rx.DropIndex(namespace, "text_field")

	err = rx.AddIndex(namespace, reindexer.IndexDef{
		Name:      "text_field",
		JSONPaths: []string{"TextField"},
		Config:    config,
		IndexType: indexType,
		FieldType: "string",
	})
	if err != nil {
		panic(fmt.Errorf("Couldn't set full text index config %s : %s", namespace, err.Error()))
	}
}

func fillReindexWithData(reindexDB *reindexer.Reindexer, namespace string, documents []string) {
	for i, document := range documents {
		item := TextItem{
			ID:        i + 1,
			TextField: document,
		}
		if _, err := reindexDB.Insert(namespace, &item); err != nil {
			panic(err)
		}
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
}
