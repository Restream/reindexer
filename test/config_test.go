package reindexer

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v4"
)

type FtConfCheck struct {
	ID int `reindex:"id,,pk"`
}

const (
	ftCfgNsName = "ft_cfg_check"
)

func init() {
	tnamespaces[ftCfgNsName] = FtConfCheck{}
}

func TestSetDefaultQueryDebug(t *testing.T) {
	t.Run("set debug level to exist ns config", func(t *testing.T) {
		ns := "ns_with_config"

		item, err := DB.Reindexer.Query(reindexer.ConfigNamespaceName).WhereString("type", reindexer.EQ, "namespaces").Exec().FetchOne()
		require.NoError(t, err)
		ncCfgExp := reindexer.DBNamespacesConfig{
			Namespace:               ns,
			LogLevel:                "trace",
			JoinCacheMode:           "on",
			Lazyload:                true,
			UnloadIdleThreshold:     rand.Int(),
			StartCopyPolicyTxSize:   rand.Int(),
			CopyPolicyMultiplier:    rand.Int(),
			TxSizeToAlwaysCopy:      rand.Int(),
			OptimizationTimeout:     rand.Int(),
			OptimizationSortWorkers: rand.Int(),
			WALSize:                 200000 + rand.Int63n(1000000),
		}
		dbCfg := item.(*reindexer.DBConfigItem)
		*dbCfg.Namespaces = append(*dbCfg.Namespaces, ncCfgExp)
		err = DB.Reindexer.Upsert(reindexer.ConfigNamespaceName, dbCfg)
		require.NoError(t, err)

		err = DB.SetDefaultQueryDebug(ns, 1)
		require.NoError(t, err)

		found := false
		item, err = DB.Reindexer.Query(reindexer.ConfigNamespaceName).WhereString("type", reindexer.EQ, "namespaces").Exec().FetchOne()
		dbCfg = item.(*reindexer.DBConfigItem)
		for _, nsCfg := range *dbCfg.Namespaces {
			if nsCfg.Namespace == ns {
				assert.Equal(t, "error", nsCfg.LogLevel) // check changed from 'trace' to 'error'

				ncCfgExp.LogLevel = nsCfg.LogLevel
				assert.Equal(t, ncCfgExp, nsCfg)
				found = true
				break
			}
		}

		assert.True(t, found)
	})

	t.Run("copy config from * if config not exist", func(t *testing.T) {
		ns := "ns_without_config"

		item, err := DB.Reindexer.Query(reindexer.ConfigNamespaceName).WhereString("type", reindexer.EQ, "namespaces").Exec().FetchOne()
		dbCfg := item.(*reindexer.DBConfigItem)

		var defaultCfg reindexer.DBNamespacesConfig
		for _, nsCfg := range *dbCfg.Namespaces {
			if nsCfg.Namespace == "*" {
				defaultCfg = nsCfg
			}
		}
		require.Equal(t, "*", defaultCfg.Namespace)

		err = DB.SetDefaultQueryDebug(ns, 1)
		require.NoError(t, err)

		found := false
		item, err = DB.Reindexer.Query(reindexer.ConfigNamespaceName).WhereString("type", reindexer.EQ, "namespaces").Exec().FetchOne()
		dbCfg = item.(*reindexer.DBConfigItem)
		for _, nsCfg := range *dbCfg.Namespaces {
			if nsCfg.Namespace == ns {
				assert.Equal(t, "none", defaultCfg.LogLevel)
				assert.Equal(t, "error", nsCfg.LogLevel) // check changed from 'none' to 'error'

				nsCfg.LogLevel, nsCfg.Namespace = defaultCfg.LogLevel, defaultCfg.Namespace
				assert.Equal(t, defaultCfg, nsCfg)
				found = true
				break
			}
		}

		assert.True(t, found)
	})
}

func TestFtConfigCompatibility(t *testing.T) {
	config := reindexer.DefaultFtFastConfig()

	addFtIndex := func(indexName string) reindexer.IndexDescription {
		err := DB.AddIndex(ftCfgNsName, reindexer.IndexDef{
			Name:      indexName,
			JSONPaths: []string{indexName},
			Config:    config,
			IndexType: "text",
			FieldType: "string",
		})
		assert.NoError(t, err)

		item, err := DBD.Query(reindexer.NamespacesNamespaceName).Where("name", reindexer.EQ, ftCfgNsName).Exec().FetchOne()
		assert.NoError(t, err)

		indexes := item.(*reindexer.NamespaceDescription).Indexes
		index := indexes[len(indexes)-1]
		return index
	}

	checkStopWordsFtConfig := func(index reindexer.IndexDescription) {
		conf := index.Config.(map[string]interface{})
		cfgStopWords := conf["stop_words"].([]interface{})
		assert.Equal(t, len(cfgStopWords), len(config.StopWords))

		for idx, wordI := range config.StopWords {
			switch wordI.(type) {
			case string:
				assert.Equal(t, wordI, cfgStopWords[idx])
			case reindexer.StopWord:
				word := wordI.(reindexer.StopWord)
				assert.Equal(t, word.Word, cfgStopWords[idx].(map[string]interface{})["word"])
				assert.Equal(t, word.IsMorpheme, cfgStopWords[idx].(map[string]interface{})["is_morpheme"])
			}
		}
	}

	t.Run("check string stop_words config with index create", func(t *testing.T) {
		stopWordsStrs := append(make([]interface{}, 0), "под", "на", "из")
		config.StopWords = stopWordsStrs
		index := addFtIndex("idxStopWordsStrs")
		checkStopWordsFtConfig(index)
	})

	t.Run("check object stop_words config with index create", func(t *testing.T) {
		stopWordsObjs := append(make([]interface{}, 0),
			reindexer.StopWord{
				Word:       "пред",
				IsMorpheme: true,
			}, reindexer.StopWord{
				Word:       "над",
				IsMorpheme: true,
			}, reindexer.StopWord{
				Word:       "за",
				IsMorpheme: false,
			})
		config.StopWords = stopWordsObjs
		index := addFtIndex("idxStopWordsObjs")
		checkStopWordsFtConfig(index)
	})

	t.Run("check mixed stop_words config with index create", func(t *testing.T) {
		stopWordsMix := append(make([]interface{}, 0),
			"под",
			reindexer.StopWord{
				Word:       "пред",
				IsMorpheme: true,
			},
			reindexer.StopWord{
				Word:       "за",
				IsMorpheme: false,
			},
			"на",
			reindexer.StopWord{
				Word:       "над",
				IsMorpheme: true,
			},
			"из")
		config.StopWords = stopWordsMix
		index := addFtIndex("idxStopWordsMix")
		checkStopWordsFtConfig(index)
	})

	checkBm25FtConfig := func(index reindexer.IndexDescription, expectedBm25k1 float64,
		expectedBm25b float64, expectedBm25Type string) {
		conf := index.Config.(map[string]interface{})
		rankFunConf := conf["bm25_config"].(map[string]interface{})
		cfgBm25k1 := rankFunConf["bm25_k1"]
		cfgBm25b := rankFunConf["bm25_b"]
		cfgBm25Type := rankFunConf["bm25_type"]
		assert.Equal(t, expectedBm25k1, cfgBm25k1)
		assert.Equal(t, expectedBm25b, cfgBm25b)
		assert.Equal(t, expectedBm25Type, cfgBm25Type)
	}

	t.Run("check bm25_k1, bm25_b, bm25_type configs with index create", func(t *testing.T) {
		expectedBm25k1 := 1.53
		expectedBm25b := 0.52
		expectedBm25Type := "bm25"
		config.Bm25Config.Bm25k1 = expectedBm25k1
		config.Bm25Config.Bm25b = expectedBm25b
		config.Bm25Config.Bm25Type = expectedBm25Type
		index := addFtIndex("idxBm25")
		checkBm25FtConfig(index, expectedBm25k1, expectedBm25b, expectedBm25Type)
	})
}
