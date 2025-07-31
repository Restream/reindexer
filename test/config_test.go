package reindexer

import (
	"math/rand"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v5"
)

const (
	testFtCfgNs = "ft_cfg_check"
	// does not need to be used in init()
	testConfigNs               = "ns_with_config"
	testEmptyConfigNs          = "ns_without_config"
	testDeletedDefaultConfigNs = "ns_with_deleted_default_config"
	testEmptyDefaultConfigNs   = "ns_with_empty_default_config"
)

func init() {
	tnamespaces[testFtCfgNs] = TestItemPk{}
}

func TestDBMSVersion(t *testing.T) {
	t.Run("checking DBMSVersion returns correct value", func(t *testing.T) {

		version, err := DB.Reindexer.DBMSVersion()
		assert.NoError(t, err)

		versionPattern := `^v\d+\.\d+\.\d+(\-\d+\-g[0-9a-f]{9,9})?$`
		re := regexp.MustCompile(versionPattern)
		match := re.MatchString(version)
		assert.True(t, match, version)
	})
}

func TestSetDefaultQueryDebug(t *testing.T) {

	getCurrentNsConfig := func(t *testing.T) (config *reindexer.DBConfigItem, defaultNsConfig *reindexer.DBNamespacesConfig) {
		item, err := DB.Reindexer.Query(reindexer.ConfigNamespaceName).
			WhereString("type", reindexer.EQ, "namespaces").
			Exec().FetchOne()
		require.NoError(t, err)
		config = item.(*reindexer.DBConfigItem)
		require.NotNil(t, config)

		for _, nsCfg := range *config.Namespaces {
			if nsCfg.Namespace == "*" {
				defaultNsConfig = &nsCfg
				return
			}
		}
		return
	}
	setNsConfig := func(t *testing.T, config *reindexer.DBConfigItem) {
		err := DB.Reindexer.Upsert(reindexer.ConfigNamespaceName, config)
		require.NoError(t, err)
	}
	validateUpdatedConfig := func(t *testing.T, ns string, expectedCfg *reindexer.DBNamespacesConfig, expectedNsItems int) {
		found := false
		item, err := DB.Reindexer.Query(reindexer.ConfigNamespaceName).
			WhereString("type", reindexer.EQ, "namespaces").
			Exec().FetchOne()
		require.NoError(t, err)
		dbCfg := item.(*reindexer.DBConfigItem)
		assert.Equal(t, len(*dbCfg.Namespaces), expectedNsItems)
		for _, nsCfg := range *dbCfg.Namespaces {
			if nsCfg.Namespace == ns {
				assert.Equal(t, *expectedCfg, nsCfg)
				found = true
				break
			}
		}

		assert.True(t, found)
	}

	t.Run("set debug level to exist ns config", func(t *testing.T) {
		const ns = testConfigNs

		item, err := DB.Reindexer.Query(reindexer.ConfigNamespaceName).
			WhereString("type", reindexer.EQ, "namespaces").
			Exec().FetchOne()
		require.NoError(t, err)
		nsCfgExp := reindexer.DBNamespacesConfig{
			Namespace:               ns,
			LogLevel:                "trace",
			JoinCacheMode:           "on",
			CopyPolicyMultiplier:    rand.Int(),
			TxSizeToAlwaysCopy:      rand.Int(),
			OptimizationTimeout:     rand.Int(),
			OptimizationSortWorkers: rand.Int(),
			WALSize:                 200000 + rand.Int63n(1000000),
		}
		dbCfg := item.(*reindexer.DBConfigItem)
		*dbCfg.Namespaces = append(*dbCfg.Namespaces, nsCfgExp)
		err = DB.Reindexer.Upsert(reindexer.ConfigNamespaceName, dbCfg)
		require.NoError(t, err)

		err = DB.SetDefaultQueryDebug(ns, 1)
		require.NoError(t, err)

		nsItemsInConfig := len(*dbCfg.Namespaces) // Namespaces count was not changed
		nsCfgExp.LogLevel = "error"
		validateUpdatedConfig(t, ns, &nsCfgExp, nsItemsInConfig)
	})

	t.Run("copy config from * if config not exist", func(t *testing.T) {
		const ns = testEmptyConfigNs

		dbCfg, defaultCfg := getCurrentNsConfig(t)
		require.NotNil(t, defaultCfg)
		nsItemsInConfig := len(*dbCfg.Namespaces) + 1 // Current namespace has to be added to the others

		err := DB.SetDefaultQueryDebug(ns, 1)
		require.NoError(t, err)

		defaultCfg.LogLevel = "error"
		defaultCfg.Namespace = ns
		validateUpdatedConfig(t, ns, defaultCfg, nsItemsInConfig)
	})

	t.Run("set default config if 'namespaces' item does not exist", func(t *testing.T) {
		const ns = testDeletedDefaultConfigNs

		backupCfg, defCfg := getCurrentNsConfig(t)
		require.NotNil(t, defCfg)

		defaultCfg := reindexer.DefaultDBNamespaceConfig("*")
		assert.Equal(t, defaultCfg.LogLevel, "none")
		require.Equal(t, *defCfg, *defaultCfg)

		// Restore config after the test
		defer setNsConfig(t, backupCfg)

		count, err := DB.Reindexer.Query(reindexer.ConfigNamespaceName).
			WhereString("type", reindexer.EQ, "namespaces").
			Delete()
		require.NoError(t, err)
		require.Equal(t, count, 1)

		err = DB.SetDefaultQueryDebug(ns, 1)
		require.NoError(t, err)

		assert.Equal(t, "none", defaultCfg.LogLevel)
		defaultCfg.LogLevel = "error"
		defaultCfg.Namespace = ns
		nsItemsInConfig := 1 // Only current namespace
		validateUpdatedConfig(t, ns, defaultCfg, nsItemsInConfig)
	})

	t.Run("set default config if 'namespaces' item exist, but empty", func(t *testing.T) {
		const ns = testEmptyDefaultConfigNs

		backupCfg, defCfg := getCurrentNsConfig(t)
		require.NotNil(t, defCfg)

		defaultCfg := reindexer.DefaultDBNamespaceConfig("*")
		assert.Equal(t, defaultCfg.LogLevel, "none")
		require.Equal(t, *defCfg, *defaultCfg)

		// Restore config after the test
		defer setNsConfig(t, backupCfg)

		updatedItems, err := DB.Reindexer.Query(reindexer.ConfigNamespaceName).
			Drop("namespaces").
			WhereString("type", reindexer.EQ, "namespaces").
			Update().FetchAll()
		require.NoError(t, err)
		require.Equal(t, len(updatedItems), 1)

		err = DB.SetDefaultQueryDebug(ns, 2)
		require.NoError(t, err)

		assert.Equal(t, "none", defaultCfg.LogLevel)
		defaultCfg.LogLevel = "warning"
		defaultCfg.Namespace = ns
		nsItemsInConfig := 1 // Only current namespace
		validateUpdatedConfig(t, ns, defaultCfg, nsItemsInConfig)
	})
}

func TestFtConfigCompatibility(t *testing.T) {
	config := reindexer.DefaultFtFastConfig()

	addFtIndex := func(indexName string) reindexer.IndexDescription {
		err := DB.AddIndex(testFtCfgNs, reindexer.IndexDef{
			Name:      indexName,
			JSONPaths: []string{indexName},
			Config:    config,
			IndexType: "text",
			FieldType: "string",
		})
		assert.NoError(t, err)

		item, err := DBD.Query(reindexer.NamespacesNamespaceName).Where("name", reindexer.EQ, testFtCfgNs).Exec().FetchOne()
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
