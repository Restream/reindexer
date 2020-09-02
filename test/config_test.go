package reindexer

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer"
)

func TestSetDefaultQueryDebug(t *testing.T) {
	t.Run("set debug level to exist ns config", func(t *testing.T) {
		ns := "ns_with_config"

		item, err := DB.Reindexer.Query(reindexer.ConfigNamespaceName).WhereString("type", reindexer.EQ, "namespaces").Exec().FetchOne()
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
