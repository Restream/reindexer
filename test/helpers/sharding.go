package helpers

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type TestShardKeyConfig struct {
	ShardID int      `yaml:"shard_id"`
	Values  []string `yaml:"values"`
}

type TestShardedNamespaceConfig struct {
	Name         string               `yaml:"namespace"`
	DefaultShard int                  `yaml:"default_shard"`
	Index        string               `yaml:"index"`
	Keys         []TestShardKeyConfig `yaml:"keys"`
}

type TestShardingConfig struct {
	Namespaces []TestShardedNamespaceConfig
	Shards     []*TestServer
}

type TestShardConfig struct {
	ShardID int         `yaml:"shard_id"`
	DSNS    []yaml.Node `yaml:"dsns"`
}

type TestShardingSerializableConfig struct {
	Version     int                          `yaml:"version"`
	Namespaces  []TestShardedNamespaceConfig `yaml:"namespaces"`
	Shards      []TestShardConfig            `yaml:"shards"`
	ThisShardID int                          `yaml:"this_shard_id"`
}

func WriteShardingConfig(t *testing.T, cfg *TestShardingConfig) {
	require.Greater(t, len(cfg.Shards), 1)
	require.Greater(t, len(cfg.Namespaces), 0)
	shardCfgs := make([]TestShardConfig, len(cfg.Shards))
	for i, s := range cfg.Shards {
		shardCfgs[i].DSNS = []yaml.Node{{Kind: yaml.ScalarNode, Style: yaml.DoubleQuotedStyle, Value: s.GetDSN()}}
		shardCfgs[i].ShardID = i
	}
	for i, s := range cfg.Shards {
		scfg := &TestShardingSerializableConfig{Version: 1, Namespaces: cfg.Namespaces, Shards: shardCfgs, ThisShardID: i}
		shardingYAML, err := yaml.Marshal(scfg)
		require.NoError(t, err)
		storagePath := s.GetFullStoragePath()
		err = os.RemoveAll(storagePath)
		require.NoError(t, err)
		err = os.MkdirAll(storagePath, os.ModePerm)
		require.NoError(t, err)

		plc, err := os.Create(storagePath + "/.reindexer.storage")
		require.NoError(t, err)
		defer plc.Close()
		_, err = plc.Write([]byte("leveldb"))
		require.NoError(t, err)

		shf, err := os.OpenFile(storagePath+"/sharding.conf", os.O_RDWR|os.O_CREATE, 0644)
		require.NoError(t, err)
		defer shf.Close()
		_, err = shf.Write(shardingYAML)
		require.NoError(t, err)

		rf, err := os.OpenFile(storagePath+"/replication.conf", os.O_RDWR|os.O_CREATE, 0644)
		require.NoError(t, err)
		defer rf.Close()
		replCfg := "server_id: " + strconv.Itoa(i) + "\n" +
			"cluster_id: 1\n"
		_, err = rf.Write([]byte(replCfg))
		require.NoError(t, err)
	}
}
