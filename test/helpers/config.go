package helpers

import (
	"os"

	"gopkg.in/yaml.v3"
)

type ClusterConf struct {
	AppName               string              `yaml:"app_name"`
	Namespaces            []string            `yaml:"namespaces"`
	UpdatesTimeoutSec     int                 `yaml:"updates_timeout_sec"`
	EnableCompression     bool                `yaml:"enable_compression"`
	SyncThreads           int                 `yaml:"sync_threads"`
	SyncsPerThread        int                 `yaml:"syncs_per_thread"`
	LogLevel              string              `yaml:"log_level"`
	RetrySyncIntervalMsec int                 `yaml:"retry_sync_interval_msec"`
	Nodes                 []ClusterNodeConfig `yaml:"nodes"`
}

type ClusterNodeConfig struct {
	DSN      string `yaml:"dsn"`
	ServerID int    `yaml:"server_id"`
}

type ReplicationConf struct {
	ClusterID int `yaml:"cluster_id"`
	ServerID  int `yaml:"server_id"`
}

func DefaultClusterConf() *ClusterConf {
	return &ClusterConf{
		AppName:               "test_cluster",
		Namespaces:            []string{},
		UpdatesTimeoutSec:     1,
		EnableCompression:     true,
		SyncThreads:           5,
		SyncsPerThread:        5,
		LogLevel:              "trace",
		RetrySyncIntervalMsec: 2000,
	}
}

func (cc *ClusterConf) ToFile(path, filename string) error {
	data, err := yaml.Marshal(&cc)
	if err != nil {
		return err
	}

	f, err := os.Create(path + "/" + filename)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (rc *ReplicationConf) ToFile(path, filename string) error {
	data, err := yaml.Marshal(&rc)
	if err != nil {
		return err
	}

	f, err := os.Create(path + "/" + filename)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func GetTmpDBDir() string {
	dir := os.Getenv("REINDEXER_TEST_DB_ROOT")
	if len(dir) != 0 {
		return dir
	}
	return os.TempDir()
}
