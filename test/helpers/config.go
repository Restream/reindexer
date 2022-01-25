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
	RetrySyncIntervalMsec int                 `yaml:"retry_sync_interval_msec"`
	Nodes                 []ClusterNodeConfig `yaml:"nodes"`
}

type ClusterNodeConfig struct {
	DSN      string `yaml:"dsn"`
	ServerID int    `yaml:"server_id"`
}

func DefaultClusterConf() *ClusterConf {
	return &ClusterConf{
		AppName:               "test_cluster",
		Namespaces:            []string{},
		UpdatesTimeoutSec:     1,
		EnableCompression:     true,
		SyncThreads:           5,
		SyncsPerThread:        5,
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

type ReplicationConf struct {
	ClusterID int `yaml:"cluster_id"`
	ServerID  int `yaml:"server_id"`
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
