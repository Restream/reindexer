package config

import (
	"fmt"

	"gopkg.in/yaml.v2"
)

type StorageConf struct {
	Path            string `yaml:"path"`
	Engine          string `yaml:"engine"`
	StartWithErrors bool   `yaml:"startwitherrors"`
}

const ServerThreadingDedicated = "dedicated"
const ServerThreadingShared = "shared"

type NetConf struct {
	HTTPAddr            string `yaml:"httpaddr"`
	HTTPThreading       string `yaml:"http_threading"` // "dedicated" or "shared"
	RPCAddr             string `yaml:"rpcaddr"`
	RPCThreading        string `yaml:"rpc_threading"` // "dedicated" or "shared"
	UnixRPCAddr         string `yaml:"urpcaddr"`
	UnixRPCThreading    string `yaml:"urpc_threading"` // "dedicated" or "shared"
	WebRoot             string `yaml:"webroot"`
	Security            bool   `yaml:"security"`
	HttpReadTimeoutSec  int    `yaml:"http_read_timeout,omitempty"`
	HttpWriteTimeoutSec int    `yaml:"http_write_timeout,omitempty"`
	MaxUpdatesSizeBytes uint   `yaml:"maxupdatessize,omitempty"`
}

type LoggerConf struct {
	ServerLog string `yaml:"serverlog"`
	CoreLog   string `yaml:"corelog"`
	HTTPLog   string `yaml:"httplog"`
	RPCLog    string `yaml:"rpclog"`
	LogLevel  string `yaml:"loglevel"`
}

type SystemConf struct {
	User                string  `yaml:"user"`
	AllocatorCacheLimit int64   `yaml:"allocator_cache_limit"`
	AllocatorCachePart  float32 `yaml:"allocator_cache_part"`
}

type DebugConf struct {
	Pprof  bool `yaml:"pprof"`
	Allocs bool `yaml:"allocs"`
}
type MetricsConf struct {
	Prometheus       bool  `yaml:"prometheus"`
	CollectPeriod    int64 `yaml:"collect_period"`
	ClientsStats     bool  `yaml:"clientsstats"`
	ReplicationStats bool  `yaml:"replicationstats"`
}
type ServerConfig struct {
	Storage StorageConf `yaml:"storage"`
	Net     NetConf     `yaml:"net"`
	Logger  LoggerConf  `yaml:"logger"`
	System  SystemConf  `yaml:"system"`
	Debug   DebugConf   `yaml:"debug"`
	Metrics MetricsConf `yaml:"metrics"`
}

func (cfg *ServerConfig) GetYamlString() (string, error) {
	b, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("rq: server config is invalid")
	}
	return string(b), nil
}

func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Storage: StorageConf{
			Path:            "/tmp/reindex",
			Engine:          "leveldb",
			StartWithErrors: false,
		},
		Net: NetConf{
			HTTPAddr:      "0.0.0.0:9088",
			HTTPThreading: "shared",
			RPCAddr:       "0.0.0.0:6534",
			RPCThreading:  "shared",
			UnixRPCAddr:   "none",
			Security:      false,
		},
		Logger: LoggerConf{
			ServerLog: "stdout",
			CoreLog:   "stdout",
			HTTPLog:   "stdout",
			LogLevel:  "error",
		},
		System: SystemConf{
			AllocatorCacheLimit: -1,
			AllocatorCachePart:  -1,
		},
		Debug: DebugConf{
			Pprof:  false,
			Allocs: false,
		},
		Metrics: MetricsConf{
			Prometheus:    false,
			CollectPeriod: 1000,
			ClientsStats:  false,
		},
	}
}
