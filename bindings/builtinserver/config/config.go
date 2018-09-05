package config

import (
	"fmt"

	"gopkg.in/yaml.v2"
)

type StorageConf struct {
	Path   string `yaml:"path"`
	Engine string `yaml:"engine"`
}

type NetConf struct {
	HTTPAddr string `yaml:"httpaddr"`
	RPCAddr  string `yaml:"rpcaddr"`
	WebRoot  string `yaml:"webroot"`
	Security bool   `yaml:"security"`
}

type LoggerConf struct {
	ServerLog string `yaml:"serverlog"`
	CoreLog   string `yaml:"corelog"`
	HTTPLog   string `yaml:"httplog"`
	RPCLog    string `yaml:"rpclog"`
	LogLevel  string `yaml:"loglevel"`
}

type SystemConf struct {
	User string `yaml:"user"`
}

type DebugConf struct {
	Pprof  bool `yaml:"pprof"`
	Allocs bool `yaml:"allocs"`
}

type ServerConfig struct {
	Storage StorageConf `yaml:"storage"`
	Net     NetConf     `yaml:"net"`
	Logger  LoggerConf  `yaml:"logger"`
	System  SystemConf  `yaml:"system"`
	Debug   DebugConf   `yaml:"debug"`
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
			Path:   "/tmp/reindex",
			Engine: "leveldb",
		},
		Net: NetConf{
			HTTPAddr: "0.0.0.0:9088",
			RPCAddr:  "0.0.0.0:6534",
			Security: false,
		},
		Logger: LoggerConf{
			ServerLog: "stdout",
			CoreLog:   "stdout",
			HTTPLog:   "stdout",
			LogLevel:  "error",
		},
		System: SystemConf{},
		Debug: DebugConf{
			Pprof:  false,
			Allocs: false,
		},
	}
}
