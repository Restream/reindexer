package helpers

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/bindings/builtinserver"
	"github.com/restream/reindexer/bindings/builtinserver/config"
)

var httpCl = http.DefaultClient

type TestServer struct {
	T        *testing.T
	RpcPort  string
	HttpPort string
	DbName   string
	db       *reindexer.Reindexer
}

func (srv *TestServer) Addr() string {
	return "127.0.0.1:" + srv.RpcPort
}

func (srv *TestServer) Run() error {
	cfg := config.DefaultServerConfig()
	cfg.Net.RPCAddr = "127.0.0.1:" + srv.RpcPort
	cfg.Net.HTTPAddr = "127.0.0.1:" + srv.HttpPort
	cfg.Storage.Path = "/tmp/reindex_" + srv.RpcPort
	cfg.Logger.LogLevel = "error"
	cfg.Logger.ServerLog = ""
	cfg.Logger.HTTPLog = ""
	cfg.Logger.RPCLog = ""
	db := reindexer.NewReindex(fmt.Sprintf("builtinserver://%s_%s", srv.DbName, srv.RpcPort), reindexer.WithServerConfig(100*time.Second, cfg))
	srv.db = db

	t := time.NewTicker(100 * time.Millisecond)
	timeout := time.After(3 * time.Second)
	for {
		select {
		case <-timeout:
			srv.T.Fatal("server run timeout")
		case <-t.C:
			resp, _ := httpCl.Get("http://127.0.0.1:" + srv.HttpPort + "/api/v1/check")
			if resp == nil {
				continue
			}
			body, err := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return nil
			}

			if resp.StatusCode == 200 && string(body) != "" {
				return nil
			}
		}
	}
}

func (srv *TestServer) Clean() error {
	return os.RemoveAll("/tmp/reindex_" + srv.RpcPort)
}

func (srv *TestServer) Stop() error {
	if srv.db == nil {
		return nil
	}
	srv.db.Close()

	t := time.NewTicker(100 * time.Millisecond)
	timeout := time.After(3 * time.Second)
	for {
		select {
		case <-timeout:
			srv.T.Fatal("server stop timeout")
		case <-t.C:
			resp, err := httpCl.Get("http://127.0.0.1:" + srv.HttpPort + "/api/v1/check")
			if err == nil {
				resp.Body.Close()
			}

			if resp == nil {
				return nil
			}
		}
	}
}
