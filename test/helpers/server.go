package helpers

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/bindings/builtinserver"
	"github.com/restream/reindexer/bindings/builtinserver/config"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	ServerTypeStandalone = "standalone"
	ServerTypeBuiltin    = "builtin"
)

var httpCl = http.DefaultClient

type TestServer struct {
	T             *testing.T
	RpcPort       string
	HttpPort      string
	DbName        string
	db            *reindexer.Reindexer
	proc          *exec.Cmd
	SrvType       string
	EnableCluster bool
}

func (srv *TestServer) GetDSN() string {
	return fmt.Sprintf("cproto://127.0.0.1:%s/%s", srv.RpcPort, srv.GetDbName())
}

func (srv *TestServer) GetRpcAddr() string {
	return "127.0.0.1:" + srv.RpcPort
}

func (srv *TestServer) GetHttpAddr() string {
	return "http://127.0.0.1:" + srv.HttpPort
}

func (srv *TestServer) GetDbName() string {
	return fmt.Sprintf("%s_%s", srv.DbName, srv.RpcPort)
}

func (srv *TestServer) GetFullStoragePath() string {
	return fmt.Sprintf("/tmp/reindex_%s/%s_%s", srv.RpcPort, srv.DbName, srv.RpcPort)
}

func (srv *TestServer) Run() error {
	cfg := config.DefaultServerConfig()
	cfg.Net.RPCAddr = "127.0.0.1:" + srv.RpcPort
	cfg.Net.HTTPAddr = "127.0.0.1:" + srv.HttpPort
	cfg.Net.RAFTCluster = srv.EnableCluster
	cfg.Storage.Path = "/tmp/reindex_" + srv.RpcPort
	cfg.Logger.LogLevel = "error"
	cfg.Logger.ServerLog = "/tmp/reindex_" + srv.RpcPort + "/server.log"
	cfg.Logger.HTTPLog = "/tmp/reindex_" + srv.RpcPort + "/http.log"
	cfg.Logger.RPCLog = "/tmp/reindex_" + srv.RpcPort + "/rpc.log"
	cfg.Logger.CoreLog = "/tmp/reindex_" + srv.RpcPort + "/core.log"
	cfg.Metrics.ClientsStats = true

	switch srv.SrvType {
	case ServerTypeStandalone:
		data, err := yaml.Marshal(&cfg)
		if err != nil {
			require.NoError(srv.T, err)
		}

		f, err := os.Create("/tmp/reindex_cluster_" + srv.RpcPort + ".conf")
		if err != nil {
			require.NoError(srv.T, err)
		}

		_, err = f.Write(data)
		if err != nil {
			require.NoError(srv.T, err)
		}

		args := []string{"--config=/tmp/reindex_cluster_" + srv.RpcPort + ".conf"}
		if srv.EnableCluster {
			args = append(args, "--enable-cluster")
		}
		cmd := exec.Command("../build/cpp_src/cmd/reindexer_server/reindexer_server", args...)
		if err := cmd.Start(); err != nil {
			require.NoError(srv.T, err)
		}
		srv.proc = cmd
	case ServerTypeBuiltin:
		db := reindexer.NewReindex(fmt.Sprintf("builtinserver://%s", srv.GetDbName()), reindexer.WithServerConfig(100*time.Second, cfg))
		srv.db = db
		err := srv.db.Status().Err
		if srv.T != nil {
			require.NoError(srv.T, err)
		} else if err != nil {
			panic(err)
		}
	default:
		if srv.T != nil {
			srv.T.Fatal("unknown server type")
		} else {
			panic("unknown server type")
		}
	}

	t := time.NewTicker(100 * time.Millisecond)
	timeout := time.After(3 * time.Second)
	for {
		select {
		case <-timeout:
			srv.T.Fatalf("server run timeout, %s", srv.HttpPort)
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

func (srv *TestServer) DB() *reindexer.Reindexer {
	return srv.db
}

func (srv *TestServer) Stop() error {
	if srv.db == nil && srv.proc == nil {
		srv.T.Fatalf("unknown server state, %#v", srv)
	}
	switch srv.SrvType {
	case ServerTypeStandalone:
		require.NoError(srv.T, srv.proc.Process.Signal(syscall.SIGINT))
	case ServerTypeBuiltin:
		srv.db.Close()
	default:
		srv.T.Fatal("unknown server type")
	}

	t := time.NewTicker(100 * time.Millisecond)
	timeout := time.After(3 * time.Second)
	for {
		select {
		case <-timeout:
			srv.T.Fatalf("server stop timeout, %#v", srv.HttpPort)
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

func CreateCluster(t *testing.T, servers []*TestServer, nsName string, nsItem interface{}) {
	clusterConf := DefaultClusterConf()
	for i := range servers {
		servers[i].EnableCluster = true
		clusterConf.Nodes = append(clusterConf.Nodes, ClusterNodeConfig{
			DSN:      fmt.Sprintf("cproto://127.0.0.1:%s/%s", servers[i].RpcPort, servers[i].GetDbName()),
			ServerID: i + 1,
		})
	}

	var wg sync.WaitGroup
	for i := range servers {
		wg.Add(1)
		go func(srv *TestServer, i int, wg *sync.WaitGroup) {
			require.NoError(t, srv.Run())
			dbTmp := reindexer.NewReindex(srv.GetDSN(), reindexer.WithCreateDBIfMissing())
			defer dbTmp.Close()
			require.NotNil(t, dbTmp)
			dbTmp.OpenNamespace(nsName, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), nsItem)

			replicationConf := ReplicationConf{
				ServerID:  i + 1,
				ClusterID: 2,
			}
			require.NoError(t, replicationConf.ToFile("/tmp/reindex_"+srv.RpcPort+"/"+srv.DbName+"_"+srv.RpcPort, "replication.conf"))
			require.NoError(t, clusterConf.ToFile("/tmp/reindex_"+srv.RpcPort+"/"+srv.DbName+"_"+srv.RpcPort, "cluster.conf"))

			require.NoError(t, srv.Stop())
			require.NoError(t, srv.Run())
			wg.Done()
		}(servers[i], i, &wg)
	}
	wg.Wait()
	time.Sleep(1 * time.Second)
}

func CreateReplication(t *testing.T, master *TestServer, slaves []*TestServer, nsName string, nsItem interface{}) {
	require.NoError(t, master.Run())
	masterDb := reindexer.NewReindex(master.GetDSN(), reindexer.WithCreateDBIfMissing())
	defer masterDb.Close()
	require.NotNil(t, masterDb)
	require.NoError(t, masterDb.Status().Err)

	err := masterDb.OpenNamespace(nsName, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), nsItem)
	require.NoError(t, err)

	var slaveDbs []*reindexer.Reindexer
	var nodes []reindexer.DBAsyncReplicationNode
	for i := range slaves {
		require.NoError(t, slaves[i].Run())
		db := reindexer.NewReindex(slaves[i].GetDSN(), reindexer.WithCreateDBIfMissing())
		defer db.Close()
		require.NotNil(t, db)
		require.NoError(t, db.Status().Err)
		err := db.OpenNamespace(nsName, reindexer.DefaultNamespaceOptions(), nsItem)
		require.NoError(t, err)
		slaveDbs = append(slaveDbs, db)

		nodes = append(nodes, reindexer.DBAsyncReplicationNode{DSN: slaves[i].GetDSN()})
		ConfigureReplication(t, slaveDbs[i], "follower", []string{nsName}, nil)
	}

	ConfigureReplication(t, masterDb, "leader", []string{nsName}, nodes)
	WaitForSyncWithMaster(t, masterDb, slaveDbs[0])
}

func ConfigureReplication(t *testing.T, rx *reindexer.Reindexer, role string, ns []string, nodes []reindexer.DBAsyncReplicationNode) {
	err := rx.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type: "async_replication",
		AsyncReplication: &reindexer.DBAsyncReplicationConfig{
			Role:              role,
			Namespaces:        ns,
			Nodes:             nodes,
			RetrySyncInterval: 100,
		},
	})
	require.NoError(t, err)
}
