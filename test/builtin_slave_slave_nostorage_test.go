package reindexer

import (
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/restream/reindexer"
	rxConfig "github.com/restream/reindexer/bindings/builtinserver/config"
	"github.com/restream/reindexer/test/helpers"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Data struct {
	A string `json:"a" reindex:"id,,pk"`
}

func TestMasterSlaveSlaveNoStorage(t *testing.T) {
	if len(DB.slaveList) > 0 || len(DB.clusterList) > 0 {
		return
	}

	adminNamespaces := []string{"admin_ns"}
	recomNamespaces := []string{"recom_ns"}
	rxCfg := []reindexer.DBAsyncReplicationNode{
		{
			DSN: "cproto://127.0.0.1:10011/adminapi",
		}, {
			DSN: "cproto://127.0.0.1:10012/recommendations",
		}, {
			DSN: "cproto://127.0.0.1:10013/frontapi",
		},
	}
	var srvCfg []map[string]string
	for i := 0; i < 3; i++ {
		u, err := url.Parse(rxCfg[i].DSN)
		require.NoError(t, err)
		srvCfg = append(srvCfg, map[string]string{
			"http": u.Hostname() + ":" + strconv.Itoa(10001+i),
			"rpc":  u.Host,
			"db":   u.Path[1:],
		})
	}

	rxSrv := make([]*reindexer.Reindexer, 3)
	rxSrv[0] = serverUp(t, srvCfg[0], 0, adminNamespaces)
	rxSrv[1] = serverUp(t, srvCfg[1], 1, append(adminNamespaces, recomNamespaces...))
	rxSrv[2] = serverUp(t, srvCfg[2], 2, append(adminNamespaces, recomNamespaces...))

	configureReplication(t, rxSrv[0], "leader", adminNamespaces, []reindexer.DBAsyncReplicationNode{rxCfg[1]})
	configureReplication(t, rxSrv[1], "follower", append(adminNamespaces, recomNamespaces...), []reindexer.DBAsyncReplicationNode{rxCfg[2]})
	configureReplication(t, rxSrv[2], "follower", nil, nil)

	cnt, err := rxSrv[0].Insert(adminNamespaces[0], Data{A: "admin_item"})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	cnt, err = rxSrv[1].Insert(recomNamespaces[0], Data{A: "recom_item"})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	helpers.WaitForSyncWithMaster(t, rxSrv[0], rxSrv[1])
	helpers.WaitForSyncWithMaster(t, rxSrv[1], rxSrv[2])

	item, err := rxSrv[2].Query(recomNamespaces[0]).Exec().FetchOne()
	require.NoError(t, err)
	assert.Equal(t, "recom_item", item.(*Data).A)

	item, err = rxSrv[2].Query(adminNamespaces[0]).Exec().FetchOne()
	require.NoError(t, err)
	assert.Equal(t, "admin_item", item.(*Data).A)
}

func serverUp(t *testing.T, cfg map[string]string, serverID int, ns []string) *reindexer.Reindexer {
	opts := make([]interface{}, 0, 2)
	opts = append(opts, reindexer.WithCreateDBIfMissing())
	srvCfg := &rxConfig.ServerConfig{
		Net: rxConfig.NetConf{
			HTTPAddr: cfg["http"],
			RPCAddr:  cfg["rpc"],
		},
	}
	opts = append(opts, reindexer.WithServerConfig(5*time.Second, srvCfg))
	rx := reindexer.NewReindex(fmt.Sprintf("builtinserver://%s", cfg["db"]), opts...)
	for i := range ns {
		rx.OpenNamespace(ns[i], reindexer.DefaultNamespaceOptions(), Data{})
	}

	err := rx.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type:        "replication",
		Replication: &reindexer.DBReplicationConfig{ServerID: serverID, ClusterID: 2},
	})
	require.NoError(t, err)

	return rx
}

func configureReplication(t *testing.T, rx *reindexer.Reindexer, role string, ns []string, nodes []reindexer.DBAsyncReplicationNode) {
	err := rx.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type:             "async_replication",
		AsyncReplication: &reindexer.DBAsyncReplicationConfig{Role: role, Namespaces: ns, Nodes: nodes},
	})
	require.NoError(t, err)
}
