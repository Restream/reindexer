package reindexer

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
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
	adminNamespaces := []string{"admin_ns"}
	recomNamespaces := []string{"recom_ns"}
	rxCfg := []map[string]string{
		{
			"http": "127.0.0.1:10001",
			"rpc":  "127.0.0.1:10011",
			"db":   "adminapi",
		}, {
			"http": "127.0.0.1:10002",
			"rpc":  "127.0.0.1:10012",
			"db":   "recommendations",
		}, {
			"http": "127.0.0.1:10003",
			"rpc":  "127.0.0.1:10013",
			"db":   "frontapi",
		},
	}
	rxSrv := make([]*reindexer.Reindexer, 3)
	rxSrv[0] = serverUp(rxCfg[0], adminNamespaces)
	rxSrv[1] = serverUp(rxCfg[1], append(adminNamespaces, recomNamespaces...))
	rxSrv[2] = serverUp(rxCfg[2], append(adminNamespaces, recomNamespaces...))

	configureReplication(t, rxCfg[0]["http"], "", "master", rxCfg[0]["db"], adminNamespaces)
	configureReplication(t, rxCfg[1]["http"], fmt.Sprintf("%s/%s", rxCfg[0]["rpc"], rxCfg[0]["db"]), "slave", rxCfg[1]["db"], adminNamespaces)
	configureReplication(t, rxCfg[2]["http"], fmt.Sprintf("%s/%s", rxCfg[1]["rpc"], rxCfg[1]["db"]), "slave", rxCfg[2]["db"], append(adminNamespaces, recomNamespaces...))

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

func serverUp(cfg map[string]string, ns []string) *reindexer.Reindexer {
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

	return rx
}

func configureReplication(t *testing.T, httpAddr, masterRPC, role, db string, ns []string) {
	masterDSN := fmt.Sprintf("cproto://%s", masterRPC)

	body := fmt.Sprintf(`{
		"replication": {
			"cluster_id": 2,
				"force_sync_on_wrong_data_hash": true,
				"role": "%s",
				"master_dsn": "%s",
				"namespaces": ["%s"]
		},
		"type": "replication"
	}`, role, masterDSN, strings.Join(ns[:], `","`))

	httpAddr = fmt.Sprintf("http://%s/api/v1/db/%s/namespaces/%%23config/items", httpAddr, db)
	req, err := http.NewRequest(http.MethodPut, httpAddr, bytes.NewReader([]byte(body)))
	require.NoError(t, err)

	_, err = (&http.Client{}).Do(req)
	require.NoError(t, err)
}
