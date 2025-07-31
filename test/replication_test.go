package reindexer

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	rxConfig "github.com/restream/reindexer/v5/bindings/builtinserver/config"
	"github.com/restream/reindexer/v5/events"
	"github.com/restream/reindexer/v5/test/helpers"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestItemStorage struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name"`
}

type Data struct {
	A string `json:"a" reindex:"id,,pk"`
}

func serverUp(t *testing.T, cfg map[string]string, serverID int, ns []string) *reindexer.Reindexer {
	opts := make([]interface{}, 0, 2)
	opts = append(opts, reindexer.WithCreateDBIfMissing())
	srvCfg := &rxConfig.ServerConfig{
		Net: rxConfig.NetConf{
			HTTPAddr:    cfg["http"],
			RPCAddr:     cfg["rpc"],
			UnixRPCAddr: cfg["urpc"],
		},
	}
	opts = append(opts, reindexer.WithServerConfig(5*time.Second, srvCfg))
	rx, err := reindexer.NewReindex(fmt.Sprintf("builtinserver://%s", cfg["db"]), opts...)
	require.NoError(t, err)
	for i := range ns {
		rx.OpenNamespace(ns[i], reindexer.DefaultNamespaceOptions(), Data{})
	}

	err = rx.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type:        "replication",
		Replication: &reindexer.DBReplicationConfig{ServerID: serverID, ClusterID: 2},
	})
	require.NoError(t, err)

	return rx
}

func TestSlaveEmptyStorage(t *testing.T) {
	// Basic force sync test for builtin replication
	if len(DB.slaveList) > 0 || len(DB.clusterList) > 0 {
		t.Skip()
	}
	const nsName = "items"

	// Start follower
	cfgSlave := rxConfig.DefaultServerConfig()
	cfgSlave.Net.HTTPAddr = "0:29089"
	cfgSlave.Net.RPCAddr = "0:26535"
	cfgSlave.Storage.Path = path.Join(helpers.GetTmpDBDir(), "reindex_slave2")
	os.RemoveAll(cfgSlave.Storage.Path)
	rxSlave, err := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfgSlave))
	require.NoError(t, err)

	stream := rxSlave.Subscribe(events.DefaultEventsStreamOptions().
		WithNamespaceOperationEvents())
	defer stream.Close(context.Background())

	// Start leader
	const masterServerId = 1
	cfgMaster := rxConfig.DefaultServerConfig()
	cfgMaster.Net.HTTPAddr = "0:29088"
	cfgMaster.Net.RPCAddr = "0:26534"
	cfgMaster.Storage.Path = path.Join(helpers.GetTmpDBDir(), "reindex_master1")
	os.RemoveAll(cfgMaster.Storage.Path)
	rxMaster, err := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfgMaster))
	require.NoError(t, err)
	{
		f, err := os.OpenFile(cfgMaster.Storage.Path+"/xxx/replication.conf", os.O_RDWR|os.O_CREATE, 0644)
		assert.NoError(t, err)
		masterConfig := "server_id: " + strconv.Itoa(masterServerId) + "\n" +
			"cluster_id: 1\n"
		_, err = f.Write([]byte(masterConfig))
		assert.NoError(t, err)
	}
	{
		f, err := os.OpenFile(cfgMaster.Storage.Path+"/xxx/async_replication.conf", os.O_RDWR|os.O_CREATE, 0644)
		assert.NoError(t, err)
		masterConfig := `retry_sync_interval_msec: 3000
role: leader
syncs_per_thread: 2
app_name: node_1
force_sync_on_logic_error: true
force_sync_on_wrong_data_hash: false
namespaces: []
nodes:
  -
    dsn: cproto://127.0.0.1:26535/xxx
`
		_, err = f.Write([]byte(masterConfig))
		assert.NoError(t, err)
	}

	nsOption := reindexer.DefaultNamespaceOptions()
	nsOption.NoStorage()

	err = rxMaster.OpenNamespace(nsName, nsOption, TestItemStorage{})
	assert.NoError(t, err)
	err = rxSlave.OpenNamespace(nsName, nsOption, TestItemStorage{})
	assert.NoError(t, err)
	for i := 0; i < 1000; i++ {
		testItem := TestItemStorage{ID: i, Name: "test_" + strconv.Itoa(i)}
		err := rxMaster.Upsert(nsName, &testItem)
		if err != nil {
			panic(err)
		}
	}
	// Check leader's data
	qMaster := rxMaster.Query(nsName)
	itMaster := qMaster.Exec()
	defer itMaster.Close()
	testDataMaster, errfm := itMaster.FetchAll()
	assert.NoError(t, errfm)
	helpers.WaitForSyncWithMaster(t, rxMaster, rxSlave)
	// Check follower's data
	qSlave := rxSlave.Query(nsName)
	itSlave := qSlave.Exec()
	defer itSlave.Close()
	testDataSlave, errfs := itSlave.FetchAll()
	assert.NoError(t, errfs)
	assert.Equal(t, testDataSlave, testDataMaster, "Data in tables are not equal\n%s\n%s", testDataSlave, testDataMaster)
	// Check sync events
	syncEvents := 0
for_loop:
	for {
		tm := time.NewTicker(3 * time.Second)
		select {
		case event, ok := <-stream.Chan():
			require.NoError(t, stream.Error())
			require.True(t, ok)

			if event.Type() == events.EventTypeNamespaceSync {
				syncEvents += 1
				require.Equal(t, event.Namespace(), nsName)
			}
		case <-tm.C:
			// No new events for some time
			break for_loop
		}
	}
	assert.Equal(t, syncEvents, 1)
}

func TestMasterSlaveSlaveNoStorage(t *testing.T) {
	// Basic replication check for servers without storage paths
	if len(DB.slaveList) > 0 || len(DB.clusterList) > 0 {
		t.Skip()
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
			"urpc": "none",
		})
	}

	rxSrv := make([]*reindexer.Reindexer, 3)
	rxSrv[0] = serverUp(t, srvCfg[0], 0, adminNamespaces)
	rxSrv[1] = serverUp(t, srvCfg[1], 1, append(adminNamespaces, recomNamespaces...))
	rxSrv[2] = serverUp(t, srvCfg[2], 2, append(adminNamespaces, recomNamespaces...))

	helpers.ConfigureReplication(t, rxSrv[0], "leader", adminNamespaces, []reindexer.DBAsyncReplicationNode{rxCfg[1]})
	helpers.ConfigureReplication(t, rxSrv[1], "follower", append(adminNamespaces, recomNamespaces...), []reindexer.DBAsyncReplicationNode{rxCfg[2]})
	helpers.ConfigureReplication(t, rxSrv[2], "follower", nil, nil)

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

func TestCycleLeaders(t *testing.T) {
	// Check if two servers are able to replicate different namespaces on each other simultaneously

	if len(DB.slaveList) > 0 || len(DB.clusterList) > 0 {
		t.Skip()
	}

	adminNamespaces := []string{"admin_ns"}
	recomNamespaces := []string{"recom_ns"}

	rxCfg := []reindexer.DBAsyncReplicationNode{
		{
			DSN:        "cproto://127.0.0.1:10021/adminapi",
			Namespaces: recomNamespaces,
		}, {
			DSN:        "cproto://127.0.0.1:10022/recommendations",
			Namespaces: adminNamespaces,
		},
	}
	var srvCfg []map[string]string
	for i := 0; i < 2; i++ {
		u, err := url.Parse(rxCfg[i].DSN)
		require.NoError(t, err)
		srvCfg = append(srvCfg, map[string]string{
			"http": u.Hostname() + ":" + strconv.Itoa(10031+i),
			"rpc":  u.Host,
			"db":   u.Path[1:],
		})
	}

	rxSrv := make([]*reindexer.Reindexer, 3)
	rxSrv[0] = serverUp(t, srvCfg[0], 0, adminNamespaces)
	rxSrv[1] = serverUp(t, srvCfg[1], 1, recomNamespaces)
	for _, ns := range recomNamespaces {
		rxSrv[0].RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), Data{})
	}
	for _, ns := range adminNamespaces {
		rxSrv[1].RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), Data{})
	}

	helpers.ConfigureReplication(t, rxSrv[0], "leader", nil, []reindexer.DBAsyncReplicationNode{rxCfg[1]})
	helpers.ConfigureReplication(t, rxSrv[1], "leader", nil, []reindexer.DBAsyncReplicationNode{rxCfg[0]})

	cnt, err := rxSrv[0].Insert(adminNamespaces[0], Data{A: "admin_item"})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	cnt, err = rxSrv[1].Insert(recomNamespaces[0], Data{A: "recom_item"})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	helpers.WaitForSyncWithMaster(t, rxSrv[0], rxSrv[1])
	helpers.WaitForSyncWithMaster(t, rxSrv[1], rxSrv[0])

	item, err := rxSrv[0].Query(recomNamespaces[0]).Exec().FetchOne()
	require.NoError(t, err)
	assert.Equal(t, "recom_item", item.(*Data).A)

	item, err = rxSrv[1].Query(adminNamespaces[0]).Exec().FetchOne()
	require.NoError(t, err)
	assert.Equal(t, "admin_item", item.(*Data).A)

	cnt, err = rxSrv[0].Insert(adminNamespaces[0], Data{A: "admin_item_1"})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	cnt, err = rxSrv[1].Insert(recomNamespaces[0], Data{A: "recom_item_1"})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)
}
