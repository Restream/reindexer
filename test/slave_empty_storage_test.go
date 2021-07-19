package reindexer

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/restream/reindexer"
	"github.com/restream/reindexer/bindings/builtinserver/config"
	"github.com/restream/reindexer/test/helpers"
	"github.com/stretchr/testify/assert"
)

type TestItemStorage struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name"`
}

func TestSlaveEmptyStorage(t *testing.T) {
	cfgMaster := config.DefaultServerConfig()
	cfgMaster.Net.HTTPAddr = "0:29088"
	cfgMaster.Net.RPCAddr = "0:26534"
	cfgMaster.Storage.Path = "/tmp/rx_master1"
	os.RemoveAll(cfgMaster.Storage.Path)
	rxMaster := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfgMaster))
	{
		f, err := os.OpenFile(cfgMaster.Storage.Path+"/xxx/replication.conf", os.O_RDWR|os.O_CREATE, 0644)
		assert.NoError(t, err)
		masterConfig := `role: master  
master_dsn: 
timeout_sec: 60
enable_compression: true
cluster_id: 2
force_sync_on_logic_error: true
force_sync_on_wrong_data_hash: false
retry_sync_interval_sec: 20
online_repl_errors_threshold: 100
namespaces: []`

		_, err = f.Write([]byte(masterConfig))
		assert.NoError(t, err)
	}

	cfgSlave := config.DefaultServerConfig()
	cfgSlave.Net.HTTPAddr = "0:29089"
	cfgSlave.Net.RPCAddr = "0:26535"
	cfgSlave.Storage.Path = "/tmp/rx_slave2"
	os.RemoveAll(cfgSlave.Storage.Path)
	rxSlave := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfgSlave))
	{
		f, err := os.OpenFile(cfgSlave.Storage.Path+"/xxx/replication.conf", os.O_RDWR|os.O_CREATE, 0644)
		assert.NoError(t, err)
		slaveConfig := `role: slave  
master_dsn: cproto://127.0.0.1:26534/xxx
timeout_sec: 60
enable_compression: true
cluster_id: 2
force_sync_on_logic_error: true
force_sync_on_wrong_data_hash: false
retry_sync_interval_sec: 20
online_repl_errors_threshold: 100   
namespaces: []`

		_, err = f.Write([]byte(slaveConfig))
		assert.NoError(t, err)
	}

	nsOption := reindexer.DefaultNamespaceOptions()
	nsOption.NoStorage()

	err := rxMaster.OpenNamespace("items", nsOption, TestItemStorage{})
	assert.NoError(t, err)
	err = rxSlave.OpenNamespace("items", nsOption, TestItemStorage{})
	assert.NoError(t, err)
	for i := 0; i < 1000; i++ {
		testItem := TestItemStorage{ID: i, Name: "test_" + strconv.Itoa(i)}
		err := rxMaster.Upsert("items", &testItem)
		if err != nil {
			panic(err)
		}
	}
	qMaster := rxMaster.Query("items")
	itMaster := qMaster.Exec()
	defer itMaster.Close()
	testDataMaster, errfm := itMaster.FetchAll()
	assert.NoError(t, errfm)
	helpers.WaitForSyncWithMaster(t, rxMaster, rxSlave)

	qSlave := rxSlave.Query("items")
	itSlave := qSlave.Exec()
	defer itSlave.Close()
	testDataSlave, errfs := itSlave.FetchAll()
	assert.NoError(t, errfs)
	assert.Equal(t, testDataSlave, testDataMaster, "Data in tables not equals\n%s\n%s", testDataSlave, testDataMaster)
}
