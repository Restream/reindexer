package reindexer

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/restream/reindexer"
	"github.com/restream/reindexer/bindings/builtinserver/config"
	"github.com/stretchr/testify/assert"
)

type TestItemStorage struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name"`
}

func WaitForSyncWithMaster(master *reindexer.Reindexer, slave *reindexer.Reindexer) {
	complete := true

	var nameBad string
	var masterBadLsn reindexer.LsnT
	var slaveBadLsn reindexer.LsnT

	for i := 0; i < 600*5; i++ {

		complete = true

		stats, err := master.GetNamespacesMemStat()
		if err != nil {
			panic(err)
		}
		slaveStats, err := slave.GetNamespacesMemStat()
		if err != nil {
			panic(err)
		}

		slaveLsnMap := make(map[string]reindexer.NamespaceMemStat)
		for _, st := range slaveStats {
			slaveLsnMap[st.Name] = *st
		}

		for _, st := range stats { // loop master namespaces stats

			if len(st.Name) == 0 || st.Name[0] == '#' {
				continue
			}

			if slaveLsn, ok := slaveLsnMap[st.Name]; ok {
				if slaveLsn.Replication.LastUpstreamLSN != st.Replication.LastLSN { //slave != master
					complete = false
					nameBad = st.Name
					masterBadLsn = st.Replication.LastLSN
					slaveBadLsn = slaveLsn.Replication.LastUpstreamLSN
					time.Sleep(100 * time.Millisecond)
					break
				}
			} else {
				complete = false
				nameBad = st.Name
				masterBadLsn.ServerId = 0
				masterBadLsn.Counter = 0
				slaveBadLsn.ServerId = 0
				slaveBadLsn.Counter = 0
				time.Sleep(100 * time.Millisecond)
				break
			}
		}
		if complete {
			for _, st := range stats {
				slaveLsn, _ := slaveLsnMap[st.Name]
				if slaveLsn.Replication.DataHash != st.Replication.DataHash {
					panic(fmt.Sprintf("Can't sync slave ns with master: ns \"%s\" slave dataHash: %d , master dataHash %d", st.Name, slaveLsn.Replication.DataHash, st.Replication.DataHash))
				}
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	panic(fmt.Sprintf("Can't sync slave ns with master: ns \"%s\" masterlsn: %d , slavelsn %d", nameBad, masterBadLsn, slaveBadLsn))
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
	WaitForSyncWithMaster(rxMaster, rxSlave)

	qSlave := rxSlave.Query("items")
	itSlave := qSlave.Exec()
	defer itSlave.Close()
	testDataSlave, errfs := itSlave.FetchAll()
	assert.NoError(t, errfs)
	assert.Equal(t, testDataSlave, testDataMaster, "Data in tables not equals\n%s\n%s", testDataSlave, testDataMaster)
}
