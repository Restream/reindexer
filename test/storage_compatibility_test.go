package reindexer

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/restream/reindexer"
	"github.com/restream/reindexer/bindings/builtinserver/config"
	"github.com/restream/reindexer/test/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func MakeLeader(t *testing.T, serverConfig *config.ServerConfig, serverID int, followerDSNs ...string) *reindexer.Reindexer {
	return MakeNode(t, serverConfig, serverID, true, followerDSNs...)
}

func MakeFollower(t *testing.T, serverConfig *config.ServerConfig, serverID int, followerDSNs ...string) *reindexer.Reindexer {
	return MakeNode(t, serverConfig, serverID, false, followerDSNs...)
}

func MakeNode(t *testing.T, serverConfig *config.ServerConfig, serverID int, isLeader bool, followerDSNs ...string) *reindexer.Reindexer {
	os.RemoveAll(serverConfig.Storage.Path)
	rx := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, serverConfig))
	{
		f, err := os.OpenFile(serverConfig.Storage.Path+"/xxx/replication.conf", os.O_RDWR|os.O_CREATE, 0644)
		require.NoError(t, err)
		defer f.Close()
		config := "server_id: " + strconv.Itoa(serverID) + "\n" +
			"cluster_id: 1\n"
		_, err = f.Write([]byte(config))
		require.NoError(t, err)
	}
	{
		f, err := os.OpenFile(serverConfig.Storage.Path+"/xxx/async_replication.conf", os.O_RDWR|os.O_CREATE, 0644)
		require.NoError(t, err)
		defer f.Close()
		role := "leader"
		if !isLeader {
			role = "follower"
		}
		config := `retry_sync_interval_msec: 3000
role: ` + role + `
syncs_per_thread: 2
app_name: node_1
force_sync_on_logic_error: true
force_sync_on_wrong_data_hash: false
namespaces: []`
		if len(followerDSNs) > 0 {
			config += "\nnodes:\n"
		}
		for _, dsn := range followerDSNs {
			config += "  - dsn: " + dsn
		}
		_, err = f.Write([]byte(config))
		require.NoError(t, err)
	}
	return rx
}

func FillData(t *testing.T, rx *reindexer.Reindexer, count int) {
	for i := 0; i < count; i++ {
		testItem := TestItemStorage{ID: i, Name: "test_" + strconv.Itoa(i)}
		err := rx.Upsert("items", &testItem)
		assert.NoError(t, err)
	}
}

func GetData(t *testing.T, rx *reindexer.Reindexer) []interface{} {
	it := rx.Query("items").Exec()
	defer it.Close()
	data, errfs := it.FetchAll()
	assert.NoError(t, errfs)
	return data
}

func GetDataFromNodes(t *testing.T, rxLeader *reindexer.Reindexer, rxFollower *reindexer.Reindexer, useLegacySync bool) []interface{} {
	dataLeader := GetData(t, rxLeader)
	if useLegacySync {
		WaitForSyncWithMasterLegacy(t, rxLeader, rxFollower)
	} else {
		helpers.WaitForSyncWithMaster(t, rxLeader, rxFollower)
	}
	dataFollower := GetData(t, rxFollower)
	assert.Equal(t, dataLeader, dataFollower, "Data in tables does not equal\n%s\n%s", dataLeader, dataFollower)
	return dataLeader
}

func GetDataFromNodesNoSync(t *testing.T, rxLeader *reindexer.Reindexer, rxFollower *reindexer.Reindexer) []interface{} {
	dataLeader := GetData(t, rxLeader)
	dataFollower := GetData(t, rxFollower)
	assert.Equal(t, dataLeader, dataFollower, "Data in tables does not equal\n%s\n%s", dataLeader, dataFollower)
	return dataLeader
}

func GetLegacyNamespacesMemStat(db *reindexer.Reindexer) ([]*LegacyNamespaceMemStat, error) {
	result := []*LegacyNamespaceMemStat{}

	descs, err := db.Query("#memstats").Exec().FetchAll()
	if err != nil {
		return nil, err
	}

	for _, desc := range descs {
		nsdesc, ok := desc.(*LegacyNamespaceMemStat)
		if ok {
			result = append(result, nsdesc)
		}
	}

	return result, nil
}

func WaitForSyncWithMasterLegacy(t *testing.T, master *reindexer.Reindexer, slave *reindexer.Reindexer) {
	complete := true

	var nameBad string
	var masterBadLsn reindexer.LsnT
	var slaveBadLsn reindexer.LsnT

	for i := 0; i < 600*5; i++ {

		complete = true

		masterStats, err := GetLegacyNamespacesMemStat(master)
		require.NoError(t, err)

		masterMemStatMap := make(map[string]LegacyNamespaceMemStat)
		for _, st := range masterStats {
			masterMemStatMap[st.Name] = *st
		}

		slaveStats, err := GetLegacyNamespacesMemStat(slave)
		require.NoError(t, err)

		slaveMemStatMap := make(map[string]LegacyNamespaceMemStat)
		for _, st := range slaveStats {
			slaveMemStatMap[st.Name] = *st
		}

		configItemNs, found := slave.Query("#config").Where("type", reindexer.EQ, "replication").Get()
		var namespaces []string
		if found {
			replication := configItemNs.(*LegacyDBConfigItem)
			namespaces = replication.Replication.Namespaces
		}

		checkNsMap := make(map[string]bool)
		if len(namespaces) > 0 {
			for _, s := range namespaces {
				checkNsMap[s] = true
			}
		} else {
			for _, st := range masterStats {
				if len(st.Name) == 0 || st.Name[0] == '#' {
					continue
				}
				checkNsMap[st.Name] = true
			}
		}

		for nsName, _ := range checkNsMap { // loop sync namespaces list (all or defined)
			masterNsData, ok := masterMemStatMap[nsName]

			if ok {
				if slaveNsData, ok := slaveMemStatMap[nsName]; ok {
					if slaveNsData.Replication.LastUpstreamLSN != masterNsData.Replication.LastLSN { //slave != master
						complete = false
						nameBad = nsName
						masterBadLsn = masterNsData.Replication.LastLSN
						slaveBadLsn = slaveNsData.Replication.LastUpstreamLSN
						break
					}
				} else {
					complete = false
					nameBad = nsName
					masterBadLsn.ServerId = 0
					masterBadLsn.Counter = 0
					slaveBadLsn.ServerId = 0
					slaveBadLsn.Counter = 0
					break
				}
			}
		}
		if complete {
			for nsName, _ := range checkNsMap {
				slaveNsData, _ := slaveMemStatMap[nsName]
				masterNsData, _ := masterMemStatMap[nsName]
				if slaveNsData.Replication.DataHash != masterNsData.Replication.DataHash {
					t.Fatalf("Can't sync slave ns with master: ns \"%s\" slave dataHash: %d , master dataHash %d", nsName, slaveNsData.Replication.DataHash, masterNsData.Replication.DataHash)
				}
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("Can't sync slave ns with master: ns \"%s\" masterlsn: %+v , slavelsn %+v", nameBad, masterBadLsn, slaveBadLsn)
}

func AwaitServerStartup(t *testing.T, dsn string) (*reindexer.Reindexer, error) {
	var err error
	for i := 0; i < 10; i++ {
		rx := reindexer.NewReindex(dsn)
		status := rx.Status()
		if status.Err == nil {
			return rx, nil
		}
		err = status.Err
		time.Sleep(time.Second * 1)
	}
	return nil, err
}

func StartupServerFromBinary(t *testing.T, serverConfig *config.ServerConfig, cprotoDSN string) (*reindexer.Reindexer, func()) {
	cmd := exec.Command(*legacyServerBinary, "--db", serverConfig.Storage.Path, "-r", serverConfig.Net.RPCAddr, "-p", serverConfig.Net.HTTPAddr)
	var stdBuffer bytes.Buffer
	mw := io.MultiWriter(os.Stdout, &stdBuffer)
	cmd.Stdout = mw
	cmd.Stderr = mw
	err := cmd.Start()
	require.NoError(t, err)
	rx, err := AwaitServerStartup(t, cprotoDSN)
	terminateF := func() {
		cmd.Process.Signal(os.Interrupt)
		st, err := cmd.Process.Wait()
		require.NoError(t, err)
		require.True(t, st.Exited())
		require.True(t, st.Success())
	}
	if err != nil {
		terminateF()
		require.NoError(t, err, "Server log: %s", stdBuffer.String())
	}
	err = rx.CloseNamespace("#memstats")
	require.NoError(t, err)
	err = rx.RegisterNamespace("#memstats", reindexer.DefaultNamespaceOptions(), LegacyNamespaceMemStat{})
	require.NoError(t, err)
	err = rx.CloseNamespace("#config")
	require.NoError(t, err)
	err = rx.RegisterNamespace("#config", reindexer.DefaultNamespaceOptions(), LegacyDBConfigItem{})
	require.NoError(t, err)
	return rx, terminateF
}

func MakeLegacyLeader(t *testing.T, cprotoDSN string, serverConfig *config.ServerConfig, serverID int) (*reindexer.Reindexer, func()) {
	return MakeLegacyNode(t, cprotoDSN, serverConfig, serverID, true, "")
}

func MakeLegacyFollower(t *testing.T, cprotoDSN string, serverConfig *config.ServerConfig, serverID int, masterDSN string) (*reindexer.Reindexer, func()) {
	return MakeLegacyNode(t, cprotoDSN, serverConfig, serverID, false, masterDSN)
}

type LegacyDBConfigItem struct {
	Type        string                          `json:"type"`
	Profiling   *reindexer.DBProfilingConfig    `json:"profiling,omitempty"`
	Namespaces  *[]reindexer.DBNamespacesConfig `json:"namespaces,omitempty"`
	Replication *LegacyDBReplicationConfig      `json:"replication,omitempty"`
}

type LegacyDBReplicationConfig struct {
	// Replication role. One of  none, slave, master
	Role string `yaml:"role"`
	// DSN to master. Only cproto schema is supported
	MasterDSN string `yaml:"master_dsn",omitempty`
	// Cluster ID - must be same for client and for master
	ClusterID int `json:"cluster_id"`
	// force resync on logic error conditions
	ForceSyncOnLogicError bool `json:"force_sync_on_logic_error"`
	// force resync on wrong data hash conditions
	ForceSyncOnWrongDataHash bool `json:"force_sync_on_wrong_data_hash"`
	// List of namespaces for replication. If emply, all namespaces. All replicated namespaces will become read only for slave
	Namespaces []string `json:"namespaces"`
}

type LegacyNamespaceMemStat struct {
	// Name of namespace
	Name string `json:"name"`
	// Replication status of namespace
	Replication struct {
		// Last Log Sequence Number (LSN) of applied namespace modification
		LastLSN reindexer.LsnT `json:"last_lsn_v2"`
		// If true, then namespace is in slave mode
		SlaveMode bool `json:"slave_mode"`
		// True enable replication
		ReplicatorEnabled bool `json:"replicator_enabled"`
		// Temporary namespace flag
		Temporary bool `json:"temporary"`
		// Number of storage's master <-> slave switches
		IncarnationCounter int64 `json:"incarnation_counter"`
		// Hashsum of all records in namespace
		DataHash uint64 `json:"data_hash"`
		// Data count
		DataCount int `json:"data_count"`
		// Write Ahead Log (WAL) records count
		WalCount int64 `json:"wal_count"`
		// Total memory consumption of Write Ahead Log (WAL)
		WalSize int64 `json:"wal_size"`
		// Data updated timestamp
		UpdatedUnixNano int64 `json:"updated_unix_nano"`
		// Current replication status
		Status string `json:"status"`
		// Origin LSN of last replication operation
		OriginLSN reindexer.LsnT `json:"origin_lsn"`
		// Last LSN of api operation (not from replication)
		LastSelfLSN reindexer.LsnT `json:"last_self_lsn"`
		// Last Log Sequence Number (LSN) of applied namespace modification
		LastUpstreamLSN reindexer.LsnT `json:"last_upstream_lsn"`
		// Last replication error code
		ErrorCode int64 `json:"error_code"`
		// Last replication error message
		ErrorMessage string `json:"error_message"`
	} `json:"replication"`
}

func MakeLegacyNode(t *testing.T, cprotoDSN string, serverConfig *config.ServerConfig, serverID int, isLeader bool, masterDSN string) (*reindexer.Reindexer, func()) {
	err := os.RemoveAll(serverConfig.Storage.Path)
	require.NoError(t, err)
	dbPath := serverConfig.Storage.Path + "/xxx/"
	writeConfigs := func() {
		{
			err := os.MkdirAll(dbPath, os.ModePerm)
			require.NoError(t, err)
			f, err := os.OpenFile(dbPath+".reindexer.storage", os.O_RDWR|os.O_CREATE, 0644)
			require.NoError(t, err)
			defer f.Close()
			_, err = f.Write(([]byte)("leveldb"))
			require.NoError(t, err)
			err = f.Sync()
			require.NoError(t, err)
		}
		{
			f, err := os.OpenFile(dbPath+"replication.conf", os.O_RDWR|os.O_CREATE, 0644)
			require.NoError(t, err)
			defer f.Close()
			role := "master"
			if !isLeader {
				role = "slave"
			}
			config := LegacyDBReplicationConfig{Role: role, MasterDSN: masterDSN, ClusterID: 1, ForceSyncOnLogicError: true, ForceSyncOnWrongDataHash: true}
			yaml, err := yaml.Marshal(config)
			require.NoError(t, err)
			_, err = f.Write(yaml)
			require.NoError(t, err)
			err = f.Sync()
			require.NoError(t, err)
		}
	}

	writeConfigs()
	return StartupServerFromBinary(t, serverConfig, cprotoDSN)
}

func TestStorageCompatibility(t *testing.T) {
	if len(DB.slaveList) > 0 || len(*legacyServerBinary) == 0 || runtime.GOOS == "windows" {
		t.SkipNow()
	}

	const baseStoragePath = "/tmp/reindex_test_storage_compatibility/"
	const dataCount = 100
	const followerServerId = 2
	cfgFollower := config.DefaultServerConfig()
	cfgFollower.Net.HTTPAddr = "0:30089"
	cfgFollower.Net.RPCAddr = "0:30535"
	cfgFollower.Logger.LogLevel = "trace"
	const followerCproto = "cproto://127.0.0.1:30535/xxx"

	const leaderServerId = 1
	cfgLeader := config.DefaultServerConfig()
	cfgLeader.Net.HTTPAddr = "0:30088"
	cfgLeader.Net.RPCAddr = "0:30534"
	cfgLeader.Logger.LogLevel = "trace"
	const leaderCproto = "cproto://127.0.0.1:30534/xxx"

	t.Run("backward compatibility", func(t *testing.T) {
		cfgFollower.Storage.Path = baseStoragePath + "backward/follower"
		cfgLeader.Storage.Path = baseStoragePath + "backward/leader"
		fillDataOnCurrentServer := func() []interface{} {
			rxFollower := MakeFollower(t, cfgFollower, followerServerId)
			require.NoError(t, rxFollower.Status().Err)
			defer rxFollower.Close()
			rxLeader := MakeLeader(t, cfgLeader, leaderServerId, followerCproto)
			require.NoError(t, rxLeader.Status().Err)
			defer rxLeader.Close()

			err := rxFollower.OpenNamespace("items", reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			err = rxLeader.OpenNamespace("items", reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			FillData(t, rxLeader, dataCount)

			return GetDataFromNodes(t, rxLeader, rxFollower, false)
		}

		readDataFromLegacyServer := func() []interface{} {
			rxLeader, terminateLeader := StartupServerFromBinary(t, cfgLeader, leaderCproto)
			defer terminateLeader()
			rxFollower, terminateFollower := StartupServerFromBinary(t, cfgFollower, followerCproto)
			defer terminateFollower()
			err := rxLeader.RegisterNamespace("items", reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			err = rxFollower.RegisterNamespace("items", reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			return GetDataFromNodesNoSync(t, rxLeader, rxFollower)
		}

		curStorageData := fillDataOnCurrentServer()
		legacyStorageData := readDataFromLegacyServer()

		assert.Equal(t, curStorageData, legacyStorageData, "Data in different storage versions are not equal\n%s\n%s", curStorageData, legacyStorageData)
		assert.Equal(t, len(curStorageData), dataCount)
	})

	t.Run("forward compatibility", func(t *testing.T) {
		cfgFollower.Storage.Path = baseStoragePath + "forward/follower"
		cfgLeader.Storage.Path = baseStoragePath + "forward/leader"
		fillDataOnLegacyServer := func() []interface{} {
			rxLeader, terminateLeader := MakeLegacyLeader(t, leaderCproto, cfgLeader, leaderServerId)
			defer terminateLeader()
			rxFollower, terminateFollower := MakeLegacyFollower(t, followerCproto, cfgFollower, followerServerId, leaderCproto)
			defer terminateFollower()

			err := rxFollower.OpenNamespace("items", reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			err = rxLeader.OpenNamespace("items", reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			FillData(t, rxLeader, dataCount)

			return GetDataFromNodes(t, rxLeader, rxFollower, true)
		}

		readDataFromCurrentServer := func() []interface{} {
			rxFollower := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfgFollower))
			require.NoError(t, rxFollower.Status().Err)
			defer rxFollower.Close()
			rxLeader := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfgLeader))
			require.NoError(t, rxLeader.Status().Err)
			defer rxLeader.Close()
			err := rxLeader.RegisterNamespace("items", reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			err = rxFollower.RegisterNamespace("items", reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			return GetDataFromNodesNoSync(t, rxLeader, rxFollower)
		}

		legacyStorageData := fillDataOnLegacyServer()
		curStorageData := readDataFromCurrentServer()
		assert.Equal(t, curStorageData, legacyStorageData, "Data in different storage versions are not equal\n%s\n%s", curStorageData, legacyStorageData)
		assert.Equal(t, len(curStorageData), dataCount)
	})
}
