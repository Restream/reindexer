package reindexer

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/bindings/builtinserver/config"
	"github.com/restream/reindexer/v5/test/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

type startupConfig struct {
	removeStorage bool
	serverID      int
	isLeader      bool
	masterDSN     string
}

func MakeLeader(t *testing.T, serverConfig *config.ServerConfig, serverID int, followerDSNs ...string) *reindexer.Reindexer {
	return MakeNode(t, serverConfig, &startupConfig{removeStorage: true, serverID: serverID, isLeader: true}, followerDSNs...)
}

func MakeLeaderNoStorageCleanup(t *testing.T, serverConfig *config.ServerConfig, serverID int, followerDSNs ...string) *reindexer.Reindexer {
	return MakeNode(t, serverConfig, &startupConfig{removeStorage: false, serverID: serverID, isLeader: true}, followerDSNs...)
}

func MakeFollower(t *testing.T, serverConfig *config.ServerConfig, serverID int, followerDSNs ...string) *reindexer.Reindexer {
	return MakeNode(t, serverConfig, &startupConfig{removeStorage: true, serverID: serverID, isLeader: false}, followerDSNs...)
}

func MakeFollowerNoStorageCleanup(t *testing.T, serverConfig *config.ServerConfig, serverID int, followerDSNs ...string) *reindexer.Reindexer {
	return MakeNode(t, serverConfig, &startupConfig{removeStorage: false, serverID: serverID, isLeader: false}, followerDSNs...)
}

func MakeNode(t *testing.T, serverConfig *config.ServerConfig, startupCfg *startupConfig, followerDSNs ...string) *reindexer.Reindexer {
	if startupCfg.removeStorage {
		err := os.RemoveAll(serverConfig.Storage.Path)
		require.NoError(t, err)
	}
	dbPath := serverConfig.Storage.Path + "/xxx/"
	{
		err := os.MkdirAll(dbPath, os.ModePerm)
		require.NoError(t, err)
		f, err := os.OpenFile(dbPath+".reindexer.storage", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
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
		config := "server_id: " + strconv.Itoa(startupCfg.serverID) + "\n" +
			"cluster_id: 1\n"
		_, err = f.Write([]byte(config))
		require.NoError(t, err)
	}
	{
		f, err := os.OpenFile(dbPath+"async_replication.conf", os.O_RDWR|os.O_CREATE, 0644)
		require.NoError(t, err)
		defer f.Close()
		role := "leader"
		if !startupCfg.isLeader {
			role = "follower"
		}
		config := `retry_sync_interval_msec: 3000
role: ` + role + `
syncs_per_thread: 2
app_name: node_1
force_sync_on_logic_error: true
force_sync_on_wrong_data_hash: false
batching_routines_count: 50
sync_threads: 2
log_level: info
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
	rx, err := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, serverConfig))
	require.NoError(t, err)
	return rx
}

func MakeLegacyNode(t *testing.T, cprotoDSN string, serverConfig *config.ServerConfig, startupCfg *startupConfig) (*reindexer.Reindexer, func()) {
	if startupCfg.removeStorage {
		err := os.RemoveAll(serverConfig.Storage.Path)
		require.NoError(t, err)
	}
	dbPath := serverConfig.Storage.Path + "/xxx/"
	writeConfigs := func() {
		{
			err := os.MkdirAll(dbPath, os.ModePerm)
			require.NoError(t, err)
			f, err := os.OpenFile(dbPath+".reindexer.storage", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			require.NoError(t, err)
			defer f.Close()
			_, err = f.Write(([]byte)("leveldb"))
			require.NoError(t, err)
			err = f.Sync()
			require.NoError(t, err)
		}
		{
			f, err := os.OpenFile(dbPath+"replication.conf", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			require.NoError(t, err)
			defer f.Close()
			role := "master"
			if !startupCfg.isLeader {
				role = "slave"
			}
			config := helpers.LegacyDBReplicationConfig{ServerID: startupCfg.serverID, Role: role, MasterDSN: startupCfg.masterDSN, ClusterID: 1, ForceSyncOnLogicError: true, ForceSyncOnWrongDataHash: true}
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

func MakeLegacyLeader(t *testing.T, cprotoDSN string, serverConfig *config.ServerConfig, serverID int) (*reindexer.Reindexer, func()) {
	return MakeLegacyNode(t, cprotoDSN, serverConfig, &startupConfig{removeStorage: true, serverID: serverID, isLeader: true, masterDSN: ""})
}

func MakeLegacyFollower(t *testing.T, cprotoDSN string, serverConfig *config.ServerConfig, serverID int, masterDSN string) (*reindexer.Reindexer, func()) {
	return MakeLegacyNode(t, cprotoDSN, serverConfig, &startupConfig{removeStorage: true, serverID: serverID, isLeader: false, masterDSN: masterDSN})
}

func MakeLegacyFollowerNoStorageCleanup(t *testing.T, cprotoDSN string, serverConfig *config.ServerConfig, serverID int, masterDSN string) (*reindexer.Reindexer, func()) {
	return MakeLegacyNode(t, cprotoDSN, serverConfig, &startupConfig{removeStorage: false, serverID: serverID, isLeader: false, masterDSN: masterDSN})
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
		helpers.WaitForSyncWithMasterV3V3(t, rxLeader, rxFollower)
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
		require.Equal(t, st.ExitCode(), 0)
	}
	if err != nil {
		terminateF()
		require.NoError(t, err, "Server log: %s", stdBuffer.String())
	}
	err = rx.CloseNamespace("#memstats")
	require.NoError(t, err)
	err = rx.RegisterNamespace("#memstats", reindexer.DefaultNamespaceOptions(), helpers.LegacyNamespaceMemStat{})
	require.NoError(t, err)
	err = rx.CloseNamespace("#config")
	require.NoError(t, err)
	err = rx.RegisterNamespace("#config", reindexer.DefaultNamespaceOptions(), helpers.LegacyDBConfigItem{})
	require.NoError(t, err)
	return rx, terminateF
}

func AwaitServerStartup(t *testing.T, dsn string) (*reindexer.Reindexer, error) {
	var err error
	for i := 0; i < 20; i++ {
		rx, err := reindexer.NewReindex(dsn)
		if err == nil {
			return rx, nil
		}
		time.Sleep(time.Second * 1)
	}
	return nil, err
}

func TestStorageCompatibility(t *testing.T) {
	if len(DB.slaveList) > 0 || len(*legacyServerBinary) == 0 || runtime.GOOS == "windows" {
		t.Skip()
	}

	baseStoragePath := path.Join(helpers.GetTmpDBDir(), "reindex_test_storage_compatibility/")
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
			rxFollower, err := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfgFollower))
			require.NoError(t, err)
			defer rxFollower.Close()
			rxLeader, err := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfgLeader))
			require.NoError(t, err)
			defer rxLeader.Close()
			err = rxLeader.RegisterNamespace("items", reindexer.DefaultNamespaceOptions(), TestItemStorage{})
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
