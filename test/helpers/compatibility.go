package helpers

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/bindings/builtinserver/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type startupConfig struct {
	removeStorage bool
	serverID      int
	isLeader      bool
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

func writeAsyncReplicationConfig(t *testing.T, dbPath string, startupCfg *startupConfig, followerDSNs ...string) {
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
	writeAsyncReplicationConfig(t, dbPath, startupCfg, followerDSNs...)

	rx, err := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, serverConfig))
	require.NoError(t, err)
	return rx
}

func MakeLegacyNode(t *testing.T, binary string, cprotoDSN string, serverConfig *config.ServerConfig, startupCfg *startupConfig, followerDSNs ...string) (*reindexer.Reindexer, func()) {
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
		writeAsyncReplicationConfig(t, dbPath, startupCfg, followerDSNs...)
	}

	writeConfigs()
	return StartupServerFromBinary(t, binary, serverConfig, cprotoDSN)
}

func MakeLegacyLeader(t *testing.T, binary string, cprotoDSN string, serverConfig *config.ServerConfig, serverID int, followerDSNs ...string) (*reindexer.Reindexer, func()) {
	return MakeLegacyNode(t, binary, cprotoDSN, serverConfig, &startupConfig{removeStorage: true, serverID: serverID, isLeader: true}, followerDSNs...)
}

func MakeLegacyFollower(t *testing.T, binary string, cprotoDSN string, serverConfig *config.ServerConfig, serverID int, followerDSNs ...string) (*reindexer.Reindexer, func()) {
	return MakeLegacyNode(t, binary, cprotoDSN, serverConfig, &startupConfig{removeStorage: true, serverID: serverID, isLeader: false}, followerDSNs...)
}

func MakeLegacyFollowerNoStorageCleanup(t *testing.T, binary string, cprotoDSN string, serverConfig *config.ServerConfig, serverID int, followerDSNs ...string) (*reindexer.Reindexer, func()) {
	return MakeLegacyNode(t, binary, cprotoDSN, serverConfig, &startupConfig{removeStorage: false, serverID: serverID, isLeader: false}, followerDSNs...)
}

func GetData(t *testing.T, rx *reindexer.Reindexer, namespace string) []interface{} {
	it := rx.Query(namespace).Exec()
	defer it.Close()
	data, errfs := it.FetchAll()
	assert.NoError(t, errfs)
	return data
}

func GetDataFromNodes(t *testing.T, rxLeader *reindexer.Reindexer, rxFollower *reindexer.Reindexer, namespace string) []interface{} {
	dataLeader := GetData(t, rxLeader, namespace)
	WaitForSyncWithLeaderLegacy(t, rxLeader, rxFollower)
	dataFollower := GetData(t, rxFollower, namespace)
	assert.Equal(t, dataLeader, dataFollower, "Data in tables does not equal\n%s\n%s", dataLeader, dataFollower)
	return dataLeader
}

func GetDataFromNodesNoSync(t *testing.T, rxLeader *reindexer.Reindexer, rxFollower *reindexer.Reindexer, namespace string) []interface{} {
	dataLeader := GetData(t, rxLeader, namespace)
	dataFollower := GetData(t, rxFollower, namespace)
	assert.Equal(t, dataLeader, dataFollower, "Data in tables does not equal\n%s\n%s", dataLeader, dataFollower)
	return dataLeader
}

func StartupServerFromBinary(t *testing.T, binary string, serverConfig *config.ServerConfig, cprotoDSN string) (*reindexer.Reindexer, func()) {
	cmd := exec.Command(binary, "--db", serverConfig.Storage.Path, "-r", serverConfig.Net.RPCAddr, "-p", serverConfig.Net.HTTPAddr)
	var stdBuffer bytes.Buffer
	mw := io.MultiWriter(os.Stdout, &stdBuffer)
	cmd.Stdout = mw
	cmd.Stderr = mw
	err := cmd.Start()
	require.NoError(t, err)
	rx, err := awaitServerStartup(t, cprotoDSN)
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
	err = rx.RegisterNamespace("#memstats", reindexer.DefaultNamespaceOptions(), reindexer.NamespaceMemStat{})
	require.NoError(t, err)
	err = rx.CloseNamespace("#config")
	require.NoError(t, err)
	err = rx.RegisterNamespace("#config", reindexer.DefaultNamespaceOptions(), reindexer.DBConfigItem{})
	require.NoError(t, err)
	return rx, terminateF
}

func awaitServerStartup(t *testing.T, dsn string) (*reindexer.Reindexer, error) {
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
