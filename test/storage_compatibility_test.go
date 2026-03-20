package reindexer

import (
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
)

func FillData(t *testing.T, rx *reindexer.Reindexer, count int) {
	for i := 0; i < count; i++ {
		testItem := TestItemStorage{ID: i, Name: "test_" + strconv.Itoa(i)}
		err := rx.Upsert("items", &testItem)
		assert.NoError(t, err)
	}
}

func TestStorageCompatibility(t *testing.T) {
	if len(DB.slaveList) > 0 || len(*legacyServerBinary) == 0 || runtime.GOOS == "windows" {
		t.Skip()
	}

	ns := "items"

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
			rxFollower := helpers.MakeFollower(t, cfgFollower, followerServerId)
			require.NoError(t, rxFollower.Status().Err)
			defer rxFollower.Close()
			rxLeader := helpers.MakeLeader(t, cfgLeader, leaderServerId, followerCproto)
			require.NoError(t, rxLeader.Status().Err)
			defer rxLeader.Close()

			err := rxFollower.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			err = rxLeader.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			FillData(t, rxLeader, dataCount)

			return helpers.GetDataFromNodes(t, rxLeader, rxFollower, ns)
		}

		readDataFromLegacyServer := func() []interface{} {
			rxLeader, terminateLeader := helpers.StartupServerFromBinary(t, *legacyServerBinary, cfgLeader, leaderCproto)
			defer terminateLeader()
			rxFollower, terminateFollower := helpers.StartupServerFromBinary(t, *legacyServerBinary, cfgFollower, followerCproto)
			defer terminateFollower()
			err := rxLeader.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			err = rxFollower.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			return helpers.GetDataFromNodesNoSync(t, rxLeader, rxFollower, ns)
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
			rxFollower, terminateFollower := helpers.MakeLegacyFollower(t, *legacyServerBinary, followerCproto, cfgFollower, followerServerId)
			defer terminateFollower()
			rxLeader, terminateLeader := helpers.MakeLegacyLeader(t, *legacyServerBinary, leaderCproto, cfgLeader, leaderServerId, followerCproto)
			defer terminateLeader()

			err := rxFollower.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			err = rxLeader.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			FillData(t, rxLeader, dataCount)

			return helpers.GetDataFromNodes(t, rxLeader, rxFollower, ns)
		}

		readDataFromCurrentServer := func() []interface{} {
			rxFollower, err := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfgFollower))
			require.NoError(t, err)
			defer rxFollower.Close()
			rxLeader, err := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfgLeader))
			require.NoError(t, err)
			defer rxLeader.Close()
			err = rxLeader.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			err = rxFollower.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemStorage{})
			require.NoError(t, err)
			return helpers.GetDataFromNodesNoSync(t, rxLeader, rxFollower, ns)
		}

		legacyStorageData := fillDataOnLegacyServer()
		curStorageData := readDataFromCurrentServer()
		assert.Equal(t, curStorageData, legacyStorageData, "Data in different storage versions are not equal\n%s\n%s", curStorageData, legacyStorageData)
		assert.Equal(t, len(curStorageData), dataCount)
	})
}
