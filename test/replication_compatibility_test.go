package reindexer

import (
	"log"
	"path"
	"runtime"
	"strconv"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/bindings/builtinserver/config"
	"github.com/restream/reindexer/v5/test/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestItemReplicationCompat struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name"`
}

func fillTestItemReplicationCompatData(t *testing.T, rx *reindexer.Reindexer, ns string, count int) {
	for i := 0; i < count; i++ {
		testItem := TestItemReplicationCompat{ID: i, Name: "test_" + strconv.Itoa(i)}
		err := rx.Upsert(ns, &testItem)
		assert.NoError(t, err)
	}
}

func getAsyncReplStats(t *testing.T, db *reindexer.Reindexer) reindexer.ReplicationStat {
	resp, err := db.Query(reindexer.ReplicationStatsNamespaceName).
		WhereString("type", reindexer.EQ, "async").
		MustExec().
		FetchOne()
	require.NoError(t, err)
	return *resp.(*reindexer.ReplicationStat)
}

func TestWALSyncCompatibility(t *testing.T) {
	if len(DB.slaveList) > 0 || len(*legacyServerBinary) == 0 || runtime.GOOS == "windows" {
		t.Skip()
	}

	ns := "items"

	baseStoragePath := path.Join(helpers.GetTmpDBDir(), "reindex_test_wal_sync_compatibility")
	const dataCount = 100
	const followerServerId = 2
	cfgFollower := config.DefaultServerConfig()
	cfgFollower.Net.HTTPAddr = "0:30189"
	cfgFollower.Net.RPCAddr = "0:30635"
	cfgFollower.Logger.LogLevel = "trace"
	const followerCproto = "cproto://127.0.0.1:30635/xxx"

	const leaderServerId = 1
	cfgLeader := config.DefaultServerConfig()
	cfgLeader.Net.HTTPAddr = "0:30188"
	cfgLeader.Net.RPCAddr = "0:30634"
	cfgLeader.Logger.LogLevel = "trace"
	const leaderCproto = "cproto://127.0.0.1:30634/xxx"

	t.Run("with legacy leader", func(t *testing.T) {
		cfgFollower.Storage.Path = path.Join(baseStoragePath, "with_legacy_leader/follower")
		cfgLeader.Storage.Path = path.Join(baseStoragePath, "with_legacy_leader/leader")

		rxFollower := helpers.MakeFollower(t, cfgFollower, followerServerId)
		require.NoError(t, rxFollower.Status().Err)
		defer rxFollower.Close()
		rxLeader, terminateLeader := helpers.MakeLegacyLeader(t, *legacyServerBinary, leaderCproto, cfgLeader, leaderServerId, followerCproto)
		defer terminateLeader()

		err := rxFollower.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemReplicationCompat{})
		require.NoError(t, err)
		err = rxLeader.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemReplicationCompat{})
		require.NoError(t, err)
		fillTestItemReplicationCompatData(t, rxLeader, ns, dataCount)

		log.Println("Awaiting online sync...")
		helpers.WaitForSyncWithLeaderLegacy(t, rxLeader, rxFollower)

		log.Println("Stopping follower...")
		rxFollower.Close()

		log.Println("Writing more data...")
		fillTestItemReplicationCompatData(t, rxLeader, ns, 2*dataCount)

		log.Println("Restarting follower...")
		rxFollower = helpers.MakeFollowerNoStorageCleanup(t, cfgFollower, followerServerId)
		require.NoError(t, rxFollower.Status().Err)
		defer rxFollower.Close()

		log.Println("Awaiting WAL sync...")
		helpers.WaitForSyncWithLeaderLegacy(t, rxLeader, rxFollower)

		stats := getAsyncReplStats(t, rxLeader)
		assert.Equal(t, stats.ForceSync.Count, int64(0))
		assert.Equal(t, stats.WALSync.Count, int64(1))
	})

	t.Run("with legacy follower", func(t *testing.T) {
		cfgFollower.Storage.Path = path.Join(baseStoragePath, "with_legacy_follower/follower")
		cfgLeader.Storage.Path = path.Join(baseStoragePath, "with_legacy_follower/leader")

		var isFollowerRunning bool
		rxFollower, terminateFollower := helpers.MakeLegacyFollower(t, *legacyServerBinary, followerCproto, cfgFollower, followerServerId)
		isFollowerRunning = true
		defer func(isFollowerRunning *bool) {
			if *isFollowerRunning {
				terminateFollower()
			}
		}(&isFollowerRunning)
		rxLeader := helpers.MakeLeader(t, cfgLeader, leaderServerId, followerCproto)
		require.NoError(t, rxLeader.Status().Err)
		defer rxLeader.Close()

		err := rxFollower.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemReplicationCompat{})
		require.NoError(t, err)
		err = rxLeader.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemReplicationCompat{})
		require.NoError(t, err)
		fillTestItemReplicationCompatData(t, rxLeader, ns, dataCount)

		log.Println("Awaiting online sync...")
		helpers.WaitForSyncWithLeaderLegacy(t, rxLeader, rxFollower)

		log.Println("Stopping follower...")
		isFollowerRunning = false
		terminateFollower()

		log.Println("Writing more data...")
		fillTestItemReplicationCompatData(t, rxLeader, ns, 2*dataCount)

		log.Println("Restarting follower...")
		rxFollower, terminateFollower = helpers.MakeLegacyFollowerNoStorageCleanup(t, *legacyServerBinary, followerCproto, cfgFollower, followerServerId)
		defer terminateFollower()

		log.Println("Awaiting WAL sync...")
		helpers.WaitForSyncWithLeaderLegacy(t, rxLeader, rxFollower)

		stats := getAsyncReplStats(t, rxLeader)
		assert.Equal(t, stats.ForceSync.Count, int64(0))
		assert.Equal(t, stats.WALSync.Count, int64(1))
	})
}
