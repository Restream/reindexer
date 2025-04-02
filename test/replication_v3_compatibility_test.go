package reindexer

import (
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/bindings/builtinserver/config"
	"github.com/restream/reindexer/v5/test/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nodeData struct {
	ServerID      int
	Cfg           *config.ServerConfig
	HTTPAddr      string
	RPCAddr       string
	LogLevel      string
	Cproto        string
	DB            *reindexer.Reindexer
	TerminateNode func()
}

func runDataQueries(t *testing.T, db *reindexer.Reindexer, ns string, minID int, maxID int) {
	singleInsertsMinID := minID
	singleInsertsMaxID := maxID - (maxID-minID)/4
	txMinID := minID + (maxID-minID)/2
	txMaxID := maxID
	// Insert single records
	for i := singleInsertsMinID; i < singleInsertsMaxID; i++ {
		cnt, err := db.Insert(ns, newTestItem(i, 5))
		assert.Equal(t, cnt, 1)
		assert.NoError(t, err)
	}

	// Upsert in tx
	tx, err := db.BeginTx(ns)
	assert.NoError(t, err)
	for i := txMinID; i < txMaxID; i++ {
		err := tx.UpsertAsync(newTestItem(i, 2), func(err error) {
			assert.NoError(t, err)
		})
		assert.NoError(t, err)
	}
	cnt, err := tx.CommitWithCount()
	assert.Equal(t, cnt, txMaxID-txMinID)
	assert.NoError(t, err)

	// Update + delete in tx
	tx, err = db.BeginTx(ns)
	assert.NoError(t, err)
	for i := txMinID; i < txMaxID; i++ {
		if i%4 != 0 {
			err = tx.UpdateAsync(newTestItem(i, 2), func(err error) {
				assert.NoError(t, err)
			})
		} else {
			err = tx.DeleteAsync(newTestItem(i, 2), func(err error) {
				assert.NoError(t, err)
			})
		}
		assert.NoError(t, err)
	}
	cnt, err = tx.CommitWithCount()
	assert.Equal(t, cnt, txMaxID-txMinID)
	assert.NoError(t, err)

	// Delete query
	_, err = db.Query(ns).
		Where("genre", reindexer.SET, []int{1, 2, 3, 4}).
		Limit(20).
		Delete()
	require.NoError(t, err)

	// Set field query
	res, err := db.Query(ns).
		Where("id", reindexer.GE, mkID(minID+10)).
		Where("id", reindexer.LE, mkID(minID+40)).
		SetExpression("year", "year+10").
		Update().FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(res), 0, "No items updated")
}

func updateAddNewField(t *testing.T, db *reindexer.Reindexer, ns string) {
	res, err := db.Query(ns).
		Set(trueRandWord(20), 17).
		Limit(5).
		Update().FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(res), 0, "No items updated")
}

func updateAddNewFieldsTx(t *testing.T, db *reindexer.Reindexer, ns string) {
	tx, err := db.BeginTx(ns)
	assert.NoError(t, err)
	for i := 0; i < 2; i++ {
		err = tx.Query().Set(trueRandWord(20), 999).
			Limit(5).
			Update().Error()
		assert.NoError(t, err)
		err = tx.Query().Set(trueRandWord(20), 1212).
			Limit(2).
			Update().Error()
		assert.NoError(t, err)
	}
	_, err = tx.CommitWithCount()
	assert.NoError(t, err)
}

func initNodes(nodes *[]nodeData, baseStoragePath string) {
	for i := range *nodes {
		(*nodes)[i].Cfg.Net.HTTPAddr = (*nodes)[i].HTTPAddr
		(*nodes)[i].Cfg.Net.RPCAddr = (*nodes)[i].RPCAddr
		(*nodes)[i].Cfg.Logger.LogLevel = (*nodes)[i].LogLevel
		(*nodes)[i].Cfg.Storage.Path = baseStoragePath + "node" + strconv.Itoa(i)
	}
}

func terminatedNodes(nodes *[]nodeData) {
	for i := range *nodes {
		if (*nodes)[i].TerminateNode != nil {
			(*nodes)[i].TerminateNode()
		}
	}
}

func TestReplV3CompatibilitySingleV4Node(t *testing.T) {
	if len(DB.slaveList) > 0 || len(*legacyServerBinary) == 0 {
		t.Skip()
	}
	/*
		leader (v4) ------ follower4 (v3)
		    |
		follower1 (v3)
		    |
		follower2 (v3)
		    |
		follower3 (v3)
	*/

	baseStoragePath := path.Join(helpers.GetTmpDBDir(), "reindex_test_replv3_comp_single/")
	const ns = "replv3ns"
	const dataCount = 1000

	nodes := []nodeData{{
		ServerID: 1,
		Cfg:      config.DefaultServerConfig(),
		HTTPAddr: "0:31088",
		RPCAddr:  "0:31534",
		LogLevel: "trace",
		Cproto:   "cproto://127.0.0.1:31534/xxx",
	}, {
		ServerID: 2,
		Cfg:      config.DefaultServerConfig(),
		HTTPAddr: "0:31089",
		RPCAddr:  "0:31535",
		LogLevel: "trace",
		Cproto:   "cproto://127.0.0.1:31535/xxx",
	}, {
		ServerID: 3,
		Cfg:      config.DefaultServerConfig(),
		HTTPAddr: "0:31090",
		RPCAddr:  "0:31536",
		LogLevel: "trace",
		Cproto:   "cproto://127.0.0.1:31536/xxx",
	}, {
		ServerID: 4,
		Cfg:      config.DefaultServerConfig(),
		HTTPAddr: "0:31091",
		RPCAddr:  "0:31537",
		LogLevel: "trace",
		Cproto:   "cproto://127.0.0.1:31537/xxx",
	}, {
		ServerID: 5,
		Cfg:      config.DefaultServerConfig(),
		HTTPAddr: "0:31092",
		RPCAddr:  "0:31538",
		LogLevel: "trace",
		Cproto:   "cproto://127.0.0.1:31538/xxx",
	},
	}

	curMinID := 0
	curMaxID := dataCount
	addNewData := func() {
		runDataQueries(t, nodes[0].DB, ns, curMinID, curMaxID)
		curMinID += dataCount
		curMaxID += dataCount
	}
	initNodes(&nodes, baseStoragePath)
	defer terminatedNodes(&nodes)

	nodes[0].DB = MakeLeader(t, nodes[0].Cfg, nodes[0].ServerID)
	nodes[0].TerminateNode = func() { nodes[0].DB.Close() }
	nodes[1].DB, nodes[1].TerminateNode = MakeLegacyFollower(t, nodes[1].Cproto, nodes[1].Cfg, nodes[1].ServerID, nodes[0].Cproto)
	nodes[2].DB, nodes[2].TerminateNode = MakeLegacyFollower(t, nodes[2].Cproto, nodes[2].Cfg, nodes[2].ServerID, nodes[1].Cproto)
	// Do not start the last node yet

	err := nodes[0].DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItem{})
	require.NoError(t, err)

	t.Run("online replication compatibility", func(t *testing.T) {
		addNewData()
		helpers.WaitForSyncV4V3(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncWithMasterV3V3(t, nodes[1].DB, nodes[2].DB)

		updateAddNewField(t, nodes[0].DB, ns)
		helpers.WaitForSyncV4V3(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncWithMasterV3V3(t, nodes[1].DB, nodes[2].DB)

		updateAddNewFieldsTx(t, nodes[0].DB, ns)
		helpers.WaitForSyncV4V3(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncWithMasterV3V3(t, nodes[1].DB, nodes[2].DB)
	})

	t.Run("wal sync compatibility", func(t *testing.T) {
		nodes[1].TerminateNode()
		nodes[1].TerminateNode = nil
		nodes[1].DB = nil

		addNewData()
		updateAddNewField(t, nodes[0].DB, ns)
		updateAddNewFieldsTx(t, nodes[0].DB, ns)
		nodes[1].DB, nodes[1].TerminateNode = MakeLegacyFollowerNoStorageCleanup(t, nodes[1].Cproto, nodes[1].Cfg, nodes[1].ServerID, nodes[0].Cproto)

		helpers.WaitForSyncV4V3(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncWithMasterV3V3(t, nodes[1].DB, nodes[2].DB)
	})

	t.Run("force sync compatibility", func(t *testing.T) {
		nodes[3].DB, nodes[3].TerminateNode = MakeLegacyFollower(t, nodes[3].Cproto, nodes[3].Cfg, nodes[3].ServerID, nodes[2].Cproto)
		nodes[4].DB, nodes[4].TerminateNode = MakeLegacyFollower(t, nodes[4].Cproto, nodes[4].Cfg, nodes[4].ServerID, nodes[0].Cproto)

		helpers.WaitForSyncWithMasterV3V3(t, nodes[2].DB, nodes[3].DB)
		helpers.WaitForSyncV4V3(t, nodes[0].DB, nodes[4].DB)
	})
}

func TestReplV3CompatibilityMultipleV4Nodes(t *testing.T) {
	if len(DB.slaveList) > 0 || len(*legacyServerBinary) == 0 {
		t.Skip()
	}
	/*
		leader (v4)
		    |
		follower1 (v4) ------ follower4 (v3)
		    |
		follower2 (v3)
		    |
		follower3 (v3)
	*/

	baseStoragePath := path.Join(helpers.GetTmpDBDir(), "reindex_test_replv3_comp_multi/")
	const ns = "replv3ns"
	const dataCount = 1000

	nodes := []nodeData{{
		ServerID: 1,
		Cfg:      config.DefaultServerConfig(),
		HTTPAddr: "127.0.0.1:31088",
		RPCAddr:  "127.0.0.1:31534",
		LogLevel: "trace",
		Cproto:   "cproto://127.0.0.1:31534/xxx",
	}, {
		ServerID: 2,
		Cfg:      config.DefaultServerConfig(),
		HTTPAddr: "127.0.0.1:31089",
		RPCAddr:  "127.0.0.1:31535",
		LogLevel: "trace",
		Cproto:   "cproto://127.0.0.1:31535/xxx",
	}, {
		ServerID: 3,
		Cfg:      config.DefaultServerConfig(),
		HTTPAddr: "127.0.0.1:31090",
		RPCAddr:  "127.0.0.1:31536",
		LogLevel: "trace",
		Cproto:   "cproto://127.0.0.1:31536/xxx",
	}, {
		ServerID: 4,
		Cfg:      config.DefaultServerConfig(),
		HTTPAddr: "127.0.0.1:31091",
		RPCAddr:  "127.0.0.1:31537",
		LogLevel: "trace",
		Cproto:   "cproto://127.0.0.1:31537/xxx",
	}, {
		ServerID: 5,
		Cfg:      config.DefaultServerConfig(),
		HTTPAddr: "0:31092",
		RPCAddr:  "0:31538",
		LogLevel: "trace",
		Cproto:   "cproto://127.0.0.1:31538/xxx",
	},
	}

	curMinID := 0
	curMaxID := dataCount
	addNewData := func() {
		runDataQueries(t, nodes[0].DB, ns, curMinID, curMaxID)
		curMinID += dataCount
		curMaxID += dataCount
	}
	initNodes(&nodes, baseStoragePath)
	defer terminatedNodes(&nodes)

	nodes[0].DB = MakeLeader(t, nodes[0].Cfg, nodes[0].ServerID, nodes[1].Cproto)
	nodes[0].TerminateNode = func() { nodes[0].DB.Close() }
	nodes[1].DB = MakeFollower(t, nodes[1].Cfg, nodes[1].ServerID)
	nodes[1].TerminateNode = func() {
		nodes[1].DB.Close()
	}
	nodes[2].DB, nodes[2].TerminateNode = MakeLegacyFollower(t, nodes[2].Cproto, nodes[2].Cfg, nodes[2].ServerID, nodes[1].Cproto)
	// Do not start the last node yet

	err := nodes[0].DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItem{})
	require.NoError(t, err)
	err = nodes[1].DB.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItem{})
	assert.NoError(t, err)
	helpers.WaitForSyncWithMaster(t, nodes[0].DB, nodes[1].DB)

	t.Run("online replication compatibility", func(t *testing.T) {
		addNewData()
		helpers.WaitForSyncWithMaster(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncWithMasterV3V3(t, nodes[1].DB, nodes[2].DB)

		updateAddNewField(t, nodes[0].DB, ns)
		helpers.WaitForSyncWithMaster(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncWithMasterV3V3(t, nodes[1].DB, nodes[2].DB)

		updateAddNewFieldsTx(t, nodes[0].DB, ns)
		helpers.WaitForSyncWithMaster(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncWithMasterV3V3(t, nodes[1].DB, nodes[2].DB)
	})

	t.Run("wal sync compatibility", func(t *testing.T) {
		nodes[1].TerminateNode()
		nodes[1].TerminateNode = nil
		nodes[1].DB = nil

		addNewData()
		updateAddNewField(t, nodes[0].DB, ns)
		updateAddNewFieldsTx(t, nodes[0].DB, ns)

		nodes[1].DB = MakeFollowerNoStorageCleanup(t, nodes[1].Cfg, nodes[1].ServerID)
		nodes[1].TerminateNode = func() {
			nodes[1].DB.Close()
		}
		err = nodes[1].DB.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItem{})
		assert.NoError(t, err)

		helpers.WaitForSyncWithMaster(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncV4V3(t, nodes[1].DB, nodes[2].DB)
	})

	t.Run("force sync compatibility with follower3", func(t *testing.T) {
		nodes[3].DB, nodes[3].TerminateNode = MakeLegacyFollower(t, nodes[3].Cproto, nodes[3].Cfg, nodes[3].ServerID, nodes[2].Cproto)

		helpers.WaitForSyncWithMasterV3V3(t, nodes[2].DB, nodes[3].DB)
	})

	t.Run("force sync compatibility with storage recreation of the follower1", func(t *testing.T) {
		nodes[1].TerminateNode()
		nodes[1].TerminateNode = nil
		nodes[1].DB = nil

		nodes[1].DB = MakeFollower(t, nodes[1].Cfg, nodes[1].ServerID)
		nodes[1].TerminateNode = func() {
			nodes[1].DB.Close()
		}

		nodes[4].DB, nodes[4].TerminateNode = MakeLegacyFollower(t, nodes[4].Cproto, nodes[4].Cfg, nodes[4].ServerID, nodes[1].Cproto)

		err = nodes[1].DB.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItem{})
		assert.NoError(t, err)

		helpers.WaitForSyncWithMaster(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncV4V3(t, nodes[1].DB, nodes[4].DB)
		time.Sleep(time.Second * 5) // Await some transition processes in the follower2 to raise the chance of actual sync call
		helpers.WaitForSyncV4V3(t, nodes[1].DB, nodes[2].DB)
		helpers.WaitForSyncWithMasterV3V3(t, nodes[2].DB, nodes[3].DB)
	})
}

func stopNode(t *testing.T, node *nodeData) {
	require.NotNil(t, node.TerminateNode)
	node.TerminateNode()
	node.TerminateNode = nil
	node.DB = nil
}

func startNodeAsV4(t *testing.T, node *nodeData, followerDSNs ...string) {
	node.DB = MakeLeaderNoStorageCleanup(t, node.Cfg, node.ServerID, followerDSNs...)
	node.TerminateNode = func() {
		node.DB.Close()
	}
}

func TestReplV3CompatibilityChainTransition(t *testing.T) {
	if len(DB.slaveList) > 0 || len(*legacyServerBinary) == 0 {
		t.Skip()
	}
	/*
		leader (v3)          leader (v4)          leader (v4)          leader (v4)
		    |                    |                    |                    |
		follower1 (v3)       follower1 (v3)       follower1 (v4)       follower1 (v4)
		    |         --->       |         --->       |         --->       |
		follower2 (v3)       follower2 (v3)       follower2 (v3)       follower2 (v4)
	*/

	baseStoragePath := path.Join(helpers.GetTmpDBDir(), "reindex_test_replv3_comp_chain/")
	const ns = "replv3ns"
	const dataCount = 1000

	nodes := []nodeData{{
		ServerID: 0,
		Cfg:      config.DefaultServerConfig(),
		HTTPAddr: "127.0.0.1:31088",
		RPCAddr:  "127.0.0.1:31534",
		LogLevel: "trace",
		Cproto:   "cproto://127.0.0.1:31534/xxx",
	}, {
		ServerID: 2,
		Cfg:      config.DefaultServerConfig(),
		HTTPAddr: "127.0.0.1:31089",
		RPCAddr:  "127.0.0.1:31535",
		LogLevel: "trace",
		Cproto:   "cproto://127.0.0.1:31535/xxx",
	}, {
		ServerID: 3,
		Cfg:      config.DefaultServerConfig(),
		HTTPAddr: "127.0.0.1:31090",
		RPCAddr:  "127.0.0.1:31536",
		LogLevel: "trace",
		Cproto:   "cproto://127.0.0.1:31536/xxx",
	},
	}
	curMinID := 0
	curMaxID := dataCount
	addNewData := func() {
		runDataQueries(t, nodes[0].DB, ns, curMinID, curMaxID)
		curMinID += dataCount
		curMaxID += dataCount
	}
	initNodes(&nodes, baseStoragePath)
	defer terminatedNodes(&nodes)

	{ // Base startup
		nodes[0].DB, nodes[0].TerminateNode = MakeLegacyLeader(t, nodes[0].Cproto, nodes[0].Cfg, nodes[0].ServerID)
		for i := 1; i < len(nodes); i++ {
			nodes[i].DB, nodes[i].TerminateNode = MakeLegacyFollower(t, nodes[i].Cproto, nodes[i].Cfg, nodes[i].ServerID, nodes[i-1].Cproto)
		}

		err := nodes[0].DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItem{})
		require.NoError(t, err)
		helpers.WaitForSyncWithMaster(t, nodes[0].DB, nodes[1].DB)
		addNewData()

		// WaitSyncs
		helpers.WaitForSyncWithMasterV3V3(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncWithMasterV3V3(t, nodes[1].DB, nodes[2].DB)
	}

	{ // Restart leader with v4
		stopNode(t, &nodes[0])
		startNodeAsV4(t, &nodes[0])
		err := nodes[0].DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItem{})
		require.NoError(t, err)

		addNewData()

		// WaitSyncs
		helpers.WaitForSyncV4V3(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncWithMasterV3V3(t, nodes[1].DB, nodes[2].DB)
	}

	{ // Restart follower1 with v4
		stopNode(t, &nodes[1])
		addNewData()

		startNodeAsV4(t, &nodes[1])
		err := nodes[1].DB.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItem{})
		require.NoError(t, err)
		helpers.ConfigureReplication(t, nodes[0].DB, "leader", nil, []reindexer.DBAsyncReplicationNode{{DSN: nodes[1].Cproto}})
		addNewData()

		// WaitSyncs
		helpers.WaitForSyncWithMaster(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncV4V3(t, nodes[1].DB, nodes[2].DB)
	}

	{ // Restart follower2 with v4
		stopNode(t, &nodes[2])
		addNewData()

		startNodeAsV4(t, &nodes[2])
		err := nodes[2].DB.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItem{})
		require.NoError(t, err)
		helpers.ConfigureReplication(t, nodes[1].DB, "leader", nil, []reindexer.DBAsyncReplicationNode{{DSN: nodes[2].Cproto}})
		addNewData()

		// WaitSyncs
		helpers.WaitForSyncWithMaster(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncWithMaster(t, nodes[1].DB, nodes[2].DB)
	}

	{ // Final online sync
		addNewData()
		helpers.WaitForSyncWithMaster(t, nodes[0].DB, nodes[1].DB)
		helpers.WaitForSyncWithMaster(t, nodes[1].DB, nodes[2].DB)
	}
}
