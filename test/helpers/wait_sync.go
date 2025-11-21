package helpers

import (
	"log"
	"runtime/debug"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	_ "github.com/restream/reindexer/v5/bindings/builtinserver"
	"github.com/stretchr/testify/require"
)

type LegacyDBReplicationConfig struct {
	// ID of the current server
	ServerID int `yaml:"server_id"`
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
	// List of namespaces for replication. If empty, all namespaces. All replicated namespaces will become read only for slave
	Namespaces []string `json:"namespaces"`
}

type LegacyDBConfigItem struct {
	Type        string                          `json:"type"`
	Profiling   *reindexer.DBProfilingConfig    `json:"profiling,omitempty"`
	Namespaces  *[]reindexer.DBNamespacesConfig `json:"namespaces,omitempty"`
	Replication *LegacyDBReplicationConfig      `json:"replication,omitempty"`
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

const syncRetries = 90

func WaitForSyncWithMaster(t *testing.T, master *reindexer.Reindexer, slave *reindexer.Reindexer) {
	complete := true

	var nameBad string
	var masterBadLsn reindexer.LsnT
	var slaveBadLsn reindexer.LsnT

	for i := 0; i < syncRetries; i++ {

		complete = true

		masterStats, err := master.GetNamespacesMemStat()
		require.NoError(t, err)

		masterMemStatMap := make(map[string]reindexer.NamespaceMemStat)
		for _, st := range masterStats {
			masterMemStatMap[st.Name] = *st
		}

		slaveStats, err := slave.GetNamespacesMemStat()
		require.NoError(t, err)

		slaveMemStatMap := make(map[string]reindexer.NamespaceMemStat)
		for _, st := range slaveStats {
			slaveMemStatMap[st.Name] = *st
		}

		configItemNs, found := master.Query("#config").Where("type", reindexer.EQ, "async_replication").Get()
		var namespaces []string
		if found {
			replication := configItemNs.(*reindexer.DBConfigItem)
			namespaces = replication.AsyncReplication.Namespaces
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

		for nsName := range checkNsMap { // loop sync namespaces list (all or defined)
			masterNsData, ok := masterMemStatMap[nsName]

			if ok {
				handleError := func(nsName string, mLSN reindexer.LsnT, sLSN reindexer.LsnT) {
					complete = false
					nameBad = nsName
					masterBadLsn = mLSN
					slaveBadLsn = sLSN
				}
				if slaveNsData, ok := slaveMemStatMap[nsName]; ok {
					if slaveNsData.Replication.LastLSN != masterNsData.Replication.LastLSN || slaveNsData.Replication.NSVersion != masterNsData.Replication.NSVersion { // slave != master
						handleError(nsName, masterNsData.Replication.LastLSN, slaveNsData.Replication.LastLSN)
						log.Printf("%s is not synchronized: %v:%v != %v:%v", nsName, slaveNsData.Replication.NSVersion, slaveNsData.Replication.LastLSN, masterNsData.Replication.NSVersion, masterNsData.Replication.LastLSN)
						break
					}
					leaderTmST, leaderTmVer := masterNsData.TagsMatcher.StateToken, masterNsData.TagsMatcher.Version
					followerTmST, followerTmVer := slaveNsData.TagsMatcher.StateToken, slaveNsData.TagsMatcher.Version
					if leaderTmVer < 0 || followerTmVer < 0 || leaderTmST != followerTmST || leaderTmVer != followerTmVer { // followers tagsmatcher is not equal to leader's one
						handleError(nsName, masterNsData.Replication.LastLSN, slaveNsData.Replication.LastLSN)
						log.Printf("%s has different tagsmatchers: (%08X:%d) vs (%08X:%d)", nsName, leaderTmST, leaderTmVer, followerTmST, followerTmVer)
						break
					}
				} else {
					complete = false
					nameBad = nsName
					masterBadLsn.ServerId = 0
					masterBadLsn.Counter = 0
					slaveBadLsn.ServerId = 0
					slaveBadLsn.Counter = 0
					log.Printf("%s is not synchronized: doesn't exist on follower", nsName)
					break
				}
			}
		}
		if complete {
			for nsName := range checkNsMap {
				slaveNsData, _ := slaveMemStatMap[nsName]
				masterNsData, _ := masterMemStatMap[nsName]
				if slaveNsData.Replication.DataHash != masterNsData.Replication.DataHash {
					t.Fatalf("Can't sync slave ns with master: ns \"%s\". Slave LSN: %v:%v, master LSN: %v:%v, slave dataHash: %d , master dataHash %d",
						nsName, slaveNsData.Replication.NSVersion, slaveNsData.Replication.LastLSN, masterNsData.Replication.NSVersion, masterNsData.Replication.LastLSN, slaveNsData.Replication.DataHash,
						masterNsData.Replication.DataHash)
				}
			}
			return
		}
		log.Printf("Awaiting sync...")
		time.Sleep(500 * time.Millisecond)
	}

	debug.PrintStack()
	t.Fatalf("Can't sync slave ns with master: ns \"%s\" masterlsn: %+v , slavelsn %+v", nameBad, masterBadLsn, slaveBadLsn)
}

func getLegacyNamespacesMemStat(db *reindexer.Reindexer) ([]*LegacyNamespaceMemStat, error) {
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

func WaitForSyncWithMasterV3V3(t *testing.T, master *reindexer.Reindexer, slave *reindexer.Reindexer) {
	complete := true

	var nameBad string
	var masterBadLsn reindexer.LsnT
	var slaveBadLsn reindexer.LsnT

	for i := 0; i < syncRetries; i++ {

		complete = true

		masterStats, err := getLegacyNamespacesMemStat(master)
		require.NoError(t, err)

		masterMemStatMap := make(map[string]LegacyNamespaceMemStat)
		for _, st := range masterStats {
			masterMemStatMap[st.Name] = *st
		}

		slaveStats, err := getLegacyNamespacesMemStat(slave)
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

		for nsName := range checkNsMap { // loop sync namespaces list (all or defined)
			masterNsData, ok := masterMemStatMap[nsName]

			if ok {
				if slaveNsData, ok := slaveMemStatMap[nsName]; ok {
					if slaveNsData.Replication.LastUpstreamLSN != masterNsData.Replication.LastLSN { // slave != master
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
			for nsName := range checkNsMap {
				slaveNsData, _ := slaveMemStatMap[nsName]
				masterNsData, _ := masterMemStatMap[nsName]
				if slaveNsData.Replication.DataHash != masterNsData.Replication.DataHash {
					t.Fatalf("Can't sync slave ns with master: ns \"%s\" slave dataHash: %d , master dataHash %d", nsName, slaveNsData.Replication.DataHash, masterNsData.Replication.DataHash)
				}
			}
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	debug.PrintStack()
	t.Fatalf("Can't sync slave ns with master: ns \"%s\" masterlsn: %+v , slavelsn %+v", nameBad, masterBadLsn, slaveBadLsn)
}

func WaitForSyncV4V3(t *testing.T, rxV4 *reindexer.Reindexer, rxV3 *reindexer.Reindexer) {
	complete := true

	var nameBad string
	var masterBadLsn reindexer.LsnT
	var slaveBadLsn reindexer.LsnT

	for i := 0; i < syncRetries; i++ {

		complete = true

		masterStats, err := rxV4.GetNamespacesMemStat()
		require.NoError(t, err)

		masterMemStatMap := make(map[string]reindexer.NamespaceMemStat)
		for _, st := range masterStats {
			masterMemStatMap[st.Name] = *st
		}

		slaveStats, err := getLegacyNamespacesMemStat(rxV3)
		require.NoError(t, err)

		slaveMemStatMap := make(map[string]LegacyNamespaceMemStat)
		for _, st := range slaveStats {
			slaveMemStatMap[st.Name] = *st
		}

		configItemNs, found := rxV3.Query("#config").Where("type", reindexer.EQ, "replication").Get()
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

		for nsName := range checkNsMap { // loop sync namespaces list (all or defined)
			masterNsData, ok := masterMemStatMap[nsName]

			if ok {
				if slaveNsData, ok := slaveMemStatMap[nsName]; ok {
					if slaveNsData.Replication.LastUpstreamLSN != masterNsData.Replication.LastLSN { // slave != master
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
			for nsName := range checkNsMap {
				slaveNsData := slaveMemStatMap[nsName]
				masterNsData := masterMemStatMap[nsName]
				if slaveNsData.Replication.DataHash != masterNsData.Replication.DataHash {
					t.Fatalf("Can't sync slave ns with master: ns \"%s\" slave dataHash: %d , master dataHash %d", nsName, slaveNsData.Replication.DataHash, masterNsData.Replication.DataHash)
				}
			}
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	debug.PrintStack()
	t.Fatalf("Can't sync slave ns with master: ns \"%s\" masterlsn: %+v , slavelsn %+v", nameBad, masterBadLsn, slaveBadLsn)
}
