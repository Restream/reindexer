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

const syncRetries = 90

const (
	syncModeLegacy  = 0
	syncModeDefault = 1
)

func WaitForSyncWithLeader(t *testing.T, leader *reindexer.Reindexer, follower *reindexer.Reindexer) {
	waitForSyncWithLeaderImpl(t, leader, follower, syncModeDefault)
}

func WaitForSyncWithLeaderLegacy(t *testing.T, leader *reindexer.Reindexer, follower *reindexer.Reindexer) {
	waitForSyncWithLeaderImpl(t, leader, follower, syncModeLegacy)
}

func waitForSyncWithLeaderImpl(t *testing.T, leader *reindexer.Reindexer, follower *reindexer.Reindexer, syncMode int) {
	complete := true

	var nameBad string
	var masterBadLsn reindexer.LsnT
	var slaveBadLsn reindexer.LsnT

	if syncMode != syncModeDefault && syncMode != syncModeLegacy {
		t.Fatalf("Unexpected sync mode value: %v", syncMode)
	}

	for i := 0; i < syncRetries; i++ {

		complete = true

		masterStats, err := leader.GetNamespacesMemStat()
		require.NoError(t, err)

		masterMemStatMap := make(map[string]reindexer.NamespaceMemStat)
		for _, st := range masterStats {
			masterMemStatMap[st.Name] = *st
		}

		slaveStats, err := follower.GetNamespacesMemStat()
		require.NoError(t, err)

		slaveMemStatMap := make(map[string]reindexer.NamespaceMemStat)
		for _, st := range slaveStats {
			slaveMemStatMap[st.Name] = *st
		}

		configItemNs, found := leader.Query("#config").Where("type", reindexer.EQ, "async_replication").Get()
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
			leaderNsData, ok := masterMemStatMap[nsName]

			if ok {
				handleError := func(nsName string, mLSN reindexer.LsnT, sLSN reindexer.LsnT) {
					complete = false
					nameBad = nsName
					masterBadLsn = mLSN
					slaveBadLsn = sLSN
				}
				if followerNsData, ok := slaveMemStatMap[nsName]; ok {
					if followerNsData.Replication.LastLSN != leaderNsData.Replication.LastLSN || followerNsData.Replication.NSVersion != leaderNsData.Replication.NSVersion { // slave != master
						handleError(nsName, leaderNsData.Replication.LastLSN, followerNsData.Replication.LastLSN)
						log.Printf("%s is not synchronized: %v:%v != %v:%v", nsName, followerNsData.Replication.NSVersion, followerNsData.Replication.LastLSN, leaderNsData.Replication.NSVersion, leaderNsData.Replication.LastLSN)
						break
					}
					leaderTmST, leaderTmVer := leaderNsData.TagsMatcher.StateToken, leaderNsData.TagsMatcher.Version
					followerTmST, followerTmVer := followerNsData.TagsMatcher.StateToken, followerNsData.TagsMatcher.Version
					if leaderTmVer < 0 || followerTmVer < 0 || leaderTmST != followerTmST || leaderTmVer != followerTmVer { // followers tagsmatcher is not equal to leader's one
						handleError(nsName, leaderNsData.Replication.LastLSN, followerNsData.Replication.LastLSN)
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
				followerNsData, _ := slaveMemStatMap[nsName]
				leaderNsData, _ := masterMemStatMap[nsName]
				if syncMode != syncModeLegacy {
					if followerNsData.Replication.Checksum != leaderNsData.Replication.Checksum {
						t.Fatalf("Can't sync follower ns with leader (checksum missmatch): ns \"%s\". Follower LSN: %v:%v, leader LSN: %v:%v, follower checksum: %d , leader checksum %d",
							nsName, followerNsData.Replication.NSVersion, followerNsData.Replication.LastLSN, leaderNsData.Replication.NSVersion, leaderNsData.Replication.LastLSN, followerNsData.Replication.Checksum,
							leaderNsData.Replication.Checksum)
					}
				}
				// Deprecated. TODO: Remove this and syncMode somewhere around v5.18.0. Issue #2417
				if followerNsData.Replication.DataHash != leaderNsData.Replication.DataHash {
					t.Fatalf("Can't sync follower ns with leader (datahash v1 missmatch): ns \"%s\". Follower LSN: %v:%v, leader LSN: %v:%v, follower dataHash: %d , leader dataHash %d",
						nsName, followerNsData.Replication.NSVersion, followerNsData.Replication.LastLSN, leaderNsData.Replication.NSVersion, leaderNsData.Replication.LastLSN, followerNsData.Replication.DataHash,
						leaderNsData.Replication.DataHash)
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
