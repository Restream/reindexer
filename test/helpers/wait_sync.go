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
