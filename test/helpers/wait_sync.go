package helpers

import (
	"log"
	"testing"
	"time"

	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/bindings/builtinserver"
	"github.com/stretchr/testify/require"
)

func WaitForSyncWithMaster(t *testing.T, master *reindexer.Reindexer, slave *reindexer.Reindexer) {
	complete := true

	var nameBad string
	var masterBadLsn reindexer.LsnT
	var slaveBadLsn reindexer.LsnT

	for i := 0; i < 600*5; i++ {

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
				if slaveNsData, ok := slaveMemStatMap[nsName]; ok {
					if slaveNsData.Replication.LastLSN != masterNsData.Replication.LastLSN || slaveNsData.Replication.NSVersion != masterNsData.Replication.NSVersion { //slave != master
						complete = false
						nameBad = nsName
						masterBadLsn = masterNsData.Replication.LastLSN
						slaveBadLsn = slaveNsData.Replication.LastLSN
						log.Printf("%s is not synchronized: %v != %v", nsName, slaveNsData.Replication.LastLSN, masterNsData.Replication.LastLSN)
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
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("Can't sync slave ns with master: ns \"%s\" masterlsn: %+v , slavelsn %+v", nameBad, masterBadLsn, slaveBadLsn)
}
