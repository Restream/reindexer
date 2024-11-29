package helpers

import (
	"testing"
	"time"

	"github.com/restream/reindexer/v3"
	_ "github.com/restream/reindexer/v3/bindings/builtinserver"
	"github.com/stretchr/testify/require"
)

func WaitForSyncWithMaster(t *testing.T, master *reindexer.Reindexer, slave *reindexer.Reindexer) {
	complete := true

	var nameBad string
	var masterBadLsn reindexer.LsnT
	var slaveBadLsn reindexer.LsnT

	for i := 0; i < 450; i++ {

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

		configItemNs, found := slave.Query("#config").Where("type", reindexer.EQ, "replication").Get()
		var namespaces []string
		if found {
			replication := configItemNs.(*reindexer.DBConfigItem)
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
			for nsName := range checkNsMap {
				slaveNsData := slaveMemStatMap[nsName]
				masterNsData := masterMemStatMap[nsName]
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
