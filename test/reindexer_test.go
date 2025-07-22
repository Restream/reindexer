package reindexer

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	_ "github.com/restream/reindexer/v5/bindings/builtin"
	_ "github.com/restream/reindexer/v5/bindings/cproto"
	// _ "github.com/restream/reindexer/v5/pprof"
)

type TestLogger struct{}

type arrayFlags []string

var DB *ReindexerWrapper
var DBD *reindexer.Reindexer

var tnamespaces map[string]interface{} = make(map[string]interface{}, 100)

var testLogger *TestLogger

var cluster arrayFlags

var (
	dsn        = flag.String("dsn", "builtin:///tmp/reindex_test/", "reindex db dsn")
	dsnSlave   = flag.String("dsnslave", "", "reindex slave db dsn")
	slaveCount = flag.Int("slavecount", 1, "reindex slave db count")

	benchmarkSeedCount = flag.Int("seedcount", 500000, "count of items for benchmark seed")
	benchmarkSeedCPU   = flag.Int("seedcpu", 1, "number threads of for seeding")
	benchmarkSeed      = flag.Int64("seed", time.Now().Unix(), "seed number for random")

	legacyServerBinary = flag.String("legacyserver", "", "legacy server binary for compatibility check")
)

func (i *arrayFlags) String() string {
	return "my string representation"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func (TestLogger) Printf(level int, format string, msg ...interface{}) {
	if level <= reindexer.TRACE {
		log.Printf(format, msg...)
	}
}

func mkID(i int) int {
	return i*17 + 8000000
}

func GetNodeForRole(nodeDb *reindexer.Reindexer, role string) (dsn string, serverID int, err error) {
	serverID = -1
	nodeStat, err := nodeDb.Query("#replicationstats").Where("type", reindexer.EQ, "cluster").Exec().FetchAll()
	if err != nil {
		return
	}
	replStat := nodeStat[0].(*reindexer.ReplicationStat)
	for _, v := range replStat.ReplicationNodeStat {
		if v.Role == role {
			return v.DSN, v.ServerID, nil
		}
	}
	err = fmt.Errorf("Can not find node for role [%s]", role)
	return
}

func TestMain(m *testing.M) {

	flag.Var(&cluster, "cluster", "array of reindex db dsn")
	flag.Parse()

	udsn, err := url.Parse(*dsn)
	if err != nil {
		panic(err)
	}

	if testing.Verbose() {
		testLogger = &TestLogger{}
	}

	opts := []interface{}{}
	if udsn.Scheme == "builtin" {
		os.RemoveAll(udsn.Path)
	} else if udsn.Scheme == "cproto" || udsn.Scheme == "cprotos" {
		opts = []interface{}{reindexer.WithCreateDBIfMissing(), reindexer.WithNetCompression(), reindexer.WithAppName("RxTestInstance")}
	} else if udsn.Scheme == "ucproto" {
		opts = []interface{}{reindexer.WithCreateDBIfMissing(), reindexer.WithAppName("RxTestInstance")}
	}

	DB = NewReindexWrapper(*dsn, cluster, 0, opts...)
	DBD = &DB.Reindexer
	if cluster != nil {
		dsnFollower, _, err := GetNodeForRole(DBD, "follower")
		if err != nil {
			panic(err)
		}
		_, leaderServeID, err := GetNodeForRole(DBD, "leader")
		if err != nil {
			panic(err)
		}
		DB = NewReindexWrapper(dsnFollower, cluster, leaderServeID, opts...)
		DBD = &DB.Reindexer
	}
	if err = DB.Status().Err; err != nil {
		panic(err)
	}

	if *dsnSlave != "" {
		DB.AddSlave(*dsnSlave, *slaveCount, reindexer.WithCreateDBIfMissing())
	}

	if cluster != nil {
		clusterDsns := make([]string, 0)
		for _, v := range cluster {
			clusterDsns = append(clusterDsns, v)
		}
		DB.AddClusterNodes(clusterDsns)
	}

	DB.SetLogger(testLogger)
	for k, v := range tnamespaces {
		DB.DropNamespace(k)

		if err := DB.OpenNamespace(k, reindexer.DefaultNamespaceOptions(), v); err != nil {
			panic(fmt.Sprintf("Namespace: %s, err: %s", k, err.Error()))
		}
	}

	if err = DB.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type: "profiling",
		Profiling: &reindexer.DBProfilingConfig{
			QueriesThresholdUS: 10,
			MemStats:           true,
			PerfStats:          true,
			QueriesPerfStats:   true,
			ActivityStats:      true,
			LongQueryLogging: &reindexer.LongQueryLoggingConfig{
				SelectItem: reindexer.LongQueryLoggingItem{
					ThresholdUS: -1,
					Normalized:  false,
				},
				UpdDelItem: reindexer.LongQueryLoggingItem{
					ThresholdUS: -1,
					Normalized:  false,
				},
				TxItem: reindexer.LongTxLoggingItem{
					ThresholdUS:              1000,
					AverageTxStepThresholdUs: 50,
				},
			}},
	}); err != nil {
		panic(err)
	}

	retCode := m.Run()

	DB.Close()
	os.Exit(retCode)
}
