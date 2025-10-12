package reindexer

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/restream/reindexer/v5"
	_ "github.com/restream/reindexer/v5/bindings/cproto"
	"github.com/restream/reindexer/v5/test/helpers"
)

type ReindexerWrapper struct {
	reindexer.Reindexer
	isMaster       bool
	slaveList      []*ReindexerWrapper
	master         *ReindexerWrapper
	dsn            string
	syncedStatus   int32
	syncMutex      sync.RWMutex
	clusterList    []*ReindexerWrapper
	leaderServerID int
}

func NewReindexWrapper(dsn string, cluster arrayFlags, leaderServerID int, options ...interface{}) *ReindexerWrapper {
	rx, err := reindexer.NewReindex(dsn, options...)
	if err != nil {
		panic(err)
	}

	if cluster != nil {
		return &ReindexerWrapper{Reindexer: *rx, isMaster: false, dsn: dsn, syncedStatus: 0, leaderServerID: leaderServerID}
	} else {
		return &ReindexerWrapper{Reindexer: *rx, isMaster: true, dsn: dsn, syncedStatus: 0, leaderServerID: 0}
	}
}

func (dbw *ReindexerWrapper) setSynced() {
	atomic.StoreInt32(&dbw.syncedStatus, 1)
}

func (dbw *ReindexerWrapper) SetSyncRequired() {
	dbw.syncMutex.RLock()
	defer dbw.syncMutex.RUnlock()
	atomic.StoreInt32(&dbw.syncedStatus, 0)
}

func (dbw *ReindexerWrapper) IsSynced() bool {
	status := atomic.LoadInt32(&dbw.syncedStatus)
	if status == 0 {
		return false
	}
	return true
}

func (dbw *ReindexerWrapper) addSlave(dsn string, options ...interface{}) *ReindexerWrapper {
	if dbw.dsn == dsn {
		return nil
	}
	slaveDb := NewReindexWrapper(dsn, cluster, dbw.leaderServerID, options...)
	slaveDb.isMaster = false
	slaveDb.master = dbw
	slaveDb.SetSyncRequired()
	dbw.slaveList = append(dbw.slaveList, slaveDb)
	dbw.setSlaveConfig(slaveDb)

	return slaveDb
}

func (dbw *ReindexerWrapper) AddSlave(dsn string, count int, options ...interface{}) []*ReindexerWrapper {
	u, err := url.Parse(dsn)
	if err != nil {
		panic(err)
	}
	if u.Scheme != "cproto" {
		panic("Follower must have dsn with cproto-scheme")
	}
	for count > 0 {
		db := dbw.addSlave(dsn, options...)
		if db == nil {
			count++
		}
		us := u
		us.Path = us.Path + strconv.Itoa(count)
		dsn = us.String()
		count--
	}
	return dbw.slaveList
}

func (dbw *ReindexerWrapper) addClusterNode(clusterNodeDsn string, options ...interface{}) *ReindexerWrapper {
	nodeDb := NewReindexWrapper(clusterNodeDsn, cluster, dbw.leaderServerID, options...)
	nodeDb.isMaster = false
	nodeDb.SetSyncRequired()
	dbw.clusterList = append(dbw.clusterList, nodeDb)

	return nodeDb
}

func (dbw *ReindexerWrapper) AddClusterNodes(clusterDsns []string, options ...interface{}) []*ReindexerWrapper {
	count := len(clusterDsns)
	if count == 1 {
		return nil
	}
	if count >= 2 {
		for _, v := range clusterDsns {
			u, err := url.Parse(v)
			if err != nil {
				panic(err)
			}
			dbw.addClusterNode(v, options...)
			us := u
			v = us.String()
			count--
		}
	}
	return dbw.clusterList
}

func (dbw *ReindexerWrapper) SetLogger(log reindexer.Logger) {
	dbw.Reindexer.SetLogger(log)
	for _, db := range dbw.slaveList {
		db.Reindexer.SetLogger(log)
	}
}

func (dbw *ReindexerWrapper) OpenNamespace(namespace string, opts *reindexer.NamespaceOptions, s interface{}) (err error) {
	err = dbw.Reindexer.OpenNamespace(namespace, opts, s)
	dbw.SetSyncRequired()
	if err != nil {
		return err
	}

	for _, db := range dbw.slaveList {
		db.RegisterNamespace(namespace, opts, s)
	}

	newTestNamespace(namespace, s)

	return err
}

func (dbw *ReindexerWrapper) DropNamespace(namespace string) error {
	err := dbw.Reindexer.DropNamespace(namespace)
	dbw.SetSyncRequired()
	if err != nil {
		return err
	}

	removeTestNamespace(namespace)
	return err
}

func (dbw *ReindexerWrapper) Query(namespace string) *queryTest {
	return newTestQuery(dbw, namespace)
}

func (dbw *ReindexerWrapper) GetBaseQuery(namespace string) *reindexer.Query {
	return dbw.Reindexer.Query(namespace)
}

func (dbw *ReindexerWrapper) execQuery(t *testing.T, qt *queryTest) *reindexer.Iterator {
	return dbw.execQueryCtx(t, context.Background(), qt)
}

func (dbw *ReindexerWrapper) waitSyncWithSingleDb(t *testing.T, sdb *ReindexerWrapper) {
	if !dbw.IsSynced() {
		dbw.syncMutex.Lock()
		defer dbw.syncMutex.Unlock()
		sdb.WaitForSyncWithMaster(t)
		dbw.setSynced()
		sdb.ResetCaches()
	}
}

func (dbw *ReindexerWrapper) waitSyncFull(t *testing.T) {
	if !dbw.IsSynced() {
		dbw.syncMutex.Lock()
		defer dbw.syncMutex.Unlock()
		for _, db := range dbw.slaveList {
			db.WaitForSyncWithMaster(t)
		}
		dbw.setSynced()
	}
}

func (dbw *ReindexerWrapper) execQueryCtx(t *testing.T, ctx context.Context, qt *queryTest) *reindexer.Iterator {
	if len(dbw.slaveList) == 0 || !qt.readOnly {
		if !qt.readOnly {
			dbw.SetSyncRequired()
		}
		return qt.q.ExecCtx(ctx)
	}
	if !qt.deepReplEqual {
		sdb := dbw.slaveList[rand.Intn(len(dbw.slaveList))]
		dbw.waitSyncWithSingleDb(t, sdb)
		slaveQuery := qt.q.MakeCopy(&sdb.Reindexer)
		return slaveQuery.ExecCtx(ctx)
	}

	baseQuery := qt.q.MakeCopy(&dbw.Reindexer)
	rm, err := baseQuery.ExecCtx(ctx).FetchAll()
	if err != nil {
		return qt.q.MustExecCtx(ctx)
	}
	m := make(map[string]interface{})
	for _, item := range rm {
		pk := getPK(qt.ns, reflect.Indirect(reflect.ValueOf(item)))
		m[pk+reflect.TypeOf(item).String()] = item
	}

	dbw.waitSyncFull(t)
	for _, db := range dbw.slaveList {
		slaveQuery := qt.q.MakeCopy(&db.Reindexer)
		rs, err := slaveQuery.ExecCtx(ctx).FetchAll()
		if err != nil {
			panic(err)
		}
		if len(rs) != len(rm) {
			panic(fmt.Errorf("Follower's response length not equal to leader's: %v != %v", len(rs), len(rm)))
		}
		for _, item := range rs {
			pk := getPK(qt.ns, reflect.Indirect(reflect.ValueOf(item)))
			if mitem, ok := m[pk+reflect.TypeOf(item).String()]; ok {
				if !reflect.DeepEqual(mitem, item) {
					panic(fmt.Errorf("Follower's response not equal to leader's. Items are different: %v vs %v", mitem, item))
				}
			} else {
				panic(fmt.Errorf("Follower's response not equal to leader's. Item '%v' is missing", pk+reflect.TypeOf(item).String()))
			}
		}
		if !reflect.DeepEqual(rm, rs) {
			panic(fmt.Errorf("Follower's response not equal to leader's: %v vs %v", rm, rs))
		}
	}

	return qt.q.MustExecCtx(ctx)
}

func (dbw *ReindexerWrapper) setSlaveConfig(slaveDb *ReindexerWrapper) {
	nodes := []reindexer.DBAsyncReplicationNode{}
	for _, node := range dbw.slaveList {
		nodes = append(nodes, reindexer.DBAsyncReplicationNode{DSN: node.dsn})
	}
	err := slaveDb.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type:        "replication",
		Replication: &reindexer.DBReplicationConfig{ServerID: 1, ClusterID: 1},
	})
	if err != nil {
		panic(err)
	}
	err = slaveDb.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type:             "async_replication",
		AsyncReplication: &reindexer.DBAsyncReplicationConfig{Namespaces: []string{}},
	})
	if err != nil {
		panic(err)
	}
	err = dbw.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type:        "replication",
		Replication: &reindexer.DBReplicationConfig{ServerID: 0, ClusterID: 1},
	})
	if err != nil {
		panic(err)
	}
	err = dbw.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type:             "async_replication",
		AsyncReplication: &reindexer.DBAsyncReplicationConfig{Role: "leader", Namespaces: []string{}, RetrySyncInterval: 1000000, Nodes: nodes},
	})
	if err != nil {
		panic(err)
	}
}

func (dbw *ReindexerWrapper) WaitForSyncWithMaster(t *testing.T) {
	helpers.WaitForSyncWithMaster(t, &dbw.master.Reindexer, &dbw.Reindexer)
	dbw.setSynced()
}

func (dbw *ReindexerWrapper) TruncateNamespace(namespace string) (err error) {
	err = dbw.Reindexer.TruncateNamespace(namespace)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) RenameNamespace(srcNsName string, dstNsName string) error {
	err := dbw.Reindexer.RenameNamespace(srcNsName, dstNsName)
	dbw.SetSyncRequired()
	if err != nil {
		return err
	}
	//change client ns tables
	for _, db := range dbw.slaveList {
		db.Reindexer.RenameNs(srcNsName, dstNsName)
	}

	renameTestNamespace(srcNsName, dstNsName)
	return err
}

func (dbw *ReindexerWrapper) CloseNamespace(namespace string) (err error) {
	err = dbw.Reindexer.CloseNamespace(namespace)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) AddIndex(namespace string, indexDef ...reindexer.IndexDef) (err error) {
	err = dbw.Reindexer.AddIndex(namespace, indexDef...)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) UpdateIndex(namespace string, indexDef reindexer.IndexDef) (err error) {
	err = dbw.Reindexer.UpdateIndex(namespace, indexDef)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) DropIndex(namespace, index string) (err error) {
	err = dbw.Reindexer.DropIndex(namespace, index)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) Upsert(namespace string, item interface{}, precepts ...string) (err error) {
	err = dbw.Reindexer.Upsert(namespace, item, precepts...)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) Insert(namespace string, item interface{}, precepts ...string) (upd int, err error) {
	upd, err = dbw.Reindexer.Insert(namespace, item, precepts...)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) Update(namespace string, item interface{}, precepts ...string) (upd int, err error) {
	upd, err = dbw.Reindexer.Update(namespace, item, precepts...)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) Delete(namespace string, item interface{}, precepts ...string) (err error) {
	err = dbw.Reindexer.Delete(namespace, item, precepts...)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) EnumMeta(t *testing.T, namespace string) ([]string, error) {
	if len(dbw.slaveList) != 0 {
		sdb := dbw.slaveList[rand.Intn(len(dbw.slaveList))]
		dbw.waitSyncWithSingleDb(t, sdb)
		return sdb.Reindexer.EnumMeta(namespace)
	}
	return dbw.Reindexer.EnumMeta(namespace)
}

func (dbw *ReindexerWrapper) PutMeta(namespace string, key string, data []byte) (err error) {
	err = dbw.Reindexer.PutMeta(namespace, key, data)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) GetMeta(t *testing.T, namespace string, key string) ([]byte, error) {
	if len(dbw.slaveList) != 0 {
		sdb := dbw.slaveList[rand.Intn(len(dbw.slaveList))]
		dbw.waitSyncWithSingleDb(t, sdb)
		return sdb.Reindexer.GetMeta(namespace, key)
	}
	return dbw.Reindexer.GetMeta(namespace, key)
}

func (dbw *ReindexerWrapper) DeleteMeta(namespace string, key string) (err error) {
	err = dbw.Reindexer.DeleteMeta(namespace, key)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) UpsertCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) (err error) {
	err = dbw.Reindexer.WithContext(ctx).Upsert(namespace, item, precepts...)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) InsertCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) (upd int, err error) {
	upd, err = dbw.Reindexer.WithContext(ctx).Insert(namespace, item, precepts...)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) UpdateCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) (upd int, err error) {
	upd, err = dbw.Reindexer.WithContext(ctx).Update(namespace, item, precepts...)
	dbw.SetSyncRequired()
	return
}

func (dbw *ReindexerWrapper) DeleteCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) (err error) {
	err = dbw.Reindexer.WithContext(ctx).Delete(namespace, item, precepts...)
	dbw.SetSyncRequired()
	return
}
