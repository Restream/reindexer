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

	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/bindings/cproto"
	"github.com/restream/reindexer/test/helpers"
)

type ReindexerWrapper struct {
	reindexer.Reindexer
	isMaster     bool
	slaveList    []*ReindexerWrapper
	master       *ReindexerWrapper
	dsn          string
	syncedStatus int32
	syncMutex    sync.RWMutex
}

func NewReindexWrapper(dsn string, options ...interface{}) *ReindexerWrapper {
	return &ReindexerWrapper{Reindexer: *reindexer.NewReindex(dsn, options...), isMaster: true, dsn: dsn, syncedStatus: 0}
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
	slaveDb := NewReindexWrapper(dsn, options...)
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

func (dbw *ReindexerWrapper) SetLogger(log reindexer.Logger) {
	dbw.Reindexer.SetLogger(log)
	for _, db := range dbw.slaveList {
		db.Reindexer.SetLogger(log)
	}
}

func (dbw *ReindexerWrapper) OpenNamespace(namespace string, opts *reindexer.NamespaceOptions, s interface{}) (err error) {
	dbw.SetSyncRequired()
	err = dbw.Reindexer.OpenNamespace(namespace, opts, s)
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
	dbw.SetSyncRequired()

	err := dbw.Reindexer.DropNamespace(namespace)
	if err != nil {
		return err
	}

	removeTestNamespce(namespace)
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

func (dbw *ReindexerWrapper) waitSyncAll(t *testing.T) {
	dbw.syncMutex.Lock()
	defer dbw.syncMutex.Unlock()
	for _, db := range dbw.slaveList {
		db.WaitForSyncWithMaster(t)
	}
	dbw.setSynced()
}

func (dbw *ReindexerWrapper) waitSyncOne(t *testing.T, sdb *ReindexerWrapper) {
	dbw.syncMutex.Lock()
	defer dbw.syncMutex.Unlock()
	sdb.WaitForSyncWithMaster(t)
	dbw.setSynced()
	sdb.ResetCaches()
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
		if !dbw.IsSynced() {
			dbw.waitSyncOne(t, sdb)
		}
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

	if !dbw.IsSynced() {
		dbw.waitSyncAll(t)
	}
	for _, db := range dbw.slaveList {
		slaveQuery := qt.q.MakeCopy(&db.Reindexer)
		rs, err := slaveQuery.ExecCtx(ctx).FetchAll()
		if err != nil {
			panic(err)
		}
		if len(rs) != len(rm) {
			panic(fmt.Errorf("Slave answer not equal to master"))
		}
		for _, item := range rs {
			pk := getPK(qt.ns, reflect.Indirect(reflect.ValueOf(item)))
			if mitem, ok := m[pk+reflect.TypeOf(item).String()]; ok {
				if !reflect.DeepEqual(mitem, item) {
					panic("Slave answer not equal to master")
				}
			} else {
				panic(fmt.Errorf("Slave answer not equal to master"))
			}
		}
		//TODO NOT GOOD - can be not equal do somthing
		//reflect.DeepEqual(rm, rs)
	}

	return qt.q.MustExecCtx(ctx)
}

func (dbw *ReindexerWrapper) setSlaveConfig(slaveDb *ReindexerWrapper) {
	err := dbw.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type:        "replication",
		Replication: &reindexer.DBReplicationConfig{Namespaces: []string{}, Role: "master", ClusterID: 1, ForceSyncOnLogicError: true, ForceSyncOnWrongDataHash: true},
	})
	if err != nil {
		panic(err)
	}

	err = slaveDb.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type:        "replication",
		Replication: &reindexer.DBReplicationConfig{Namespaces: []string{}, Role: "slave", MasterDSN: *dsn, ClusterID: 1, ForceSyncOnLogicError: true, ForceSyncOnWrongDataHash: true},
	})
	if err != nil {
		panic(err)
	}

}

func (dbw *ReindexerWrapper) WaitForSyncWithMaster(t *testing.T) {

	helpers.WaitForSyncWithMaster(t, &dbw.master.Reindexer, &dbw.Reindexer)
	dbw.setSynced()

}

func (dbw *ReindexerWrapper) TruncateNamespace(namespace string) error {
	dbw.SetSyncRequired()
	return dbw.Reindexer.TruncateNamespace(namespace)
}

func (dbw *ReindexerWrapper) RenameNamespace(srcNsName string, dstNsName string) error {
	dbw.SetSyncRequired()

	err := dbw.Reindexer.RenameNamespace(srcNsName, dstNsName)
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

func (dbw *ReindexerWrapper) CloseNamespace(namespace string) error {
	dbw.SetSyncRequired()
	return dbw.Reindexer.CloseNamespace(namespace)
}

func (dbw *ReindexerWrapper) Upsert(namespace string, item interface{}, precepts ...string) error {
	dbw.SetSyncRequired()
	return dbw.Reindexer.Upsert(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) Insert(namespace string, item interface{}, precepts ...string) (int, error) {
	dbw.SetSyncRequired()
	return dbw.Reindexer.Insert(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) Update(namespace string, item interface{}, precepts ...string) (int, error) {
	dbw.SetSyncRequired()
	return dbw.Reindexer.Update(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) Delete(namespace string, item interface{}, precepts ...string) error {
	dbw.SetSyncRequired()
	return dbw.Reindexer.Delete(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) UpsertCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) error {
	dbw.SetSyncRequired()
	return dbw.Reindexer.WithContext(ctx).Upsert(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) InsertCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) (int, error) {
	dbw.SetSyncRequired()
	return dbw.Reindexer.WithContext(ctx).Insert(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) UpdateCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) (int, error) {
	dbw.SetSyncRequired()
	return dbw.Reindexer.WithContext(ctx).Update(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) DeleteCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) error {
	dbw.SetSyncRequired()
	return dbw.Reindexer.WithContext(ctx).Delete(namespace, item, precepts...)
}
