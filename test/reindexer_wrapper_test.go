package reindexer

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/bindings/cproto"
	// _ "github.com/restream/reindexer/bindings/builtinserver"
)

type ReindexerWrapper struct {
	reindexer.Reindexer
	isMaster     bool
	slaveList    []*ReindexerWrapper
	master       *ReindexerWrapper
	dsn          string
	syncedStatus int32
}

func NewReindexWrapper(dsn string, options ...interface{}) *ReindexerWrapper {
	return &ReindexerWrapper{Reindexer: *reindexer.NewReindex(dsn, options), isMaster: true, dsn: dsn, syncedStatus: 0}
}

func (dbw *ReindexerWrapper) SetSynced(sync bool) {
	if sync {
		atomic.StoreInt32(&dbw.syncedStatus, 1)
	} else {
		atomic.StoreInt32(&dbw.syncedStatus, 0)
	}
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
	slaveDb := NewReindexWrapper(dsn, options)
	slaveDb.isMaster = false
	slaveDb.master = dbw
	slaveDb.SetSynced(false)
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
	dbw.SetSynced(false)
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
	dbw.SetSynced(false)

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

func (dbw *ReindexerWrapper) execQuery(qt *queryTest) *reindexer.Iterator {
	return dbw.execQueryCtx(context.Background(), qt)
}

func (dbw *ReindexerWrapper) execQueryCtx(ctx context.Context, qt *queryTest) *reindexer.Iterator {
	if len(dbw.slaveList) == 0 || !qt.readOnly {
		if !qt.readOnly {
			dbw.SetSynced(false)
		}
		return qt.q.ExecCtx(ctx)
	}
	if !qt.deepReplEqual {
		sdb := dbw.slaveList[rand.Intn(len(dbw.slaveList))]
		if !dbw.IsSynced() {
			sdb.WaitForSyncWithMaster()
			dbw.SetSynced(true)
			sdb.ResetCaches()
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
	for _, db := range dbw.slaveList {
		if !dbw.IsSynced() {
			db.WaitForSyncWithMaster()
		}
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
	dbw.SetSynced(true)
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

func (dbw *ReindexerWrapper) WaitForSyncWithMaster() {
	complete := true

	var nameBad string
	var masterBadLsn int64
	var slaveBadLsn int64

	for i := 0; i < 600*5; i++ {

		complete = true

		stats, err := dbw.master.GetNamespacesMemStat()
		if err != nil {
			panic(err)
		}
		slaveStats, err := dbw.GetNamespacesMemStat()
		if err != nil {
			panic(err)
		}

		slaveLsnMap := make(map[string]int64)
		for _, st := range slaveStats {
			slaveLsnMap[st.Name] = st.Replication.LastLSN
		}

		for _, st := range stats {
			if len(st.Name) == 0 || st.Name[0] == '#' {
				continue
			}

			if slaveLsn, ok := slaveLsnMap[st.Name]; ok {
				if slaveLsn != st.Replication.LastLSN {
					complete = false
					nameBad = st.Name
					masterBadLsn = st.Replication.LastLSN
					slaveBadLsn = slaveLsn
					time.Sleep(100 * time.Millisecond)
					break
				}
			} else {
				complete = false
				nameBad = st.Name
				masterBadLsn = 0
				slaveBadLsn = 0
				time.Sleep(100 * time.Millisecond)
				break
			}
		}
		if complete {
			dbw.SetSynced(true)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	panic(fmt.Sprintf("Can't sync slave ns with master: ns \"%s\" masterlsn: %d , slavelsn %d", nameBad, masterBadLsn, slaveBadLsn))
}

func (dbw *ReindexerWrapper) TruncateNamespace(namespace string) error {
	dbw.SetSynced(false)
	return dbw.Reindexer.TruncateNamespace(namespace)
}

func (dbw *ReindexerWrapper) CloseNamespace(namespace string) error {
	dbw.SetSynced(false)
	return dbw.Reindexer.CloseNamespace(namespace)
}

func (dbw *ReindexerWrapper) Upsert(namespace string, item interface{}, precepts ...string) error {
	dbw.SetSynced(false)
	return dbw.Reindexer.Upsert(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) Insert(namespace string, item interface{}, precepts ...string) (int, error) {
	dbw.SetSynced(false)
	return dbw.Reindexer.Insert(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) Update(namespace string, item interface{}, precepts ...string) (int, error) {
	dbw.SetSynced(false)
	return dbw.Reindexer.Update(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) Delete(namespace string, item interface{}, precepts ...string) error {
	dbw.SetSynced(false)
	return dbw.Reindexer.Delete(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) UpsertCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) error {
	dbw.SetSynced(false)
	return dbw.Reindexer.WithContext(ctx).Upsert(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) InsertCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) (int, error) {
	dbw.SetSynced(false)
	return dbw.Reindexer.WithContext(ctx).Insert(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) UpdateCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) (int, error) {
	dbw.SetSynced(false)
	return dbw.Reindexer.WithContext(ctx).Update(namespace, item, precepts...)
}

func (dbw *ReindexerWrapper) DeleteCtx(ctx context.Context, namespace string, item interface{}, precepts ...string) error {
	dbw.SetSynced(false)
	return dbw.Reindexer.WithContext(ctx).Delete(namespace, item, precepts...)
}
