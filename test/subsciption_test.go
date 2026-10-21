package reindexer

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type subscriptionTestItem struct {
	ID    int    `json:"id" reindex:"id,,pk"`
	Genre int64  `json:"genre"`
	Year  int    `json:"year"`
	Name  string `json:"name"`
}

const (
	testSubNs1 = "test_namespace_sub1"
	testSubNs2 = "test_namespace_sub2"
	testSubNs3 = "test_namespace_sub3"

	testSubscriptionTypesInitNs = "test_subscription_types_init"
	testSubscriptionTypesNs     = "test_subscription_types"
)

func init() {
	tnamespaces[testSubNs1] = TestItem{}
	tnamespaces[testSubNs2] = TestItem{}
	tnamespaces[testSubNs3] = TestItem{}
}

func newSubscriptionTestItemID(id int) *subscriptionTestItem {
	return &subscriptionTestItem{
		ID:    id,
		Year:  rand.Int()%50 + 2000,
		Genre: int64(rand.Int() % 50),
		Name:  randString(),
	}
}

func newSubscriptionTestItem() *subscriptionTestItem {
	return newSubscriptionTestItemID(0)
}

func fillTestSubNamespace(t *testing.T, name string, cnt int) {
	for i := 0; i < cnt; i++ {
		err := DB.Upsert(name, newTestItem(i, 5))
		require.NoError(t, err)
	}
}

func createStream(t *testing.T, opts *events.EventsStreamOptions) *events.EventsStream {
	stream := DBD.Subscribe(opts)
	require.NotNil(t, stream)
	require.NoError(t, stream.Error())
	return stream
}

func readAllEvents(t *testing.T, stream *events.EventsStream) *map[string][]*events.Event {
	res := make(map[string][]*events.Event)

	for {
		tm := time.NewTicker(3 * time.Second)
		select {
		case event, ok := <-stream.Chan():
			require.NoError(t, stream.Error())
			require.True(t, ok)
			res[event.Namespace()] = append(res[event.Namespace()], event)
		case <-tm.C:
			require.NoError(t, stream.Error())
			return &res
		}
	}
}

func runMainOperationsSet(t *testing.T, ns string) {
	// Basic data operations
	err := DB.Upsert(ns, newSubscriptionTestItem(), "id=serial()")
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		cnt, err := DB.Insert(ns, newSubscriptionTestItem(), "id=serial()")
		require.NoError(t, err)
		require.Equal(t, 1, cnt)
	}

	cnt, err := DB.Update(ns, newSubscriptionTestItemID(1))
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	err = DB.Delete(ns, &subscriptionTestItem{ID: 1})
	require.NoError(t, err)

	cnt, err = DB.Update(ns, newSubscriptionTestItemID(1))
	require.NoError(t, err)
	require.Equal(t, 0, cnt)

	// Queries
	updRes, err := DB.Query(ns).Set("name", "updated name").Update().FetchAll()
	require.NoError(t, err)
	require.Equal(t, 3, len(updRes))

	delRes, err := DB.Query(ns).Where("id", reindexer.LE, 3).Delete()
	require.NoError(t, err)
	require.Equal(t, 2, delRes)

	// Transaction
	tx, err := DB.BeginTx(ns)
	require.NoError(t, err)
	err = tx.Upsert(newSubscriptionTestItemID(123))
	require.NoError(t, err)

	err = tx.Insert(newSubscriptionTestItem(), "id=serial()")
	require.NoError(t, err)

	err = tx.Update(newSubscriptionTestItemID(4))
	require.NoError(t, err)

	err = tx.Delete(&subscriptionTestItem{ID: 4})
	require.NoError(t, err)

	updRes, err = tx.Query().Where("id", reindexer.EQ, 123).Set("name", "updated name").Update().FetchAll()
	require.NoError(t, err)
	require.Equal(t, 0, len(updRes))

	cnt, err = tx.Query().Where("id", reindexer.EQ, 123).Delete()
	require.NoError(t, err)
	require.Equal(t, 0, cnt)

	tx.MustCommit()

	// Indexes operations
	err = DB.AddIndex(ns, reindexer.IndexDef{Name: "name",
		JSONPaths: []string{"name"},
		IndexType: "text",
		FieldType: "string"})
	require.NoError(t, err)

	err = DB.AddIndex(ns, reindexer.IndexDef{Name: "year",
		JSONPaths: []string{"year"},
		IndexType: "hash",
		FieldType: "int"})
	require.NoError(t, err)

	err = DB.UpdateIndex(ns, reindexer.IndexDef{Name: "name",
		JSONPaths:   []string{"name"},
		IndexType:   "text",
		FieldType:   "string",
		CollateMode: "utf8"})
	require.NoError(t, err)

	err = DB.DropIndex(ns, "year")
	require.NoError(t, err)

	// Config namespace modifications
	err = DB.SetDefaultQueryDebug(ns, reindexer.TRACE)
	require.NoError(t, err)
	err = DB.SetDefaultQueryDebug(ns, reindexer.INFO)
	require.NoError(t, err)

	// More namespace operations
	err = DB.DropNamespace(ns)
	require.NoError(t, err)
}

func runFullOperationsSet(t *testing.T, nsInitial string, nsRenamed string) {
	// Namepsace operations
	err := DB.OpenNamespace(nsInitial, reindexer.DefaultNamespaceOptions(), subscriptionTestItem{})
	require.NoError(t, err)

	err = DB.RenameNamespace(nsInitial, nsRenamed)
	require.NoError(t, err)

	runMainOperationsSet(t, nsRenamed)
}

func runFullOperationsSetNoRename(t *testing.T, ns string) {
	// Namepsace operations
	err := DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), subscriptionTestItem{})
	require.NoError(t, err)

	runMainOperationsSet(t, ns)
}

func validateEventsCount(t *testing.T, expected *map[string]int, actual *map[string][]*events.Event) {
	assert.Equal(t, len(*expected), len(*actual), "Event maps: Expected: %v, Actual: %v", *expected, *actual)
	for name, count := range *expected {
		assert.Equal(t, count, len((*actual)[name]), "Event maps: Expected: %v, Actual: %v", *expected, *actual)
	}
}

func validateEventsLSNNotEmpty(t *testing.T, events []*events.Event) {
	require.Greater(t, len(events), 0)
	for i, e := range events {
		assert.Falsef(t, e.NamespaceVersion().IsEmpty(), "event index is %d; event: %v", i, *e)
		assert.Falsef(t, e.LSN().IsEmpty(), "event index is %d; event: %v", i, *e)
	}
}

func validateEventsLSNEmpty(t *testing.T, events []*events.Event) {
	require.Greater(t, len(events), 0)
	for i, e := range events {
		assert.Truef(t, e.NamespaceVersion().IsEmpty(), "event index is %d; event: %v", i, *e)
		assert.Truef(t, e.LSN().IsEmpty(), "event index is %d; event: %v", i, *e)
	}
}

func validateEventsDBNameNotEmpty(t *testing.T, events []*events.Event) {
	require.Greater(t, len(events), 0)
	db := events[0].Database()
	require.Greaterf(t, len(db), 0, "event: %v", *events[0])
	for i, e := range events {
		assert.Equalf(t, e.Database(), db, "event index is %d; event: %v", i, *e)
	}
}

func validateEventsDBNameEmpty(t *testing.T, events []*events.Event) {
	require.Greater(t, len(events), 0)
	for i, e := range events {
		assert.Equalf(t, len(e.Database()), 0, "event index is %d; event: %v", i, *e)
	}
}

func validateEventsTimestamp(t *testing.T, min time.Time, max time.Time, events []*events.Event) {
	require.Greater(t, len(events), 0)

	ts := events[0].Timestamp()
	for i, e := range events {
		assert.GreaterOrEqualf(t, e.Timestamp(), ts, "event index is %d; event: %v", i, *e)
		ts = e.Timestamp()
		assert.GreaterOrEqualf(t, ts, min, "event index is %d; event: %v", i, *e)
		assert.LessOrEqualf(t, ts, max, "event index is %d; event: %v", i, *e)
	}
}

func validateEventsTimestampEmpty(t *testing.T, events []*events.Event) {
	require.Greater(t, len(events), 0)

	for i, e := range events {
		assert.Truef(t, e.Timestamp().IsZero(), "event index is %d; event: %v", i, *e)
	}
}

func validateEventsSequence(t *testing.T, name string, expected []events.EventType, stream *events.EventsStream) []*events.Event {
	require.Greater(t, len(expected), 0, "name: %s", name)
	events := make([]*events.Event, 0, len(expected))

	for {
		tm := time.NewTicker(3 * time.Second)
		select {
		case event, ok := <-stream.Chan():
			require.NoErrorf(t, stream.Error(), "name: %s", name)
			require.True(t, ok, "name: %s", name)
			require.Equalf(t, expected[len(events)], event.Type(), "event index is %d; name: %s", len(events), name)

			events = append(events, event)
			if len(events) == len(expected) {
				return events
			}
		case <-tm.C:
			t.Fatalf("Events stream timeout. Name: %s", name)
			return events
		}
	}
}

func initTestSubscriptionEventTypes(t *testing.T, nsInitial string, nsRenamed string) (finalNs string, expectedMap map[string][]events.EventType, runOperationsSet func()) {
	expectedMap = map[string][]events.EventType{
		"all": {
			events.EventTypeAddNamespace, events.EventTypeIndexAdd, events.EventTypeSetTagsMatcher, events.EventTypeSetSchema,
			events.EventTypeRenameNamespace,
			events.EventTypePutMeta, events.EventTypeItemUpsert, events.EventTypePutMeta, events.EventTypeItemInsert,
			events.EventTypePutMeta, events.EventTypeItemInsert, events.EventTypePutMeta, events.EventTypeItemInsert,
			events.EventTypeItemUpdate, events.EventTypeItemDelete,
			events.EventTypeItemUpdate, events.EventTypeItemUpdate, events.EventTypeItemUpdate, // Update query
			events.EventTypeItemDelete, events.EventTypeItemDelete, // Delete query
			// Tx events
			events.EventTypeBeginTx,
			events.EventTypeItemUpsertTx, events.EventTypePutMetaTx, events.EventTypeItemInsertTx,
			events.EventTypeItemUpdateTx, events.EventTypeItemDeleteTx,
			events.EventTypeItemUpdateTx, // Update query in tx
			events.EventTypeItemDeleteTx, // Delete query in tx
			events.EventTypeCommitTx,
			// Index events
			events.EventTypeIndexAdd, events.EventTypeIndexAdd, events.EventTypeIndexUpdate, events.EventTypeIndexDrop,
			events.EventTypeItemUpsert, events.EventTypeItemUpsert, // Config namespace updates
			events.EventTypeDropNamespace,
		},
		"all_renamed": {
			events.EventTypePutMeta, events.EventTypeItemUpsert, events.EventTypePutMeta, events.EventTypeItemInsert,
			events.EventTypePutMeta, events.EventTypeItemInsert, events.EventTypePutMeta, events.EventTypeItemInsert,
			events.EventTypeItemUpdate, events.EventTypeItemDelete,
			events.EventTypeItemUpdate, events.EventTypeItemUpdate, events.EventTypeItemUpdate, // Update query
			events.EventTypeItemDelete, events.EventTypeItemDelete, // Delete query
			// Tx events
			events.EventTypeBeginTx,
			events.EventTypeItemUpsertTx, events.EventTypePutMetaTx, events.EventTypeItemInsertTx,
			events.EventTypeItemUpdateTx, events.EventTypeItemDeleteTx,
			events.EventTypeItemUpdateTx, // Update query in tx
			events.EventTypeItemDeleteTx, // Delete query in tx
			events.EventTypeCommitTx,
			// Index events
			events.EventTypeIndexAdd, events.EventTypeIndexAdd, events.EventTypeIndexUpdate, events.EventTypeIndexDrop,
			events.EventTypeDropNamespace,
		},
		"namespace_ops": {
			events.EventTypeAddNamespace,
			events.EventTypeRenameNamespace,
			events.EventTypeDropNamespace},
		"index_ops": {
			events.EventTypeIndexAdd,
			events.EventTypeIndexAdd, events.EventTypeIndexAdd,
			events.EventTypeIndexUpdate, events.EventTypeIndexDrop,
		},
		"documents_ops": {
			events.EventTypePutMeta, events.EventTypeItemUpsert, events.EventTypePutMeta, events.EventTypeItemInsert,
			events.EventTypePutMeta, events.EventTypeItemInsert, events.EventTypePutMeta, events.EventTypeItemInsert,
			events.EventTypeItemUpdate, events.EventTypeItemDelete,
			events.EventTypeItemUpdate, events.EventTypeItemUpdate, events.EventTypeItemUpdate, // Update query
			events.EventTypeItemDelete, events.EventTypeItemDelete, // Delete query
		},
		"index+schema_ops": {
			events.EventTypeIndexAdd, events.EventTypeSetSchema,
			events.EventTypeIndexAdd, events.EventTypeIndexAdd,
			events.EventTypeIndexUpdate, events.EventTypeIndexDrop,
		},
		"all_tx_ops": {
			events.EventTypeBeginTx,
			events.EventTypeItemUpsertTx, events.EventTypePutMetaTx, events.EventTypeItemInsertTx,
			events.EventTypeItemUpdateTx, events.EventTypeItemDeleteTx,
			events.EventTypeItemUpdateTx, // Update query in tx
			events.EventTypeItemDeleteTx, // Delete query in tx
			events.EventTypeCommitTx,
		},
		"commit_tx_ops": {
			events.EventTypeCommitTx,
		},
	}

	if len(DB.slaveList) > 0 || len(DB.clusterList) > 0 {
		delete(expectedMap, "all_renamed")
		expectedMap["all"] = []events.EventType{
			events.EventTypeAddNamespace, events.EventTypeIndexAdd, events.EventTypeSetTagsMatcher, events.EventTypeSetSchema,
			events.EventTypePutMeta, events.EventTypeItemUpsert, events.EventTypePutMeta, events.EventTypeItemInsert,
			events.EventTypePutMeta, events.EventTypeItemInsert, events.EventTypePutMeta, events.EventTypeItemInsert,
			events.EventTypeItemUpdate, events.EventTypeItemDelete,
			events.EventTypeItemUpdate, events.EventTypeItemUpdate, events.EventTypeItemUpdate, // Update query
			events.EventTypeItemDelete, events.EventTypeItemDelete, // Delete query
			// Tx events
			events.EventTypeBeginTx,
			events.EventTypeItemUpsertTx, events.EventTypePutMetaTx, events.EventTypeItemInsertTx,
			events.EventTypeItemUpdateTx, events.EventTypeItemDeleteTx,
			events.EventTypeItemUpdateTx, // Update query in tx
			events.EventTypeItemDeleteTx, // Delete query in tx
			events.EventTypeCommitTx,
			// Index events
			events.EventTypeIndexAdd, events.EventTypeIndexAdd, events.EventTypeIndexUpdate, events.EventTypeIndexDrop,
			events.EventTypeItemUpsert, events.EventTypeItemUpsert, // Config namespace updates
			events.EventTypeDropNamespace,
		}
		expectedMap["namespace_ops"] = []events.EventType{
			events.EventTypeAddNamespace,
			events.EventTypeDropNamespace,
		}

		runOperationsSet = func() {
			runFullOperationsSetNoRename(t, nsInitial)
		}
		finalNs = nsInitial
	} else {
		runOperationsSet = func() {
			runFullOperationsSet(t, nsInitial, nsRenamed)
		}
		finalNs = nsRenamed
	}
	return
}

func TestBasicSubscription(t *testing.T) {
	streams := make([]*events.EventsStream, 3)

	const (
		ns1Items = 10
		ns2Items = 20
		ns3Items = 15
	)

	streams[0] = createStream(t, events.DefaultEventsStreamOptions().
		WithNamespacesList(testSubNs1, testSubNs2).WithLSN().WithServerID().WithShardID())
	defer streams[0].Close(context.Background())
	streams[1] = createStream(t, events.DefaultEventsStreamOptions().
		WithNamespacesList(testSubNs2).WithLSN().WithServerID().WithShardID())
	defer streams[1].Close(context.Background())

	fillTestSubNamespace(t, testSubNs1, ns1Items)
	fillTestSubNamespace(t, testSubNs2, ns2Items)
	fillTestSubNamespace(t, testSubNs3, ns3Items)

	validateEventsCount(t, &map[string]int{testSubNs1: ns1Items, testSubNs2: ns2Items}, readAllEvents(t, streams[0]))
	validateEventsCount(t, &map[string]int{testSubNs2: ns2Items}, readAllEvents(t, streams[1]))

	err := streams[1].Close(context.Background())
	assert.NoError(t, err)

	fillTestSubNamespace(t, testSubNs1, ns1Items)
	validateEventsCount(t, &map[string]int{testSubNs1: ns1Items}, readAllEvents(t, streams[0]))

	streams[2] = createStream(t, events.DefaultEventsStreamOptions().WithDocModifyEvents())
	defer streams[2].Close(context.Background())
	fillTestSubNamespace(t, testSubNs1, ns1Items*2)
	fillTestSubNamespace(t, testSubNs2, ns2Items*2)
	fillTestSubNamespace(t, testSubNs3, ns3Items*2)
	validateEventsCount(t, &map[string]int{testSubNs1: ns1Items * 2, testSubNs2: ns2Items * 2}, readAllEvents(t, streams[0]))
	validateEventsCount(t, &map[string]int{testSubNs1: ns1Items * 2, testSubNs2: ns2Items * 2, testSubNs3: ns3Items * 2}, readAllEvents(t, streams[2]))
}

func TestSubscriptionMaxStreams(t *testing.T) {
	const MaxSubs = 32
	streams := make([]*events.EventsStream, 0, MaxSubs)
	for i := 0; i < MaxSubs; i++ {
		stream := DBD.Subscribe(events.DefaultEventsStreamOptions())
		require.NoError(t, stream.Error(), i)
		select {
		case _, ok := <-stream.Chan():
			require.True(t, ok, "Expecting opened channel")
		default:
		}
		require.NotNil(t, stream.Chan())
		defer stream.Close(context.Background())
		streams = append(streams, stream)
	}
	stream := DBD.Subscribe(events.DefaultEventsStreamOptions())
	require.Error(t, stream.Error())
	select {
	case _, ok := <-stream.Chan():
		require.False(t, ok, "Expecting closed channel")
	default:
	}

	err := streams[0].Close(context.Background())
	require.NoError(t, err)
	stream = DBD.Subscribe(events.DefaultEventsStreamOptions())
	require.NoError(t, stream.Error())
	select {
	case _, ok := <-stream.Chan():
		require.True(t, ok, "Expecting opened channel")
	default:
	}
	defer stream.Close(context.Background())
}

func TestSubscriptionEventTypes(t *testing.T) {
	// This test may contain 'rename' operation and v4 replication does not support it.
	// The version of this test for the pipelines with replication will skip case for the 'rename' event.
	initialNs := testSubscriptionTypesInitNs
	ns := testSubscriptionTypesNs
	ns, expectedMap, runOperationsSet := initTestSubscriptionEventTypes(t, initialNs, ns)

	t.Run("all operations", func(t *testing.T) {
		name := "all"
		expected, ok := expectedMap[name]
		require.True(t, ok)

		stream := DBD.Subscribe(events.DefaultEventsStreamOptions().
			WithConfigNamespace().WithTimestamp())
		defer stream.Close(context.Background())
		beg := time.Now()
		runOperationsSet()
		events := validateEventsSequence(t, name, expected, stream)
		end := time.Now()

		validateEventsLSNEmpty(t, events)
		validateEventsDBNameEmpty(t, events)
		validateEventsTimestamp(t, beg, end, events)
	})

	t.Run("all operations for the specific namespace", func(t *testing.T) {
		name := "all_renamed"
		expected, ok := expectedMap[name]
		if !ok {
			t.Skip("Do not check rename event")
		}
		require.True(t, ok)

		stream := DBD.Subscribe(events.DefaultEventsStreamOptions().
			WithNamespacesList(ns).WithDBName())
		defer stream.Close(context.Background())
		runOperationsSet()
		events := validateEventsSequence(t, name, expected, stream)

		validateEventsLSNEmpty(t, events)
		validateEventsDBNameNotEmpty(t, events)
		validateEventsTimestampEmpty(t, events)
	})

	t.Run("namespace related operations", func(t *testing.T) {
		name := "namespace_ops"
		expected, ok := expectedMap[name]
		require.True(t, ok)

		stream := DBD.Subscribe(events.DefaultEventsStreamOptions().
			WithNamespaceOperationEvents().WithConfigNamespace())
		defer stream.Close(context.Background())
		runOperationsSet()
		events := validateEventsSequence(t, name, expected, stream)

		validateEventsLSNEmpty(t, events)
		validateEventsDBNameEmpty(t, events)
		validateEventsTimestampEmpty(t, events)
	})

	t.Run("indexes operations", func(t *testing.T) {
		name := "index_ops"
		expected, ok := expectedMap[name]
		require.True(t, ok)

		stream := DBD.Subscribe(events.DefaultEventsStreamOptions().
			WithIndexModifyEvents().WithLSN())
		defer stream.Close(context.Background())
		runOperationsSet()
		events := validateEventsSequence(t, name, expected, stream)

		validateEventsLSNNotEmpty(t, events)
		validateEventsDBNameEmpty(t, events)
		validateEventsTimestampEmpty(t, events)
	})

	t.Run("documents operations", func(t *testing.T) {
		name := "documents_ops"
		expected, ok := expectedMap[name]
		require.True(t, ok)

		stream := DBD.Subscribe(events.DefaultEventsStreamOptions().
			WithDocModifyEvents().WithServerID())
		defer stream.Close(context.Background())
		runOperationsSet()
		events := validateEventsSequence(t, name, expected, stream)

		validateEventsLSNEmpty(t, events)
		validateEventsDBNameEmpty(t, events)
		validateEventsTimestampEmpty(t, events)
	})

	t.Run("indexes and schema operations", func(t *testing.T) {
		name := "index+schema_ops"
		expected, ok := expectedMap[name]
		require.True(t, ok)

		stream := DBD.Subscribe(events.DefaultEventsStreamOptions().
			WithIndexModifyEvents().WithEvents(events.EventTypeSetSchema).WithShardID())
		defer stream.Close(context.Background())
		runOperationsSet()
		events := validateEventsSequence(t, name, expected, stream)

		validateEventsLSNEmpty(t, events)
		validateEventsDBNameEmpty(t, events)
		validateEventsTimestampEmpty(t, events)
	})

	t.Run("all tx operations", func(t *testing.T) {
		name := "all_tx_ops"
		expected, ok := expectedMap[name]
		require.True(t, ok)

		stream := DBD.Subscribe(events.DefaultEventsStreamOptions().WithAllTransactionEvents())
		defer stream.Close(context.Background())
		runOperationsSet()
		validateEventsSequence(t, name, expected, stream)
	})

	t.Run("tx commit operations", func(t *testing.T) {
		name := "commit_tx_ops"
		expected, ok := expectedMap[name]
		require.True(t, ok)

		stream := DBD.Subscribe(events.DefaultEventsStreamOptions().
			WithNamespacesList(ns).WithTransactionCommitEvents())
		defer stream.Close(context.Background())
		runOperationsSet()
		validateEventsSequence(t, name, expected, stream)
	})

	t.Run("parallel streams operations", func(t *testing.T) {

		subsAwaitCh := make(chan bool)
		validationAwaitCh := make(chan bool)
		totalRoutines := 0
		wg := sync.WaitGroup{}

		runNewRoutine := func(name string, opts *events.EventsStreamOptions) {
			if _, ok := expectedMap[name]; !ok {
				return
			}
			wg.Add(1)
			totalRoutines += 1

			go func() {
				defer wg.Done()
				expected, ok := expectedMap[name]
				require.True(t, ok, "name: %s", name)

				stream := DBD.Subscribe(opts)
				defer stream.Close(context.Background())
				subsAwaitCh <- true
				<-validationAwaitCh
				validateEventsSequence(t, name, expected, stream)
			}()
		}

		runNewRoutine("all", events.DefaultEventsStreamOptions().WithConfigNamespace())
		runNewRoutine("all_renamed", events.DefaultEventsStreamOptions().WithNamespacesList(ns))
		runNewRoutine("namespace_ops", events.DefaultEventsStreamOptions().WithNamespaceOperationEvents().WithConfigNamespace())
		runNewRoutine("index_ops", events.DefaultEventsStreamOptions().WithIndexModifyEvents())
		runNewRoutine("documents_ops", events.DefaultEventsStreamOptions().WithDocModifyEvents())
		runNewRoutine("index+schema_ops", events.DefaultEventsStreamOptions().WithIndexModifyEvents().WithEvents(events.EventTypeSetSchema))
		runNewRoutine("all_tx_ops", events.DefaultEventsStreamOptions().WithAllTransactionEvents())
		runNewRoutine("commit_tx_ops", events.DefaultEventsStreamOptions().WithNamespacesList(ns).WithTransactionCommitEvents())

		ticker := time.NewTicker(5 * time.Second)
		require.Equal(t, len(expectedMap), totalRoutines)
	loop:
		for {
			select {
			case <-ticker.C:
				t.Fatal("Startup timeout")
				return
			case <-subsAwaitCh:
				totalRoutines -= 1
				if totalRoutines == 0 {
					break loop
				}
			}
		}

		runOperationsSet()
		close(validationAwaitCh)

		wg.Wait()
	})
}
