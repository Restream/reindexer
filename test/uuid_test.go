package reindexer

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/restream/reindexer/v4"
	"github.com/restream/reindexer/v4/bindings/builtinserver/config"
	"github.com/restream/reindexer/v4/test/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestUuidStruct struct {
	ID        int      `reindex:"id,,pk"`
	Uuid      string   `reindex:"uuid,hash,uuid" json:"uuid"`
	UuidArray []string `reindex:"uuid_array,hash,uuid" json:"uuid_array"`
	_         struct{} `reindex:"id+uuid,,composite"`
}

type TestIntItemWithUuidTagStruct struct {
	ID   int `reindex:"id,,pk"`
	Uuid int `reindex:"uuid,hash,uuid" json:"uuid"`
}

type TestInt64ItemWithUuidTagStruct struct {
	ID   int   `reindex:"id,,pk"`
	Uuid int64 `reindex:"uuid,hash,uuid" json:"uuid"`
}

type TestFloatItemWithUuidTagStruct struct {
	ID   int     `reindex:"id,,pk"`
	Uuid float32 `reindex:"uuid,hash,uuid" json:"uuid"`
}

type TestDoubleItemWithUuidTagStruct struct {
	ID   int     `reindex:"id,,pk"`
	Uuid float64 `reindex:"uuid,hash,uuid" json:"uuid"`
}

type TestComplexItemWithUuidTagStruct struct {
	ID   int       `reindex:"id,,pk"`
	Uuid complex64 `reindex:"uuid,hash,uuid" json:"uuid"`
}

type TestByteItemWithUuidTagStruct struct {
	ID   int  `reindex:"id,,pk"`
	Uuid byte `reindex:"uuid,hash,uuid" json:"uuid"`
}

type TestBoolItemWithUuidTagStruct struct {
	ID   int  `reindex:"id,,pk"`
	Uuid bool `reindex:"uuid,hash,uuid" json:"uuid"`
}

type TestUuidStructNoIdx struct {
	ID   int    `reindex:"id,,pk"`
	Uuid string `json:"uuid"`
}

type TestUuidStructNoTag struct {
	ID   int    `reindex:"id,,pk"`
	Uuid string `reindex:"uuid,hash" json:"uuid"`
}

const TestUuidItemCreateNs = "test_uuid_item_create"

const TestIntItemWithUuidTagNs = "test_int_item_with_uuid_tag"
const TestInt64ItemWithUuidTagNs = "test_int64_item_with_uuid_tag"
const TestFloatItemWithUuidTagNs = "test_float_item_with_uuid_tag"
const TestDoubleItemWithUuidTagNs = "test_double_item_with_uuid_tag"
const TestComplexItemWithUuidTagNs = "test_complex_item_with_uuid_tag"
const TestByteItemWithUuidTagNs = "test_byte_item_with_uuid_tag"
const TestBoolItemWithUuidTagNs = "test_bool_item_with_uuid_tag"

func checkExplainSelect(t *testing.T, it reindexer.Iterator, item interface{}) {
	require.NoError(t, it.Error())
	assert.Equal(t, it.Count(), 1)
	for it.Next() {
		require.NoError(t, it.Error())
		assert.EqualValues(t, it.Object(), item)
	}
	explainRes, err := it.GetExplainResults()
	require.NoError(t, err)
	checkExplain(t, explainRes.Selectors, []expectedExplain{
		{
			Field:   "uuid",
			Method:  "index",
			Keys:    1,
			Matched: 1,
		},
	}, "")
}

func configureAndStartServer(httpaddr string, rpcaddr string, path string) *reindexer.Reindexer {
	cfg := config.DefaultServerConfig()
	cfg.Net.HTTPAddr = httpaddr
	cfg.Net.RPCAddr = rpcaddr
	cfg.Storage.Path = path
	os.RemoveAll(cfg.Storage.Path)
	rx := reindexer.NewReindex("builtinserver://uudb", reindexer.WithServerConfig(time.Second*100, cfg))
	return rx
}

func init() {
	tnamespaces[TestUuidItemCreateNs] = TestUuidStruct{}
}

func TestUuidItemCreate(t *testing.T) {
	t.Parallel()

	const ns = TestUuidItemCreateNs

	t.Run("add item with valid uuid", func(t *testing.T) {
		item := TestUuidStruct{
			ID: rand.Intn(100), Uuid: randUuid(), UuidArray: randUuidArray(rand.Intn(5)),
		}
		err := DB.Upsert(ns, item)
		require.NoError(t, err)
	})

	t.Run("add item with invalid uuid", func(t *testing.T) {
		item := TestUuidStruct{
			ID: rand.Intn(100), Uuid: randString(), UuidArray: randUuidArray(rand.Intn(5)),
		}
		err := DB.Upsert(ns, item)
		require.Error(t, err)
		errExp := "UUID cannot contain char"
		assert.Contains(t, err.Error(), errExp)
	})

	t.Run("add item with invalid array uuid", func(t *testing.T) {
		item := TestUuidStruct{
			ID: rand.Intn(100), Uuid: randUuid(), UuidArray: randStringArr(rand.Intn(5) + 1),
		}
		err := DB.Upsert(ns, item)
		require.Error(t, err)
		errExp := "UUID cannot contain char"
		assert.Contains(t, err.Error(), errExp)
	})

	t.Run("add item with invalid uuid and array uuid", func(t *testing.T) {
		item := TestUuidStruct{
			ID: rand.Intn(100), Uuid: randString(), UuidArray: randStringArr(rand.Intn(5) + 1),
		}
		err := DB.Upsert(ns, item)
		require.Error(t, err)
		errExp := "UUID cannot contain char"
		assert.Contains(t, err.Error(), errExp)
	})
}

func TestNotStringItemsWithUuidTag(t *testing.T) {
	t.Parallel()

	nsOpts := reindexer.DefaultNamespaceOptions()

	t.Run("cant open ns when int field with uuid tag", func(t *testing.T) {
		errExpr := "UUID index is not applicable with 'int64' field, only with 'string'"
		assert.EqualError(t, DB.OpenNamespace(TestIntItemWithUuidTagNs, nsOpts,
			TestIntItemWithUuidTagStruct{}), errExpr)
	})

	t.Run("cant open ns when int64 field with uuid tag", func(t *testing.T) {
		errExpr := "UUID index is not applicable with 'int64' field, only with 'string'"
		assert.EqualError(t, DB.OpenNamespace(TestInt64ItemWithUuidTagNs, nsOpts,
			TestInt64ItemWithUuidTagStruct{}), errExpr)
	})

	t.Run("cant open ns when float field with uuid tag", func(t *testing.T) {
		errExpr := "UUID index is not applicable with 'double' field, only with 'string'"
		assert.EqualError(t, DB.OpenNamespace(TestFloatItemWithUuidTagNs, nsOpts,
			TestFloatItemWithUuidTagStruct{}), errExpr)
	})

	t.Run("cant open ns when double field with uuid tag", func(t *testing.T) {
		errExpr := "UUID index is not applicable with 'double' field, only with 'string'"
		assert.EqualError(t, DB.OpenNamespace(TestDoubleItemWithUuidTagNs, nsOpts,
			TestDoubleItemWithUuidTagStruct{}), errExpr)
	})

	t.Run("cant open ns when complex field with uuid tag", func(t *testing.T) {
		errExpr := "rq: Invalid reflection type of index"
		assert.EqualError(t, DB.OpenNamespace(TestComplexItemWithUuidTagNs, nsOpts,
			TestComplexItemWithUuidTagStruct{}), errExpr)
	})

	t.Run("cant open ns when byte field with uuid tag", func(t *testing.T) {
		errExpr := "UUID index is not applicable with 'int' field, only with 'string'"
		assert.EqualError(t, DB.OpenNamespace(TestByteItemWithUuidTagNs, nsOpts,
			TestByteItemWithUuidTagStruct{}), errExpr)
	})

	t.Run("cant open ns when bool field with uuid tag", func(t *testing.T) {
		errExpr := "UUID index is not applicable with 'bool' field, only with 'string'"
		assert.EqualError(t, DB.OpenNamespace(TestBoolItemWithUuidTagNs, nsOpts,
			TestBoolItemWithUuidTagStruct{}), errExpr)
	})
}

func TestUuidClientCproto(t *testing.T) {

	ns := "test_uuid_cproto_connect"

	t.Run("test add uuid index on non-indexed string", func(t *testing.T) {
		rx1 := configureAndStartServer("0:29188", "0:26634", "/tmp/rx_uuid1")
		defer rx1.Close()
		require.NoError(t, rx1.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), &TestUuidStructNoIdx{}))

		for i := 0; i < 50; i++ {
			testItem := TestUuidStructNoIdx{ID: i, Uuid: randUuid()}
			err := rx1.Upsert(ns, &testItem)
			require.NoError(t, err)
		}
		item := TestUuidStructNoIdx{ID: 50, Uuid: randUuid()}
		rx1.Upsert(ns, item)

		rx2 := reindexer.NewReindex("cproto://127.0.0.1:26634/uudb")
		defer rx2.Close()
		require.NoError(t, rx2.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), &TestUuidStructNoIdx{}))

		it1 := rx2.Query(ns).Where("uuid", reindexer.EQ, item.Uuid).MustExec()
		defer it1.Close()
		assert.Equal(t, 1, it1.Count())
		for it1.Next() {
			assert.EqualValues(t, it1.Object(), &item)
		}

		rx3 := reindexer.NewReindex("cproto://127.0.0.1:26634/uudb")
		defer rx3.Close()
		require.NoError(t, rx3.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), &TestUuidStructNoIdx{}))

		// add uuid index on string field
		err := rx3.AddIndex(ns, reindexer.IndexDef{
			Name: "uuid", JSONPaths: []string{"uuid"}, IndexType: "hash", FieldType: "uuid"})
		require.NoError(t, err)

		// make select with same filter
		it2 := rx2.Query(ns).Where("uuid", reindexer.EQ, item.Uuid).MustExec()
		defer it2.Close()
		assert.Equal(t, it2.Count(), 1)
		for it2.Next() {
			assert.EqualValues(t, it2.Object(), &item)
		}

		// add new item
		item = TestUuidStructNoIdx{ID: 51, Uuid: randUuid()}
		rx3.Upsert(ns, item)

		// make select with new item, uuid index must be used
		it3 := rx2.Query(ns).Where("uuid", reindexer.EQ, item.Uuid).Explain().MustExec()
		defer it3.Close()
		checkExplainSelect(t, *it3, &item)
	})

	t.Run("test update index from string to uuid", func(t *testing.T) {
		rx1 := configureAndStartServer("0:29188", "0:26634", "/tmp/rx_uuid2")
		defer rx1.Close()
		require.NoError(t, rx1.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), &TestUuidStructNoTag{}))

		for i := 0; i < 50; i++ {
			testItem := TestUuidStructNoTag{ID: i, Uuid: randUuid()}
			err := rx1.Upsert(ns, &testItem)
			require.NoError(t, err)
		}
		item := TestUuidStructNoTag{ID: 50, Uuid: randUuid()}
		rx1.Upsert(ns, item)

		rx2 := reindexer.NewReindex("cproto://127.0.0.1:26634/uudb")
		defer rx2.Close()
		require.NoError(t, rx2.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), &TestUuidStructNoTag{}))

		it1 := rx2.Query(ns).Where("uuid", reindexer.EQ, item.Uuid).MustExec()
		defer it1.Close()
		assert.Equal(t, 1, it1.Count())
		for it1.Next() {
			assert.EqualValues(t, it1.Object(), &item)
		}

		rx3 := reindexer.NewReindex("cproto://127.0.0.1:26634/uudb")
		defer rx3.Close()
		require.NoError(t, rx3.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), &TestUuidStructNoTag{}))

		// update index field type from string to uuid
		err := rx3.UpdateIndex(ns, reindexer.IndexDef{
			Name: "uuid", JSONPaths: []string{"uuid"}, IndexType: "hash", FieldType: "uuid"})
		require.NoError(t, err)

		// make select with same filter
		it2 := rx2.Query(ns).Where("uuid", reindexer.EQ, item.Uuid).Explain().MustExec()
		defer it2.Close()
		checkExplainSelect(t, *it2, &item)

		// add new item
		item = TestUuidStructNoTag{ID: 51, Uuid: randUuid()}
		rx3.Upsert(ns, item)

		// make select with new item, uuid index must be used
		it3 := rx2.Query(ns).Where("uuid", reindexer.EQ, item.Uuid).Explain().MustExec()
		defer it3.Close()
		checkExplainSelect(t, *it3, &item)
	})
}

func TestUuidClientBuiltinserver(t *testing.T) {

	ns := "test_uuid_builtinserver_connect"

	t.Run("test add uuid index on non-indexed string", func(t *testing.T) {
		rx1 := configureAndStartServer("0:29188", "0:26634", "/tmp/rx_uuid11")
		defer rx1.Close()
		rx2 := configureAndStartServer("0:29189", "0:26635", "/tmp/rx_uuid12")
		defer rx2.Close()
		helpers.ConfigureReplication(t, rx1, "leader", nil, []reindexer.DBAsyncReplicationNode{{DSN: "cproto://127.0.0.1:26635/uudb", Namespaces: nil}})
		helpers.ConfigureReplication(t, rx2, "follower", nil, nil)

		nsOption := reindexer.DefaultNamespaceOptions()
		nsOption.NoStorage()

		err := rx1.OpenNamespace(ns, nsOption, TestUuidStructNoIdx{})
		require.NoError(t, err)
		err = rx2.OpenNamespace(ns, nsOption, TestUuidStructNoIdx{})
		require.NoError(t, err)

		helpers.WaitForSyncWithMaster(t, rx1, rx2)
		for i := 0; i < 50; i++ {
			testItem := TestUuidStructNoIdx{ID: i, Uuid: randUuid()}
			err := rx1.Upsert(ns, &testItem)
			require.NoError(t, err)
		}
		item := TestUuidStructNoIdx{ID: 50, Uuid: randUuid()}
		err = rx1.Upsert(ns, item)
		require.NoError(t, err)
		helpers.WaitForSyncWithMaster(t, rx1, rx2)

		it1 := rx2.Query(ns).Where("uuid", reindexer.EQ, item.Uuid).MustExec()
		defer it1.Close()
		require.NoError(t, it1.Error())
		assert.Equal(t, 1, it1.Count())
		for it1.Next() {
			require.NoError(t, it1.Error())
			assert.EqualValues(t, it1.Object(), &item)
		}

		rx3 := reindexer.NewReindex("cproto://127.0.0.1:26634/uudb")
		defer rx3.Close()
		require.NoError(t, rx3.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), &TestUuidStructNoIdx{}))

		// add uuid index on string field
		err = rx3.AddIndex(ns, reindexer.IndexDef{
			Name: "uuid", JSONPaths: []string{"uuid"}, IndexType: "hash", FieldType: "uuid"})
		require.NoError(t, err)
		helpers.WaitForSyncWithMaster(t, rx1, rx2)

		// make select with same filter
		it2 := rx2.Query(ns).Where("uuid", reindexer.EQ, item.Uuid).MustExec()
		defer it2.Close()
		require.NoError(t, it2.Error())
		assert.Equal(t, it2.Count(), 1)
		for it2.Next() {
			require.NoError(t, it2.Error())
			assert.EqualValues(t, it2.Object(), &item)
		}

		// add new item
		item = TestUuidStructNoIdx{ID: 51, Uuid: randUuid()}
		err = rx3.Upsert(ns, item)
		require.NoError(t, err)
		helpers.WaitForSyncWithMaster(t, rx1, rx2)

		// make select with new item, uuid index must be used
		it3 := rx2.Query(ns).Where("uuid", reindexer.EQ, item.Uuid).Explain().MustExec()
		require.NoError(t, it3.Error())
		defer it3.Close()
		checkExplainSelect(t, *it3, &item)
	})

	t.Run("test update index from string to uuid", func(t *testing.T) {
		rx1 := configureAndStartServer("0:29188", "0:26634", "/tmp/rx_uuid21")
		defer rx1.Close()
		rx2 := configureAndStartServer("0:29189", "0:26635", "/tmp/rx_uuid22")
		defer rx2.Close()
		helpers.ConfigureReplication(t, rx1, "leader", nil, []reindexer.DBAsyncReplicationNode{{DSN: "cproto://127.0.0.1:26635/uudb", Namespaces: nil}})
		helpers.ConfigureReplication(t, rx2, "follower", nil, nil)

		nsOption := reindexer.DefaultNamespaceOptions()
		nsOption.NoStorage()

		err := rx1.OpenNamespace(ns, nsOption, TestUuidStructNoTag{})
		require.NoError(t, err)
		err = rx2.OpenNamespace(ns, nsOption, TestUuidStructNoTag{})
		require.NoError(t, err)

		for i := 0; i < 50; i++ {
			testItem := TestUuidStructNoTag{ID: i, Uuid: randUuid()}
			err := rx1.Upsert(ns, &testItem)
			require.NoError(t, err)
		}
		item := TestUuidStructNoTag{ID: 50, Uuid: randUuid()}
		err = rx1.Upsert(ns, item)
		require.NoError(t, err)
		helpers.WaitForSyncWithMaster(t, rx1, rx2)

		it1 := rx2.Query(ns).Where("uuid", reindexer.EQ, item.Uuid).MustExec()
		defer it1.Close()
		require.NoError(t, it1.Error())
		assert.Equal(t, 1, it1.Count())
		for it1.Next() {
			require.NoError(t, it1.Error())
			assert.EqualValues(t, it1.Object(), &item)
		}

		rx3 := reindexer.NewReindex("cproto://127.0.0.1:26634/uudb")
		defer rx3.Close()
		require.NoError(t, rx3.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), &TestUuidStructNoTag{}))

		// update index field type from string to uuid
		err = rx3.UpdateIndex(ns, reindexer.IndexDef{
			Name: "uuid", JSONPaths: []string{"uuid"}, IndexType: "hash", FieldType: "uuid"})
		require.NoError(t, err)
		helpers.WaitForSyncWithMaster(t, rx1, rx2)

		// make select with same filter
		it2 := rx2.Query(ns).Where("uuid", reindexer.EQ, item.Uuid).Explain().MustExec()
		defer it2.Close()
		require.NoError(t, it2.Error())
		checkExplainSelect(t, *it2, &item)

		// add new item
		item = TestUuidStructNoTag{ID: 51, Uuid: randUuid()}
		err = rx3.Upsert(ns, item)
		require.NoError(t, err)
		helpers.WaitForSyncWithMaster(t, rx1, rx2)

		it3 := rx2.Query(ns).Where("uuid", reindexer.EQ, item.Uuid).Explain().MustExec()
		defer it3.Close()
		require.NoError(t, it3.Error())
		checkExplainSelect(t, *it3, &item)
	})
}
