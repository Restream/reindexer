package reindexer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v5"
)

type TestItemPk struct {
	ID int `json:"id" reindex:"id,,pk"`
}

const CacheSize = 200

const (
	testCacheItemsNs        = "test_namespace_cache_items"
	testNoLimitCacheItemsNs = "test_namespace_no_limit_cache_items"
	testDisabledObjCacheNs  = "test_namespace_disabled_obj_cache"
)

func init() {
	tnamespaces[testCacheItemsNs] = TestItemPk{}
	tnamespaces[testNoLimitCacheItemsNs] = TestItemPk{}
	tnamespaces[testDisabledObjCacheNs] = TestItemPk{}
}

func (item *TestItemPk) DeepCopy() interface{} {
	return &TestItemPk{}
}

func prepareCacheItems(t *testing.T, ns string, opts *reindexer.NamespaceOptions) {
	item := TestItemPk{}
	DBD.CloseNamespace(ns)
	err := DBD.OpenNamespace(ns, opts, TestItemPk{})
	require.NoError(t, err)

	// fill obj cache > max count
	for i := 0; i < CacheSize+1000; i++ {
		itemIns := item
		itemIns.ID = i + 2
		n, err := DBD.Insert(ns, itemIns)
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}

	items, err := DBD.Query(ns).Exec().FetchAll()
	require.NoError(t, err)
	require.Equal(t, 1200, len(items))
}

func TestNamespaceCacheItemsWithObjCacheSize(t *testing.T) {
	opts := reindexer.DefaultNamespaceOptions().ObjCacheSize(CacheSize)
	prepareCacheItems(t, testCacheItemsNs, opts)
	require.Equal(t, int64(CacheSize), DBD.Status().Cache.CurSize)
	DBD.ResetCaches()
	require.Equal(t, int64(0), DBD.Status().Cache.CurSize)
}

func TestNamespaceCacheItemsWithoutObjCacheSize(t *testing.T) {
	opts := reindexer.DefaultNamespaceOptions()
	prepareCacheItems(t, testNoLimitCacheItemsNs, opts)
	require.Equal(t, int64(CacheSize+1000), DBD.Status().Cache.CurSize)
	DBD.ResetCaches()
	require.Equal(t, int64(0), DBD.Status().Cache.CurSize)
}

func TestNamespaceCacheItemsWithConfDisableObjCache(t *testing.T) {
	opts := reindexer.DefaultNamespaceOptions().DisableObjCache()
	prepareCacheItems(t, testDisabledObjCacheNs, opts)
	require.Equal(t, int64(0), DBD.Status().Cache.CurSize)
}
