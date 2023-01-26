package reindexer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v4"
)

func init() {
	tnamespaces["test_namespace_cache_items"] = NamespaceCacheItem{}
	tnamespaces["test_namespace_no_limit_cache_items"] = NamespaceCacheItem{}
	tnamespaces["test_namespace_disabled_obj_cache"] = NamespaceCacheItem{}
}

const CacheSize = 200

type NamespaceCacheItem struct {
	ID int `json:"id" reindex:"id,,pk"`
}

func (item *NamespaceCacheItem) DeepCopy() interface{} {
	return &NamespaceCacheItem{}
}

func prepareCacheItems(t *testing.T, namespaceName string, opts *reindexer.NamespaceOptions) {
	ns := namespaceName
	item := NamespaceCacheItem{}
	DBD.CloseNamespace(ns)
	err := DBD.OpenNamespace(ns, opts, NamespaceCacheItem{})
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
	prepareCacheItems(t, "test_namespace_cache_items", opts)
	require.Equal(t, int64(CacheSize), DBD.Status().Cache.CurSize)
	DBD.ResetCaches()
	require.Equal(t, int64(0), DBD.Status().Cache.CurSize)
}

func TestNamespaceCacheItemsWithoutObjCacheSize(t *testing.T) {
	opts := reindexer.DefaultNamespaceOptions()
	prepareCacheItems(t, "test_namespace_no_limit_cache_items", opts)
	require.Equal(t, int64(CacheSize+1000), DBD.Status().Cache.CurSize)
	DBD.ResetCaches()
	require.Equal(t, int64(0), DBD.Status().Cache.CurSize)
}

func TestNamespaceCacheItemsWithConfDisableObjCache(t *testing.T) {
	opts := reindexer.DefaultNamespaceOptions().DisableObjCache()
	prepareCacheItems(t, "test_namespace_disabled_obj_cache", opts)
	require.Equal(t, int64(0), DBD.Status().Cache.CurSize)
}
