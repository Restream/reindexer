//go:build embedding_test

package reindexer

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/bindings"
)

type TestEmbedItemHNWS struct {
	ID    int                                `reindex:"id,,pk"`
	Name  string                             `reindex:"name,text" json:"name"`
	Value int64                              `reindex:"value,hash" json:"value"`
	Vec   [kTestFloatVectorDimension]float32 `json:"vec"`
}

type TestEmbedItemIvf struct {
	ID  int                                `reindex:"id,,pk"`
	Vec [kTestFloatVectorDimension]float32 `json:"vec"`
}

const (
	testHNWSEmbedNs = "test_embedding_hnws"
	testIvfEmbedNs  = "test_embedding_ivf"
)

func init() {
	tnamespaces[testHNWSEmbedNs] = TestEmbedItemHNWS{}
	tnamespaces[testIvfEmbedNs] = TestEmbedItemIvf{}
}

func newTestEmbedItemHNWS(id int) interface{} {
	result := &TestEmbedItemHNWS{
		ID:    mkID(id),
		Name:  strconv.Itoa(id),
		Value: int64(id),
	}
	return result
}

func newTestEmbedItemIvf(id int, pkgsCount int) interface{} {
	result := &TestEmbedItemIvf{
		ID: mkID(id),
	}
	vect := randVect(kTestFloatVectorDimension)
	for i := 0; i < kTestFloatVectorDimension; i++ {
		result.Vec[i] = vect[i]
	}
	return result
}

func setEmbeddersConfig(t *testing.T) {
	nsConfig := make([]reindexer.DBEmbeddersConfig, 1)
	nsConfig[0].CacheTag = "*"
	nsConfig[0].MaxCacheItems = 10
	nsConfig[0].HitToCache = 0
	item := reindexer.DBConfigItem{
		Type:      "caches",
		Embedders: &nsConfig,
	}
	err := DB.Upsert(reindexer.ConfigNamespaceName, item)
	require.NoError(t, err)
}

func TestEmbedUpsertKnnIndex(t *testing.T) {
	setEmbeddersConfig(t)

	connectConfig := &bindings.EmbedderConnectionPoolConfig{
		Connections:    3,
		ConnectTimeout: 500,
		ReadTimeout:    500,
		WriteTimeout:   500,
	}
	embedderConfig := &bindings.EmbedderConfig{
		URL:                  "http://127.0.0.1:8000",
		Fields:               []string{"name", "value"},
		CacheTag:             "HNSW",
		EmbeddingStrategy:    "always",
		ConnectionPoolConfig: connectConfig,
	}
	embeddingConfig := &bindings.EmbeddingConfig{
		UpsertEmbedder: embedderConfig,
	}
	hnswSTOpts := reindexer.FloatVectorIndexOpts{
		Metric:             "inner_product",
		Dimension:          kTestFloatVectorDimension,
		M:                  8,
		EfConstruction:     100,
		StartSize:          256,
		MultithreadingMode: 1,
		EmbeddingConfig:    embeddingConfig,
	}
	indexDef := reindexer.IndexDef{
		Name:      "vec",
		JSONPaths: []string{"vec"},
		IndexType: "hnsw",
		FieldType: "float_vector",
		Config:    hnswSTOpts,
	}
	err := DB.AddIndex(testHNWSEmbedNs, indexDef)
	require.NoError(t, err)

	rand.Seed(time.Now().UnixNano())

	done := make(chan bool)
	wg := sync.WaitGroup{}
	writer := func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond * 1):
				if rand.Intn(2) > 0 {
					ctx, cancel := context.WithCancel(context.Background())
					err := DB.UpsertCtx(ctx, testHNWSEmbedNs, newTestEmbedItemHNWS(rand.Intn(1000000)))
					cancel()
					require.NoError(t, err)
				} else {
					tx, err := DB.BeginTx(testHNWSEmbedNs)
					require.NoError(t, err)
					itemCount := 15
					for i := 0; i < itemCount; i++ {
						err := tx.UpsertAsync(newTestEmbedItemHNWS(rand.Intn(1000000)), func(err error) {
							require.NoError(t, err)
						})
						require.NoError(t, err)
					}
					cnt, err := tx.CommitWithCount()
					require.Equal(t, cnt, itemCount)
					require.NoError(t, err)
				}
			}
		}
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go writer()
	}
	time.Sleep(time.Millisecond * 10000)
	close(done)
	wg.Wait()

	it1 := newTestQuery(DB, testHNWSEmbedNs).Where("vec", reindexer.EMPTY, nil).Exec(t)
	defer it1.Close()
	require.NoError(t, it1.Error())
	require.Equal(t, it1.Count(), 0)

	it2 := newTestQuery(DB, testHNWSEmbedNs).Exec(t)
	require.NoError(t, it2.Error())
	defer it2.Close()
	require.Greater(t, it2.Count(), 0)

	{
		item, err := DBD.Query(reindexer.PerfstatsNamespaceName).Where("name", reindexer.EQ, testHNWSEmbedNs).Exec().FetchOne()
		require.NoError(t, err)
		indexes := item.(*reindexer.NamespacePerfStat).Indexes
		for i := 0; i < len(indexes); i++ {
			if indexes[i].Name == "vec" {
				require.Greater(t, indexes[i].UpsertEmbedder.TotalQueriesCount, uint64(0))
				require.Greater(t, indexes[i].UpsertEmbedder.MaxLatencyUs, uint64(0))
				require.Greater(t, indexes[i].UpsertEmbedder.TotalAvgLatencyUs, uint64(0))
				require.Equal(t, indexes[i].UpsertEmbedder.TotalQueriesCount, indexes[i].UpsertEmbedder.TotalEmbedDocumentsCount)
			}
		}
	}
	{
		item, err := DBD.Query(reindexer.MemstatsNamespaceName).Where("name", reindexer.EQ, testHNWSEmbedNs).Exec().FetchOne()
		require.NoError(t, err)
		indexes := item.(*reindexer.NamespaceMemStat).Indexes
		for i := 0; i < len(indexes); i++ {
			if indexes[i].Name == "vec" {
				require.Equal(t, indexes[i].UpsertEmbedder.Status.LastRequestResult, "OK")
			}
		}
	}

}

func TestEmbedQueryKnnIndex(t *testing.T) {
	setEmbeddersConfig(t)

	connectConfig := &bindings.EmbedderConnectionPoolConfig{
		Connections:    3,
		ConnectTimeout: 500,
		ReadTimeout:    500,
		WriteTimeout:   500,
	}
	embedderConfig := &bindings.EmbedderConfig{
		URL:                  "http://127.0.0.1:8000",
		ConnectionPoolConfig: connectConfig,
		CacheTag:             "IVF",
	}
	embeddingConfig := &bindings.EmbeddingConfig{
		QueryEmbedder: embedderConfig,
	}
	ivfOpts := reindexer.FloatVectorIndexOpts{
		Metric:          "cosine",
		Dimension:       kTestFloatVectorDimension,
		CentroidsCount:  32,
		EmbeddingConfig: embeddingConfig,
	}
	indexDef := reindexer.IndexDef{
		Name:      "vec",
		JSONPaths: []string{"vec"},
		IndexType: "ivf",
		FieldType: "float_vector",
		Config:    ivfOpts,
	}
	err := DB.AddIndex(testIvfEmbedNs, indexDef)
	require.NoError(t, err)

	rand.Seed(time.Now().UnixNano())

	FillTestItemsWithFuncParts(testIvfEmbedNs, 0, kTestIVFFloatVectorMaxElements, kTestIVFFloatVectorMaxElements/10, 0, newTestEmbedItemIvf)

	done := make(chan bool)
	wg := sync.WaitGroup{}
	writer := func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond * 1):
				ivfSearchParams, err := reindexer.NewIndexIvfSearchParam(10, reindexer.BaseKnnSearchParam{}.SetK(1000))
				require.NoError(t, err)

				rand.Seed(time.Now().UnixNano())
				searchText := strconv.Itoa(rand.Int())
				// do it twice to touch cache
				for i := 0; i < 2; i++ {
					it := newTestQuery(DB, testIvfEmbedNs).WhereKnnString("vec", searchText, ivfSearchParams).Exec(t)
					defer it.Close()
					require.NoError(t, it.Error())
					require.Greater(t, it.Count(), 0)
				}
			}
		}
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go writer()
	}
	time.Sleep(time.Millisecond * 10000)
	close(done)
	wg.Wait()

	item, err := DBD.Query(reindexer.PerfstatsNamespaceName).Where("name", reindexer.EQ, testIvfEmbedNs).Exec().FetchOne()
	require.NoError(t, err)
	indexes := item.(*reindexer.NamespacePerfStat).Indexes
	for i := 0; i < len(indexes); i++ {
		if indexes[i].Name == "vec" {
			require.Greater(t, indexes[i].QueryEmbedder.TotalQueriesCount, uint64(0))
			require.Greater(t, indexes[i].QueryEmbedder.MaxLatencyUs, uint64(0))
			require.Greater(t, indexes[i].QueryEmbedder.TotalAvgLatencyUs, uint64(0))
			require.Equal(t, indexes[i].QueryEmbedder.TotalQueriesCount, indexes[i].QueryEmbedder.TotalEmbedDocumentsCount)
		}
	}
}
