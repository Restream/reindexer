package reindexer

import (
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/require"
)

type TestItemStreamingKnn struct {
	ID         int                                `reindex:"id,,pk"`
	FilterBool bool                               `json:"filter_bool"`
	Vec        [kTestFloatVectorDimension]float32 `reindex:"vec,hnsw,m=16,ef_construction=200,start_size=500,metric=l2"`
}

const testStreamingKnnNs = "test_items_streaming_knn"

func init() {
	tnamespaces[testStreamingKnnNs] = TestItemStreamingKnn{}
}

func newTestItemStreamingKnn(id int, pkgsCount int) any {
	item := &TestItemStreamingKnn{
		ID:         id,
		FilterBool: id%2 == 0,
	}
	vec := randVect(kTestFloatVectorDimension)
	copy(item.Vec[:], vec)
	return item
}

func TestStreamingKnnHnsw(t *testing.T) {
	const kMaxElements = 500
	const kLimit = 10

	FillTestItemsWithFuncParts(testStreamingKnnNs, 0, kMaxElements, kMaxElements/10, 0, newTestItemStreamingKnn)

	vec := randVect(kTestFloatVectorDimension)
	streamParams, err := reindexer.NewIndexHnswSearchParam(1, reindexer.BaseKnnSearchParam{})
	require.NoError(t, err)

	refParams, err := reindexer.NewIndexHnswSearchParam(kMaxElements*2, reindexer.BaseKnnSearchParam{}.SetK(kMaxElements))
	require.NoError(t, err)

	refItems, refRanks, err := DB.GetBaseQuery(testStreamingKnnNs).
		Where("filter_bool", reindexer.EQ, true).
		WhereKnn("vec", vec, refParams).
		WithRank().
		Exec().
		FetchAllWithRank()
	require.NoError(t, err)
	require.NotEmpty(t, refItems)

	refRankByID := make(map[int]float32, len(refItems))
	for i, raw := range refItems {
		id := raw.(*TestItemStreamingKnn).ID
		refRankByID[id] = refRanks[i]
		if i > 0 {
			require.GreaterOrEqual(t, refRanks[i], refRanks[i-1])
		}
	}

	streamItems, streamRanks, err := DB.GetBaseQuery(testStreamingKnnNs).
		Where("filter_bool", reindexer.EQ, true).
		WhereKnn("vec", vec, streamParams).
		Limit(kLimit).
		WithRank().
		Exec().
		FetchAllWithRank()
	require.NoError(t, err)
	require.LessOrEqual(t, len(streamItems), kLimit)
	require.Equal(t, len(streamItems), min(len(refItems), kLimit))

	for i, raw := range streamItems {
		item := raw.(*TestItemStreamingKnn)
		refRank, ok := refRankByID[item.ID]
		require.True(t, ok, "streaming id %d must pass the post-filter", item.ID)
		require.Equal(t, refRank, streamRanks[i], "rank mismatch for id %d", item.ID)
		if i > 0 {
			require.GreaterOrEqual(t, streamRanks[i], streamRanks[i-1])
		}
	}

	const kOffset = 5
	streamOffsetItems, streamOffsetRanks, err := DB.GetBaseQuery(testStreamingKnnNs).
		Where("filter_bool", reindexer.EQ, true).
		WhereKnn("vec", vec, streamParams).
		Offset(kOffset).
		Limit(kLimit).
		WithRank().
		Exec().
		FetchAllWithRank()
	require.NoError(t, err)
	require.LessOrEqual(t, len(streamOffsetItems), kLimit)
	for i, raw := range streamOffsetItems {
		item := raw.(*TestItemStreamingKnn)
		refRank, ok := refRankByID[item.ID]
		require.True(t, ok, "offset streaming id %d must be in reference set", item.ID)
		require.Equal(t, refRank, streamOffsetRanks[i], "rank mismatch for id %d", item.ID)
		if i > 0 {
			require.GreaterOrEqual(t, streamOffsetRanks[i], streamOffsetRanks[i-1])
		}
	}
}
