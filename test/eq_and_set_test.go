package reindexer

import (
	"math/rand"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestItemJoinInto struct {
	ID             int               `json:"id" reindex:"id,,pk"`
	TestItemJoined []*TestItemJoined `json:"test_item_joined" reindex:"test_item_joined,,joined"`
}

type TestItemJoined struct {
	ID     int    `reindex:"id,,pk"`
	Name   string `reindex:"name"`
	JoinID int    `reindex:"join_id"`
}

type TestItemWithouIndexes struct {
	ID   int    `reindex:"id,,pk"`
	Name string `json:"name"`
	Year int    `json:"year"`
}

const (
	testNumberOfArgumentsInSetAndEqNs   = "test_items_set_eq_number"
	testEmptySetAndEqNs                 = "test_items_empty_set_eq"
	testEmptySetAndEqWithoutIdxNs       = "test_items_not_indexed"
	testEmptySetAndEqWithCompositeIdxNs = "test_items_set_eq_composite_idx"
	testJoinOnSetAndEqNs1               = "test_items_join_into"
	testJoinOnSetAndEqNs2               = "test_items_joined"
)

func init() {
	tnamespaces[testNumberOfArgumentsInSetAndEqNs] = TestItemSimple{}
	tnamespaces[testEmptySetAndEqNs] = TestItemSimple{}
	tnamespaces[testEmptySetAndEqWithoutIdxNs] = TestItemWithouIndexes{}
	tnamespaces[testEmptySetAndEqWithCompositeIdxNs] = TestItemCmplxPK{}
	tnamespaces[testJoinOnSetAndEqNs1] = TestItemJoinInto{}
	tnamespaces[testJoinOnSetAndEqNs2] = TestItemJoined{}
}

func generateRandomNumberIds(maxCount int) []int {
	ids := make([]int, 1)
	for i := 0; i < rand.Intn(maxCount)+1; i++ {
		id := rand.Intn(1000)
		ids = append(ids, id)
	}
	return ids
}

func TestNumberOfArgumentsInSetAndEq(t *testing.T) {
	t.Parallel()

	const ns = testNumberOfArgumentsInSetAndEqNs
	testItem := TestItemSimple{ID: rand.Intn(1000), Name: randString()}

	err := DB.Upsert(ns, testItem)
	require.NoError(t, err)

	ids := generateRandomNumberIds(5)
	ids = append(ids, testItem.ID)

	t.Run("eq can take nil", func(t *testing.T) {
		query := DBD.Query(ns).Where("name", reindexer.EQ, nil)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("eq can take 0 arguments", func(t *testing.T) {
		query := DBD.Query(ns).WhereString("name", reindexer.EQ)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("eq can take more than 1 arguments", func(t *testing.T) {
		query := DBD.Query(ns).WhereInt("id", reindexer.EQ, ids...)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.NotEmpty(t, result)
	})

	t.Run("set can take nil", func(t *testing.T) {
		query := DBD.Query(ns).Where("name", reindexer.SET, nil)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("set can take 0 arguments", func(t *testing.T) {
		query := DBD.Query(ns).WhereInt("id", reindexer.SET)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("set can take more than 1 arguments", func(t *testing.T) {
		query := DBD.Query(ns).WhereInt("id", reindexer.SET, ids...)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.NotEmpty(t, result)
	})
}

func TestEmptySetAndEq(t *testing.T) {
	t.Parallel()

	const ns = testEmptySetAndEqNs
	testItem := TestItemSimple{ID: rand.Intn(1000), Name: randString()}

	err := DB.Upsert(ns, testItem)
	require.NoError(t, err)

	t.Run("eq and empty set in where condition", func(t *testing.T) {
		query := DBD.Query(ns).
			WhereString("name", reindexer.EQ, testItem.Name).
			WhereInt("year", reindexer.SET)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("set and empty eq in where condition", func(t *testing.T) {
		query := DBD.Query(ns).
			WhereString("name", reindexer.EQ).
			WhereInt("year", reindexer.SET, testItem.Year)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("empty set and eq in where condition", func(t *testing.T) {
		query := DBD.Query(ns).
			WhereString("name", reindexer.EQ).
			WhereInt("year", reindexer.SET)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestEmptySetAndEqWithoutIdx(t *testing.T) {
	t.Parallel()

	const ns = testEmptySetAndEqWithoutIdxNs
	testItem := TestItemWithouIndexes{ID: rand.Intn(1000), Name: randString(), Year: rand.Intn(1000)}

	err := DB.Upsert(ns, testItem)
	require.NoError(t, err)

	t.Run("empty eq in where condition", func(t *testing.T) {
		query := DBD.Query(ns).
			WhereString("name", reindexer.EQ)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("empty set in where condition", func(t *testing.T) {
		query := DBD.Query(ns).
			WhereInt("year", reindexer.SET)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("eq and empty set in where condition", func(t *testing.T) {
		query := DBD.Query(ns).
			WhereString("name", reindexer.EQ, testItem.Name).
			WhereInt("year", reindexer.SET)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("set and empty eq in where condition", func(t *testing.T) {
		query := DBD.Query(ns).
			WhereString("name", reindexer.EQ).
			WhereInt("year", reindexer.SET, testItem.Year)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("empty set and eq in where condition", func(t *testing.T) {
		query := DBD.Query(ns).
			WhereString("name", reindexer.EQ).
			WhereInt("year", reindexer.SET)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestEmptySetAndEqWithCompositeIdx(t *testing.T) {
	t.Parallel()

	const ns = testEmptySetAndEqWithCompositeIdxNs
	testItem := TestItemCmplxPK{ID: rand.Int31(), SubID: randString()}
	err := DB.Upsert(ns, testItem)
	require.NoError(t, err)

	t.Run("empty eq in where condition", func(t *testing.T) {
		query := DBD.Query(ns).
			WhereString("subid", reindexer.EQ)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)

	})

	t.Run("empty set in where condition", func(t *testing.T) {
		query := DBD.Query(ns).
			WhereInt("id", reindexer.SET)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("eq and empty set in where condition", func(t *testing.T) {
		query := DBD.Query(ns).
			WhereString("subid", reindexer.EQ, testItem.SubID).
			WhereInt("id", reindexer.SET)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("set and empty eq in where condition", func(t *testing.T) {
		query := DBD.Query(ns).
			WhereString("subid", reindexer.EQ).
			WhereInt32("id", reindexer.SET, testItem.ID)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)

	})

	t.Run("empty set and eq in where condition", func(t *testing.T) {
		query := DBD.Query(ns).
			WhereString("subid", reindexer.EQ).
			WhereInt("id", reindexer.SET)
		result, err := query.Exec().FetchAll()
		require.NoError(t, err)
		assert.Nil(t, result)

	})
}

func TestJoinOnSetAndEq(t *testing.T) {
	t.Parallel()

	const (
		ns1 = testJoinOnSetAndEqNs1
		ns2 = testJoinOnSetAndEqNs2
	)

	joinId := rand.Intn(1000)
	err := DB.Upsert(ns1, TestItemJoinInto{ID: joinId})
	require.NoError(t, err)

	err = DB.Upsert(ns2, TestItemJoined{ID: rand.Intn(1000), Name: randString(), JoinID: joinId})
	require.NoError(t, err)

	t.Run("join on set and eq are the same", func(t *testing.T) {
		queryEq := DBD.Query(ns1).
			Join(DBD.Query(ns2), "test_item_joined").
			On("id", reindexer.EQ, "join_id")
		resultEq, err := queryEq.Exec().FetchAll()
		require.NoError(t, err)

		querySet := DBD.Query(ns1).
			Join(DBD.Query(ns2), "test_item_joined").
			On("id", reindexer.SET, "join_id")
		resultSet, err := querySet.Exec().FetchAll()
		require.NoError(t, err)

		assert.NotEmpty(t, resultEq)
		assert.EqualValues(t, resultEq, resultSet)
	})
}
