package reindexer

import (
	"math/rand"
	"testing"

	"github.com/restream/reindexer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestCompositeSubstitutionStruct struct {
	ID      int `reindex:"id,,pk"`
	First1  int `reindex:"first1,-" json:"first1"`
	First2  int `reindex:"first2,-" json:"first2"`
	Second1 int `reindex:"second1,-" json:"second1"`
	Second2 int `reindex:"second2,-" json:"second2"`
	Third   int `reindex:"third,-" json:"third"`

	_ struct{} `reindex:"first1+first2,,composite"`
	_ struct{} `reindex:"second1+second2,,composite"`
	_ struct{} `reindex:"first1+third,,composite"`
}

type TestItemCompositeNoSubstitution struct {
	ID     int      `reindex:"id,hash,pk"`
	First  int      `reindex:"first,-"`
	Second int      `json:"second,-"`
	_      struct{} `reindex:"id+second,,composite"`
}

type TestItemPkCompositeNoSubstitution struct {
	ID     int      `reindex:"id,-"`
	First  int      `reindex:"first,-"`
	Second int      `json:"second,-"`
	_      struct{} `reindex:"first+second,,composite,pk"`
}

const testCompositeIndexesSubstitutionNs = "test_composite_indexes_substitution"
const testCompositeIndexesNoSubstitutionNs = "test_composite_indexes_no_substitution"
const testPkCompositeIndexesNoSubstitutionNs = "test_pk_composite_indexes_no_substitution"

func init() {
	tnamespaces[testCompositeIndexesSubstitutionNs] = TestCompositeSubstitutionStruct{}
	tnamespaces[testCompositeIndexesNoSubstitutionNs] = TestItemCompositeNoSubstitution{}
	tnamespaces[testPkCompositeIndexesNoSubstitutionNs] = TestItemPkCompositeNoSubstitution{}
}

func TestCompositeIndexesSubstitution(t *testing.T) {
	t.Parallel()

	const ns = testCompositeIndexesSubstitutionNs
	item := TestCompositeSubstitutionStruct{
		ID: rand.Intn(100), First1: rand.Intn(1000), First2: rand.Intn(1000), Second1: rand.Intn(1000), Second2: rand.Intn(1000), Third: rand.Intn(1000),
	}

	err := DB.Upsert(ns, item)
	require.NoError(t, err)

	t.Run("basic substitution", func(t *testing.T) {
		it := DB.Query(ns).Where("first1", reindexer.EQ, item.First1).Where("first2", reindexer.EQ, item.First2).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first1+first2",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
		}, "")
	})

	t.Run("basic substitution of the second crossing idx", func(t *testing.T) {
		it := DB.Query(ns).Where("first1", reindexer.EQ, item.First1).Where("third", reindexer.EQ, item.Third).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first1+third",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
		}, "")
	})

	t.Run("reversed basic substitution", func(t *testing.T) {
		it := DB.Query(ns).Where("first2", reindexer.EQ, item.First2).Where("first1", reindexer.EQ, item.First1).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first1+first2",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
		}, "")
	})

	t.Run("reversed basic substitution with extra idx in the midle", func(t *testing.T) {
		// No substitution here yet
		it := DB.Query(ns).Where("first2", reindexer.EQ, item.First2).Where("id", reindexer.EQ, item.ID).Where("first1", reindexer.EQ, item.First1).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "id",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:       "first2",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "first1",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
		}, "")
	})

	t.Run("reversed basic substitution with extra idxs on the beginning", func(t *testing.T) {
		it := DB.Query(ns).Where("id", reindexer.EQ, item.ID).Where("first2", reindexer.EQ, item.First2).Where("first1", reindexer.EQ, item.First1).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "id",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:   "first1+first2",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
		}, "")
	})

	t.Run("reversed basic substitution with extra idxs in the end", func(t *testing.T) {
		it := DB.Query(ns).Where("first2", reindexer.EQ, item.First2).Where("first1", reindexer.EQ, item.First1).Where("id", reindexer.EQ, item.ID).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first1+first2",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:   "id",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
		}, "")
	})

	t.Run("substitution with another composite idx", func(t *testing.T) {
		it := DB.Query(ns).
			Where("second2", reindexer.EQ, item.Second2).
			Where("first2", reindexer.EQ, item.First2).
			Where("first1", reindexer.EQ, item.First1).
			Where("second1", reindexer.EQ, item.Second1).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first1+first2",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:       "second2",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "second1",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
		}, "")
	})

	t.Run("multiple indexes substitution", func(t *testing.T) {
		it := DB.Query(ns).Where("id", reindexer.EQ, item.ID).Where("first2", reindexer.EQ, item.First2).Where("first1", reindexer.EQ, item.First1).
			Where("second1", reindexer.EQ, item.Second1).Where("second2", reindexer.EQ, item.Second2).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "id",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:   "first1+first2",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:   "second1+second2",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
		}, "")
	})

	t.Run("multiple indexes substitution with id in the middle", func(t *testing.T) {
		it := DB.Query(ns).Where("first2", reindexer.EQ, item.First2).Where("first1", reindexer.EQ, item.First1).
			Where("id", reindexer.EQ, item.ID).
			Where("second1", reindexer.EQ, item.Second1).Where("second2", reindexer.EQ, item.Second2).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first1+first2",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:   "id",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:   "second1+second2",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
		}, "")
	})

	t.Run("no substitution with OR", func(t *testing.T) {
		// Expecting no index substitution with OR condition
		it := DB.Query(ns).Where("first2", reindexer.EQ, item.First2).Or().Where("first1", reindexer.EQ, item.First1).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "-scan",
				Method:  "scan",
				Matched: 1,
			},
			{
				Field:       "first2 or first1",
				Method:      "scan",
				Comparators: 2,
				Matched:     1,
			},
		}, "")
	})

	t.Run("no substitution with OR and mixed indexes in brackets", func(t *testing.T) {
		// Expecting no index substitution with OR condition, when index parts are distributed between brackets
		it := DB.Query(ns).
			OpenBracket().
			Where("first2", reindexer.EQ, item.First2).Where("second2", reindexer.EQ, item.Second2).
			CloseBracket().
			Or().
			OpenBracket().
			Where("second1", reindexer.EQ, item.Second1).Where("first1", reindexer.EQ, item.First1).
			CloseBracket().Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "-scan",
				Method:  "scan",
				Matched: 1,
			},
			{
				Field: "(first2 and second2)",
				Selectors: []expectedExplain{
					{
						Field:       "first2",
						Method:      "scan",
						Comparators: 1,
						Matched:     1,
					},
					{
						Field:       "second2",
						Method:      "scan",
						Comparators: 1,
						Matched:     1,
					},
				},
			},
			{
				Field: "or (second1 and first1)",
				Selectors: []expectedExplain{
					{
						Field:       "second1",
						Method:      "scan",
						Comparators: 1,
						Matched:     0,
					},
					{
						Field:       "first1",
						Method:      "scan",
						Comparators: 1,
						Matched:     0,
					},
				},
			},
		}, "")
	})

	t.Run("substitution with OR and separated multiple indexes in brackets", func(t *testing.T) {
		it := DB.Query(ns).
			OpenBracket().
			Where("id", reindexer.EQ, item.ID).
			Where("first1", reindexer.EQ, item.First1).
			Where("first2", reindexer.EQ, item.First2).
			CloseBracket().
			Or().
			OpenBracket().
			Where("second2", reindexer.EQ, item.Second2).
			Where("second1", reindexer.EQ, item.Second1).
			Where("id", reindexer.EQ, item.ID).
			CloseBracket().Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "-scan",
				Method:  "scan",
				Matched: 1,
			},
			{
				Field: "(id and first1+first2)",
				Selectors: []expectedExplain{
					{
						Field:   "id",
						Method:  "index",
						Keys:    1,
						Matched: 1,
					},
					{
						Field:   "first1+first2",
						Method:  "index",
						Keys:    1,
						Matched: 1,
					},
				},
			},
			{
				Field: "or (second1+second2 and id)",
				Selectors: []expectedExplain{
					{
						Field:   "second1+second2",
						Method:  "index",
						Keys:    1,
						Matched: 0,
					},
					{
						Field:   "id",
						Method:  "index",
						Keys:    1,
						Matched: 0,
					},
				},
			},
		}, "")
	})
}

func TestCompositeIndexNoSubstitutionWithoutSomeIndexes(t *testing.T) {
	t.Parallel()

	const ns = testCompositeIndexesNoSubstitutionNs
	item := TestItemCompositeNoSubstitution{
		ID: rand.Intn(100), First: rand.Intn(1000), Second: rand.Intn(1000),
	}

	err := DB.Upsert(ns, item)
	require.NoError(t, err)

	t.Run("no substitution idx and not idxed composited field", func(t *testing.T) {
		it := DB.Query(ns).Where("first", reindexer.EQ, item.First).Where("second", reindexer.EQ, item.Second).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "-scan",
				Method:  "scan",
				Keys:    0,
				Matched: 1,
			},
			{
				Field:       "first",
				Method:      "scan",
				Keys:        0,
				Matched:     1,
				Comparators: 1,
			},
			{
				Field:       "second",
				Method:      "scan",
				Keys:        0,
				Matched:     1,
				Comparators: 1,
			},
		}, "")
	})

	t.Run("no substitution composited idx and not idxed composited field", func(t *testing.T) {
		it := DB.Query(ns).Where("id", reindexer.EQ, item.ID).Where("second", reindexer.EQ, item.Second).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:       "id",
				Method:      "index",
				Keys:        1,
				Matched:     1,
				Comparators: 0,
			},
			{
				Field:       "second",
				Method:      "scan",
				Keys:        0,
				Matched:     1,
				Comparators: 1,
			},
		}, "")
	})

	t.Run("no substitution composited idx and not composited idx", func(t *testing.T) {
		it := DB.Query(ns).Where("id", reindexer.EQ, item.ID).Where("first", reindexer.EQ, item.First).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:       "id",
				Method:      "index",
				Keys:        1,
				Matched:     1,
				Comparators: 0,
			},
			{
				Field:       "first",
				Method:      "scan",
				Keys:        0,
				Matched:     1,
				Comparators: 1,
			},
		}, "")
	})
}

func TestPkCompositeIndexNoSubstitutionWithoutSomeIndexes(t *testing.T) {
	t.Parallel()

	const ns = testPkCompositeIndexesNoSubstitutionNs
	item := TestItemPkCompositeNoSubstitution{
		ID: rand.Intn(100), First: rand.Intn(1000), Second: rand.Intn(1000),
	}

	err := DB.Upsert(ns, item)
	require.NoError(t, err)

	t.Run("no substitution composited idx and not idxed composited field", func(t *testing.T) {
		it := DB.Query(ns).Where("first", reindexer.EQ, item.First).Where("second", reindexer.EQ, item.Second).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "-scan",
				Method:  "scan",
				Keys:    0,
				Matched: 1,
			},
			{
				Field:       "first",
				Method:      "scan",
				Keys:        0,
				Matched:     1,
				Comparators: 1,
			},
			{
				Field:       "second",
				Method:      "scan",
				Keys:        0,
				Matched:     1,
				Comparators: 1,
			},
		}, "")
	})

	t.Run("no substitution idx and not idxed composited field", func(t *testing.T) {
		it := DB.Query(ns).Where("id", reindexer.EQ, item.ID).Where("second", reindexer.EQ, item.Second).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "-scan",
				Method:  "scan",
				Keys:    0,
				Matched: 1,
			},
			{
				Field:       "id",
				Method:      "scan",
				Keys:        0,
				Matched:     1,
				Comparators: 1,
			},
			{
				Field:       "second",
				Method:      "scan",
				Keys:        0,
				Matched:     1,
				Comparators: 1,
			},
		}, "")
	})

	t.Run("no substitution idx and composited idx", func(t *testing.T) {
		it := DB.Query(ns).Where("id", reindexer.EQ, item.ID).Where("first", reindexer.EQ, item.First).Explain().Exec(t)
		defer it.Close()
		assert.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "-scan",
				Method:  "scan",
				Keys:    0,
				Matched: 1,
			},
			{
				Field:       "id",
				Method:      "scan",
				Keys:        0,
				Matched:     1,
				Comparators: 1,
			},
			{
				Field:       "first",
				Method:      "scan",
				Keys:        0,
				Matched:     1,
				Comparators: 1,
			},
		}, "")
	})
}
