package reindexer

import (
	"encoding/json"
	"log"
	"math/rand"
	"strconv"
	"testing"

	"github.com/restream/reindexer/v3"
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

type TestItemMultipleCompositeSubindexes struct {
	ID     int      `reindex:"id,hash,pk"`
	First  int      `reindex:"first,-"`
	Second int      `reindex:"second,-"`
	Third  int      `reindex:"third,-"`
	Fourth int      `reindex:"fourth,-"`
	_      struct{} `reindex:"first+second,,composite"`
	_      struct{} `reindex:"first+second+third,,composite"`
	_      struct{} `reindex:"first+second+third+fourth,,composite"`
}

type TestItemCompositesLimit struct {
	ID     int      `reindex:"id,hash,pk"`
	First  int      `reindex:"first,-"`
	Second int      `reindex:"second,-"`
	Third  int      `reindex:"third,-"`
	Fourth int      `reindex:"fourth,-"`
	_      struct{} `reindex:"first+second,,composite"`
	_      struct{} `reindex:"first+second+third,,composite"`
	_      struct{} `reindex:"first+second+fourth,,composite"`
}

const testCompositeIndexesSubstitutionNs = "test_composite_indexes_substitution"
const testMultipleCompositeSubindexes = "test_composite_indexes_multiple_subindexes"
const testCompositesLimit = "test_composites_limit"

func init() {
	tnamespaces[testCompositeIndexesSubstitutionNs] = TestCompositeSubstitutionStruct{}
	tnamespaces[testMultipleCompositeSubindexes] = TestItemMultipleCompositeSubindexes{}
	tnamespaces[testCompositesLimit] = TestItemCompositesLimit{}
}

func printExplainRes(res *reindexer.ExplainResults) {
	j, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}
	log.Println(string(j))
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
		it := DB.Query(ns).
			Where("first1", reindexer.EQ, item.First1).
			Where("first2", reindexer.EQ, item.First2).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first1+first2",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
		}, "")
	})

	t.Run("basic substitution with type conversion", func(t *testing.T) {
		it := DB.Query(ns).
			Where("first1", reindexer.EQ, strconv.Itoa(item.First1)).
			Where("first2", reindexer.EQ, strconv.Itoa(item.First2)).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
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
		it := DB.Query(ns).
			Where("first1", reindexer.EQ, item.First1).
			Where("third", reindexer.EQ, item.Third).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
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
		it := DB.Query(ns).
			Where("first2", reindexer.EQ, item.First2).
			Where("first1", reindexer.EQ, item.First1).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
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
		it := DB.Query(ns).
			Where("first2", reindexer.EQ, item.First2).
			Where("id", reindexer.EQ, item.ID).
			Where("first1", reindexer.EQ, item.First1).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
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

	t.Run("reversed basic substitution with extra idxs on the beginning", func(t *testing.T) {
		it := DB.Query(ns).
			Where("id", reindexer.EQ, item.ID).
			Where("first2", reindexer.EQ, item.First2).
			Where("first1", reindexer.EQ, item.First1).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
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
		it := DB.Query(ns).
			Where("first2", reindexer.EQ, item.First2).
			Where("first1", reindexer.EQ, item.First1).
			Where("id", reindexer.EQ, item.ID).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
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
			Where("second1", reindexer.EQ, item.Second1).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "second1+second2",
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

	t.Run("multiple indexes substitution", func(t *testing.T) {
		it := DB.Query(ns).
			Where("id", reindexer.EQ, item.ID).
			Where("first2", reindexer.EQ, item.First2).
			Where("first1", reindexer.EQ, item.First1).
			Where("second1", reindexer.EQ, item.Second1).
			Where("second2", reindexer.EQ, item.Second2).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
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
		it := DB.Query(ns).
			Where("first2", reindexer.EQ, item.First2).
			Where("first1", reindexer.EQ, item.First1).
			Where("id", reindexer.EQ, item.ID).
			Where("second1", reindexer.EQ, item.Second1).
			Where("second2", reindexer.EQ, item.Second2).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
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

	t.Run("multiple indexes substitution with id in the end", func(t *testing.T) {
		it := DB.Query(ns).
			Where("first2", reindexer.EQ, item.First2).
			Where("first1", reindexer.EQ, item.First1).
			Where("second1", reindexer.EQ, item.Second1).
			Where("second2", reindexer.EQ, item.Second2).
			Where("id", reindexer.EQ, item.ID).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
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
			{
				Field:   "id",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
		}, "")
	})

	t.Run("multiple indexes substitution with multiple conditions by indexes parts", func(t *testing.T) {
		it := DB.Query(ns).
			Where("second2", reindexer.EQ, item.Second2).
			Where("first2", reindexer.EQ, item.First2).
			Where("first1", reindexer.EQ, item.First1).
			Where("second1", reindexer.EQ, item.Second1).
			Where("first2", reindexer.EQ, item.First2).
			Where("second2", reindexer.EQ, item.Second2).
			Where("id", reindexer.EQ, item.ID).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "second1+second2",
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
				Field:   "id",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
		}, "")
	})

	t.Run("no substitution with OR", func(t *testing.T) {
		// Expecting no index substitution with OR condition
		it := DB.Query(ns).
			Where("first2", reindexer.EQ, item.First2).
			Or().
			Where("first1", reindexer.EQ, item.First1).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
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
			CloseBracket().
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
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
			CloseBracket().
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
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

	t.Run("substitution with OR and separated multiple indexes brackets", func(t *testing.T) {
		// Expecting no substitution here due to operands priority
		it := DB.Query(ns).
			Where("first1", reindexer.EQ, item.First1).
			Where("first2", reindexer.EQ, item.First2).
			Or().
			Where("second2", reindexer.EQ, item.Second2).
			Where("second1", reindexer.EQ, item.Second1).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "-scan",
				Method:  "scan",
				Matched: 1,
			},
			{
				Field:       "first1",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "first2 or second2",
				Method:      "scan",
				Comparators: 2,
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

	t.Run("substitution with OR in another condition", func(t *testing.T) {
		it := DB.Query(ns).
			Where("first1", reindexer.EQ, item.First1).
			Where("second2", reindexer.EQ, item.Second2).
			Or().
			Where("second1", reindexer.EQ, item.Second1).
			Where("first2", reindexer.EQ, item.First2).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first1+first2",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:       "second2 or second1",
				Method:      "scan",
				Comparators: 2,
				Matched:     1,
			},
		}, "")
	})

	t.Run("substitution with OR in another condition in brackets", func(t *testing.T) {
		it := DB.Query(ns).
			Where("first1", reindexer.EQ, item.First1).
			OpenBracket().
			Where("second2", reindexer.EQ, item.Second2).
			Or().
			Where("second1", reindexer.EQ, item.Second1).
			CloseBracket().
			Where("first2", reindexer.EQ, item.First2).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first1+first2",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:       "second2 or second1",
				Method:      "scan",
				Comparators: 2,
				Matched:     1,
			},
		}, "")
	})

	t.Run("substitution with multiple brackets", func(t *testing.T) {
		it := DB.Query(ns).
			OpenBracket().
			Where("first1", reindexer.EQ, item.First1).
			Where("second2", reindexer.EQ, item.Second2).
			CloseBracket().
			OpenBracket().
			OpenBracket().
			Where("second1", reindexer.EQ, item.Second1).
			CloseBracket().
			Where("first2", reindexer.EQ, item.First2).
			CloseBracket().
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
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

	t.Run("substitution with multiple brackets and multiple conditions by index parts", func(t *testing.T) {
		it := DB.Query(ns).
			Where("first1", reindexer.EQ, item.First1).
			OpenBracket().
			Where("first1", reindexer.EQ, item.First1).
			Where("second2", reindexer.EQ, item.Second2).
			Where("first2", reindexer.EQ, item.First2).
			CloseBracket().
			OpenBracket().
			OpenBracket().
			Where("second1", reindexer.EQ, item.Second1).
			CloseBracket().
			Where("second2", reindexer.EQ, item.Second2).
			Where("first2", reindexer.EQ, item.First2).
			CloseBracket().
			Where("second2", reindexer.EQ, item.Second2).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
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

	t.Run("substitution with multiple brackets and multiple conditions by index parts and conflicting conditions", func(t *testing.T) {
		it := DB.Query(ns).
			Where("first1", reindexer.EQ, item.First1).
			OpenBracket().
			Where("first1", reindexer.EQ, item.First1).
			Where("second2", reindexer.EQ, item.Second2).
			Where("first2", reindexer.EQ, item.First2).
			CloseBracket().
			OpenBracket().
			OpenBracket().
			Where("second1", reindexer.EQ, item.Second1+2).
			CloseBracket().
			Where("second2", reindexer.EQ, item.Second2).
			Where("first2", reindexer.EQ, item.First2).
			CloseBracket().
			Where("second2", reindexer.EQ, item.Second2).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 0)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "second1+second2",
				Method:  "index",
				Keys:    0,
				Matched: 0,
			},
			{
				Field:   "first1+first2",
				Method:  "index",
				Keys:    1,
				Matched: 0,
			},
		}, "")
	})
}

func TestCompositeIndexesBestSubstitution(t *testing.T) {
	t.Parallel()

	const ns = testMultipleCompositeSubindexes
	item := TestItemMultipleCompositeSubindexes{
		ID: rand.Intn(100), First: rand.Intn(1000), Second: rand.Intn(1000), Third: rand.Intn(1000), Fourth: rand.Intn(1000),
	}
	err := DB.Upsert(ns, item)
	require.NoError(t, err)

	t.Run("basic largest composite indexes selection", func(t *testing.T) {
		it := DB.Query(ns).
			Where("first", reindexer.EQ, item.First).
			Where("second", reindexer.EQ, item.Second).
			Where("third", reindexer.EQ, item.Third).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first+second+third",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
		}, "")
	})

	t.Run("largest composite indexes selection with brackets", func(t *testing.T) {
		it := DB.Query(ns).
			OpenBracket().
			Where("first", reindexer.EQ, item.First).
			Where("second", reindexer.EQ, item.Second).
			CloseBracket().
			OpenBracket().
			Where("fourth", reindexer.EQ, item.Fourth).
			Where("third", reindexer.EQ, item.Third).
			CloseBracket().
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first+second+third+fourth",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
		}, "")
	})

	t.Run("largest composite indexes selection with OR in the midle. Case 1", func(t *testing.T) {
		it := DB.Query(ns).
			Where("first", reindexer.EQ, item.First).
			Where("fourth", reindexer.EQ, item.Fourth).
			Or().
			Where("id", reindexer.EQ, item.ID).
			Where("third", reindexer.EQ, item.Third).
			Where("second", reindexer.EQ, item.Second).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first+second+third",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:       "fourth or id",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
				Keys:        1,
			},
		}, "")
	})

	t.Run("largest composite indexes selection with OR in the midle. Case 2", func(t *testing.T) {
		it := DB.Query(ns).
			Where("first", reindexer.EQ, item.First).
			Where("fourth", reindexer.EQ, item.Fourth).
			Or().
			Where("third", reindexer.EQ, item.Third).
			Where("second", reindexer.EQ, item.Second).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first+second",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:       "fourth or third",
				Method:      "scan",
				Comparators: 2,
				Matched:     1,
			},
		}, "")
	})

	t.Run("largest composite indexes selection with OR in the midle. Case 3", func(t *testing.T) {
		it := DB.Query(ns).
			Where("second", reindexer.EQ, item.Second).
			Where("fourth", reindexer.EQ, item.Fourth).
			Or().
			Where("third", reindexer.EQ, item.Third).
			Where("first", reindexer.EQ, item.First).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first+second",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:       "fourth or third",
				Method:      "scan",
				Comparators: 2,
				Matched:     1,
			},
		}, "")
	})

	t.Run("No composite indexes substitution with OR in the midle", func(t *testing.T) {
		it := DB.Query(ns).
			Where("fourth", reindexer.EQ, item.Fourth).
			Where("second", reindexer.EQ, item.Second).
			Or().
			Where("third", reindexer.EQ, item.Third).
			Where("first", reindexer.EQ, item.First).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "-scan",
				Method:  "scan",
				Matched: 1,
			},
			{
				Field:       "fourth",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "second or third",
				Method:      "scan",
				Comparators: 2,
				Matched:     1,
			},
			{
				Field:       "first",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
		}, "")
	})
}

func appendRandInts(arr []int, skipValue int, size int) []int {
	for len(arr) < size {
		v := rand.Intn(3000)
		if v == skipValue {
			continue
		}
		arr = append(arr, v)
	}
	return arr
}

func createRandIntArrayWithValue(val int, size int) []int {
	arr := make([]int, 0, size)
	arr = append(arr, val)
	return appendRandInts(arr, val, size)
}

func TestCompositeSubstitutionLimit(t *testing.T) {
	t.Parallel()

	const ns = testCompositesLimit
	item := TestItemCompositesLimit{
		ID: rand.Intn(100), First: rand.Intn(1000), Second: rand.Intn(1000), Third: rand.Intn(1000), Fourth: rand.Intn(1000),
	}
	err := DB.Upsert(ns, item)
	require.NoError(t, err)

	t.Run("substitution of the 3 parts composite within limit (first index)", func(t *testing.T) {
		first := createRandIntArrayWithValue(item.First, 20)
		second := createRandIntArrayWithValue(item.Second, 20)
		third := createRandIntArrayWithValue(item.Third, 10)
		fourth := createRandIntArrayWithValue(item.Fourth, 2)

		it := DB.Query(ns).
			Where("first", reindexer.SET, first).
			Where("second", reindexer.SET, second).
			Where("third", reindexer.SET, third).
			Where("fourth", reindexer.SET, fourth).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first+second+fourth",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:       "third",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
		}, "")
	})

	t.Run("substitution of the 3 parts composite within limit (second index)", func(t *testing.T) {
		first := createRandIntArrayWithValue(item.First, 20)
		second := createRandIntArrayWithValue(item.Second, 20)
		third := createRandIntArrayWithValue(item.Third, 2)
		fourth := createRandIntArrayWithValue(item.Fourth, 10)

		it := DB.Query(ns).
			Where("first", reindexer.SET, first).
			Where("second", reindexer.SET, second).
			Where("third", reindexer.SET, third).
			Where("fourth", reindexer.SET, fourth).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first+second+third",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:       "fourth",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
		}, "")
	})

	t.Run("substitution of the 2 parts composite when 3 parts indexes' value sets are too large", func(t *testing.T) {
		first := createRandIntArrayWithValue(item.First, 20)
		second := createRandIntArrayWithValue(item.Second, 10)
		third := createRandIntArrayWithValue(item.Third, 20)
		fourth := createRandIntArrayWithValue(item.Fourth, 20)

		it := DB.Query(ns).
			Where("first", reindexer.SET, first).
			Where("second", reindexer.SET, second).
			Where("third", reindexer.SET, third).
			Where("fourth", reindexer.SET, fourth).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first+second",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
			{
				Field:       "third",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "fourth",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
		}, "")
	})

	t.Run("substitution of the 3 parts composite with single value set exceeding the limit", func(t *testing.T) {
		first := createRandIntArrayWithValue(item.First, 4000)
		second := createRandIntArrayWithValue(item.Second, 1)
		third := createRandIntArrayWithValue(item.Third, 1)

		it := DB.Query(ns).
			Where("first", reindexer.SET, first).
			Where("second", reindexer.SET, second).
			Where("third", reindexer.SET, third).
			Explain().Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, it.Count(), 1)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:   "first+second+third",
				Method:  "index",
				Keys:    1,
				Matched: 1,
			},
		}, "")
	})
}
