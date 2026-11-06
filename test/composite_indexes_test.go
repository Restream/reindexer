package reindexer

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/restream/reindexer/v5"
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

type TestItemCompositeUpsert struct {
	Id             int      `reindex:"id,hash,pk"`
	First          int      `reindex:"first,-"`
	Second         int      `reindex:"second,-"`
	NamedComposite struct{} `reindex:"first+second,,composite"` // The name is assigned for testing. Don't give composite fields names!
}

const (
	testCompositeIndexesSubstitutionNs = "test_composite_indexes_substitution"
	testMultipleCompositeSubindexesNs  = "test_composite_indexes_multiple_subindexes"
	testCompositesLimitNs              = "test_composites_limit"
	testCompositeUpsertNs              = "test_composites_upsert"
)

func init() {
	tnamespaces[testCompositeIndexesSubstitutionNs] = TestCompositeSubstitutionStruct{}
	tnamespaces[testMultipleCompositeSubindexesNs] = TestItemMultipleCompositeSubindexes{}
	tnamespaces[testCompositesLimitNs] = TestItemCompositesLimit{}
	tnamespaces[testCompositeUpsertNs] = TestItemCompositeUpsert{}
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
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:     "first1+third",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
		}, "")
	})

	t.Run("reversed basic substitution with extra idx in the middle", func(t *testing.T) {
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
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "id",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:     "id",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "id",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:     "second1+second2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:     "id",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "second1+second2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "id",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "second1+second2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "second1+second2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "id",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:     "second1+second2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "id",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:       "first2",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "or first1",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     0,
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
						FieldType:   "indexed",
						Method:      "scan",
						Comparators: 1,
						Matched:     1,
					},
					{
						Field:       "second2",
						FieldType:   "indexed",
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
						FieldType:   "indexed",
						Method:      "scan",
						Comparators: 1,
						Matched:     0,
					},
					{
						Field:       "first1",
						FieldType:   "indexed",
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
						Field:     "id",
						FieldType: "indexed",
						Method:    "index",
						Keys:      1,
						Matched:   1,
					},
					{
						Field:     "first1+first2",
						FieldType: "indexed",
						Method:    "index",
						Keys:      1,
						Matched:   1,
					},
				},
			},
			{
				Field: "or (second1+second2 and id)",
				Selectors: []expectedExplain{
					{
						Field:     "second1+second2",
						FieldType: "indexed",
						Method:    "index",
						Keys:      1,
						Matched:   0,
					},
					{
						Field:     "id",
						FieldType: "indexed",
						Method:    "index",
						Keys:      1,
						Matched:   0,
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
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "second1",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "first2",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "or second2",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     0,
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
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:       "second2",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "or second1",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     0,
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
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:       "second2",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "or second1",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     0,
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
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "second1+second2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:     "second1+second2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:     "second1+second2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      0,
				Matched:   0,
			},
			{
				Field:     "first1+first2",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   0,
			},
		}, "")
	})

	t.Run("substitution after other indexes drop", func(t *testing.T) {
		checkSubstitution := func() {
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
					Field:     "first1+third",
					FieldType: "indexed",
					Method:    "index",
					Keys:      1,
					Matched:   1,
				},
			}, "")
		}

		err := DB.DropIndex(ns, "second1+second2")
		require.NoError(t, err)
		// Check substitution right after composite deletion
		checkSubstitution()

		err = DB.DropIndex(ns, "second1")
		require.NoError(t, err)
		err = DB.DropIndex(ns, "second2")
		require.NoError(t, err)
		// Check substitution after other indexes deletion
		checkSubstitution()
	})

	t.Run("no substitution after current index drop", func(t *testing.T) {
		err := DB.DropIndex(ns, "first1+third")
		require.NoError(t, err)
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
				Field:   "-scan",
				Method:  "scan",
				Matched: 1,
			},
			{
				Field:       "first1",
				FieldType:   "indexed",
				Method:      "scan",
				Matched:     1,
				Comparators: 1,
			},
			{
				Field:       "third",
				FieldType:   "indexed",
				Method:      "scan",
				Matched:     1,
				Comparators: 1,
			},
		}, "")
	})
}

func TestCompositeIndexesBestSubstitution(t *testing.T) {
	t.Parallel()

	const ns = testMultipleCompositeSubindexesNs
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
				Field:     "first+second+third",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
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
				Field:     "first+second+third+fourth",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
		}, "")
	})

	t.Run("largest composite indexes selection with OR in the middle. Case 1", func(t *testing.T) {
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
				Field:     "first+second+third",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:       "fourth",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "or id",
				FieldType:   "indexed",
				Method:      "index",
				Comparators: 0,
				Matched:     0,
				Keys:        1,
			},
		}, "")
	})

	t.Run("largest composite indexes selection with OR in the middle. Case 2", func(t *testing.T) {
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
				Field:     "first+second",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:       "fourth",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "or third",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     0,
			},
		}, "")
	})

	t.Run("largest composite indexes selection with OR in the middle. Case 3", func(t *testing.T) {
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
				Field:     "first+second",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:       "fourth",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "or third",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     0,
			},
		}, "")
	})

	t.Run("No composite indexes substitution with OR in the middle", func(t *testing.T) {
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
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "first",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "second",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "or third",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     0,
			},
		}, "")
	})
}

func TestCompositeSubstitutionLimit(t *testing.T) {
	t.Parallel()

	const ns = testCompositesLimitNs
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
				Field:     "first+second+fourth",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:       "third",
				FieldType:   "indexed",
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
				Field:     "first+second+third",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:       "fourth",
				FieldType:   "indexed",
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
				Field:     "first+second",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
			{
				Field:       "third",
				FieldType:   "indexed",
				Method:      "scan",
				Comparators: 1,
				Matched:     1,
			},
			{
				Field:       "fourth",
				FieldType:   "indexed",
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
				Field:     "first+second+third",
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			},
		}, "")
	})
}

func TestCompositeUpsert(t *testing.T) {
	t.Parallel()

	const ns = testCompositeUpsertNs
	item := TestItemCompositeUpsert{
		Id:     rand.Intn(100),
		First:  rand.Intn(1000),
		Second: rand.Intn(1000),
	}
	err := DB.Upsert(ns, item)
	require.NoError(t, err)

	j, err := DB.Query(ns).ExecToJson().FetchAll()
	require.NoError(t, err)
	require.Greater(t, len(j), 0)
	require.NotContains(t, string(j), "NamedComposite")
}
