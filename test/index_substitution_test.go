package reindexer

import (
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/require"
)

type TestItemIndexSubstitution struct {
	ID                  int `reindex:"id,,pk"`
	FirstRegField       int `reindex:"reg_multipath,,appendable"`
	SecondRegField      int `reindex:"reg_multipath,,appendable"`
	RegFieldDiffName    int `reindex:"reg_field_diff_name"`
	SparseFieldDiffName int `reindex:"sparse_field_diff_name,,sparse"`
	RegFieldSameName    int `reindex:"RegFieldSameName"`
	SparseFieldSameName int `reindex:"SparseFieldSameName,,sparse"`
}

const (
	testNamespaceIndexSubstitution = "test_index_substitution"
)

func init() {
	tnamespaces[testNamespaceIndexSubstitution] = TestItemIndexSubstitution{}
}

func TestIndexSubstitition(t *testing.T) {
	t.Parallel()

	ns := testNamespaceIndexSubstitution

	tx := newTestTx(DB, ns)
	tx.Upsert(&TestItemIndexSubstitution{ID: 1,
		FirstRegField:       1,
		SecondRegField:      2,
		RegFieldDiffName:    3,
		SparseFieldDiffName: 3,
		RegFieldSameName:    4,
		SparseFieldSameName: 4})
	tx.Upsert(&TestItemIndexSubstitution{ID: 2,
		FirstRegField:       2,
		SecondRegField:      1,
		RegFieldDiffName:    103,
		SparseFieldDiffName: 103,
		RegFieldSameName:    104,
		SparseFieldSameName: 104})
	tx.MustCommit()

	t.Run("no substitution for index with multiple jsonpaths", func(t *testing.T) {
		t.Parallel()

		testImpl := func(t *testing.T, fieldName string, value int) {
			explainRes := DB.Query(ns).WhereInt(fieldName, reindexer.EQ, value).Explain().ExecAndVerify(t)
			require.NotNil(t, explainRes)

			printExplainRes(explainRes)
			checkExplain(t, explainRes.Selectors, []expectedExplain{
				{
					Field:   "-scan",
					Method:  "scan",
					Matched: 2,
				},
				{
					Field:       fieldName,
					FieldType:   "non-indexed",
					Method:      "scan",
					Matched:     1,
					Comparators: 1,
				},
			}, "")
		}

		t.Run("regular index 1", func(t *testing.T) {
			t.Parallel()
			testImpl(t, "FirstRegField", 1)
		})
		t.Run("regular index 2", func(t *testing.T) {
			t.Parallel()
			testImpl(t, "SecondRegField", 1)
		})
	})

	t.Run("handling for appendable index", func(t *testing.T) {
		t.Parallel()

		indexName := "reg_multipath"
		explainRes := DB.Query(ns).WhereInt(indexName, reindexer.EQ, 1).Explain().ExecAndVerify(t)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:     indexName,
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   2,
			},
		}, "")
	})

	t.Run("substitution for index by jsonpath", func(t *testing.T) {
		t.Parallel()

		testImpl := func(t *testing.T, fieldName string, indexName string, value int) {
			explainRes := DB.Query(ns).WhereInt(fieldName, reindexer.EQ, value).Explain().ExecAndVerify(t)
			require.NotNil(t, explainRes)

			printExplainRes(explainRes)
			checkExplain(t, explainRes.Selectors, []expectedExplain{
				{
					Field:     indexName,
					FieldType: "indexed",
					Method:    "index",
					Keys:      1,
					Matched:   1,
				},
			}, "")
		}

		t.Run("regular index with different name and jsonpath (by jsonpath)", func(t *testing.T) {
			t.Parallel()
			testImpl(t, "RegFieldDiffName", "reg_field_diff_name", 3)
		})
		t.Run("regular index with different name and jsonpath (by index name)", func(t *testing.T) {
			t.Parallel()
			testImpl(t, "reg_field_diff_name", "reg_field_diff_name", 103)
		})
		t.Run("regular index with same name and jsonpath", func(t *testing.T) {
			t.Parallel()
			testImpl(t, "RegFieldSameName", "RegFieldSameName", 104)
		})
		t.Run("sparse index with different name and jsonpath (by jsonpath)", func(t *testing.T) {
			t.Parallel()
			testImpl(t, "SparseFieldDiffName", "sparse_field_diff_name", 103)
		})
		t.Run("sparse index with different name and jsonpath (by index name)", func(t *testing.T) {
			t.Parallel()
			testImpl(t, "sparse_field_diff_name", "sparse_field_diff_name", 3)
		})
		t.Run("sparse index with same name and jsonpath", func(t *testing.T) {
			t.Parallel()
			testImpl(t, "SparseFieldSameName", "SparseFieldSameName", 4)
		})
	})
}
