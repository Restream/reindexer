package reindexer

import (
	"fmt"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/require"
)

type TestSelectTextItem struct {
	ID   int      `reindex:"id,,pk"`
	Name string   `reindex:"name,text"`
	_    struct{} `reindex:"id+name=comp_idx,text,composite"`
}

const testSelectFuncNs = "test_select_func"

func init() {
	tnamespaces[testSelectFuncNs] = TestSelectTextItem{}
}

func FillTestSelectTextItems(names []string) {
	tx := newTestTx(DB, testSelectFuncNs)
	for i := 0; i < len(names); i++ {
		item := TestSelectTextItem{
			ID:   mkID(i),
			Name: names[i],
		}
		if err := tx.Upsert(&item); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func checkSelectFunc(t *testing.T, qt *queryTest, expected string) {
	res_slice, err := qt.MustExec(t).FetchAll()
	require.NoError(t, err)
	require.Len(t, res_slice, 1)
	res := res_slice[0].(*TestSelectTextItem)
	require.EqualValues(t, expected, res.Name)
}

func TestSelectFunctions(t *testing.T) {
	t.Parallel()

	const ns = testSelectFuncNs
	words := []string{"some wordrx", "w(here rx fin)d", "somerxhere"}
	FillTestSelectTextItems(words)

	delimiters := []string{".", "=", " = "}

	t.Run("check select_function highlight", func(t *testing.T) {
		for _, delim := range delimiters {
			q := DB.Query(ns).Where("name", reindexer.EQ, "rx").
				Functions(fmt.Sprintf("name%shighlight(<,>)", delim))
			checkSelectFunc(t, q, "w(here <rx> fin)d")
		}
	})

	t.Run("check select_function snippet", func(t *testing.T) {
		for _, delim := range delimiters {
			q := DB.Query(ns).Where("name", reindexer.EQ, "rx").
				Functions(fmt.Sprintf("name%ssnippet(<,>,2,3,'!','#')", delim))
			checkSelectFunc(t, q, "!e <rx> fi#")
		}
	})

	t.Run("check select_function snippet_n", func(t *testing.T) {
		for _, delim := range delimiters {
			q := DB.Query(ns).Where("name", reindexer.EQ, "rx").
				Functions(fmt.Sprintf("name%ssnippet_n('<','>',10,2,pre_delim='[',post_delim=']',left_bound='(',right_bound=')',with_area=1)", delim))
			checkSelectFunc(t, q, "[[2,11]here <rx> f]")
		}
	})

	t.Run("check can't select_function snippet with composite nonstring idx field", func(t *testing.T) {
		q := DB.Query(ns).Where("comp_idx", reindexer.EQ, "rx").Functions("comp_idx=snippet(<,>,3,3,'!','!')")
		result, err := q.Exec(t).FetchAll()
		require.ErrorContains(t, err, "Unable to apply snippet function to the non-string field 'id'")
		require.Nil(t, result)
	})

}
