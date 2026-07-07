package reindexer

import (
	"testing"

	rx "github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/cjson"
	"github.com/stretchr/testify/require"
)

const testQueryErrorNs = "test_query_error_namespace"

type queryErrorItem struct {
	ID   int    `json:"id" reindex:"id,,pk"`
	Name string `json:"name" reindex:"name"`
}

type unsupportedQueryExpression struct{}

func (unsupportedQueryExpression) Type() int {
	return 0
}

func (unsupportedQueryExpression) Serialize(*cjson.Serializer) {
	panic("unsupported expression Serialize should not be called")
}

func init() {
	tnamespaces[testQueryErrorNs] = queryErrorItem{}
}

func TestQueryGetErr(t *testing.T) {
	const ns = testQueryErrorNs

	require.NoError(t, DB.Upsert(ns, queryErrorItem{ID: 1, Name: "one"}))

	item, found, err := DB.Reindexer.Query(ns).WhereInt("id", rx.EQ, 1).GetErr()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, item.(*queryErrorItem).ID)

	item, found, err = DB.Reindexer.Query(ns).WhereInt("id", rx.EQ, 2).GetErr()
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, item)

	json, found, err := DB.Reindexer.Query(ns).WhereInt("id", rx.EQ, 1).GetJsonErr()
	require.NoError(t, err)
	require.True(t, found)
	require.Contains(t, string(json), `"id":1`)

	item, found, err = DB.Reindexer.Query(ns).Sort("id", false, struct{}{}).GetErr()
	require.Error(t, err)
	require.False(t, found)
	require.Nil(t, item)

	json, found, err = DB.Reindexer.Query(ns).Sort("id", false, struct{}{}).GetJsonErr()
	require.Error(t, err)
	require.False(t, found)
	require.Nil(t, json)
}

func TestQueryGetWrappersStillPanicOnError(t *testing.T) {
	const ns = testQueryErrorNs

	require.Panics(t, func() {
		DB.Reindexer.Query(ns).Sort("id", false, struct{}{}).Get()
	})
	require.Panics(t, func() {
		DB.Reindexer.Query(ns).Sort("id", false, struct{}{}).GetJson()
	})
}

func TestQueryBuilderErrors(t *testing.T) {
	const ns = testQueryErrorNs

	it := DB.Reindexer.Query(ns).CloseBracket().Exec()
	require.Error(t, it.Error())
	it.Close()

	it = DB.Reindexer.Query(ns).Sort("id", false, struct{}{}).Exec()
	require.Error(t, it.Error())
	it.Close()

	it = DB.Reindexer.Query(ns).Sort("id", false, nil).Exec()
	require.Error(t, it.Error())
	it.Close()

	it = DB.Reindexer.Query(ns).On("id", rx.EQ, "id").Exec()
	require.Error(t, it.Error())
	it.Close()

	require.NotPanics(t, func() {
		it = DB.Reindexer.Query(ns).
			WhereExpressions(unsupportedQueryExpression{}, rx.EQ, rx.Values{Values: []any{1}}).
			Exec()
	})
	require.Error(t, it.Error())
	it.Close()
}

func TestQueryRepeatedExecReturnsError(t *testing.T) {
	const ns = testQueryErrorNs

	q := DB.Reindexer.Query(ns).WhereInt("id", rx.EQ, 1)
	it := q.Exec()
	require.NoError(t, it.Error())
	it2 := q.Exec()
	require.Error(t, it2.Error())
	it2.Close()
	it.Close()
}
