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

type customQueryExpression struct {
	serialized *bool
}

func (customQueryExpression) Type() int {
	return 0
}

func (e customQueryExpression) Serialize(ser *cjson.Serializer) {
	*e.serialized = true
	rx.Field{Name: "id"}.Serialize(ser)
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
	require.NoError(t, DB.Upsert(ns, queryErrorItem{ID: 1, Name: "one"}))

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

	serialized := false
	item, found, err := DB.Reindexer.Query(ns).
		WhereExpressions(customQueryExpression{serialized: &serialized}, rx.EQ, rx.Values{Values: []any{1}}).
		GetErr()
	require.NoError(t, err)
	require.True(t, serialized)
	require.True(t, found)
	require.Equal(t, 1, item.(*queryErrorItem).ID)

	it = DB.Reindexer.Query(ns).
		WhereExpressions(nil, rx.EQ, rx.Values{Values: []any{1}}).
		Exec()
	require.Error(t, it.Error())
	it.Close()

	subQuery := DB.Reindexer.Query(ns).Sort("id", false, struct{}{})
	it = DB.Reindexer.Query(ns).WhereQuery(subQuery, rx.EQ, 1).Exec()
	require.EqualError(t, it.Error(), "rq: Invalid reflection type struct")
	it.Close()
	_, _, err = subQuery.GetErr()
	require.EqualError(t, err, "rq: Invalid reflection type struct")
}

func TestQueryRepeatedExecPanics(t *testing.T) {
	const ns = testQueryErrorNs

	q := DB.Reindexer.Query(ns).WhereInt("id", rx.EQ, 1)
	it := q.Exec()
	defer it.Close()
	require.NoError(t, it.Error())
	require.Panics(t, func() {
		q.Exec()
	})
}
