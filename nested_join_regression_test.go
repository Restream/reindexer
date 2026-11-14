package reindexer

import (
	"testing"

	"github.com/restream/reindexer/v5/cjson"
)

func makeNestedRegressionQuery() (root, level1, level2 *Query) {
	root = newQuery(nil, "nested_regression_root", nil)
	level1 = newQuery(nil, "nested_regression_level_1", nil)
	level2 = newQuery(nil, "nested_regression_level_2", nil)
	level1.InnerJoin(level2, "level2").On("id", EQ, "id")
	root.InnerJoin(level1, "level1").On("id", EQ, "id")
	return root, level1, level2
}

func TestNestedQueryCloseIsRecursive(t *testing.T) {
	root, level1, level2 := makeNestedRegressionQuery()
	root.close()

	if !level1.closed {
		t.Error("direct joined query was not closed")
	}
	if !level2.closed {
		t.Error("nested joined query was not closed")
	}
}

// QueryFormatV1 cannot recursively frame joined subqueries. Until it can, the
// client must reject a nested query instead of sending a flattened query tree.
func TestNestedJoinIsRejectedForQueryFormatV1(t *testing.T) {
	root, _, _ := makeNestedRegressionQuery()
	ser := cjson.NewSerializer(nil)
	if err := (&reindexerImpl{}).appendJoinQueriesV1(root, &ser, false); err == nil {
		t.Fatal("nested join was silently serialized using QueryFormatV1")
	}
}
