package reindexer

import (
	"fmt"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
)

type TestItemDistinctMulti struct {
	ID int `reindex:"id,,pk"`
	V0 int64
	V1 string
}

func init() {
	tnamespaces["test_distinct_multi"] = TestItemDistinctMulti{}
}

func TestDistinctMultiField(t *testing.T) {
	const testNamespace = "test_distinct_multi"

	err := DBD.OpenNamespace(testNamespace, reindexer.DefaultNamespaceOptions(), TestItemDistinctMulti{})
	assert.NoError(t, err, "Can't open namespace \"%s\"", testNamespace)

	err = DBD.Upsert(testNamespace, &TestItemDistinctMulti{ID: 0, V0: 10, V1: "s100"})
	assert.NoError(t, err)

	err = DBD.Upsert(testNamespace, &TestItemDistinctMulti{ID: 1, V0: 11, V1: "s100"})
	assert.NoError(t, err)

	err = DBD.Upsert(testNamespace, &TestItemDistinctMulti{ID: 2, V0: 10, V1: "s100"})
	assert.NoError(t, err)

	err = DBD.Upsert(testNamespace, &TestItemDistinctMulti{ID: 3, V0: 11, V1: "s100"})
	assert.NoError(t, err)

	it := DBD.Query(testNamespace).Distinct("V0", "V1").Exec()
	assert.Equal(t, 2, it.Count())
	aggRes := it.AggResults()
	assert.Equal(t, 1, len(aggRes))
	assert.Equal(t, aggRes[0].Type, "distinct")
	assert.Equal(t, len(aggRes[0].Distincts), 2)
	assert.Equal(t, fmt.Sprintf("%v", aggRes[0].Distincts[0]), "[10 s100]")
	assert.Equal(t, fmt.Sprintf("%v", aggRes[0].Distincts[1]), "[11 s100]")

}
