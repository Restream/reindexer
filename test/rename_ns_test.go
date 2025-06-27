package reindexer

import (
	//	"fmt"

	"strconv"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestItem1 struct {
	ID   int    `reindex:"id,,pk"`
	Year int    `reindex:"year,tree"`
	Name string `reindex:"name"`
}

type TestItem2 struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name"`
}

const (
	testRenameNs   = "test_rename_namespace"
	testRenameNsTo = "test_rename_namespace_to"
	testExistNs    = "test_exist_namespace"
)

func GetAllDataFromNamespace(t *testing.T, namespace string) (item []interface{}, err error) {
	q := DB.Query(namespace).Sort("id", false)
	it := q.Exec(t)
	defer it.Close()
	return it.FetchAll()
}

func TestRenameNamespace(t *testing.T) {
	if len(DB.slaveList) > 0 || len(DB.clusterList) > 0 { // TODO: Enable this, when rename will be supported in replication
		t.Skip()
	}

	err := DB.OpenNamespace(testRenameNs, reindexer.DefaultNamespaceOptions(), TestItem1{})
	assert.NoError(t, err, "Can't open namespace \"%s\"", testRenameNs)

	for index := 0; index < 10; index++ {
		err = DB.Upsert(testRenameNs, TestItem1{index, 2, "nameTest" + strconv.Itoa(index)})
		assert.NoError(t, err, "Can't Upsert data to namespace")
	}
	testRNdata, err := GetAllDataFromNamespace(t, testRenameNs)
	assert.NoError(t, err, "Can't get data from namespace")

	err = DB.OpenNamespace(testExistNs, reindexer.DefaultNamespaceOptions(), TestItem2{})
	assert.NoError(t, err, "Can't open namespace \"%s\"", testExistNs)

	//ok
	err = DB.RenameNamespace(testRenameNs, testRenameNsTo)
	assert.NoError(t, err, "Can't rename namespace src = \"%s\" dst = \"%s\"", testRenameNs, testRenameNsTo)
	testRNdataTo, err := GetAllDataFromNamespace(t, testRenameNsTo)
	assert.NoError(t, err, "Can't get data from namespace")
	assert.Equal(t, testRNdata, testRNdataTo, "Data in tables not equals\n%s\n%s", testRNdata, testRNdataTo)

	// rename to equal name
	err = DB.RenameNamespace(testRenameNsTo, testRenameNsTo)
	assert.NoError(t, err, "Can't rename namespace src = \"%s\" dst = \"%s\"", testRenameNsTo, testRenameNsTo)
	testRNdataTo, err = GetAllDataFromNamespace(t, testRenameNsTo)
	assert.NoError(t, err, "Can't get data from namespace")
	assert.Equal(t, testRNdata, testRNdataTo, "Data in tables not equals\n%s\n%s", testRNdata, testRNdataTo)

	// rename to empty namespace
	err = DB.RenameNamespace(testRenameNsTo, "")
	assert.Error(t, err, "Can't rename namespace src = \"%s\" to empty name", testRenameNsTo)
	testRNdataTo, err = GetAllDataFromNamespace(t, testRenameNsTo)
	assert.NoError(t, err, "Can't get data from namespace")
	assert.Equal(t, testRNdata, testRNdataTo, "Data in tables not equals\n%s\n%s", testRNdata, testRNdataTo)

	// rename to system namespace
	err = DB.RenameNamespace(testRenameNsTo, "#rename_namespace")
	assert.Error(t, err, "rename to system namespace err src = \"%s\" dst = \"%s\"", testRenameNsTo, "#rename_namespace")
	testRNdataTo, err = GetAllDataFromNamespace(t, testRenameNsTo)
	assert.NoError(t, err, "Can't get data from namespace")
	assert.Equal(t, testRNdata, testRNdataTo, "Data in tables not equals\n%s\n%s", testRNdata, testRNdataTo)

	// add more data to the renamed namespace
	for index := 10; index < 15; index++ {
		item := TestItem1{index, 2, "nameTest" + strconv.Itoa(index)}
		err = DB.Upsert(testRenameNsTo, item)
		require.NoError(t, err, "Can't Upsert data to namespace")
		testRNdata = append(testRNdata, &item)
	}
	testRNdataTo, err = GetAllDataFromNamespace(t, testRenameNsTo)
	assert.NoError(t, err, "Can't get data from namespace")
	assert.Equal(t, testRNdata, testRNdataTo, "Data in tables not equals\n%s\n%s", testRNdata, testRNdataTo)

	// rename to existing namespace
	err = DB.RenameNamespace(testRenameNsTo, testExistNs)
	assert.NoError(t, err, "rename to existing namespace err src = \"%s\" dst = \"%s\"", testRenameNsTo, testExistNs)
	testRNdataTo, err = GetAllDataFromNamespace(t, testExistNs)
	assert.NoError(t, err, "Can't get data from namespace")
	assert.Equal(t, testRNdata, testRNdataTo, "Data in tables not equals\n%s\n%s", testRNdata, testRNdataTo)
}
