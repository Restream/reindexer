package reindexer

import (
	//	"fmt"

	"strconv"
	"testing"

	"github.com/restream/reindexer"
	"github.com/stretchr/testify/assert"
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

func GetAllDataFromNamespace(t *testing.T, namespace string) (item []interface{}, err error) {
	q := DB.Query(namespace)
	it := q.Exec(t)
	defer it.Close()
	return it.FetchAll()
}

func TestRenameNamespace(t *testing.T) {
	if len(DB.slaveList) > 0 || len(DB.clusterList) > 0 { // TODO: Enable this, when rename will be supported in replication
		t.Skip()
	}

	const testRenameNamespace = "test_rename_namespace"
	const testRenameNamespaceTo = "test_rename_namespace_to"
	const testExistNamespace = "test_exist_namespace"

	err := DB.OpenNamespace(testRenameNamespace, reindexer.DefaultNamespaceOptions(), TestItem1{})
	assert.NoError(t, err, "Can't open namespace \"%s\"", testRenameNamespace)

	for index := 0; index < 10; index++ {
		err = DB.Upsert(testRenameNamespace, TestItem1{index, 2, "nameTest" + strconv.Itoa(index)})
		assert.NoError(t, err, "Can't Upsert data to namespace")
	}
	testRNdata, err := GetAllDataFromNamespace(t, testRenameNamespace)
	assert.NoError(t, err, "Can't get data from namespace")

	err = DB.OpenNamespace(testExistNamespace, reindexer.DefaultNamespaceOptions(), TestItem2{})
	assert.NoError(t, err, "Can't open namespace \"%s\"", testExistNamespace)

	//ok
	err = DB.RenameNamespace(testRenameNamespace, testRenameNamespaceTo)
	assert.NoError(t, err, "Can't rename namespace src = \"%s\" dst = \"%s\"", testRenameNamespace, testRenameNamespaceTo)
	testRNdataTo, err := GetAllDataFromNamespace(t, testRenameNamespaceTo)
	assert.NoError(t, err, "Can't get data from namespace")
	assert.Equal(t, testRNdata, testRNdataTo, "Data in tables not equals\n%s\n%s", testRNdata, testRNdataTo)

	// rename to equal name
	err = DB.RenameNamespace(testRenameNamespaceTo, testRenameNamespaceTo)
	assert.NoError(t, err, "Can't rename namespace src = \"%s\" dst = \"%s\"", testRenameNamespaceTo, testRenameNamespaceTo)
	testRNdataTo, err = GetAllDataFromNamespace(t, testRenameNamespaceTo)
	assert.NoError(t, err, "Can't get data from namespace")
	assert.Equal(t, testRNdata, testRNdataTo, "Data in tables not equals\n%s\n%s", testRNdata, testRNdataTo)

	// rename to empty namespace
	err = DB.RenameNamespace(testRenameNamespaceTo, "")
	assert.Error(t, err, "Can't rename namespace src = \"%s\" to empty name", testRenameNamespaceTo)
	testRNdataTo, err = GetAllDataFromNamespace(t, testRenameNamespaceTo)
	assert.NoError(t, err, "Can't get data from namespace")
	assert.Equal(t, testRNdata, testRNdataTo, "Data in tables not equals\n%s\n%s", testRNdata, testRNdataTo)

	// rename to system namespace
	err = DB.RenameNamespace(testRenameNamespaceTo, "#rename_namespace")
	assert.Error(t, err, "rename to system namespace err src = \"%s\" dst = \"%s\"", testRenameNamespaceTo, "#rename_namespace")
	testRNdataTo, err = GetAllDataFromNamespace(t, testRenameNamespaceTo)
	assert.NoError(t, err, "Can't get data from namespace")
	assert.Equal(t, testRNdata, testRNdataTo, "Data in tables not equals\n%s\n%s", testRNdata, testRNdataTo)

	// rename to existing namespace
	err = DB.RenameNamespace(testRenameNamespaceTo, testExistNamespace)
	assert.NoError(t, err, "rename to existing namespace err src = \"%s\" dst = \"%s\"", testRenameNamespaceTo, testExistNamespace)
	testRNdataTo, err = GetAllDataFromNamespace(t, testExistNamespace)
	assert.NoError(t, err, "Can't get data from namespace")
	assert.Equal(t, testRNdata, testRNdataTo, "Data in tables not equals\n%s\n%s", testRNdata, testRNdataTo)

}
