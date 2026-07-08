package reindexer

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	_ "github.com/restream/reindexer/v5/bindings/builtinserver"
	"github.com/restream/reindexer/v5/bindings/builtinserver/config"
	"github.com/restream/reindexer/v5/test/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ScvTestItem struct {
	ID int `reindex:"id,,pk"`
}

const testBuiltinNs = "test_builtin_ns"

func TestBuiltinServer(t *testing.T) {
	if len(DB.slaveList) > 0 || len(DB.clusterList) > 0 {
		t.Skip()
	}

	const ns = testBuiltinNs

	cfg1 := config.DefaultServerConfig()
	cfg1.Net.HTTPAddr = "0:29088"
	cfg1.Net.RPCAddr = "0:26534"
	cfg1.Storage.Path = path.Join(helpers.GetTmpDBDir(), "reindex_builtinserver_test1")
	os.RemoveAll(cfg1.Storage.Path)

	rx1, err := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfg1))
	require.NoError(t, err)
	defer rx1.Close()
	assert.NoError(t, rx1.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), &ScvTestItem{}))

	cfg2 := config.DefaultServerConfig()
	cfg2.Net.HTTPAddr = "0:29089"
	cfg2.Net.RPCAddr = "0:26535"
	cfg2.Storage.Path = path.Join(helpers.GetTmpDBDir(), "reindex_builtinserver_test2")
	os.RemoveAll(cfg2.Storage.Path)

	rx2, err := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfg2))
	require.NoError(t, err)
	defer rx2.Close()
	assert.NoError(t, rx2.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), &ScvTestItem{}))

	rx3, err := reindexer.NewReindex("cproto://127.0.0.1:26535/xxx")
	require.NoError(t, err)
	defer rx3.Close()
	assert.NoError(t, rx3.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), &ScvTestItem{}))

	unixSocketPath := path.Join(helpers.GetTmpDBDir(), "reindexer_builtinserver_test.sock")
	cfg4 := config.DefaultServerConfig()
	cfg4.Net.HTTPAddr = "0:29090"
	cfg4.Net.RPCAddr = "0:26536"
	cfg4.Storage.Path = path.Join(helpers.GetTmpDBDir(), "reindex_builtinserver_test4")
	cfg4.Net.UnixRPCAddr = unixSocketPath
	os.RemoveAll(cfg4.Storage.Path)

	rx4, err := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfg4))
	require.NoError(t, err)
	defer rx4.Close()
	assert.NoError(t, rx4.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), &ScvTestItem{}))

	rx5, err := reindexer.NewReindex(fmt.Sprintf("ucproto://%s:/xxx", unixSocketPath))
	require.NoError(t, err)
	defer rx5.Close()
	assert.NoError(t, rx5.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), &ScvTestItem{}))

	cfg4.Net.HTTPAddr = "0:29091"
	cfg4.Net.RPCAddr = "0:26537"
	db, err := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfg4))
	require.NotNil(t, db)
	require.ErrorContains(t, err, "Cannot enable storage for namespace '#config' on path")
}
