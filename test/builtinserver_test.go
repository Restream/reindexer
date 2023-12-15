package reindexer

import (
	"os"
	"testing"
	"time"

	"github.com/restream/reindexer/v3"
	_ "github.com/restream/reindexer/v3/bindings/builtinserver"
	"github.com/restream/reindexer/v3/bindings/builtinserver/config"
	"github.com/stretchr/testify/assert"
)

type ScvTestItem struct {
	ID int `reindex:"id,,pk"`
}

func TestBuiltinServer(t *testing.T) {
	cfg1 := config.DefaultServerConfig()
	cfg1.Net.HTTPAddr = "0:29088"
	cfg1.Net.RPCAddr = "0:26534"
	cfg1.Storage.Path = "/tmp/reindex_builtinserver_test1"

	os.RemoveAll(cfg1.Storage.Path)
	rx1 := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfg1))
	defer rx1.Close()
	assert.NoError(t, rx1.Status().Err)
	assert.NoError(t, rx1.OpenNamespace("testns", reindexer.DefaultNamespaceOptions(), &ScvTestItem{}))

	cfg2 := config.DefaultServerConfig()
	cfg2.Net.HTTPAddr = "0:29089"
	cfg2.Net.RPCAddr = "0:26535"
	cfg2.Storage.Path = "/tmp/reindex_builtinserver_test2"

	os.RemoveAll(cfg2.Storage.Path)
	rx2 := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfg2))
	defer rx2.Close()
	assert.NoError(t, rx2.Status().Err)
	assert.NoError(t, rx2.OpenNamespace("testns", reindexer.DefaultNamespaceOptions(), &ScvTestItem{}))

	rx3 := reindexer.NewReindex("cproto://127.0.0.1:26535/xxx")
	defer rx3.Close()
	assert.NoError(t, rx3.Status().Err)
	assert.NoError(t, rx3.OpenNamespace("testns", reindexer.DefaultNamespaceOptions(), &ScvTestItem{}))

	cfg4 := config.DefaultServerConfig()
	cfg4.Net.HTTPAddr = "0:29090"
	cfg4.Net.RPCAddr = "0:26536"
	cfg4.Storage.Path = "/tmp/reindex_builtinserver_test4"
	cfg4.Net.UnixRPCAddr = "/tmp/reindexer_builtinserver_test.sock"

	os.RemoveAll(cfg4.Storage.Path)
	rx4 := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfg4))
	defer rx4.Close()
	assert.NoError(t, rx4.Status().Err)
	assert.NoError(t, rx4.OpenNamespace("testns", reindexer.DefaultNamespaceOptions(), &ScvTestItem{}))

	rx5 := reindexer.NewReindex("ucproto:///tmp/reindexer_builtinserver_test.sock:/xxx")
	defer rx5.Close()
	assert.NoError(t, rx5.Status().Err)
	assert.NoError(t, rx5.OpenNamespace("testns", reindexer.DefaultNamespaceOptions(), &ScvTestItem{}))
}
