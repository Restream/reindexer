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
	cfg1.Storage.Path = "/tmp/rx_builtinserver_test1"

	os.RemoveAll(cfg1.Storage.Path)
	rx1 := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfg1))
	assert.NoError(t, rx1.Status().Err)
	assert.NoError(t, rx1.OpenNamespace("testns", reindexer.DefaultNamespaceOptions(), &ScvTestItem{}))

	cfg2 := config.DefaultServerConfig()
	cfg2.Net.HTTPAddr = "0:29089"
	cfg2.Net.RPCAddr = "0:26535"
	cfg2.Storage.Path = "/tmp/rx_builtinserver_test2"

	os.RemoveAll(cfg2.Storage.Path)
	rx2 := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfg2))
	assert.NoError(t, rx2.Status().Err)
	assert.NoError(t, rx2.OpenNamespace("testns", reindexer.DefaultNamespaceOptions(), &ScvTestItem{}))

	rx3 := reindexer.NewReindex("cproto://127.0.0.1:26535/xxx")
	assert.NoError(t, rx3.Status().Err)
	assert.NoError(t, rx3.OpenNamespace("testns", reindexer.DefaultNamespaceOptions(), &ScvTestItem{}))

	rx3.Close()
	rx2.Close()
	rx1.Close()

}
