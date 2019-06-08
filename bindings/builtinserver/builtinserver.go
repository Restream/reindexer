package builtinserver

// #include "server/cbinding/server_c.h"
// #include <stdlib.h>
import "C"
import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/restream/reindexer/bindings"
	"github.com/restream/reindexer/bindings/builtin"
	"github.com/restream/reindexer/bindings/builtinserver/config"
)

var defaultStartupTimeout time.Duration = time.Minute * 3
var defaultShutdownTimeout time.Duration = defaultStartupTimeout

func init() {
	C.init_reindexer_server()
	bindings.RegisterBinding("builtinserver", new(BuiltinServer))
}

func err2go(ret C.reindexer_error) error {
	if ret.what != nil {
		defer C.free(unsafe.Pointer(ret.what))
		return bindings.NewError("rq:"+C.GoString(ret.what), int(ret.code))
	}
	return nil
}

func str2c(str string) C.reindexer_string {
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&str))
	return C.reindexer_string{p: unsafe.Pointer(hdr.Data), n: C.int(hdr.Len)}
}

func checkStorageReady() bool {
	return int(C.check_server_ready()) == 1
}

type BuiltinServer struct {
	builtin         bindings.RawBinding
	wg              sync.WaitGroup
	shutdownTimeout time.Duration
}

func (server *BuiltinServer) stopServer(timeout time.Duration) error {
	if err := err2go(C.stop_reindexer_server()); err != nil {
		return err
	}

	c := make(chan struct{})
	go func() {
		defer close(c)
		server.wg.Wait()
	}()

	select {
	case <-c:
		return nil
	case <-time.After(timeout):
		return bindings.NewError("Shutdown server timeout is expired", bindings.ErrLogic)
	}
}

func (server *BuiltinServer) Init(u *url.URL, options ...interface{}) error {
	if server.builtin != nil {
		return bindings.NewError("already initialized", bindings.ErrConflict)
	}

	server.builtin = &builtin.Builtin{}
	startupTimeout := defaultStartupTimeout
	server.shutdownTimeout = defaultShutdownTimeout
	serverCfg := config.DefaultServerConfig()

	for _, option := range options {
		switch v := option.(type) {
		case bindings.OptionCgoLimit:
		case bindings.OptionBuiltinWithServer:
			if v.StartupTimeout != 0 {
				startupTimeout = v.StartupTimeout
			}
			if v.ServerConfig != nil {
				serverCfg = v.ServerConfig
			}
			if v.ShutdownTimeout != 0 {
				server.shutdownTimeout = v.ShutdownTimeout
			}
		default:
			fmt.Printf("Unknown builtinserver option: %v\n", option)
		}
	}

	yamlStr, err := serverCfg.GetYamlString()
	if err != nil {
		return err
	}

	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		err := err2go(C.start_reindexer_server(str2c(yamlStr)))
		if err != nil {
			panic(err)
		}
	}()

	tTimeout := time.Now().Add(startupTimeout)
	for !checkStorageReady() {
		if time.Now().After(tTimeout) {
			panic(bindings.NewError("Server startup timeout expired.", bindings.ErrLogic))
		}
		time.Sleep(time.Second)
	}

	pass, _ := u.User.Password()

	var rx C.uintptr_t = 0
	if err := err2go(C.get_reindexer_instance(str2c(u.Host), str2c(u.User.Username()), str2c(pass), &rx)); err != nil {
		return err
	}

	url := *u
	url.Path = ""

	options = append(options, bindings.OptionReindexerInstance{Instance: uintptr(rx)})
	return server.builtin.Init(&url, options...)
}

func (server *BuiltinServer) Clone() bindings.RawBinding {
	return &BuiltinServer{}
}

func (server *BuiltinServer) OpenNamespace(ctx context.Context, namespace string, enableStorage, dropOnFileFormatError bool) error {
	return server.builtin.OpenNamespace(ctx, namespace, enableStorage, dropOnFileFormatError)
}

func (server *BuiltinServer) CloseNamespace(ctx context.Context, namespace string) error {
	return server.builtin.CloseNamespace(ctx, namespace)
}

func (server *BuiltinServer) DropNamespace(ctx context.Context, namespace string) error {
	return server.builtin.DropNamespace(ctx, namespace)
}

func (server *BuiltinServer) EnableStorage(ctx context.Context, namespace string) error {
	return server.builtin.EnableStorage(ctx, namespace)
}

func (server *BuiltinServer) AddIndex(ctx context.Context, namespace string, indexDef bindings.IndexDef) error {
	return server.builtin.AddIndex(ctx, namespace, indexDef)
}

func (server *BuiltinServer) UpdateIndex(ctx context.Context, namespace string, indexDef bindings.IndexDef) error {
	return server.builtin.UpdateIndex(ctx, namespace, indexDef)
}

func (server *BuiltinServer) DropIndex(ctx context.Context, namespace, index string) error {
	return server.builtin.DropIndex(ctx, namespace, index)
}

func (server *BuiltinServer) PutMeta(ctx context.Context, namespace, key, data string) error {
	return server.builtin.PutMeta(ctx, namespace, key, data)
}

func (server *BuiltinServer) GetMeta(ctx context.Context, namespace, key string) (bindings.RawBuffer, error) {
	return server.builtin.GetMeta(ctx, namespace, key)
}

func (server *BuiltinServer) ModifyItem(ctx context.Context, nsHash int, namespace string, format int, data []byte, mode int, percepts []string, stateToken int) (bindings.RawBuffer, error) {
	return server.builtin.ModifyItem(ctx, nsHash, namespace, format, data, mode, percepts, stateToken)
}

func (server *BuiltinServer) BeginTx(ctx context.Context, namespace string) (bindings.TxCtx, error) {
	return server.builtin.BeginTx(ctx, namespace)
}

func (server *BuiltinServer) CommitTx(txCtx *bindings.TxCtx) (bindings.RawBuffer, error) {
	return server.builtin.CommitTx(txCtx)
}

func (server *BuiltinServer) RollbackTx(txCtx *bindings.TxCtx) error {
	return server.builtin.RollbackTx(txCtx)
}
func (server *BuiltinServer) ModifyItemTx(txCtx *bindings.TxCtx, format int, data []byte, mode int, precepts []string, stateToken int) error {
	return server.builtin.ModifyItemTx(txCtx, format, data, mode, precepts, stateToken)
}

func (server *BuiltinServer) Select(ctx context.Context, query string, asJson bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	return server.builtin.Select(ctx, query, asJson, ptVersions, fetchCount)
}

func (server *BuiltinServer) SelectQuery(ctx context.Context, rawQuery []byte, asJson bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	return server.builtin.SelectQuery(ctx, rawQuery, asJson, ptVersions, fetchCount)
}

func (server *BuiltinServer) DeleteQuery(ctx context.Context, nsHash int, rawQuery []byte) (bindings.RawBuffer, error) {
	return server.builtin.DeleteQuery(ctx, nsHash, rawQuery)
}

func (server *BuiltinServer) UpdateQuery(ctx context.Context, nsHash int, rawQuery []byte) (bindings.RawBuffer, error) {
	return server.builtin.UpdateQuery(ctx, nsHash, rawQuery)
}

func (server *BuiltinServer) Commit(ctx context.Context, namespace string) error {
	return server.builtin.Commit(ctx, namespace)
}

func (server *BuiltinServer) EnableLogger(logger bindings.Logger) {
	server.builtin.EnableLogger(logger)
}

func (server *BuiltinServer) DisableLogger() {
	server.builtin.DisableLogger()
}

func (server *BuiltinServer) Finalize() error {
	if err := server.stopServer(server.shutdownTimeout); err != nil {
		return err
	}
	C.destroy_reindexer_server()
	server.builtin = nil
	server.shutdownTimeout = 0
	return nil
}

func (server *BuiltinServer) Status() (status bindings.Status) {
	return server.builtin.Status()
}

func (server *BuiltinServer) Ping(ctx context.Context) error {
	return server.builtin.Ping(ctx)
}
