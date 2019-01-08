package builtinserver

// #include "server/cbinding/server_c.h"
// #include <stdlib.h>
import "C"
import (
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

func (server *BuiltinServer) OpenNamespace(namespace string, enableStorage, dropOnFileFormatError bool) error {
	return server.builtin.OpenNamespace(namespace, enableStorage, dropOnFileFormatError)
}

func (server *BuiltinServer) CloseNamespace(namespace string) error {
	return server.builtin.CloseNamespace(namespace)
}

func (server *BuiltinServer) DropNamespace(namespace string) error {
	return server.builtin.DropNamespace(namespace)
}

func (server *BuiltinServer) EnableStorage(namespace string) error {
	return server.builtin.EnableStorage(namespace)
}

func (server *BuiltinServer) AddIndex(namespace string, indexDef bindings.IndexDef) error {
	return server.builtin.AddIndex(namespace, indexDef)
}

func (server *BuiltinServer) UpdateIndex(namespace string, indexDef bindings.IndexDef) error {
	return server.builtin.UpdateIndex(namespace, indexDef)
}

func (server *BuiltinServer) DropIndex(namespace, index string) error {
	return server.builtin.DropIndex(namespace, index)
}

func (server *BuiltinServer) PutMeta(namespace, key, data string) error {
	return server.builtin.PutMeta(namespace, key, data)
}

func (server *BuiltinServer) GetMeta(namespace, key string) (bindings.RawBuffer, error) {
	return server.builtin.GetMeta(namespace, key)
}

func (server *BuiltinServer) ModifyItem(nsHash int, namespace string, format int, data []byte, mode int, percepts []string, stateToken int) (bindings.RawBuffer, error) {
	return server.builtin.ModifyItem(nsHash, namespace, format, data, mode, percepts, stateToken)
}

func (server *BuiltinServer) BeginTx(namespace string) (bindings.TxCtx, error) {
	return server.builtin.BeginTx(namespace)
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

func (server *BuiltinServer) Select(query string, withItems bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	return server.builtin.Select(query, withItems, ptVersions, fetchCount)
}

func (server *BuiltinServer) SelectQuery(rawQuery []byte, withItems bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	return server.builtin.SelectQuery(rawQuery, withItems, ptVersions, fetchCount)
}

func (server *BuiltinServer) DeleteQuery(nsHash int, rawQuery []byte) (bindings.RawBuffer, error) {
	return server.builtin.DeleteQuery(nsHash, rawQuery)
}

func (server *BuiltinServer) Commit(namespace string) error {
	return server.builtin.Commit(namespace)
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

func (server *BuiltinServer) Ping() error {
	return server.builtin.Ping()
}
