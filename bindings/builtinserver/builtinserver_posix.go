// +build !windows
//go:generate sh -c "cd ../.. && mkdir -p build && cd build && cmake -DLINK_RESOURCES=On -DGO_BUILTIN_SERVER_EXPORT_PKG_PATH=\"../../bindings/builtinserver\" .. && make reindexer_server_library reindexer -j4"

package builtinserver

// #cgo pkg-config: libreindexer_server
// #cgo CXXFLAGS: -std=c++20 -g -O2 -Wall -Wpedantic -Wextra
// #cgo CFLAGS: -std=c99 -g -O2 -Wall -Wpedantic -Wno-unused-variable
// #cgo LDFLAGS:  -rdynamic -g
import "C"
