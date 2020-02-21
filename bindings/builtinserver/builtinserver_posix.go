// +build !windows
//go:generate sh -c "cd ../.. && mkdir -p build && cd build && cmake -DGO_BUILTIN_SERVER_EXPORT_PKG_PATH=\"../../bindings/builtinserver\" .. && make reindexer_server_library reindexer -j8"

package builtinserver

// #cgo CXXFLAGS: -std=c++11 -g -O2 -Wall -Wpedantic -Wextra -I../../cpp_src 
// #cgo CFLAGS: -std=c99 -g -O2 -Wall -Wpedantic -Wno-unused-variable -I../../cpp_src 
// #cgo LDFLAGS: -L${SRCDIR}/../../build/cpp_src/ -L${SRCDIR}/../../build/cpp_src/server/ -lreindexer_server_library  -lreindexer -lleveldb -lsnappy -ldl -lreindexer_server_resources -lm -lstdc++ -g
import "C"
