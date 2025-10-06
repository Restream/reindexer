// +build !windows
//go:generate sh -c "cd ../.. && mkdir -p build && cd build && cmake -DGO_BUILTIN_EXPORT_PKG_PATH=\"../bindings/builtin\" .. && make reindexer -j4"

package builtin

// #cgo pkg-config: libreindexer
// #cgo CXXFLAGS: -std=c++20 -g -O2 -Wall -Wpedantic -Wextra
// #cgo CFLAGS: -std=c99 -g -O2 -Wall -Wpedantic -Wno-unused-variable
// #cgo LDFLAGS:  -rdynamic -g
import "C"
