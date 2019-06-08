// +build !windows
//go:generate sh -c "cd ../.. && mkdir -p build && cd build && cmake -DLINK_RESOURCES=On .. && make reindexer_server_library -j4"

package builtinserver

// #cgo CXXFLAGS: -std=c++11 -g -O2 -Wall -Wpedantic -Wextra -I../../cpp_src
// #cgo CFLAGS: -std=c99 -g -O2 -Wall -Wpedantic -Wno-unused-variable -I../../cpp_src
// #cgo LDFLAGS: -L${SRCDIR}/../../build/cpp_src/ -L${SRCDIR}/../../build/cpp_src/server/ -lreindexer_server_library -lresources -lreindexer -ldl -lleveldb -lsnappy -lstdc++ -lm -g
import "C"
