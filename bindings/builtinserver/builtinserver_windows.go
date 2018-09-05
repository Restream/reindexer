// +build windows
//go:generate cmd /c cd ..\.. && mkdir build & cd build && cmake -G "MinGW Makefiles" -DLINK_RESOURCES=On -DWITH_GPERF=Off -DCMAKE_BUILD_TYPE=Release .. && cmake --build . --target reindexer reindexer_server_library -- -j4

package builtin

// #cgo CXXFLAGS: -std=c++11 -g -O2 -Wall -Wpedantic -Wextra -I../../cpp_src
// #cgo CFLAGS: -std=c99 -g -O2 -Wall -Wpedantic -Wno-unused-variable -I../../cpp_src
// #cgo LDFLAGS: -L${SRCDIR}/../../build/cpp_src/ -L${SRCDIR}/../../build/cpp_src/server/ -lreindexer -lreindexer_server_library -lresources -lleveldb -lsnappy -lstdc++ -g -lshlwapi -ldbghelp
import "C"
