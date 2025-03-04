// +build windows
//go:generate cmd /c cd ..\.. && mkdir build & cd build && cmake -G "MinGW Makefiles" -DCMAKE_BUILD_TYPE=Release .. && cmake --build . --target reindexer -- -j4

package builtin

// #cgo CXXFLAGS: -std=c++20 -g -O2 -Wall -Wpedantic -Wextra -I../../cpp_src
// #cgo CFLAGS: -std=c99 -g -O2 -Wall -Wpedantic -Wno-unused-variable -I../../cpp_src
// #cgo LDFLAGS: -L${SRCDIR}/../../build/cpp_src/ -lreindexer -lleveldb -lsnappy -g -lgomp -lshlwapi -ldbghelp -lws2_32
import "C"
