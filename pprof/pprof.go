package pprof

// #include <stdlib.h>
// #include "pprof.h"
import "C"

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

func init() {
	http.Handle("/debug/cgo/pprof/heap", http.HandlerFunc(ProfileHeap))
	http.Handle("/debug/cgo/pprof/cmdline", http.HandlerFunc(Cmdline))
	http.Handle("/debug/cgo/pprof/symbol", http.HandlerFunc(Symbol))
	http.Handle("/debug/cgo/symbolz", http.HandlerFunc(Symbol))
	// CPU profiling for cgo doesn't work via gperf_tools library. Use go pprof and symbolizer with REINDEXER_CGOBACKTRACE instead
	//http.Handle("/debug/cgo/pprof/profile", http.HandlerFunc(Profile))
}

// Cmdline responds with the running program's
// command line, with arguments separated by NUL bytes.
// The package initialization registers it as /debug/pprof/cmdline.
func Cmdline(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, strings.Join(os.Args, "\x00"))
}

func sleep(w http.ResponseWriter, d time.Duration) {
	var clientGone <-chan bool
	if cn, ok := w.(http.CloseNotifier); ok {
		clientGone = cn.CloseNotify()
	}
	select {
	case <-time.After(d):
	case <-clientGone:
	}
}

func ProfileHeap(w http.ResponseWriter, r *http.Request) {

	w.Write([]byte(getHeapProfile()))
}

// Symbol looks up the program counters listed in the request,
// responding with a table mapping program counters to function names.
// The package initialization registers it as /debug/pprof/symbol.
func Symbol(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	// We have to read the whole POST body before
	// writing any output. Buffer the output here.
	var buf bytes.Buffer

	// We don't know how many symbols we have, but we
	// do have symbol information. Pprof only cares whether
	// this number is 0 (no symbols available) or > 0.
	fmt.Fprintf(&buf, "num_symbols: 1\n")

	var b *bufio.Reader
	if r.Method == "POST" {
		b = bufio.NewReader(r.Body)
	} else {
		b = bufio.NewReader(strings.NewReader(r.URL.RawQuery))
	}

	for {
		word, err := b.ReadSlice('+')
		if err == nil {
			word = word[0 : len(word)-1] // trim +
		}
		pc, _ := strconv.ParseUint(string(word), 0, 64)
		if pc != 0 {
			f := resolveSymbol(uintptr(pc))
			if f != "" {
				fmt.Fprintf(&buf, "%#x %s\n", pc, f)
			}
		}

		// Wait until here to check for err; the last
		// symbol will have an err because it doesn't end in +.
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(&buf, "reading request: %v\n", err)
			}
			break
		}
	}
	w.Write(buf.Bytes())
}

func getHeapProfile() string {
	cstr := C.cgo_pprof_get_heapprofile()
	profile := C.GoString(cstr)

	C.free(unsafe.Pointer(cstr))
	return profile
}

func resolveSymbol(addr uintptr) string {
	cstr := C.cgo_pprof_lookup_symbol(unsafe.Pointer(addr))
	defer C.free(unsafe.Pointer(cstr))
	symbol := C.GoString(cstr)

	// Make output beauty:
	// trucate long undemangled names
	if strings.HasPrefix(symbol, "_ZN") && len(symbol) > 20 {
		return symbol[:20] + "..."
	}

	tmpl := 0
	out := make([]byte, 0, len(symbol))
	for p := 0; p < len(symbol); p++ {
		// strip out std:: and std::__1
		if strings.HasPrefix(symbol[p:], "std::__1::") {
			p += 9
			continue
		}
		if strings.HasPrefix(symbol[p:], "std::") {
			p += 4
			continue
		}
		// strip out c++ templates args
		c := symbol[p]
		switch c {
		case '<':
			tmpl++
		case '>':
			tmpl--
		default:
			if tmpl == 0 {
				out = append(out, byte(c))
			}
		}
	}
	return string(out)
}
