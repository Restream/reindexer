// +build cgosymbolizer

package builtin

// extern void cgoTraceback(void*);
// extern void cgoSymbolizer(void*);
import "C"
import (
	"runtime"
	"unsafe"
)

func init() {
	runtime.SetCgoTraceback(0, unsafe.Pointer(C.cgoTraceback), nil, unsafe.Pointer(C.cgoSymbolizer))
}
