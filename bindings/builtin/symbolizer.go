// +build cgosymbolizer

package builtin

// extern void cgoTraceback(void*);
// extern void cgoSymbolizer(void*);
// extern void cgoSignalsInit();
import "C"
import (
	"runtime"
	"unsafe"
)

func init() {
	C.cgoSignalsInit()
	runtime.SetCgoTraceback(0, unsafe.Pointer(C.cgoTraceback), nil, unsafe.Pointer(C.cgoSymbolizer))
}
