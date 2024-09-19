package builtin

// extern void cgoTraceback(void*);
// extern void cgoSymbolizer(void*);
// extern void cgoSignalsInit(void);
import "C"
import (
	"os"
	"runtime"
	"unsafe"
)

func init() {
	if os.Getenv("REINDEXER_CGOBACKTRACE") != "" {
		C.cgoSignalsInit()
		runtime.SetCgoTraceback(0, unsafe.Pointer(C.cgoTraceback), nil, unsafe.Pointer(C.cgoSymbolizer))
	}
}
