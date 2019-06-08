// +build pprof_jemalloc

package pprof

// #include <jemalloc/jemalloc.h>
// #include <stdlib.h>
// #cgo LDFLAGS: -ljemalloc
import "C"
import "unsafe"

type MemStats struct {
	CurrentAllocatedBytes        uint64
	HeapSize                     uint64
	CentralCacheFreeBytes        uint64
	TransferCacheFreeBytes       uint64
	ThreadCacheFreeBytes         uint64
	PageheapFreeBytes            uint64
	PageheapUnmappedBytes        uint64
	MaxTotalThreadCacheBytes     uint64
	CurrentTotalThreadCacheBytes uint64
	AggressiveMemoryDecommit     uint64
}

func GetMemStats() MemStats {

	jemallocInitEpoch()
	return MemStats{
		CurrentAllocatedBytes: jemallocGetVar("stats.allocated"),
		HeapSize:              jemallocGetVar("stats.resident"),
		PageheapFreeBytes:     jemallocGetVar("stats.active") - jemallocGetVar("stats.allocated"),
		PageheapUnmappedBytes: jemallocGetVar("stats.retained"),
	}

}

func jemallocInitEpoch() {
	epoch := C.uint64_t(1)
	sz := C.size_t(unsafe.Sizeof(epoch))
	var cname = C.CString("epoch")
	defer C.free(unsafe.Pointer(cname))
	C.mallctl(cname, unsafe.Pointer(&epoch), &sz, unsafe.Pointer(&epoch), sz)
}

func jemallocGetVar(name string) uint64 {
	var val C.size_t
	sz := C.size_t(unsafe.Sizeof(val))
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	C.mallctl(cname, unsafe.Pointer(&val), &sz, nil, 0)
	return uint64(val)
}
