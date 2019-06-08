// +build !pprof_jemalloc

package pprof

// #include <gperftools/malloc_extension_c.h>
// #include <stdlib.h>
// #cgo LDFLAGS: -ltcmalloc_and_profiler
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

	return MemStats{
		CurrentAllocatedBytes:        tcmallocGetNumericProperty("generic.current_allocated_bytes"),
		HeapSize:                     tcmallocGetNumericProperty("generic.heap_size"),
		CentralCacheFreeBytes:        tcmallocGetNumericProperty("tcmalloc.central_cache_free_bytes"),
		TransferCacheFreeBytes:       tcmallocGetNumericProperty("tcmalloc.transfer_cache_free_bytes"),
		ThreadCacheFreeBytes:         tcmallocGetNumericProperty("tcmalloc.thread_cache_free_bytes"),
		PageheapFreeBytes:            tcmallocGetNumericProperty("tcmalloc.pageheap_free_bytes"),
		PageheapUnmappedBytes:        tcmallocGetNumericProperty("tcmalloc.pageheap_unmapped_bytes"),
		MaxTotalThreadCacheBytes:     tcmallocGetNumericProperty("tcmalloc.max_total_thread_cache_bytes"),
		CurrentTotalThreadCacheBytes: tcmallocGetNumericProperty("tcmalloc.current_total_thread_cache_bytes"),
		AggressiveMemoryDecommit:     tcmallocGetNumericProperty("tcmalloc.aggressive_memory_decommit"),
	}

}

func tcmallocGetNumericProperty(name string) uint64 {
	var val C.size_t
	var cname = C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	C.MallocExtension_GetNumericProperty(cname, &val)
	return uint64(val)
}
