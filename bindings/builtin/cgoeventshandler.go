package builtin

// #include "core/cbinding/reindexer_c.h"
// #include "reindexer_cgo.h"
// #include <stdlib.h>
import "C"

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/restream/reindexer/v5/bindings"
)

type CGOEventsHandler struct {
	eventsHander      bindings.EventsHandler
	wg                sync.WaitGroup
	mtx               sync.Mutex
	terminate         int32
	hasReadingRoutine bool
}

func NewCGOEventsHandler(eh bindings.EventsHandler) *CGOEventsHandler {
	if eh != nil {
		handler := CGOEventsHandler{eventsHander: eh}
		return &handler
	}
	return nil
}

func (h *CGOEventsHandler) Subscribe(rx C.uintptr_t, opts *bindings.SubscriptionOptions) error {
	bEventsSubOpts, err := json.Marshal(opts)
	if err != nil {
		return err
	}
	sEventsSubOpts := string(bEventsSubOpts)

	h.mtx.Lock()
	defer h.mtx.Unlock()

	err = err2go(C.reindexer_subscribe(rx, str2c(sEventsSubOpts)))
	if err == nil && !h.hasReadingRoutine {
		h.wg.Add(1)
		go h.eventsReadingRoutine(rx)
		h.hasReadingRoutine = true
	}
	return err
}

func (h *CGOEventsHandler) Unsubscribe(rx C.uintptr_t) error {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	atomic.StoreInt32(&h.terminate, 1)
	err := err2go(C.reindexer_unsubscribe(rx))
	h.wg.Wait()
	atomic.StoreInt32(&h.terminate, 0)
	h.hasReadingRoutine = false
	return err
}

type RawStackCBuffer struct {
	cbuf C.reindexer_buffer
}

func (buf *RawStackCBuffer) Free() {
}

func (buf *RawStackCBuffer) GetBuf() []byte {
	if buf.cbuf.data == nil || buf.cbuf.len == 0 {
		return nil
	}
	length := int(buf.cbuf.len)
	return (*[1 << 30]byte)(unsafe.Pointer(buf.cbuf.data))[:length:length]
}

// Converts C-array into Go buffer. C-array will NOT be deallocated automatically
func arr_ret2go_static(ret C.reindexer_array_ret) ([]RawStackCBuffer, error) {
	if ret.err_code != 0 {
		defer C.free(unsafe.Pointer(uintptr(ret.data)))
		if err := ctxErr(int(ret.err_code)); err != nil {
			return nil, err
		}
		return nil, bindings.NewError("rq:"+C.GoString((*C.char)(unsafe.Pointer(uintptr(ret.data)))), int(ret.err_code))
	}

	sz := uint32(ret.out_size)
	if ret.out_buffers == nil || sz == 0 {
		return nil, nil
	}
	cslice := (*[1 << 30]C.reindexer_buffer)(unsafe.Pointer(ret.out_buffers))[:sz:sz]
	rbufs := make([]RawStackCBuffer, len(cslice))
	for i := 0; i < len(cslice); i++ {
		rbufs[i].cbuf = cslice[i]
	}
	return rbufs, nil
}

func (h *CGOEventsHandler) eventsReadingRoutine(rx C.uintptr_t) {
	defer h.wg.Done()

	const kEventsCbufsCount = 2048
	var cbufs [kEventsCbufsCount]C.reindexer_buffer

	for {
		terminate := atomic.LoadInt32(&h.terminate)
		if terminate == 1 {
			return
		}
		bufs, err := arr_ret2go_static(C.reindexer_read_events(rx, &cbufs[0], C.uint32_t(kEventsCbufsCount)))
		terminate = atomic.LoadInt32(&h.terminate)
		if terminate == 1 {
			return
		}
		if err == nil {
			for i := 0; i < len(bufs); i++ {
				h.eventsHander.OnEvent(&bufs[i])
			}
			if len(bufs) > 0 {
				C.reindexer_erase_events(rx, C.uint32_t(len(bufs)))
			}
		} else {
			h.eventsHander.OnError(err)
		}
		if len(bufs) == 0 {
			time.Sleep(25 * time.Millisecond)
		}
	}
}
