package builtin

// #include "core/cbinding/reindexer_c.h"
// #include "reindexer_cgo.h"
// #include <stdlib.h>
import "C"
import (
	"context"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const maxCtxID = math.MaxUint64 & (math.MaxUint64 >> 3) //Discard 3 most significant bits for cgo flags
const ctxWatcherPeriod = time.Millisecond * 20
const watcherChSize = 1000

var ctxIDCounter uint64 = 1

var resultChPool = sync.Pool{
	New: func() interface{} {
		newCh := make(chan struct{})
		return &newCh
	},
}

//Wrapper of 'chan ctxWatcherCmd' for correct processing of the closed chan if StopWatchOnCtx called after Finalize
type watcherTSWrapper struct {
	ch       chan ctxWatcherCmd
	isClosed bool
	mtx      sync.RWMutex
}

//CtxWatcher perfomrs contexts canceling on expiration with some delay
type CtxWatcher struct {
	resultChPool sync.Pool
	cmdChArr     []watcherTSWrapper
	watchDelay   time.Duration
}

//CCtxWrapper is a wrapper over C-context
type CCtxWrapper struct {
	cCtx         C.reindexer_ctx_info
	goCtx        context.Context
	isCancelable bool
	watcherCh    *watcherTSWrapper
}

type ctxWatcherCmdCode int

const (
	cWCCAdd    ctxWatcherCmdCode = 0
	cWCCRemove ctxWatcherCmdCode = 1
)

type ctxWatcherCmd struct {
	ctx     CCtxWrapper
	cmdCode ctxWatcherCmdCode
}

// NewCtxWatcher creates new CtxWatcher
func NewCtxWatcher(watchersPoolSize int, watchDelay time.Duration) *CtxWatcher {
	if watchersPoolSize < 1 {
		watchersPoolSize = defWatchersPoolSize
	}
	watcher := CtxWatcher{cmdChArr: make([]watcherTSWrapper, watchersPoolSize), watchDelay: watchDelay}
	for i := 0; i < watchersPoolSize; i++ {
		watcher.cmdChArr[i] = watcherTSWrapper{ch: make(chan ctxWatcherCmd, watcherChSize), isClosed: false}
		go watcher.watchRoutine(watcher.cmdChArr[i].ch)
	}
	return &watcher
}

func (watcher *CtxWatcher) waitOnContext(ctx context.Context, ctxInfo C.reindexer_ctx_info, awaitCh chan struct{}) {
for_loop:
	for {
		select {
		case <-awaitCh:
			break for_loop
		default:
		}

		select {
		case <-awaitCh:
			break for_loop
		case <-ctx.Done():
			var ret C.reindexer_error
			if err := ctx.Err(); err == context.DeadlineExceeded {
				ret = C.reindexer_cancel_context(ctxInfo, C.cancel_on_timeout)
			} else {
				ret = C.reindexer_cancel_context(ctxInfo, C.cancel_expilicitly)
			}
			if ret.code == 0 {
				<-awaitCh
				break for_loop
			}
			runtime.Gosched()
		}
	}
	watcher.putAwaitCh(awaitCh)
}

func (watcher *CtxWatcher) getAwaitCh() chan struct{} {
	return *resultChPool.Get().(*chan struct{})
}

func (watcher *CtxWatcher) putAwaitCh(ch chan struct{}) {
	resultChPool.Put(&ch)
}

//StartWatchOnCtx creates context wrapper and puts it to the watch queue
func (watcher *CtxWatcher) StartWatchOnCtx(ctx context.Context) (CCtxWrapper, error) {
	if ctx.Done() != nil {
		var execTimeout int64
		if err := ctx.Err(); err != nil {
			return CCtxWrapper{}, err
		}

		if deadline, ok := ctx.Deadline(); ok {
			execTimeout = int64(deadline.Sub(time.Now()) / time.Millisecond)
			if execTimeout <= 0 {
				return CCtxWrapper{}, context.DeadlineExceeded
			}
		}

		ctxID := atomic.AddUint64(&ctxIDCounter, 1)
		if ctxID > maxCtxID {
			if atomic.CompareAndSwapUint64(&ctxIDCounter, ctxID, 2) {
				ctxID = 1
			} else {
				ctxID = atomic.AddUint64(&ctxIDCounter, 1)
			}
		}

		watcherID := rand.Intn(len(watcher.cmdChArr))
		watcher.cmdChArr[watcherID].mtx.RLock()
		defer watcher.cmdChArr[watcherID].mtx.RUnlock()
		ctxInfo := CCtxWrapper{
			cCtx: C.reindexer_ctx_info{
				ctx_id:       C.uint64_t(ctxID),
				exec_timeout: C.int64_t(execTimeout),
			},
			goCtx:        ctx,
			isCancelable: true,
			watcherCh:    &watcher.cmdChArr[watcherID],
		}

		ctxInfo.watcherCh.ch <- ctxWatcherCmd{
			ctx:     ctxInfo,
			cmdCode: cWCCAdd,
		}

		return ctxInfo, nil
	}

	return CCtxWrapper{
		cCtx: C.reindexer_ctx_info{
			ctx_id:       0,
			exec_timeout: 0,
		},
		goCtx:        nil,
		isCancelable: false,
		watcherCh:    nil,
	}, nil
}

//StopWatchOnCtx removes context from watch queue
func (watcher *CtxWatcher) StopWatchOnCtx(ctxInfo CCtxWrapper) {
	if ctxInfo.isCancelable {
		ctxInfo.watcherCh.mtx.RLock()
		defer ctxInfo.watcherCh.mtx.RUnlock()
		if !ctxInfo.watcherCh.isClosed {
			ctxInfo.watcherCh.ch <- ctxWatcherCmd{
				ctx:     ctxInfo,
				cmdCode: cWCCRemove,
			}
		}
	}
}

func (watcher *CtxWatcher) watchRoutine(watchCh chan ctxWatcherCmd) {
	type ctxWatcherNode struct {
		ctx        CCtxWrapper
		watchStart time.Time
		awaitCh    chan struct{}
	}
	contexts := make(map[uint64]ctxWatcherNode)
	ticker := time.NewTicker(ctxWatcherPeriod)
	for {
		select {
		case cmd, ok := <-watchCh:
			if !ok {
				return
			}
			ctxID := uint64(cmd.ctx.cCtx.ctx_id)
			if cmd.cmdCode == cWCCAdd {
				contexts[ctxID] = ctxWatcherNode{
					ctx:        cmd.ctx,
					watchStart: time.Now().Add(watcher.watchDelay),
					awaitCh:    nil,
				}
			} else {
				if node, ok := contexts[ctxID]; ok {
					if node.awaitCh != nil {
						node.awaitCh <- struct{}{}
					}
					delete(contexts, ctxID)
				}
			}
		case now := <-ticker.C:
			for ctxID, node := range contexts {
				now = now.Add(ctxWatcherPeriod)
				if now.After(node.watchStart) && node.awaitCh == nil {
					ch := watcher.getAwaitCh()
					contexts[ctxID] = ctxWatcherNode{
						ctx:        node.ctx,
						watchStart: node.watchStart,
						awaitCh:    ch,
					}
					go watcher.waitOnContext(node.ctx.goCtx, node.ctx.cCtx, ch)
				}
			}
		}

	}
}

//Finalize CtxWatcher
func (watcher *CtxWatcher) Finalize() error {
	for idx := range watcher.cmdChArr {
		chWr := &watcher.cmdChArr[idx]

		chWr.mtx.Lock()
		defer chWr.mtx.Unlock()
		chWr.isClosed = true
		close(chWr.ch)
	}
	return nil
}
