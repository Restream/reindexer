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
const watcherChSize = 500

var ctxIDCounter uint64 = 1

var resultChPool = sync.Pool{
	New: func() interface{} {
		newCh := make(chan struct{})
		return &newCh
	},
}

//CtxWatcher perfomrs contexts canceling on expiration with some delay
type CtxWatcher struct {
	resultChPool sync.Pool
	cmdChArr     []chan ctxWatcherCmd
	watchDelay   time.Duration
}

//CCtxWrapper is a wrapper over C-context
type CCtxWrapper struct {
	cCtx         C.reindexer_ctx_info
	goCtx        context.Context
	isCancelable bool
	watcherCh    chan ctxWatcherCmd
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
	watcher := CtxWatcher{cmdChArr: make([]chan ctxWatcherCmd, watchersPoolSize), watchDelay: watchDelay}
	for i := 0; i < watchersPoolSize; i++ {
		watcher.cmdChArr[i] = make(chan ctxWatcherCmd, watcherChSize)
		go watcher.watchRoutine(watcher.cmdChArr[i])
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

	var ctxID uint64
	if ctx.Done() != nil {
		ctxID = atomic.AddUint64(&ctxIDCounter, 1)
		if ctxID > maxCtxID {
			if atomic.CompareAndSwapUint64(&ctxIDCounter, ctxID, 2) {
				ctxID = 1
			} else {
				ctxID = atomic.AddUint64(&ctxIDCounter, 1)
			}
		}
	}

	watcherID := rand.Intn(len(watcher.cmdChArr))
	ctxInfo := CCtxWrapper{
		cCtx: C.reindexer_ctx_info{
			ctx_id:       C.uint64_t(ctxID),
			exec_timeout: C.int64_t(execTimeout),
		},
		goCtx:        ctx,
		isCancelable: bool(ctxID > 0),
		watcherCh:    watcher.cmdChArr[watcherID],
	}

	if ctxInfo.isCancelable {
		cmd := ctxWatcherCmd{
			ctx:     ctxInfo,
			cmdCode: cWCCAdd,
		}
		ctxInfo.watcherCh <- cmd
	}

	return ctxInfo, nil
}

//StopWatchOnCtx removes context from watch queue
func (watcher *CtxWatcher) StopWatchOnCtx(ctxInfo CCtxWrapper) {
	if ctxInfo.isCancelable {
		cmd := ctxWatcherCmd{
			ctx:     ctxInfo,
			cmdCode: cWCCRemove,
		}
		ctxInfo.watcherCh <- cmd
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
	for _, ch := range watcher.cmdChArr {
		close(ch)
	}
	return nil
}
