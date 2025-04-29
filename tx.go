package reindexer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	otelattr "go.opentelemetry.io/otel/attribute"

	"github.com/restream/reindexer/v5/bindings"
	"github.com/restream/reindexer/v5/cjson"
)

const maxAsyncRequests = 500
const asyncResponseQueueSize = 2 * maxAsyncRequests
const retriesOnInvalidStateCnt = 1

// Tx is transaction object. Transaction are performs atomic namespace update.
// There are synchronous and async transaction available. To start transaction method `db.BeginTx()` is used.
// This method creates transaction object
type Tx struct {
	namespace    string
	started      bool
	finalized    bool
	db           *reindexerImpl
	ns           *reindexerNamespace
	asyncRspCnt  uint32
	ctx          bindings.TxCtx
	cmplCh       chan modifyInfo
	cmplCond     *sync.Cond
	lock         sync.Mutex
	asyncErr     error
	asyncErrLock sync.RWMutex
}

func newTx(db *reindexerImpl, namespace string, ctx context.Context) (tx *Tx, err error) {
	tx = &Tx{db: db, namespace: namespace}
	if tx.ns, err = tx.db.getNS(tx.namespace); err != nil {
		return nil, err
	}
	if err = tx.startTxCtx(ctx); err != nil {
		return nil, err
	}

	return tx, nil
}

func (tx *Tx) startTx() (err error) {
	return tx.startTxCtx(context.Background())
}

func (tx *Tx) startTxCtx(ctx context.Context) (err error) {
	err = tx.checkFinalization()
	if err != nil {
		return
	}

	if tx.started {
		return nil
	}

	tx.asyncRspCnt = 0
	tx.started = true

	tx.ctx, err = tx.db.binding.BeginTx(ctx, tx.namespace)
	if err != nil {
		return err
	}

	tx.ctx.UserCtx = ctx
	tx.cmplCh = nil
	tx.cmplCond = nil

	return nil
}

func (tx *Tx) startAsyncRoutines() error {
	if tx.cmplCh == nil {
		tx.cmplCh = make(chan modifyInfo, asyncResponseQueueSize)
		tx.cmplCond = sync.NewCond(&tx.lock)
		go tx.cmplHandlingRoutine(tx.cmplCh)
	}

	return tx.checkReqCount()
}

func (tx *Tx) Insert(item interface{}, precepts ...string) error {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.Insert", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.Insert", tx.namespace)).ObserveDuration()
	}

	err := tx.startTx()
	if err != nil {
		return err
	}

	return tx.modifyInternal(item, nil, modeInsert, precepts...)
}

func (tx *Tx) Update(item interface{}, precepts ...string) error {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.Update", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.Update", tx.namespace)).ObserveDuration()
	}

	err := tx.startTx()
	if err != nil {
		return err
	}

	return tx.modifyInternal(item, nil, modeUpdate, precepts...)
}

// Upsert (Insert or Update) item to namespace
func (tx *Tx) Upsert(item interface{}, precepts ...string) error {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.Upsert", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.Upsert", tx.namespace)).ObserveDuration()
	}

	err := tx.startTx()
	if err != nil {
		return err
	}

	return tx.modifyInternal(item, nil, modeUpsert, precepts...)
}

// UpsertJSON (Insert or Update) item to namespace
func (tx *Tx) UpsertJSON(json []byte, precepts ...string) error {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.UpsertJSON", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.UpsertJSON", tx.namespace)).ObserveDuration()
	}

	err := tx.startTx()
	if err != nil {
		return err
	}

	return tx.modifyInternal(nil, json, modeUpsert, precepts...)
}

// Delete - remove item by id from namespace
func (tx *Tx) Delete(item interface{}, precepts ...string) error {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.Delete", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.Delete", tx.namespace)).ObserveDuration()
	}

	err := tx.startTx()
	if err != nil {
		return err
	}

	return tx.modifyInternal(item, nil, modeDelete, precepts...)
}

// DeleteJSON - remove item by id from namespace
func (tx *Tx) DeleteJSON(json []byte, precepts ...string) error {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.DeleteJSON", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.DeleteJSON", tx.namespace)).ObserveDuration()
	}

	err := tx.startTx()
	if err != nil {
		return err
	}

	return tx.modifyInternal(nil, json, modeDelete, precepts...)
}

func (tx *Tx) handleRecoverFromAsyncOp(rec interface{}) error {
	tx.cmplCond.L.Lock()
	atomic.AddUint32(&tx.asyncRspCnt, ^uint32(0))
	tx.cmplCond.Broadcast()
	tx.cmplCond.L.Unlock()
	switch x := rec.(type) {
	case error:
		return x
	default:
		return bindings.NewError("Unknown panic type", ErrCodeLogic)
	}
}

// InsertAsync Insert item to namespace. Calls completion on result
func (tx *Tx) InsertAsync(item interface{}, cmpl bindings.Completion, precepts ...string) (err error) {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.InsertAsync", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.InsertAsync", tx.namespace)).ObserveDuration()
	}

	err = tx.startTx()
	if err != nil {
		return err
	}

	if err = tx.startAsyncRoutines(); err != nil {
		return err
	}

	defer func() {
		if rec := recover(); rec != nil {
			err = tx.handleRecoverFromAsyncOp(rec)
		}
	}()

	return tx.modifyInternalAsync(item, nil, modeInsert, cmpl, retriesOnInvalidStateCnt, precepts...)
}

// UpdateAsync Update item to namespace. Calls completion on result
func (tx *Tx) UpdateAsync(item interface{}, cmpl bindings.Completion, precepts ...string) (err error) {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.UpdateAsync", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.UpdateAsync", tx.namespace)).ObserveDuration()
	}

	err = tx.startTx()
	if err != nil {
		return err
	}

	if err = tx.startAsyncRoutines(); err != nil {
		return err
	}

	defer func() {
		if rec := recover(); rec != nil {
			err = tx.handleRecoverFromAsyncOp(rec)
		}
	}()

	return tx.modifyInternalAsync(item, nil, modeUpdate, cmpl, retriesOnInvalidStateCnt, precepts...)
}

// UpsertAsync (Insert or Update) item to namespace. Calls completion on result
func (tx *Tx) UpsertAsync(item interface{}, cmpl bindings.Completion, precepts ...string) (err error) {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.UpsertAsync", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.UpsertAsync", tx.namespace)).ObserveDuration()
	}

	err = tx.startTx()
	if err != nil {
		return err
	}

	if err = tx.startAsyncRoutines(); err != nil {
		return err
	}

	defer func() {
		if rec := recover(); rec != nil {
			err = tx.handleRecoverFromAsyncOp(rec)
		}
	}()

	return tx.modifyInternalAsync(item, nil, modeUpsert, cmpl, retriesOnInvalidStateCnt, precepts...)
}

// UpsertJSONAsync (Insert or Update) item to index. Calls completion on result
func (tx *Tx) UpsertJSONAsync(json []byte, cmpl bindings.Completion, precepts ...string) error {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.UpsertJSONAsync", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.UpsertJSONAsync", tx.namespace)).ObserveDuration()
	}

	err := tx.startTx()
	if err != nil {
		return err
	}

	if err = tx.startAsyncRoutines(); err != nil {
		return err
	}

	return tx.modifyInternalAsync(nil, json, modeUpsert, cmpl, retriesOnInvalidStateCnt, precepts...)
}

// DeleteAsync - remove item by id from namespace. Calls completion on result
func (tx *Tx) DeleteAsync(item interface{}, cmpl bindings.Completion, precepts ...string) (err error) {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.DeleteAsync", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.DeleteAsync", tx.namespace)).ObserveDuration()
	}

	err = tx.startTx()
	if err != nil {
		return err
	}

	if err = tx.startAsyncRoutines(); err != nil {
		return err
	}

	defer func() {
		if rec := recover(); rec != nil {
			err = tx.handleRecoverFromAsyncOp(rec)
		}
	}()

	return tx.modifyInternalAsync(item, nil, modeDelete, cmpl, retriesOnInvalidStateCnt, precepts...)
}

// DeleteJSONAsync - remove item by id from namespace. Calls completion on result
func (tx *Tx) DeleteJSONAsync(json []byte, cmpl bindings.Completion, precepts ...string) error {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.DeleteJSONAsync", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.DeleteJSONAsync", tx.namespace)).ObserveDuration()
	}

	err := tx.startTx()
	if err != nil {
		return err
	}

	if err = tx.startAsyncRoutines(); err != nil {
		return err
	}

	return tx.modifyInternalAsync(nil, json, modeDelete, cmpl, retriesOnInvalidStateCnt, precepts...)
}

func (tx *Tx) checkFinalization() error {
	if tx.finalized {
		return bindings.NewError("Tx is already finalized", bindings.ErrLogic)
	}
	return nil
}

// CommitWithCount apply changes, and return count of changed items
func (tx *Tx) CommitWithCount() (count int, err error) {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.CommitWithCount", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.CommitWithCount", tx.namespace)).ObserveDuration()
	}

	if !tx.started {
		return 0, nil
	}

	if count, err = tx.commitInternal(); err != nil {
		return
	}

	return
}

// Commit - apply changes. Commit also waits for all async operations done, and then apply changes.
// if any error occurred during prepare process, then tx.Commit should
// return an error. So it is enough, to check error returned by Commit - to be sure
// that all data has been successfully committed or not.
func (tx *Tx) Commit() error {
	_, err := tx.CommitWithCount()
	return err
}

// MustCommit apply changes and starts panic on errors
func (tx *Tx) MustCommit() int {
	count, err := tx.CommitWithCount()
	if err != nil {
		panic(err)
	}

	return count
}

// AwaitResults awaits async requests completion
func (tx *Tx) AwaitResults() *Tx {
	if tx.cmplCh != nil && atomic.LoadUint32(&tx.asyncRspCnt) > 0 {
		tx.cmplCond.L.Lock()
		for atomic.LoadUint32(&tx.asyncRspCnt) > 0 {
			tx.cmplCond.Wait()
		}
		tx.cmplCond.L.Unlock()
	}
	return tx
}

// Query creates Query in transaction for Update or Delete or Read
// Read-committed isolation is available for read operations.
// Changes made in active transaction is invisible to current and another transactions.

func (tx *Tx) Query() *Query {
	return tx.db.queryTx(tx.namespace, tx)
}

// finalize transaction
func (tx *Tx) finalize() {
	if tx.cmplCh != nil {
		close(tx.cmplCh)
		tx.cmplCh = nil
	}
	if tx.ctx.Result != nil {
		tx.ctx.Result.Free()
		tx.ctx.Result = nil
	}
	tx.finalized = true
}

func (tx *Tx) modifyInternal(item interface{}, json []byte, mode int, precepts ...string) (err error) {
	if item == nil && json == nil {
		return fmt.Errorf("rq: nil value in transaction item modify call for '%s' namespace", tx.namespace)
	}

	for tryCount := 0; tryCount < 2; tryCount++ {
		err := func() error {
			ser := cjson.NewPoolSerializer()
			defer ser.Close()
			format := 0
			stateToken := 0

			if format, stateToken, err = packItem(tx.ns, item, json, ser); err != nil {
				return err
			}

			return tx.db.binding.ModifyItemTx(&tx.ctx, format, ser.Bytes(), mode, precepts, stateToken)
		}()

		if err != nil {
			rerr, ok := err.(bindings.Error)
			if ok && rerr.Code() == bindings.ErrStateInvalidated {
				it := tx.db.query(tx.ns.name).Limit(0).ExecCtx(tx.ctx.UserCtx)
				it.Close()
				err = rerr
				continue
			}
			return err
		}
		return nil
	}
	return nil
}

type modifyInfo struct {
	err      error
	cmpl     bindings.Completion
	item     interface{}
	json     []byte
	mode     int
	precepts []string
	retries  uint32
}

func (tx *Tx) setAsyncError(err error) {
	if err != nil {
		tx.asyncErrLock.Lock()
		if tx.asyncErr == nil {
			tx.asyncErr = err
		}
		tx.asyncErrLock.Unlock()
	}
}

func (tx *Tx) cmplHandlingRoutine(cmplCh chan modifyInfo) {
	for {
		if modifyRes, ok := <-cmplCh; ok {
			err := modifyRes.err
			if err != nil {
				rerr, ok := err.(bindings.Error)
				if ok && rerr.Code() == bindings.ErrStateInvalidated && modifyRes.retries > 0 {
					it := tx.db.query(tx.ns.name).Limit(0).ExecCtx(tx.ctx.UserCtx)
					err = it.Error()
					it.Close()
				}
			}
			if err == nil && modifyRes.retries > 0 {
				tx.modifyInternalAsync(modifyRes.item, modifyRes.json, modifyRes.mode, modifyRes.cmpl, modifyRes.retries-1, modifyRes.precepts...)
				continue
			}
			modifyRes.cmpl(err)

			tx.setAsyncError(err)
			tx.cmplCond.L.Lock()
			atomic.AddUint32(&tx.asyncRspCnt, ^uint32(0))
			tx.cmplCond.Broadcast()
			tx.cmplCond.L.Unlock()
		} else {
			return
		}
	}
}

func (tx *Tx) modifyInternalAsync(item interface{}, json []byte, mode int, cmpl bindings.Completion, retriesRemain uint32, precepts ...string) (err error) {
	internalCmpl := func(buf bindings.RawBuffer, err error) {
		if buf != nil {
			buf.Free()
		}
		if err != nil {
			tx.cmplCh <- modifyInfo{err: err, cmpl: cmpl, item: item, json: json, mode: mode, precepts: precepts, retries: retriesRemain}
		} else {
			tx.cmplCh <- modifyInfo{err: nil, cmpl: cmpl}
		}
	}

	if item == nil && json == nil {
		err := fmt.Errorf("rq: nil value in transaction item modify async call for '%s' namespace", tx.namespace)
		internalCmpl(nil, err)
		return err
	}

	ser := cjson.NewPoolSerializer()
	defer ser.Close()
	format := 0
	stateToken := 0

	if format, stateToken, err = packItem(tx.ns, item, json, ser); err != nil {
		internalCmpl(nil, err)
		return err
	}

	tx.db.binding.ModifyItemTxAsync(&tx.ctx, format, ser.Bytes(), mode, precepts, stateToken, internalCmpl)

	return nil
}

func (tx *Tx) checkReqCount() error {
	for {
		asyncRspCnt := atomic.LoadUint32(&tx.asyncRspCnt)
		if asyncRspCnt < maxAsyncRequests {
			if atomic.CompareAndSwapUint32(&tx.asyncRspCnt, asyncRspCnt, asyncRspCnt+1) {
				tx.asyncErrLock.RLock()
				err := tx.asyncErr
				tx.asyncErrLock.RUnlock()
				if err != nil {
					tx.cmplCond.L.Lock()
					atomic.AddUint32(&tx.asyncRspCnt, ^uint32(0))
					tx.cmplCond.Broadcast()
					tx.cmplCond.L.Unlock()
				}
				return err
			}
		} else {
			tx.cmplCond.L.Lock()
			for atomic.LoadUint32(&tx.asyncRspCnt) == maxAsyncRequests {
				tx.cmplCond.Wait()
			}
			tx.cmplCond.L.Unlock()
		}
	}
}

// Commit apply changes
func (tx *Tx) commitInternal() (count int, err error) {
	count = 0
	err = tx.checkFinalization()
	if err != nil {
		return
	}

	tx.AwaitResults()
	defer tx.finalize()
	if tx.asyncErr != nil {
		asyncErr := tx.asyncErr
		err = tx.db.binding.RollbackTx(&tx.ctx)
		if err == nil {
			err = asyncErr
		}
		return 0, err
	}

	out, err := tx.db.binding.CommitTx(&tx.ctx)
	if err != nil {
		return 0, err
	}
	defer out.Free()

	rdSer := newSerializer(out.GetBuf())

	rawQueryParams := rdSer.readRawQueryParams(func(nsid int) {
		tx.ns.cjsonState.ReadPayloadType(&rdSer.Serializer, tx.db.binding, tx.ns.name)
	})

	if rawQueryParams.count == 0 {
		return
	}

	count = rawQueryParams.count
	for i := 0; i < rawQueryParams.count; i++ {
		_ = rdSer.readRawtItemParams(rawQueryParams.shardId)
	}

	return
}

// Rollback transaction.
// It is safe to call Rollback after Commit
func (tx *Tx) Rollback() error {
	if tx.db.otelTracer != nil {
		defer tx.db.startTracingSpan(tx.ctx.UserCtx, "Reindexer.Tx.Rollback", otelattr.String("rx.ns", tx.namespace)).End()
	}

	if tx.db.promMetrics != nil {
		defer prometheus.NewTimer(tx.db.promMetrics.clientCallsLatency.WithLabelValues("Tx.Rollback", tx.namespace)).ObserveDuration()
	}

	tx.AwaitResults()
	tx.asyncErr = nil
	defer tx.finalize()
	return tx.db.binding.RollbackTx(&tx.ctx)
}
