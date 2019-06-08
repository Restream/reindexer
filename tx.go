package reindexer

import (
	"context"
	"time"

	"github.com/restream/reindexer/bindings"
	"github.com/restream/reindexer/cjson"
)

type Tx struct {
	namespace        string
	started          bool
	forceCommitCount uint32
	db               *reindexerImpl
	ns               *reindexerNamespace
	counter          uint32
	ctx              bindings.TxCtx
}

func newTx(db *reindexerImpl, namespace string, ctx context.Context) (*Tx, error) {
	tx := &Tx{db: db, namespace: namespace, forceCommitCount: 0}
	tx.startTxCtx(ctx)
	tx.ns, _ = tx.db.getNS(tx.namespace)

	return tx, nil
}

func newTxAutocommit(db *reindexerImpl, namespace string, forceCommitCount uint32) (*Tx, error) {
	tx := &Tx{db: db, namespace: namespace, forceCommitCount: forceCommitCount}
	tx.startTx()
	tx.ns, _ = tx.db.getNS(tx.namespace)

	return tx, nil
}

func (tx *Tx) startTx() (err error) {
	return tx.startTxCtx(context.Background())
}

func (tx *Tx) startTxCtx(ctx context.Context) (err error) {
	if tx.started {
		return nil
	}
	tx.counter = 0
	tx.started = true
	tx.ctx, err = tx.db.binding.BeginTx(ctx, tx.namespace)
	if err != nil {
		return err
	}
	tx.ctx.UserCtx = ctx
	return nil
}

func (tx *Tx) Insert(item interface{}, precepts ...string) error {
	tx.startTx()
	return tx.modifyInternal(item, nil, modeInsert, precepts...)
}

func (tx *Tx) Update(item interface{}, precepts ...string) error {
	tx.startTx()
	return tx.modifyInternal(item, nil, modeUpdate, precepts...)
}

// Upsert (Insert or Update) item to index
func (tx *Tx) Upsert(item interface{}, precepts ...string) error {
	tx.startTx()
	return tx.modifyInternal(item, nil, modeUpsert, precepts...)
}

// UpsertJSON (Insert or Update) item to index
func (tx *Tx) UpsertJSON(json []byte, precepts ...string) error {
	tx.startTx()
	return tx.modifyInternal(nil, json, modeUpsert, precepts...)
}

// Delete - remove item by id from namespace
func (tx *Tx) Delete(item interface{}, precepts ...string) error {
	tx.startTx()
	return tx.modifyInternal(item, nil, modeDelete, precepts...)

}

// DeleteJSON - remove item by id from namespace
func (tx *Tx) DeleteJSON(json []byte, precepts ...string) error {
	tx.startTx()
	return tx.modifyInternal(nil, json, modeDelete, precepts...)
}

// Commit apply changes with count
func (tx *Tx) CommitWithCount(updatedAt *time.Time) (count int, err error) {
	if !tx.started {
		return 0, nil
	}
	if count, err = tx.commitInternal(); err != nil {
		return
	}
	if updatedAt == nil {
		now := time.Now().UTC()
		updatedAt = &now
	}
	tx.db.setUpdatedAt(tx.ctx.UserCtx, tx.ns, *updatedAt)

	return
}

// Commit apply changes
func (tx *Tx) Commit(updatedAt *time.Time) error {
	_, err := tx.CommitWithCount(updatedAt)
	return err
}

func (tx *Tx) MustCommit(updatedAt *time.Time) int {
	count, err := tx.CommitWithCount(updatedAt)
	if err != nil {
		panic(err)
	}
	return count
}

func (tx *Tx) modifyInternal(item interface{}, json []byte, mode int, precepts ...string) (err error) {

	for tryCount := 0; tryCount < 2; tryCount++ {
		ser := cjson.NewPoolSerializer()
		defer ser.Close()
		format := 0
		stateToken := 0

		if format, stateToken, err = packItem(tx.ns, item, json, ser); err != nil {
			return err
		}

		err := tx.db.binding.ModifyItemTx(&tx.ctx, format, ser.Bytes(), mode, precepts, stateToken)

		if err != nil {
			rerr, ok := err.(bindings.Error)
			if ok && rerr.Code() == bindings.ErrStateInvalidated {
				tx.db.query(tx.ns.name).Limit(0).ExecCtx(tx.ctx.UserCtx)
				err = rerr
				continue
			}
			return err
		}
		tx.counter++
		if tx.counter > tx.forceCommitCount && tx.forceCommitCount != 0 {
			tx.counter = 0
			_, err := tx.commitInternal()
			return err
		}
		return nil
	}
	return nil

}

// Commit apply changes
func (tx *Tx) commitInternal() (count int, err error) {
	count = 0
	out, err := tx.db.binding.CommitTx(&tx.ctx)
	if err != nil {
		return 0, err
	}
	defer out.Free()

	rdSer := newSerializer(out.GetBuf())

	rawQueryParams := rdSer.readRawQueryParams(func(nsid int) {
		tx.ns.cjsonState.ReadPayloadType(&rdSer.Serializer)
	})

	if rawQueryParams.count == 0 {
		return
	}

	tx.ns.cacheLock.Lock()

	for i := 0; i < rawQueryParams.count; i++ {
		count++
		item := rdSer.readRawtItemParams()
		delete(tx.ns.cacheItems, item.id)
	}

	tx.ns.cacheLock.Unlock()

	return
}

// Rollback update
func (tx *Tx) Rollback() error {
	err := tx.db.binding.RollbackTx(&tx.ctx)
	if err != nil {
		return err
	}
	return nil
}
