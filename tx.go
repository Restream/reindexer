package reindexer

import (
	"fmt"
	"time"
)

// Tx Is pseudo transaction object. Rollback is not implemented yet, and data will always updated
type Tx struct {
	txNs      *reindexerNamespace
	namespace string
	db        *Reindexer
	started   bool
}

func newTx(db *Reindexer, namespace string) (*Tx, error) {
	tx := &Tx{db: db, namespace: namespace}

	return tx, nil
}

func (tx *Tx) startTx() error {
	if tx.started {
		return nil
	}
	var err error
	tx.txNs, err = tx.db.getNS(tx.namespace)
	tx.started = true
	return err
}

// Insert (only) item to index
func (tx *Tx) Insert(s interface{}) (int, error) {
	if err := tx.startTx(); err != nil {
		return 0, err
	}
	return tx.db.modifyItem(tx.txNs.name, tx.txNs, s, nil, modeInsert)
}

func (tx *Tx) Update(s interface{}) (int, error) {
	if err := tx.startTx(); err != nil {
		return 0, err
	}
	return tx.db.modifyItem(tx.txNs.name, tx.txNs, s, nil, modeUpdate)
}

// Upsert (Insert or Update) item to index
func (tx *Tx) Upsert(s interface{}) error {
	if err := tx.startTx(); err != nil {
		return err
	}
	_, err := tx.db.modifyItem(tx.txNs.name, tx.txNs, s, nil, modeUpsert)
	return err
}

// UpsertJSON (Insert or Update) item to index
func (tx *Tx) UpsertJSON(json []byte) error {
	if err := tx.startTx(); err != nil {
		return err
	}
	_, err := tx.db.modifyItem(tx.txNs.name, tx.txNs, nil, json, modeUpsert)
	return err
}

// Delete - remove item by id from namespace
func (tx *Tx) Delete(s interface{}) error {
	if err := tx.startTx(); err != nil {
		return err
	}
	_, err := tx.db.modifyItem(tx.txNs.name, tx.txNs, s, nil, modeDelete)
	return err
}

// DeleteJSON - remove item by id from namespace
func (tx *Tx) DeleteJSON(json []byte) error {
	if err := tx.startTx(); err != nil {
		return err
	}
	_, err := tx.db.modifyItem(tx.txNs.name, tx.txNs, json, nil, modeDelete)
	return err
}

// Commit apply changes
func (tx *Tx) Commit(updatedAt *time.Time) error {
	if !tx.started {
		return nil
	}
	if err := tx.commitInternal(); err != nil {
		return err
	}
	if updatedAt == nil {
		now := time.Now().UTC()
		updatedAt = &now
	}
	tx.db.setUpdatedAt(tx.txNs, *updatedAt)

	return nil
}

func (tx *Tx) MustCommit(updatedAt *time.Time) {
	err := tx.Commit(updatedAt)
	if err != nil {
		panic(err)
	}
}

// Commit apply changes
func (tx *Tx) commitInternal() error {
	return tx.db.binding.Commit(tx.namespace)
}

// Rollback update
func (tx *Tx) Rollback() error {
	fmt.Printf("Sorry tx.rollback is not implemented properly yet.")
	return tx.commitInternal()
}
