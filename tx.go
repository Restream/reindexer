package reindexer

import "time"

// Tx is Transaction object. When begin transaction creates clone of namespace and update clone
// On commit updated namespace clone will be atomicaly replace origin namespace
// Om rollback updated namespace clone will be removed
// There are only 1 active transaction on 1 namespace is possible
type Tx struct {
	txNs      *reindexerNamespace
	namespace string
	db        *Reindexer
	started   bool
}

func txNs(namespace string) string {
	return namespace + "#tx"
}

func newTx(db *Reindexer, namespace string) (*Tx, error) {
	tx := &Tx{db: db, namespace: namespace}

	return tx, nil
}

func (tx *Tx) startTx() error {
	if tx.started {
		return nil
	}
	err := tx.db.CloneNamespace(tx.namespace, txNs(tx.namespace))
	if err != nil {
		return err
	}
	tx.txNs, err = tx.db.getNS(txNs(tx.namespace))
	tx.started = true
	return err
}

// Upsert (Insert or Update) item to index
func (tx *Tx) Upsert(s interface{}) error {
	if err := tx.startTx(); err != nil {
		return err
	}
	return tx.db.upsert(tx.txNs, s)
}

// UpsertJSON (Insert or Update) item to index
func (tx *Tx) UpsertJSON(b []byte) error {
	if err := tx.startTx(); err != nil {
		return err
	}
	return tx.db.upsertJSON(tx.txNs, b)
}

// Delete - remove item by id from namespace
func (tx *Tx) Delete(s interface{}) error {
	if err := tx.startTx(); err != nil {
		return err
	}
	return tx.db.delete(tx.txNs, s)
}

// DeleteJSON - remove item by id from namespace
func (tx *Tx) DeleteJSON(b []byte) error {
	if err := tx.startTx(); err != nil {
		return err
	}
	return tx.db.deleteJSON(tx.txNs, b)
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
		now := time.Now()
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
	err := tx.db.binding.Commit(txNs(tx.namespace))
	if err != nil {
		return err
	}
	return tx.db.RenameNamespace(txNs(tx.namespace), tx.namespace)
}

// Rollback update
func (tx *Tx) Rollback() error {
	if !tx.started {
		return nil
	}

	return tx.db.DeleteNamespace(txNs(tx.namespace))
}
