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
