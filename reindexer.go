package reindexer

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"io"

	"github.com/restream/reindexer/bindings"
	"github.com/restream/reindexer/dsl"
)

var logger Logger = &nullLogger{}

// Map from cond name to index type
var queryTypes = map[string]int{
	"EQ":     EQ,
	"GT":     GT,
	"LT":     LT,
	"GE":     GE,
	"LE":     LE,
	"SET":    SET,
	"RANGE":  RANGE,
	"ANY":    ANY,
	"EMPTY":  EMPTY,
	"ALLSET": ALLSET,
}

func GetCondType(name string) (int, error) {
	cond, ok := queryTypes[strings.ToUpper(name)]
	if ok {
		return cond, nil
	} else {
		return 0, ErrCondType
	}
}

// Map from index type to cond name
var queryNames = map[int]string{
	EQ:    "EQ",
	GT:    "GT",
	LT:    "LT",
	GE:    "GE",
	LE:    "LE",
	SET:   "SET",
	RANGE: "RANGE",
	ANY:   "ANY",
	EMPTY: "EMPTY",
}

func GetCondName(cond int) (string, error) {
	name, ok := queryNames[cond]
	if ok {
		return name, nil
	} else {
		return "", ErrCondType
	}
}

// Reindexer The reindxer state struct
type Reindexer struct {
	lock        sync.RWMutex
	ns          map[string]*reindexerNamespace
	storagePath string
	strPool     stringPool
	binding     bindings.RawBinding
	debugLevels map[string]int
}

type cacheItem struct {
	item interface{}
	// version of item, for cachins
	version int
	// timestamp of last read
	readTimestamp int
}

type reindexerNamespace struct {
	Item       interface{}
	UpdatedAt  time.Time
	cacheItems map[int]cacheItem
	cacheLock  sync.RWMutex
	fields     []reindexerField
	rtype      reflect.Type
	name       string
	lock       sync.RWMutex
	codec      ItemCodec
	opts       NamespaceOptions
	storageErr error
}

type reindexerField struct {
	idxName   string
	jsonPath  string
	idxType   int
	fieldIdx  int
	isPK      bool
	isDict    bool
	isJoined  bool
	fieldType reflect.Kind
	elemType  reflect.Kind // elemType > 0 for fieldType's values slice and array
	subField  []reindexerField
}

// Interface for append joined items
type Joinable interface {
	Join(field string, subitems []interface{}, context interface{})
}

type Clonable interface {
	ClonePtrSlice(src []interface{}) []interface{}
}

// Logger interface for reindexer
type Logger interface {
	Printf(level int, fmt string, msg ...interface{})
}

type nullLogger struct {
}

type ItemCodec interface {
	Encode(item interface{}, wr io.Writer) error
	Decode(data []byte, item interface{}) error
}

type jsonCodec struct {
}

func (jsonCodec) Encode(item interface{}, wr io.Writer) error {
	return json.NewEncoder(wr).Encode(item)
}

func (jsonCodec) Decode(data []byte, item interface{}) error {
	return json.Unmarshal(data, item)
}

func (nullLogger) Printf(level int, fmt string, msg ...interface{}) {
}

var (
	errNsNotFound        = errors.New("rq: Namespace is not found")
	errNsExists          = errors.New("rq: Namespace is already exists")
	errInvalidReflection = errors.New("rq: Invalid reflection type of index")
	errStorageNotEnabled = errors.New("rq: Storage is not enabled, can't save")
	ErrEmptyNamespace    = errors.New("rq: empty namespace name")
	ErrEmptyFieldName    = errors.New("rq: empty field name in filter")
	ErrCondType          = errors.New("rq: cond type not found")
	ErrOpInvalid         = errors.New("rq: op is invalid")
	ErrNoPK              = errors.New("rq: No pk field in struct")
	ErrWrongType         = errors.New("rq: Wrong type of item")
	ErrMustBePointer     = errors.New("rq: Argument must be a pointer to element, not element")
)

// NewReindex Create new instanse of Reindexer DB
// Returns pointer to created instance
func NewReindex(uri string) *Reindexer {

	binding := bindings.GetBinding(uri)

	if binding == nil {
		panic(fmt.Errorf("Reindex binding '%s' is not avalable, can't create DB, %d ", uri))
	}

	return &Reindexer{
		ns:          make(map[string]*reindexerNamespace, 100),
		strPool:     stringPool{strings: make(map[string]stringEntry, 1000)},
		binding:     binding,
		debugLevels: make(map[string]int, 100),
	}
}

// EnableStorage enables persistent storage of data
// Current implemetation is GOB based, slow
func (db *Reindexer) EnableStorage(storagePath string) {
	os.MkdirAll(storagePath, os.ModePerm)
	db.storagePath = storagePath + "/"
}

// SetLogger sets logger interface for output reindexer logs
// Current implemetation is GOB based, slow
func (db *Reindexer) SetLogger(log Logger) {
	if log != nil {
		logger = log
		db.binding.EnableLogger(log)
	} else {
		logger = &nullLogger{}
		db.binding.DisableLogger()
	}
}

// NamespaceOptions is options for namespace
type NamespaceOptions struct {
	// TTL of objects in cache
	CacheTTL int
	// Force Cache data on upser
	ForceCacheUpsert bool
	// Only in memory namespace
	NoPersistent bool
}

// DefaultNamespaceOptions return defailt namespace options
func DefaultNamespaceOptions() NamespaceOptions {
	return NamespaceOptions{
		CacheTTL:         0,
		ForceCacheUpsert: false,
		NoPersistent:     false,
	}
}

// NewNamespace Create new namespace and index based on passed struct.
// IndexDescription fields of struct are marked by `reindex:` tag
func (db *Reindexer) NewNamespace(namespace string, opts NamespaceOptions, s interface{}) error {
	t := reflect.TypeOf(s)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	db.lock.Lock()
	oldNs, ok := db.ns[namespace]
	if ok {
		db.lock.Unlock()
		// Ns exists, and have different type
		if oldNs.rtype.Name() != t.Name() {
			panic(errNsExists)
		}
		// Ns exists, and have the same type.
		return nil
	}

	err := db.binding.AddNamespace(namespace)
	if err != nil {
		db.lock.Unlock()
		return err
	}

	ns := &reindexerNamespace{
		Item:       s,
		cacheItems: make(map[int]cacheItem, 100),
		rtype:      t,
		name:       namespace,
		codec:      new(jsonCodec),
		opts:       opts,
	}
	ns.fields, err = db.createIndex(namespace, t, false, "")
	if err != nil {
		db.lock.Unlock()
		return err
	}

	db.ns[namespace] = ns
	db.lock.Unlock()

	if len(db.storagePath) != 0 && !opts.NoPersistent {

		if ns.storageErr = db.binding.EnableStorage(namespace, db.storagePath); ns.storageErr != nil {
			return ns.storageErr
		}

		if b, err := db.getMeta(namespace, "updated"); err == nil {
			err = json.Unmarshal(b, &ns.UpdatedAt)
			if err != nil {
				logger.Printf(ERROR, "Can't unmarashal %s to ts", string(b))
			}
		}
	}

	return nil
}

// CreateIndex Create new namespace and index based on passed struct.
// IndexDescription fields of struct are marked by `reindex:` tag
func (db *Reindexer) CreateIndex(namespace string, s interface{}) error {
	return db.NewNamespace(namespace, DefaultNamespaceOptions(), s)
}

// Create new namespace and index based on passed struct.
// IndexDescription fields of struct are marked by `reindex:` tag
func (db *Reindexer) createIndex(namespace string, st reflect.Type, subArray bool, jsonBasePath string) (fields []reindexerField, err error) {

	if len(jsonBasePath) != 0 {
		jsonBasePath = jsonBasePath + "."
	}
	fields = make([]reindexerField, 0, 100)

	for i := 0; i < st.NumField(); i++ {

		t := st.Field(i).Type
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		f := reindexerField{fieldIdx: i, fieldType: t.Kind()}
		// Get and parse tags
		tagsSlice := strings.Split(st.Field(i).Tag.Get("reindex"), ",")

		jsonPaths := strings.Split(st.Field(i).Tag.Get("json"), ",")

		jsonPath := jsonPaths[0]
		if len(jsonPath) == 0 && !st.Field(i).Anonymous {
			jsonPath = st.Field(i).Name
		}
		jsonPath = jsonBasePath + jsonPath

		if len(tagsSlice) > 0 && tagsSlice[0] == "-" {
			continue
		}

		if len(tagsSlice) > 2 && tagsSlice[2] == "composite" && t.Kind() == reflect.Struct {
			if t.Kind() != reflect.Struct || t.NumField() != 0 {
				return nil, fmt.Errorf("'composite' tag allowed only on empty on structs: Invalid tags %v on field %s", tagsSlice, st.Field(i).Name)
			}
			db.binding.AddIndex(namespace, tagsSlice[0], "", IndexCompositeHash, false, false)
			continue
		}

		if t.Kind() == reflect.Struct {
			if subfields, err := db.createIndex(namespace, t, subArray, jsonPath); err != nil {
				return nil, err
			} else if len(subfields) > 0 {
				f.subField = subfields
			} else {
				continue
			}
		} else if (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) &&
			(t.Elem().Kind() == reflect.Struct ||
				(t.Elem().Kind() == reflect.Ptr && t.Elem().Elem().Kind() == reflect.Struct)) {
			// Check if field nested slice of struct
			if len(tagsSlice) > 0 && len(tagsSlice[0]) > 0 {
				if len(tagsSlice) > 2 && tagsSlice[2] == "joined" {
					f.isJoined = true
					f.idxName = tagsSlice[0]
				} else {
					return nil, fmt.Errorf("Only 'joined' tags allowed on []struct. Invalid tags %v on field %s", tagsSlice, st.Field(i).Name)
				}
			} else if subfields, err := db.createIndex(namespace, t.Elem(), true, jsonPath); err != nil {
				return nil, err
			} else if len(subfields) > 0 {
				f.subField = subfields
			} else {
				continue
			}
		} else {
			if len(tagsSlice) < 1 {
				continue
			}
			f.idxName = tagsSlice[0]
			reqIdxType := ""
			if len(tagsSlice) > 1 {
				reqIdxType = tagsSlice[1]
			}
			if len(tagsSlice) > 2 {
				f.isDict = (tagsSlice[2] == "dict")
				f.isPK = (tagsSlice[2] == "pk")
				if f.isPK && len(f.idxName) == 0 {
					f.idxName = st.Field(i).Name
				}
			}
			isArray := t.Kind() == reflect.Slice || t.Kind() == reflect.Array || subArray

			if len(f.idxName) > 0 {
				if f.idxType, err = getIndexByType(t, reqIdxType); err != nil {
					return nil, err
				}
				if err = db.binding.AddIndex(namespace, f.idxName, jsonPath, f.idxType, isArray, f.isPK); err != nil {
					return nil, err
				}
			}
			if len(f.idxName) == 0 && !f.isPK && !f.isDict {
				continue
			}
			if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
				f.elemType = t.Elem().Kind()
			}
		}

		fields = append(fields, f)
	}
	return fields, nil
}

// Upsert (Insert or Update) item in index
// Item must be the same type as item passed to CreateIndex
func (db *Reindexer) Upsert(namespace string, s interface{}) error {
	ns, err := db.getNS(namespace)
	if err != nil {
		return err
	}
	ns.lock.Lock()
	defer ns.lock.Unlock()

	if err := db.upsert(ns, s); err != nil {
		return err
	}
	return db.binding.Commit(namespace)
}

func (db *Reindexer) getNS(namespace string) (*reindexerNamespace, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	ns, ok := db.ns[namespace]
	if !ok {
		return nil, errNsNotFound
	}
	return ns, nil

}

func (db *Reindexer) upsert(ns *reindexerNamespace, s interface{}) error {

	v := reflect.Indirect(reflect.ValueOf(s))
	t := reflect.TypeOf(s)

	ser := newPoolSerializer()
	defer ser.close()

	ser.writeString(ns.name)

	if b, ok := s.([]byte); !ok {
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		} else {
			// Get pointer of element
			panic(ErrMustBePointer)
		}
		if ns.rtype.Name() != t.Name() {
			panic(ErrWrongType)
		}

		// reserve int for save len
		p := ser.pos
		ser.writeCInt(0)

		if err := ns.codec.Encode(s, ser); err != nil {
			return err
		}

		pp := ser.pos
		ser.pos = p
		ser.writeCInt(pp - p - 4)
		ser.pos = pp

	} else {
		ser.writeBytes(b)
	}

	id, version, err := db.upsertItem(ser.buf)

	if err != nil {
		return err
	}

	// Save to cache
	if ns.opts.ForceCacheUpsert {
		db.strPool.mergeStrings(v, ns.fields)
		ns.cacheLock.Lock()
		ns.cacheItems[id] = cacheItem{item: s, version: version}
		ns.cacheLock.Unlock()
	}

	return nil
}

func (db *Reindexer) upsertJSON(ns *reindexerNamespace, b []byte) error {

	ser := newPoolSerializer()
	defer ser.close()

	ser.writeString(ns.name)
	ser.writeBytes(b)

	_, _, err := db.upsertItem(ser.buf)
	return err
}

func (db *Reindexer) packItem(val reflect.Value, fields []reindexerField, ser *serializer) error {

	for _, f := range fields {
		v := reflect.Indirect(val.Field(f.fieldIdx))
		if !v.IsValid() || f.isJoined {
			continue
		}
		tk := f.fieldType
		if (tk == reflect.Slice || tk == reflect.Array) && f.subField != nil {
			// Check if field nested slice of struct
			for i := 0; i < v.Len(); i++ {
				if err := db.packItem(v.Index(i), f.subField, ser); err != nil {
					return err
				}
			}
			continue
		}
		if tk == reflect.Struct {
			if err := db.packItem(v, f.subField, ser); err != nil {
				return err
			}
			continue
		}

		if len(f.idxName) == 0 || !f.isPK {
			continue
		}

		ser.writeString(f.idxName)
		if tk == reflect.Slice || tk == reflect.Array {
			// Upsert slice of elements
			ser.writeCInt(v.Len())
			for j := 0; j < v.Len(); j++ {
				if err := ser.writeValue(v.Index(j)); err != nil {
					return err
				}
			}
		} else {
			// Upsert simple element
			ser.writeCInt(1)
			if err := ser.writeValue(v); err != nil {
				return err
			}
		}
	}
	return nil
}

// Delete - remove item by pk from namespace
func (db *Reindexer) Delete(namespace string, s interface{}) error {

	ns, err := db.getNS(namespace)
	if err != nil {
		return err
	}
	ns.lock.Lock()
	defer ns.lock.Unlock()

	if err := db.delete(ns, s); err != nil {
		return err
	}
	err = db.binding.Commit(namespace)
	return err
}

func (db *Reindexer) delete(ns *reindexerNamespace, s interface{}) error {

	b, ok := s.([]byte)

	ser := newPoolSerializer()
	defer ser.close()
	ser.writeString(ns.name)

	if !ok {
		t := reflect.TypeOf(s)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if ns.rtype.Name() != t.Name() {
			panic(ErrWrongType)
		}
		// delete by packed pk values
		ser.writeCInt(1)
		if err := db.packItem(reflect.Indirect(reflect.ValueOf(s)), ns.fields, ser); err != nil {
			return err
		}
	} else {
		// delete by json. c++ code will extract pk from json
		ser.writeCInt(0)
		ser.writeBytes(b)
	}

	iddel, _, err := db.deleteItem(ser.buf)
	if err != nil {
		return err
	}
	// Update cache
	ns.cacheLock.Lock()
	if it, ok := ns.cacheItems[iddel]; ok {
		db.strPool.deleteStrings(reflect.Indirect(reflect.ValueOf(it.item)), ns.fields)
		delete(ns.cacheItems, iddel)
	}
	ns.cacheLock.Unlock()
	return err
}

func (db *Reindexer) deleteJSON(ns *reindexerNamespace, b []byte) error {

	ser := newPoolSerializer()
	defer ser.close()
	ser.writeString(ns.name)

	// delete by json. c++ code will extract pk from json
	ser.writeCInt(0)
	ser.writeBytes(b)

	iddel, _, err := db.deleteItem(ser.buf)
	if err != nil {
		return err
	}
	// Update cache
	ns.cacheLock.Lock()
	if it, ok := ns.cacheItems[iddel]; ok {
		db.strPool.deleteStrings(reflect.Indirect(reflect.ValueOf(it.item)), ns.fields)
		delete(ns.cacheItems, iddel)
	}
	ns.cacheLock.Unlock()
	return err
}

// DeleteNamespace - delete whole namespace from DB
func (db *Reindexer) DeleteNamespace(namespace string) error {
	db.lock.Lock()
	_, ok := db.ns[namespace]
	if !ok {
		db.lock.Unlock()
		return errNsNotFound
	}
	delete(db.ns, namespace)
	db.lock.Unlock()

	return db.binding.DeleteNamespace(namespace)
}

// RenameNamespace - rename namespace in DB. If dst namespace already exists, it will be overwriten
func (db *Reindexer) RenameNamespace(src string, dst string) error {
	db.lock.Lock()
	srcNs, ok := db.ns[src]
	if !ok {
		db.lock.Unlock()
		return errNsNotFound
	}
	delete(db.ns, src)
	srcNs.name = dst
	db.ns[dst] = srcNs
	db.lock.Unlock()
	return db.binding.RenameNamespace(src, dst)
}

// CloneNamespace - make a copy of namespace in DB. If dst namespace already exists, method will fail
func (db *Reindexer) CloneNamespace(src string, dst string) error {
	db.lock.Lock()
	srcNs, ok := db.ns[src]
	if !ok {
		db.lock.Unlock()
		return errNsNotFound
	}
	_, ok = db.ns[dst]
	if ok {
		db.lock.Unlock()
		return errNsExists
	}

	srcNs.lock.RLock()
	dstNs := &reindexerNamespace{
		Item:       srcNs.Item,
		UpdatedAt:  srcNs.UpdatedAt,
		cacheItems: make(map[int]cacheItem, len(srcNs.cacheItems)),
		fields:     srcNs.fields,
		rtype:      srcNs.rtype,
		name:       dst,
		codec:      srcNs.codec,
	}

	srcNs.cacheLock.RLock()
	for k, v := range srcNs.cacheItems {
		dstNs.cacheItems[k] = v
	}
	srcNs.cacheLock.RUnlock()

	srcNs.lock.RUnlock()

	db.ns[dst] = dstNs

	db.lock.Unlock()

	return db.binding.CloneNamespace(src, dst)
}

// SetDefaultQueryDebug sets default debug level for queries to namespaces
func (db *Reindexer) SetDefaultQueryDebug(namespace string, level int) {
	db.lock.Lock()
	db.debugLevels[namespace] = level
	db.lock.Unlock()
}

// Query Create new Query for building request
func (db *Reindexer) Query(namespace string) *Query {
	dbgLvl := 0
	ok := false
	db.lock.Lock()
	if dbgLvl, ok = db.debugLevels[namespace]; !ok {
		if dbgLvl, ok = db.debugLevels["*"]; !ok {
			dbgLvl = 0
		}
	}
	db.lock.Unlock()

	q := newQuery(db, namespace)
	q.DebugLevel = dbgLvl
	return q
}

// BeginTx - start update transaction
func (db *Reindexer) BeginTx(namespace string) (*Tx, error) {
	return newTx(db, namespace)
}

// MustBeginTx - start update transaction, panic on error
func (db *Reindexer) MustBeginTx(namespace string) *Tx {
	tx, err := newTx(db, namespace)
	if err != nil {
		panic(err)
	}
	return tx
}

func getIndexByType(t reflect.Type, reqType string) (int, error) {

	switch t.Kind() {
	case reflect.Bool:
		return IndexBool, nil
	case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int16,
		reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint16:
		if reqType == "tree" {
			return IndexInt, nil
		} else if reqType == "hash" || reqType == "" {
			return IndexIntHash, nil
		} else if reqType == "-" {
			return IndexIntStore, nil
		}
	case reflect.Int64, reflect.Uint64:
		if reqType == "tree" {
			return IndexInt64, nil
		} else if reqType == "hash" || reqType == "" {
			return IndexInt64Hash, nil
		} else if reqType == "-" {
			return IndexInt64Store, nil
		}
	case reflect.String:
		if reqType == "fulltext" {
			return IndexNewFullText, nil
		} else if reqType == "text" {
			return IndexFullText, nil
		} else if reqType == "tree" {
			return IndexTree, nil
		} else if reqType == "hash" || reqType == "" {
			return IndexHash, nil
		} else if reqType == "-" {
			return IndexStrStore, nil
		}
	case reflect.Float32, reflect.Float64:
		if reqType == "tree" || reqType == "" {
			return IndexDouble, nil
		} else if reqType == "-" {
			return IndexDoubleStore, nil
		}
	case reflect.Array, reflect.Slice, reflect.Ptr:
		return getIndexByType(t.Elem(), reqType)
	}
	return -1, errInvalidReflection
}

// SetUpdatedAt - set updated at time for namespace
func (db *Reindexer) setUpdatedAt(ns *reindexerNamespace, updatedAt time.Time) error {
	ns.UpdatedAt = updatedAt
	b, _ := json.Marshal(ns.UpdatedAt)

	if len(db.storagePath) != 0 && !ns.opts.NoPersistent {
		if err := db.putMeta(ns.name, "updated", b); err != nil {
			logger.Printf(INFO, "rq: Can't put meta to '%s'", ns.name)
		}
	}
	return nil
}

// GetUpdatedAt - get updated at time of namespace
func (db *Reindexer) GetUpdatedAt(namespace string) (*time.Time, error) {
	ns, err := db.getNS(namespace)
	if err != nil {
		return nil, err
	}

	return &ns.UpdatedAt, nil
}

func (db *Reindexer) QueryFrom(d dsl.DSL) (*Query, error) {
	if d.Namespace == "" {
		return nil, ErrEmptyNamespace
	}

	q := db.Query(d.Namespace).Offset(d.Offset).Limit(d.Limit)

	if d.Distinct != "" {
		q.Distinct(d.Distinct)
	}
	if d.Sort.Field != "" {
		q.Sort(d.Sort.Field, d.Sort.Desc)
	}

	for _, filter := range d.Filters {
		if filter.Field == "" {
			return nil, ErrEmptyFieldName
		}
		if filter.Value == nil {
			continue
		}

		cond, err := GetCondType(filter.Cond)
		if err != nil {
			return nil, err
		}

		switch strings.ToUpper(filter.Op) {
		case "":
			q.Where(filter.Field, cond, filter.Value)
		case "NOT":
			q.Not().Where(filter.Field, cond, filter.Value)
		default:
			return nil, ErrOpInvalid
		}
	}

	return q, nil
}

func walkUniqNamespaces(nsArray []*reindexerNamespace, visitor func(ns *reindexerNamespace)) {
	for i := 0; i < len(nsArray); i++ {
		uniq := true
		for j := 0; j < i; j++ {
			if nsArray[j] == nsArray[i] {
				uniq = false
				break
			}
		}
		if uniq {
			visitor(nsArray[i])
		}
	}
}
