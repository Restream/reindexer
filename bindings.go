//go:generate make -j4
package reindexer

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/restream/reindexer/bindings"
)

// IndexDescription types
const (
	// HashMap index. Fast. Unordered. Items can't be sorted by this index. Query range unavailble
	IndexHash = bindings.IndexHash
	// TreeMap string index. Bit slowly than HashMap index. Ordered. Items can be sorted by this index. Query range is possible
	IndexTree = bindings.IndexTree
	// TreeMap int index. Ordered, Items can be sorted by this index. Query range is possible
	IndexInt = bindings.IndexInt
	// HashMap int index. Unordered. Items can't be sorted by this index. Query range unavailble
	IndexIntHash = bindings.IndexIntHash
	// TreeMap int64 index. Ordered, Items can be sorted by this index. Query range is possible
	IndexInt64 = bindings.IndexInt64
	// HashMap int64 index. Unordered. Items can't be sorted by this index. Query range unavailble
	IndexInt64Hash = bindings.IndexInt64Hash
	// TreeMap double index. Ordered, Items can be sorted by this index. Query range is possible
	IndexDouble = bindings.IndexDouble
	// FullText
	IndexFullText = bindings.IndexFullText
	// NewFullText
	IndexNewFullText = bindings.IndexNewFullText
	// Composite, tree
	IndexComposite = bindings.IndexComposite
	// Composite, unordered
	IndexCompositeHash = bindings.IndexCompositeHash
	// Bool, store
	IndexBool = bindings.IndexBool
	// Int store
	IndexIntStore = bindings.IndexIntStore
	// Int64 store
	IndexInt64Store = bindings.IndexInt64Store
	// Int64 store
	IndexStrStore = bindings.IndexStrStore
	// Double store
	IndexDoubleStore = bindings.IndexDoubleStore
)

// Condition types
const (
	// Equal '='
	EQ = bindings.EQ
	// Greater '>'
	GT = bindings.GT
	// Lower '<'
	LT = bindings.LT
	// Greater or equal '>=' (GT|EQ)
	GE = bindings.GE
	// Lower or equal '<'
	LE = bindings.LE
	// One of set 'IN []'
	SET = bindings.SET
	// All of set
	ALLSET = bindings.ALLSET
	// In range
	RANGE = bindings.RANGE
	// Any value
	ANY = bindings.ANY
	// Empty value (usualy zero len array)
	EMPTY = bindings.EMPTY
)

const (
	// ERROR Log level
	ERROR = bindings.ERROR
	// WARNING Log level
	WARNING = bindings.WARNING
	// INFO Log level
	INFO = bindings.INFO
	// TRACE Log level
	TRACE = bindings.TRACE
)

type CInt bindings.CInt

func (db *Reindexer) deleteItem(data []byte) (iddel int, version int, err error) {

	out, err := db.binding.DeleteItem(data)
	defer out.Free()
	if err != nil {
		return 0, 0, err
	}

	rdSer := newSerializer(out.GetBuf())

	rdSer.readCInt()
	rdSer.readCInt()
	iddel = rdSer.readCInt()
	version = rdSer.readCInt()
	return iddel, version, err
}

func (db *Reindexer) upsertItem(data []byte) (id int, version int, err error) {

	out, err := db.binding.UpsertItem(data)
	defer out.Free()
	if err != nil {
		return 0, 0, err
	}

	rdSer := newSerializer(out.GetBuf())

	rdSer.readCInt()
	rdSer.readCInt()
	id = rdSer.readCInt()
	version = rdSer.readCInt()
	return id, version, err
}

func (db *Reindexer) putMeta(namespace, key string, data []byte) error {

	var ser serializer
	ser.writeString(namespace)
	ser.writeString(key)
	ser.writeBytes(data)

	return db.binding.PutMeta(ser.buf)
}

func (db *Reindexer) getMeta(namespace, key string) ([]byte, error) {

	var ser serializer
	ser.writeString(namespace)
	ser.writeString(key)

	out, err := db.binding.GetMeta(ser.GetBuf())
	defer out.Free()

	if err != nil {
		return nil, err
	}

	rdSer := newSerializer(out.GetBuf())
	b := rdSer.readBytes()
	bb := make([]byte, len(b), cap(b))
	copy(bb, b)
	return bb, nil
}

func getJoinedField(val reflect.Value, fields []reindexerField, name string) (ret reflect.Value) {

	for _, f := range fields {
		v := reflect.Indirect(reflect.Indirect(val).Field(f.fieldIdx))
		if !v.IsValid() {
			continue
		}
		if f.fieldType == reflect.Struct {
			if r := getJoinedField(v, f.subField, name); r.IsValid() {
				return r
			}
		}
		if f.isJoined && name == f.idxName {
			return v
		}
	}
	return ret
}

func getFromCache(ns *reindexerNamespace, id int) interface{} {
	if item, ok := ns.cacheItems[id]; ok {
		return item.item
	}
	panic(fmt.Errorf("Internal error - not found item id='%d' on namespace '%s'", id, ns.name))
}

func (db *Reindexer) loadToCache(ns []*reindexerNamespace, rawResult []byte) /*[]map[int]cacheItem,*/ error {

	ser := newSerializer(rawResult)

	// skip total count
	_ = ser.readCInt()

	count := ser.readCInt()
	var res [][]int

	walkUniqNamespaces(ns, func(ns *reindexerNamespace) { ns.cacheLock.RLock() })
	for i := 0; i < count; i++ {
		id := ser.readCInt()
		version := ser.readCInt()
		if it, ok := ns[0].cacheItems[id]; !ok || version > it.version {
			_ = version
			if res == nil {
				res = make([][]int, len(ns))
			}
			res[0] = append(res[0], id)
		}
		jres := ser.readCInt()
		for j := 0; j < jres; j++ {
			jires := ser.readCInt()

			for ji := 0; ji < jires; ji++ {
				id = ser.readCInt()
				version = ser.readCInt()
				if it, ok := ns[j+1].cacheItems[id]; !ok || version > it.version {
					if res == nil {
						res = make([][]int, len(ns))
					}
					res[j+1] = append(res[j+1], id)
				}
			}
		}
	}
	walkUniqNamespaces(ns, func(ns *reindexerNamespace) { ns.cacheLock.RUnlock() })

	for n, ids := range res {
		items, err := db.getItems(ns[n], ids)

		if err != nil {
			return err
		}
		ns[n].cacheLock.Lock()
		for i, id := range ids {
			ns[n].cacheItems[id] = items[i]
		}
		ns[n].cacheLock.Unlock()
	}
	return nil
}

func (db *Reindexer) rawResultToPtrSlice(
	ns []*reindexerNamespace,
	rawResult []byte,
	initItems []interface{},
	initSubitems []interface{},
	copy bool,
	useCache bool,
) (
	items []interface{},
	subitems []interface{},
	totalCount int,
	err error,
) {

	if useCache {
		if err := db.loadToCache(ns, rawResult); err != nil {
			return nil, nil, 0, err
		}
	}
	ser := newSerializer(rawResult)
	totalCount = ser.readCInt()
	count := ser.readCInt()

	if initSubitems == nil && len(ns) > 1 {
		// if we have >1 namespaces, then reserve approx count of subitems.
		subitems = make([]interface{}, 0, (len(rawResult)/8)-count)
	} else {
		subitems = initSubitems[:0]
	}
	if initItems == nil {
		items = make([]interface{}, 0, count)
	} else {
		items = initItems[:0]
	}

	var cloner Clonable

	walkUniqNamespaces(ns, func(ns *reindexerNamespace) { ns.cacheLock.RLock() })
	for i := 0; i < count; i++ {
		id := ser.readCInt()
		_ = ser.readCInt()
		var item interface{}
		if useCache {
			item = getFromCache(ns[0], id)
		} else {
			item, err = db.deserializeItem(&ser, ns[0])
			if err != nil {
				return nil, nil, 0, err
			}
		}
		jres := ser.readCInt()
		for j := 0; j < jres; j++ {
			jires := ser.readCInt()
			for ji := 0; ji < jires; ji++ {
				iid := ser.readCInt()
				_ = ser.readCInt()
				if useCache {
					subitems = append(subitems, getFromCache(ns[j+1], iid))
				} else {
					subitem, err := db.deserializeItem(&ser, ns[j+1])
					if err != nil {
						return nil, nil, 0, err
					}
					subitems = append(subitems, subitem)
				}
			}
		}

		if (jres != 0 || copy) && cloner == nil {
			if c, ok := item.(Clonable); ok {
				cloner = c
			} else {
				jtype := reflect.TypeOf(item).Elem()
				jitem := reflect.Indirect(reflect.New(jtype))
				jitem.Set(reflect.ValueOf(item).Elem())
				item = jitem.Addr().Interface()
			}
		}
		items = append(items, item)
	}
	walkUniqNamespaces(ns, func(ns *reindexerNamespace) { ns.cacheLock.RUnlock() })

	if cloner != nil {
		items = cloner.ClonePtrSlice(items)
	}

	return items, subitems, totalCount, err
}

func (db *Reindexer) joinResults(ns []*reindexerNamespace, rawResult []byte, jnames []string, items, subitems []interface{}, ctx interface{}, useCache bool) {

	if len(ns) < 2 {
		return
	}

	ser := newSerializer(rawResult)
	_ = ser.readCInt()
	count := ser.readCInt()

	scnt := 0
	for i := 0; i < count; i++ {
		ser.skipCInts(2)
		if !useCache {
			ser.readBytes()
		}
		item := items[i]
		jres := ser.readCInt()
		if jres != 0 {
			if jjitem, ok := item.(Joinable); ok {
				for j := 0; j < jres; j++ {
					jires := ser.readCInt()
					if jires != 0 {
						for ji := 0; ji < jires; ji++ {
							ser.skipCInts(2)
							if !useCache {
								ser.readBytes()
							}
						}
						jjitem.Join(jnames[j], subitems[scnt:scnt+jires], ctx)
						scnt += jires
					}
				}
			} else {
				// No Joinable inteface, using reflect. slow
				for j := 0; j < jres; j++ {
					jires := ser.readCInt()
					if jires != 0 {
						v := getJoinedField(reflect.ValueOf(item), ns[0].fields, jnames[j])
						if !v.IsValid() {
							panic(fmt.Errorf("Can't find field with tag '%s' in struct '%s' for put join results from '%s'",
								jnames[j],
								ns[0].rtype,
								ns[j+1].name))
						}
						v.Set(reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(ns[j+1].rtype)), jires, jires))
						for ji := 0; ji < jires; ji++ {
							ser.skipCInts(2)
							if !useCache {
								ser.readBytes()
							}
							v.Index(ji).Set(reflect.ValueOf(subitems[scnt]))
							scnt++
						}
					}
				}
			}
		}
	}
}

func (db *Reindexer) deserializeItem(ser *serializer, ns *reindexerNamespace) (interface{}, error) {
	b := ser.readBytes()
	item := reflect.New(ns.rtype).Interface()
	if err := ns.codec.Decode(b, item); err != nil {
		return nil, err
	}
	_ = b
	return item, nil
	//		db.strPool.mergeStrings(reflect.Indirect(reflect.ValueOf(item)), ns.fields)
}

func (db *Reindexer) getItems(ns *reindexerNamespace, ids []int) (res []cacheItem, err error) {

	var ser serializer
	ser.writeString(ns.name)
	ser.writeCInt(len(ids))

	for _, id := range ids {
		ser.writeCInt(id)
	}

	out, err := db.binding.GetItems(ser.GetBuf())
	defer out.Free()
	if err != nil {
		return nil, err
	}

	rdSer := newSerializer(out.GetBuf())
	res = make([]cacheItem, 0, len(ids))

	for !rdSer.eof() {
		id := rdSer.readCInt()
		version := rdSer.readCInt()
		item, err := db.deserializeItem(&rdSer, ns)
		if err != nil {
			return nil, err
		}
		if id != ids[len(res)] {
			panic(0)
		}
		res = append(res, cacheItem{item: item, version: version})
	}
	return res, nil
}

// Select items from database. Query is SQL statement
// Return slice of interfaces to items
func (db *Reindexer) Select(query string) *Iterator {

	// TODO: do not parse query string twice in go and cpp
	namespace := ""
	querySlice := strings.Split(strings.ToLower(query), " ")

	if len(querySlice) == 2 && querySlice[0] == "describe" {
		if querySlice[1] == "*" {
			descs, err := db.DescribeNamespaces()
			if err != nil {
				return newIterator(nil, err, nil)
			}
			res := []interface{}{}
			for _, desc := range descs {
				res = append(res, desc)
			}
			return newIterator(res, nil, reflect.TypeOf(NamespaceDescription{}))

		}
		desc, err := db.DescribeNamespace(querySlice[1])
		return newIterator([]interface{}{desc}, err, reflect.TypeOf(NamespaceDescription{}))
	}

	for i := range querySlice {
		if strings.Compare(querySlice[i], "from") == 0 && i+1 < len(querySlice) {
			namespace = querySlice[i+1]
			break
		}
	}

	nsArray := make([]*reindexerNamespace, 0, 3)
	if ns, err := db.getNS(namespace); err == nil {
		ns.lock.RLock()
		defer ns.lock.RUnlock()
		nsArray = append(nsArray, ns)
	} else {
		return newIterator(nil, err, nil)
	}

	useCache := true
	result, err := db.binding.Select(query, !useCache)
	defer result.Free()
	if err != nil {
		return newIterator(nil, err, nil)
	}

	items, _, _, err := db.rawResultToPtrSlice(nsArray, result.GetBuf(), nil, nil, false, useCache)
	return newIterator(items, err, nsArray[0].rtype)
}

// Execute query
func (db *Reindexer) selectQuery(q *Query) *Iterator {

	var nsArrayP [16]*reindexerNamespace
	nsArray := nsArrayP[:0:16]

	if ns, err := db.getNS(q.Namespace); err == nil {
		nsArray = append(nsArray, ns)
	} else {
		return initIterator(&q.iterator, nil, err, nil)
	}

	ser := q.ser
	ser.writeCInt(queryEnd)
	for _, sq := range q.joinQueries {
		ser.writeCInt(sq.joinType)
		ser.append(sq.ser)
		ser.writeCInt(queryEnd)
		if ns, err := db.getNS(sq.Namespace); err == nil {
			nsArray = append(nsArray, ns)
		} else {
			return initIterator(&q.iterator, nil, err, nil)
		}
	}

	walkUniqNamespaces(nsArray, func(ns *reindexerNamespace) { ns.lock.RLock() })

	useCache := !q.noObjCache
	result, err := db.binding.SelectQuery(&q.Query, !useCache, ser.GetBuf())
	defer result.Free()

	if err != nil || result.GetBuf() == nil {
		walkUniqNamespaces(nsArray, func(ns *reindexerNamespace) { ns.lock.RUnlock() })
		return initIterator(&q.iterator, nil, err, nil)
	}

	items, subitems, totalCount, err := db.rawResultToPtrSlice(nsArray, result.GetBuf(), q.items, q.subitems, q.copy, useCache)
	q.items = items
	q.subitems = subitems
	walkUniqNamespaces(nsArray, func(ns *reindexerNamespace) { ns.lock.RUnlock() })
	db.joinResults(nsArray, result.GetBuf(), q.joinToFields, items, subitems, q.context, useCache)
	q.totalCount = totalCount
	return initIterator(&q.iterator, items, err, nsArray[0].rtype)
}

// Execute query
func (db *Reindexer) deleteQuery(q *Query) (int, error) {

	ns, err := db.getNS(q.Namespace)
	if err != nil {
		return 0, err
	}

	ns.lock.Lock()
	defer ns.lock.Unlock()

	result, err := db.binding.DeleteQuery(&q.Query, q.ser.GetBuf())
	defer result.Free()
	if err != nil {
		return 0, err
	}

	ser := newSerializer(result.GetBuf())
	// skip total count
	_ = ser.readCInt()
	cnt := ser.readCInt()

	ns.cacheLock.Lock()
	for i := 0; i < cnt; i++ {
		iddel := ser.readCInt()
		_ = ser.readCInt()
		_ = ser.readCInt()
		// Update cache
		if _, ok := ns.cacheItems[iddel]; ok {
			//	db.strPool.deleteStrings(reflect.Indirect(reflect.ValueOf(it.item)), ns.fields)
			delete(ns.cacheItems, iddel)
		}

	}
	if !ser.eof() {
		panic("something wrong with deletequery response")
	}

	ns.cacheLock.Unlock()

	return cnt, err
}

// Get local thread reindexer usage stats
func (db *Reindexer) GetStats() bindings.Stats {
	return db.binding.GetStats()
}

// Reset local thread reindexer usage stats
func (db *Reindexer) ResetStats() {
	db.binding.ResetStats()
}
