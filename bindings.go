//go:generate make -j4 -C cpp_src
package reindexer

import (
	"fmt"
	"reflect"
	"strconv"
	"unsafe"

	"strings"

	"github.com/restream/reindexer/bindings"
	"github.com/restream/reindexer/cjson"
)

type CInt bindings.CInt

const (
	modeInsert = bindings.ModeInsert
	modeUpdate = bindings.ModeUpdate
	modeUpsert = bindings.ModeUpsert
	modeDelete = bindings.ModeDelete
)

func (db *Reindexer) modifyItem(namespace string, ns *reindexerNamespace, item interface{}, json []byte, mode int, precepts ...string) (count int, err error) {

	if ns == nil {
		ns, err = db.getNS(namespace)
		if err != nil {
			return 0, err
		}
	}

	ser := cjson.NewPoolSerializer()
	defer ser.Close()

	if err = packItem(ns, item, json, ser, precepts...); err != nil {
		return
	}

	out, err := db.binding.ModifyItem(ser.Bytes(), mode)

	defer out.Free()
	if err != nil {
		return 0, err
	}

	rdSer := newSerializer(out.GetBuf())
	rawQueryParams := rdSer.readRawQueryParams()

	if rawQueryParams.count == 0 {
		return 0, err
	}

	resultp := rdSer.readRawtItemParams()

	ns.cacheLock.Lock()
	delete(ns.cacheItems, resultp.id)
	ns.cacheLock.Unlock()

	return rawQueryParams.count, err
}

func packItem(ns *reindexerNamespace, item interface{}, json []byte, ser *cjson.Serializer, precepts ...string) error {

	ser.PutString(ns.name)

	if item != nil {
		json, _ = item.([]byte)
	}

	if json == nil {
		t := reflect.TypeOf(item)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if ns.rtype.Name() != t.Name() {
			panic(ErrWrongType)
		}

		ser.PutCInt(bindings.FormatCJson)

		// reserve int for save len
		pos := len(ser.Bytes())
		ser.PutCInt(0)
		pos1 := len(ser.Bytes())

		enc := ns.cjsonState.NewEncoder()
		if err := enc.Encode(item, ser); err != nil {
			return err
		}

		*(*CInt)(unsafe.Pointer(&ser.Bytes()[pos])) = CInt(len(ser.Bytes()) - pos1)
	} else {
		ser.PutCInt(bindings.FormatJson)
		ser.PutBytes(json)
	}

	ser.PutCInt(len(precepts))
	for _, precept := range precepts {
		ser.PutString(precept)
	}

	return nil
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

func (db *Reindexer) putMeta(namespace, key string, data []byte) error {

	var ser cjson.Serializer
	ser.PutString(namespace)
	ser.PutString(key)
	ser.PutBytes(data)

	return db.binding.PutMeta(ser.Bytes())
}

func (db *Reindexer) getMeta(namespace, key string) ([]byte, error) {

	var ser cjson.Serializer
	ser.PutString(namespace)
	ser.PutString(key)

	out, err := db.binding.GetMeta(ser.Bytes())
	defer out.Free()

	if err != nil {
		return nil, err
	}

	rdSer := newSerializer(out.GetBuf())
	b := rdSer.GetBytes()
	bb := make([]byte, len(b), cap(b))
	copy(bb, b)
	return bb, nil
}

func unpackItem(ns *reindexerNamespace, params rawResultItemParams, allowUnsafe bool) (item interface{}, err error) {
	useCache := ns.deepCopyIface || allowUnsafe
	needCopy := ns.deepCopyIface && !allowUnsafe

	if useCache {
		ns.cacheLock.RLock()
		if citem, ok := ns.cacheItems[params.id]; ok && citem.version == params.version {
			item = citem.item
		}
		ns.cacheLock.RUnlock()
	}

	if item == nil {
		if useCache {
			ns.cacheLock.Lock()
			if citem, ok := ns.cacheItems[params.id]; ok && citem.version == params.version {
				item = citem.item
			} else {
				item = reflect.New(ns.rtype).Interface()
				dec := ns.cjsonState.NewDecoder()
				if err = dec.Decode(params.cptr, item); err != nil {
					ns.cacheLock.Unlock()
					return
				}
				ns.cacheItems[params.id] = cacheItem{item: item, version: params.version}
			}
			ns.cacheLock.Unlock()
		} else {
			item = reflect.New(ns.rtype).Interface()
			dec := ns.cjsonState.NewDecoder()
			if err = dec.Decode(params.cptr, item); err != nil {
				return
			}
			// Reset needCopy, because item already separate
			needCopy = false
		}
	}

	if needCopy {
		if deepCopy, ok := item.(DeepCopy); ok {
			item = deepCopy.DeepCopy()
		} else {
			panic(fmt.Errorf("Internal error %s must implement DeepCopy interface", reflect.TypeOf(item).Name()))
		}
	}

	return
}

func (db *Reindexer) updatePayloadTypes(ns []*reindexerNamespace, ser *resultSerializer, rawQueryParams rawResultQueryParams) {
	if rawQueryParams.nsCount != len(ns) {
		panic(fmt.Errorf("Internal error: wrong namespaces count. Expected %d, got %d", len(ns), rawQueryParams.nsCount))
	}
	for i := 0; i < rawQueryParams.nsCount; i++ {
		if ns[i].cjsonState.PayloadTypeVersion() != rawQueryParams.nsVersions[i] {
			b, err := db.binding.GetPayloadType(ser.Bytes(), i)
			if err != nil {
				panic(err)
			}
			ns[i].cjsonState.ReadPayloadType(b.GetBuf())
			if ns[i].cjsonState.PayloadTypeVersion() != rawQueryParams.nsVersions[i] {
				panic(fmt.Errorf(
					"Internal error: wrong tagsMatcher version. Expected %d, got %d",
					rawQueryParams.nsVersions[i],
					ns[i].cjsonState.PayloadTypeVersion()),
				)
			}
		}
	}
}

func (db *Reindexer) rawResultToJson(rawResult []byte, jsonName string, totalName string, initJson []byte, initOffsets []int) (json []byte, offsets []int, err error) {

	ser := newSerializer(rawResult)
	rawQueryParams := ser.readRawQueryParams()

	jsonReserveLen := len(rawResult) + len(totalName) + len(jsonName) + 20
	if cap(initJson) < jsonReserveLen {
		initJson = make([]byte, 0, jsonReserveLen)
	} else {
		initJson = initJson[:0]
	}
	jsonBuf := cjson.NewSerializer(initJson)

	if cap(initOffsets) < rawQueryParams.count {
		offsets = make([]int, 0, rawQueryParams.count)
	} else {
		offsets = initOffsets[:0]
	}

	jsonBuf.WriteString("{\"")

	if len(totalName) != 0 {
		jsonBuf.WriteString(totalName)
		jsonBuf.WriteString("\":")
		jsonBuf.WriteString(strconv.Itoa(rawQueryParams.totalcount))
		jsonBuf.WriteString(",\"")
	}

	jsonBuf.WriteString(jsonName)
	jsonBuf.WriteString("\":[")

	for i := 0; i < rawQueryParams.count; i++ {
		_ = ser.readRawtItemParams()
		if i != 0 {
			jsonBuf.WriteString(",")
		}
		offsets = append(offsets, len(jsonBuf.Bytes()))
		jsonBuf.Write(ser.GetBytes())

		if ser.GetCInt() != 0 {
			panic("Sorry, not implemented: Can't return join query results as json")
		}
	}
	jsonBuf.WriteString("]}")

	return jsonBuf.Bytes(), offsets, nil
}

func (db *Reindexer) prepareQuery(q *Query, asJson bool) (result bindings.RawBuffer, err error) {
	if len(q.joinQueries) != 0 && len(q.mergedQueries) != 0 {
		return nil, ErrMergeAndJoinInOneQuery
	}

	if ns, err := db.getNS(q.Namespace); err == nil {
		q.nsArray = append(q.nsArray, ns)
	} else {
		return nil, err
	}

	ser := q.ser
	ser.PutCInt(queryEnd)
	for _, sq := range q.joinQueries {
		ser.PutCInt(sq.joinType)
		ser.Append(sq.ser)
		ser.PutCInt(queryEnd)
		if ns, err := db.getNS(sq.Namespace); err == nil {
			q.nsArray = append(q.nsArray, ns)
		} else {
			return nil, err
		}
	}
	for _, sq := range q.mergedQueries {
		ser.PutCInt(merge)
		ser.Append(sq.ser)
		ser.PutCInt(queryEnd)
		if ns, err := db.getNS(sq.Namespace); err == nil {
			q.nsArray = append(q.nsArray, ns)
		} else {
			return nil, err
		}
	}

	result, err = db.binding.SelectQuery(asJson, ser.Bytes())

	if err == nil && result.GetBuf() == nil {
		panic(fmt.Errorf("result.Buffer is nil"))
	}
	return
}

// Execute query
func (db *Reindexer) execQuery(q *Query) *Iterator {
	result, err := db.prepareQuery(q, false)
	if err != nil {
		return errIterator(err)
	}
	iter := newIterator(q, result, q.nsArray, q.joinToFields, q.joinHandlers, q.context)
	db.updatePayloadTypes(q.nsArray, &iter.ser, iter.rawQueryParams)
	return iter
}

func (db *Reindexer) execJSONQuery(q *Query, jsonRoot string) *JSONIterator {
	result, err := db.prepareQuery(q, true)
	if err != nil {
		return errJSONIterator(err)
	}
	defer result.Free()
	q.json, q.jsonOffsets, err = db.rawResultToJson(result.GetBuf(), jsonRoot, q.totalName, q.json, q.jsonOffsets)
	if err != nil {
		return errJSONIterator(err)
	}
	return newJSONIterator(q, q.json, q.jsonOffsets)
}

func (db *Reindexer) prepareSQL(namespace, query string, asJson bool) (result bindings.RawBuffer, nsArray []*reindexerNamespace, err error) {
	nsArray = make([]*reindexerNamespace, 0, 3)
	querySlice := strings.Split(strings.ToLower(query), " ")
	if len(querySlice) > 0 && querySlice[0] == "describe" {
		nsArray = append(nsArray, &reindexerNamespace{
			rtype:      reflect.TypeOf(NamespaceDescription{}),
			cjsonState: cjson.NewState(),
		})
	} else {
		var ns *reindexerNamespace
		if ns, err = db.getNS(namespace); err == nil {
			nsArray = append(nsArray, ns)
		} else {
			return
		}
	}
	result, err = db.binding.Select(query, asJson)
	return
}

func (db *Reindexer) execSQL(namespace, query string) *Iterator {
	result, nsArray, err := db.prepareSQL(namespace, query, false)
	if err != nil {
		return errIterator(err)
	}
	iter := newIterator(nil, result, nsArray, nil, nil, nil)
	db.updatePayloadTypes(nsArray, &iter.ser, iter.rawQueryParams)
	return iter
}

func (db *Reindexer) execSQLAsJSON(namespace string, query string) *JSONIterator {
	result, _, err := db.prepareSQL(namespace, query, true)
	if err != nil {
		return errJSONIterator(err)
	}
	defer result.Free()
	json, jsonOffsets, err := db.rawResultToJson(result.GetBuf(), namespace, "", nil, nil)
	if err != nil {
		return errJSONIterator(err)
	}
	return newJSONIterator(nil, json, jsonOffsets)
}

// Execute query
func (db *Reindexer) deleteQuery(q *Query) (int, error) {

	ns, err := db.getNS(q.Namespace)
	if err != nil {
		return 0, err
	}

	result, err := db.binding.DeleteQuery(q.ser.Bytes())
	defer result.Free()
	if err != nil {
		return 0, err
	}

	ser := newSerializer(result.GetBuf())
	// skip total count
	rawQueryParams := ser.readRawQueryParams()

	ns.cacheLock.Lock()
	for i := 0; i < rawQueryParams.count; i++ {
		params := ser.readRawtItemParams()
		jres := ser.GetCInt()
		if jres != 0 {
			panic("Internal error: joined items in delete query result")
		}
		// Update cache
		if _, ok := ns.cacheItems[params.id]; ok {
			delete(ns.cacheItems, params.id)
		}

	}
	ns.cacheLock.Unlock()
	if !ser.Eof() {
		panic("Internal error: data after end of delete query result")
	}

	return rawQueryParams.count, err
}
