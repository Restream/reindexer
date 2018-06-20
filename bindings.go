package reindexer

import (
	"fmt"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/restream/reindexer/bindings"
	"github.com/restream/reindexer/cjson"
)

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

	out, err := db.binding.ModifyItem(ns.nsHash, ser.Bytes(), mode)

	if err != nil {
		return 0, err
	}
	defer out.Free()

	rdSer := newSerializer(out.GetBuf())
	rawQueryParams := rdSer.readRawQueryParams(func(nsid int) {
		ns.cjsonState.ReadPayloadType(&rdSer.Serializer)
	})

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

	ser.PutVString(ns.name)

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

		ser.PutVarCUInt(bindings.FormatCJson)

		// reserve int for save len
		pos := len(ser.Bytes())
		ser.PutUInt32(0)
		pos1 := len(ser.Bytes())

		enc := ns.cjsonState.NewEncoder()
		if err := enc.Encode(item, ser); err != nil {
			return err
		}

		*(*uint32)(unsafe.Pointer(&ser.Bytes()[pos])) = uint32(len(ser.Bytes()) - pos1)
	} else {
		ser.PutVarCUInt(bindings.FormatJson)
		ser.PutBytes(json)
	}

	ser.PutVarCUInt(len(precepts))
	for _, precept := range precepts {
		ser.PutVString(precept)
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
	return db.binding.PutMeta(namespace, key, string(data))
}

func (db *Reindexer) getMeta(namespace, key string) ([]byte, error) {

	out, err := db.binding.GetMeta(namespace, key)
	if err != nil {
		return nil, err
	}
	defer out.Free()

	rdSer := newSerializer(out.GetBuf())
	s := rdSer.GetVString()
	return []byte(s), nil
}

func unpackItem(ns nsArrayEntry, params rawResultItemParams, allowUnsafe bool, nonCacheableData bool) (item interface{}, err error) {
	useCache := (ns.deepCopyIface || allowUnsafe) && !nonCacheableData && ns.cacheItems != nil
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
				dec := ns.localCjsonState.NewDecoder()
				if params.cptr != 0 {
					err = dec.DecodeCPtr(params.cptr, item)
				} else if params.data != nil {
					err = dec.Decode(params.data, item)
				} else {
					panic(fmt.Errorf("Internal error while decoding item id %d from ns %s: cptr and data are both null", params.id, ns.name))
				}
				if err != nil {
					ns.cacheLock.Unlock()
					return
				}
				ns.cacheItems[params.id] = cacheItem{item: item, version: params.version}
			}
			ns.cacheLock.Unlock()
		} else {
			item = reflect.New(ns.rtype).Interface()
			dec := ns.localCjsonState.NewDecoder()
			if params.cptr != 0 {
				err = dec.DecodeCPtr(params.cptr, item)
			} else if params.data != nil {
				err = dec.Decode(params.data, item)
			} else {
				panic(fmt.Errorf("Internal error while decoding item id %d from ns %s: cptr and data are both null", params.id, ns.name))
			}
			if err != nil {
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

	if len(totalName) != 0 && rawQueryParams.totalcount != 0 {
		jsonBuf.WriteString(totalName)
		jsonBuf.WriteString("\":")
		jsonBuf.WriteString(strconv.Itoa(rawQueryParams.totalcount))
		jsonBuf.WriteString(",\"")
	}

	jsonBuf.WriteString(jsonName)
	jsonBuf.WriteString("\":[")

	for i := 0; i < rawQueryParams.count; i++ {
		item := ser.readRawtItemParams()
		if i != 0 {
			jsonBuf.WriteString(",")
		}
		offsets = append(offsets, len(jsonBuf.Bytes()))
		jsonBuf.Write(item.data)

		if ser.GetVarUInt() != 0 {
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
		q.nsArray = append(q.nsArray, nsArrayEntry{ns, ns.cjsonState.Copy()})
	} else {
		return nil, err
	}

	ser := q.ser
	ser.PutVarCUInt(queryEnd)
	for _, sq := range q.joinQueries {
		ser.PutVarCUInt(sq.joinType)
		ser.Append(sq.ser)
		ser.PutVarCUInt(queryEnd)
		if ns, err := db.getNS(sq.Namespace); err == nil {
			q.nsArray = append(q.nsArray, nsArrayEntry{ns, ns.cjsonState.Copy()})
		} else {
			return nil, err
		}
	}
	for _, sq := range q.mergedQueries {
		ser.PutVarCUInt(merge)
		ser.Append(sq.ser)
		ser.PutVarCUInt(queryEnd)
		if ns, err := db.getNS(sq.Namespace); err == nil {
			q.nsArray = append(q.nsArray, nsArrayEntry{ns, ns.cjsonState.Copy()})
		} else {
			return nil, err
		}
	}

	ptVersions := make([]int32, 0, 16)
	for _, ns := range q.nsArray {
		ptVersions = append(ptVersions, ns.localCjsonState.Version^ns.localCjsonState.CacheToken)
	}
	fetchCount := q.fetchCount
	if asJson {
		// json iterator not support fetch queries
		fetchCount = -1
	}
	result, err = db.binding.SelectQuery(ser.Bytes(), asJson, ptVersions, fetchCount)

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

func (db *Reindexer) prepareSQL(namespace, query string, asJson bool) (result bindings.RawBuffer, nsArray []nsArrayEntry, err error) {
	nsArray = make([]nsArrayEntry, 0, 3)
	var ns *reindexerNamespace

	if ns, err = db.getNS(namespace); err != nil {
		return
	}

	nsArray = append(nsArray, nsArrayEntry{ns, ns.cjsonState.Copy()})

	ptVersions := make([]int32, 0, 16)
	for _, ns := range nsArray {
		ptVersions = append(ptVersions, ns.localCjsonState.Version^ns.localCjsonState.CacheToken)
	}

	result, err = db.binding.Select(query, asJson, ptVersions, defaultFetchCount)
	return
}

func (db *Reindexer) execSQL(namespace, query string) *Iterator {
	result, nsArray, err := db.prepareSQL(namespace, query, false)
	if err != nil {
		return errIterator(err)
	}
	iter := newIterator(nil, result, nsArray, nil, nil, nil)
	return iter
}

func (db *Reindexer) execSQLAsJSON(namespace string, query string) *JSONIterator {
	result, _, err := db.prepareSQL(namespace, query, true)
	if err != nil {
		return errJSONIterator(err)
	}
	defer result.Free()
	json, jsonOffsets, err := db.rawResultToJson(result.GetBuf(), namespace, "total", nil, nil)
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

	result, err := db.binding.DeleteQuery(ns.nsHash, q.ser.Bytes())
	if err != nil {
		return 0, err
	}
	defer result.Free()

	ser := newSerializer(result.GetBuf())
	// skip total count
	rawQueryParams := ser.readRawQueryParams()

	ns.cacheLock.Lock()
	for i := 0; i < rawQueryParams.count; i++ {
		params := ser.readRawtItemParams()
		jres := ser.GetVarUInt()
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

func WithCgoLimit(cgoLimit int) interface{} {
	return bindings.OptionCgoLimit{cgoLimit}
}

func WithConnPoolSize(connPoolSize int) interface{} {
	return bindings.OptionConnPoolSize{connPoolSize}
}
