package reindexer

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/restream/reindexer/bindings"
	"github.com/restream/reindexer/bindings/builtinserver/config"
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

	for tryCount := 0; tryCount < 2; tryCount++ {
		ser := cjson.NewPoolSerializer()
		defer ser.Close()

		format := 0
		stateToken := 0

		if format, stateToken, err = packItem(ns, item, json, ser); err != nil {
			return
		}

		out, err := db.binding.ModifyItem(ns.nsHash, ns.name, format, ser.Bytes(), mode, precepts, stateToken)

		if err != nil {
			rerr, ok := err.(bindings.Error)
			if ok && rerr.Code() == bindings.ErrStateInvalidated {
				db.Query(ns.name).Limit(0).Exec().Close()
				err = rerr
				continue
			}
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
	return 0, err
}

func packItem(ns *reindexerNamespace, item interface{}, json []byte, ser *cjson.Serializer) (format int, stateToken int, err error) {

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

		format = bindings.FormatCJson

		enc := ns.cjsonState.NewEncoder()
		if stateToken, err = enc.Encode(item, ser); err != nil {
			return
		}

	} else {
		format = bindings.FormatJson
		ser.Write(json)
	}

	return
}

func (db *Reindexer) getNS(namespace string) (*reindexerNamespace, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	ns, ok := db.ns[strings.ToLower(namespace)]
	if !ok {
		return nil, errNsNotFound
	}
	return ns, nil
}

func (db *Reindexer) PutMeta(namespace, key string, data []byte) error {
	return db.binding.PutMeta(namespace, key, string(data))
}

func (db *Reindexer) GetMeta(namespace, key string) ([]byte, error) {

	out, err := db.binding.GetMeta(namespace, key)
	if err != nil {
		return nil, err
	}
	defer out.Free()
	ret := make([]byte, len(out.GetBuf()))
	copy(ret, out.GetBuf())
	return ret, nil
}

func unpackItem(ns *nsArrayEntry, params *rawResultItemParams, allowUnsafe bool, nonCacheableData bool) (item interface{}, err error) {
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

func (db *Reindexer) rawResultToJson(rawResult []byte, jsonName string, totalName string, initJson []byte, initOffsets []int) (json []byte, offsets []int, explain []byte, err error) {

	ser := newSerializer(rawResult)
	rawQueryParams := ser.readRawQueryParams()
	explain = rawQueryParams.explainResults

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

		if (rawQueryParams.flags&bindings.ResultsWithJoined) != 0 && ser.GetVarUInt() != 0 {
			panic("Sorry, not implemented: Can't return join query results as json")
		}
	}
	jsonBuf.WriteString("]}")

	return jsonBuf.Bytes(), offsets, explain, nil
}

func (db *Reindexer) prepareQuery(q *Query, asJson bool) (result bindings.RawBuffer, err error) {

	if ns, err := db.getNS(q.Namespace); err == nil {
		q.nsArray = append(q.nsArray, nsArrayEntry{ns, ns.cjsonState.Copy()})
	} else {
		return nil, err
	}

	ser := q.ser
	for _, sq := range q.mergedQueries {
		if ns, err := db.getNS(sq.Namespace); err == nil {
			q.nsArray = append(q.nsArray, nsArrayEntry{ns, ns.cjsonState.Copy()})
		} else {
			return nil, err
		}
	}

	for _, sq := range q.joinQueries {
		if ns, err := db.getNS(sq.Namespace); err == nil {
			q.nsArray = append(q.nsArray, nsArrayEntry{ns, ns.cjsonState.Copy()})
		} else {
			return nil, err
		}
	}

	for _, mq := range q.mergedQueries {
		for _, sq := range mq.joinQueries {
			if ns, err := db.getNS(sq.Namespace); err == nil {
				q.nsArray = append(q.nsArray, nsArrayEntry{ns, ns.cjsonState.Copy()})
			} else {
				return nil, err
			}
		}
	}

	ser.PutVarCUInt(queryEnd)
	for _, sq := range q.joinQueries {
		ser.PutVarCUInt(sq.joinType)
		ser.Append(sq.ser)
		ser.PutVarCUInt(queryEnd)
	}

	for _, mq := range q.mergedQueries {
		ser.PutVarCUInt(merge)
		ser.Append(mq.ser)
		ser.PutVarCUInt(queryEnd)
		for _, sq := range mq.joinQueries {
			ser.PutVarCUInt(sq.joinType)
			ser.Append(sq.ser)
			ser.PutVarCUInt(queryEnd)
		}
	}

	for _, ns := range q.nsArray {
		q.ptVersions = append(q.ptVersions, ns.localCjsonState.Version^ns.localCjsonState.StateToken)
	}
	fetchCount := q.fetchCount
	if asJson {
		// json iterator not support fetch queries
		fetchCount = -1
	}
	result, err = db.binding.SelectQuery(ser.Bytes(), asJson, q.ptVersions, fetchCount)

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
	var explain []byte
	q.json, q.jsonOffsets, explain, err = db.rawResultToJson(result.GetBuf(), jsonRoot, q.totalName, q.json, q.jsonOffsets)
	if err != nil {
		return errJSONIterator(err)
	}
	return newJSONIterator(q, q.json, q.jsonOffsets, explain)
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
		ptVersions = append(ptVersions, ns.localCjsonState.Version^ns.localCjsonState.StateToken)
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
	json, jsonOffsets, explain, err := db.rawResultToJson(result.GetBuf(), namespace, "total", nil, nil)
	if err != nil {
		return errJSONIterator(err)
	}
	return newJSONIterator(nil, json, jsonOffsets, explain)
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
		if (rawQueryParams.flags&bindings.ResultsWithJoined) != 0 && ser.GetVarUInt() != 0 {
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

func (db *Reindexer) resetCaches() {
	db.lock.RLock()
	nsArray := make([]*reindexerNamespace, 0, len(db.ns))
	for _, ns := range db.ns {
		nsArray = append(nsArray, ns)
	}
	db.lock.RUnlock()
	for _, ns := range nsArray {
		ns.cacheLock.Lock()
		if ns.cacheItems != nil {
			ns.cacheItems = make(map[int]cacheItem)
		}
		ns.cacheLock.Unlock()
		ns.cjsonState.Reset()
		db.Query(ns.name).Limit(0).Exec().Close()
	}
}

func WithCgoLimit(cgoLimit int) interface{} {
	return bindings.OptionCgoLimit{cgoLimit}
}

func WithConnPoolSize(connPoolSize int) interface{} {
	return bindings.OptionConnPoolSize{connPoolSize}
}

func WithRetryAttempts(read int, write int) interface{} {
	return bindings.OptionRetryAttempts{read, write}
}

func WithServerConfig(startupTimeout time.Duration, serverConfig *config.ServerConfig) interface{} {
	return bindings.OptionBuiltinWithServer{ServerConfig: serverConfig, StartupTimeout: startupTimeout}
}
