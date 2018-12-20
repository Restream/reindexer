package reindexer

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unsafe"

	"github.com/restream/reindexer/bindings"
	"github.com/restream/reindexer/cjson"
)

// Constants for query serialization
const (
	queryCondition      = bindings.QueryCondition
	queryDistinct       = bindings.QueryDistinct
	querySortIndex      = bindings.QuerySortIndex
	queryJoinOn         = bindings.QueryJoinOn
	queryLimit          = bindings.QueryLimit
	queryOffset         = bindings.QueryOffset
	queryReqTotal       = bindings.QueryReqTotal
	queryDebugLevel     = bindings.QueryDebugLevel
	queryAggregation    = bindings.QueryAggregation
	querySelectFilter   = bindings.QuerySelectFilter
	queryExplain        = bindings.QueryExplain
	QuerySelectFunction = bindings.QuerySelectFunction
	QueryEqualPosition  = bindings.QueryEqualPosition
	queryEnd            = bindings.QueryEnd
)

// Constants for calc total
const (
	modeNoCalc        = bindings.ModeNoCalc
	modeCachedTotal   = bindings.ModeCachedTotal
	modeAccurateTotal = bindings.ModeAccurateTotal
)

// Operator
const (
	opAND = bindings.OpAnd
	opOR  = bindings.OpOr
	opNOT = bindings.OpNot
)

// Join type
const (
	innerJoin   = bindings.InnerJoin
	orInnerJoin = bindings.OrInnerJoin
	leftJoin    = bindings.LeftJoin
	merge       = bindings.Merge
)

const (
	cInt32Max      = bindings.CInt32Max
	valueInt       = bindings.ValueInt
	valueBool      = bindings.ValueBool
	valueInt64     = bindings.ValueInt64
	valueDouble    = bindings.ValueDouble
	valueString    = bindings.ValueString
	valueComposite = bindings.ValueComposite
	valueTuple     = bindings.ValueTuple
)

const (
	defaultFetchCount = 100
)

type nsArrayEntry struct {
	*reindexerNamespace
	localCjsonState cjson.State
}

// Query to DB object
type Query struct {
	Namespace     string
	db            *Reindexer
	nextOp        int
	ser           cjson.Serializer
	root          *Query
	joinQueries   []*Query
	mergedQueries []*Query
	joinToFields  []string
	joinHandlers  []JoinHandler
	context       interface{}
	joinType      int
	closed        bool
	initBuf       [256]byte
	nsArray       []nsArrayEntry
	ptVersions    []int32
	iterator      Iterator
	jsonIterator  JSONIterator
	items         []interface{}
	json          []byte
	jsonOffsets   []int
	totalName     string
	executed      bool
	fetchCount    int
}

var queryPool sync.Pool

// Create new DB query
func newQuery(db *Reindexer, namespace string) *Query {
	var q *Query
	obj := queryPool.Get()
	if obj != nil {
		q = obj.(*Query)
	}
	if q == nil {
		q = &Query{}
		q.ser = cjson.NewSerializer(q.initBuf[:0])
	} else {
		q.nextOp = 0
		q.root = nil
		q.joinType = 0
		q.context = nil
		q.joinToFields = q.joinToFields[:0]
		q.joinQueries = q.joinQueries[:0]
		q.joinHandlers = q.joinHandlers[:0]
		q.mergedQueries = q.mergedQueries[:0]
		q.ptVersions = q.ptVersions[:0]
		q.ser = cjson.NewSerializer(q.ser.Bytes()[:0])
		q.closed = false
		q.totalName = ""
		q.executed = false
		q.nsArray = q.nsArray[:0]
	}

	q.Namespace = namespace
	q.db = db
	q.nextOp = opAND
	q.fetchCount = defaultFetchCount

	q.ser.PutVString(namespace)
	return q
}

// Where - Add where condition to DB query
// For composite indexes keys must be []interface{}, with value of each subindex
func (q *Query) Where(index string, condition int, keys interface{}) *Query {
	t := reflect.TypeOf(keys)
	v := reflect.ValueOf(keys)

	q.ser.PutVarCUInt(queryCondition)
	q.ser.PutVString(index)
	q.ser.PutVarCUInt(q.nextOp)
	q.ser.PutVarCUInt(condition)
	q.nextOp = opAND

	if keys == nil {
		q.ser.PutVarUInt(0)
	} else if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		q.ser.PutVarCUInt(v.Len())
		for i := 0; i < v.Len(); i++ {
			q.putValue(v.Index(i))
		}
	} else {
		q.ser.PutVarCUInt(1)
		q.putValue(v)
	}
	return q
}

func (q *Query) putValue(v reflect.Value) error {
	k := v.Kind()
	if k == reflect.Ptr || k == reflect.Interface {
		v = v.Elem()
		k = v.Kind()
	}

	switch k {
	case reflect.Bool:
		q.ser.PutVarCUInt(valueBool)
		if v.Bool() {
			q.ser.PutVarUInt(1)
		} else {
			q.ser.PutVarUInt(0)
		}
	case reflect.Uint:
		if unsafe.Sizeof(int(0)) == unsafe.Sizeof(int64(0)) {
			q.ser.PutVarCUInt(valueInt64)
		} else {
			q.ser.PutVarCUInt(valueInt)
		}

		q.ser.PutVarInt(int64(v.Uint()))
	case reflect.Int:
		if unsafe.Sizeof(int(0)) == unsafe.Sizeof(int64(0)) {
			q.ser.PutVarCUInt(valueInt64)
		} else {
			q.ser.PutVarCUInt(valueInt)
		}
		q.ser.PutVarInt(v.Int())
	case reflect.Int16, reflect.Int32, reflect.Int8:
		q.ser.PutVarCUInt(valueInt)
		q.ser.PutVarInt(v.Int())
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		q.ser.PutVarCUInt(valueInt)
		q.ser.PutVarInt(int64(v.Uint()))
	case reflect.Int64:
		q.ser.PutVarCUInt(valueInt64)
		q.ser.PutVarInt(v.Int())
	case reflect.Uint64:
		q.ser.PutVarCUInt(valueInt64)
		q.ser.PutVarInt(int64(v.Uint()))
	case reflect.String:
		q.ser.PutVarCUInt(valueString)
		q.ser.PutVString(v.String())
	case reflect.Float32, reflect.Float64:
		q.ser.PutVarCUInt(valueDouble)
		q.ser.PutDouble(v.Float())
	case reflect.Slice, reflect.Array:
		q.ser.PutVarCUInt(valueTuple)
		q.ser.PutVarCUInt(v.Len())
		for i := 0; i < v.Len(); i++ {
			q.putValue(v.Index(i))
		}
	default:
		panic(fmt.Errorf("rq: Invalid reflection type %s", v.Kind().String()))
	}
	return nil
}

// WhereInt - Add where condition to DB query with int args
func (q *Query) WhereInt(index string, condition int, keys ...int) *Query {

	q.ser.PutVarCUInt(queryCondition).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(condition)
	q.nextOp = opAND

	q.ser.PutVarCUInt(len(keys))
	for _, v := range keys {
		q.ser.PutVarCUInt(valueInt).PutVarInt(int64(v))
	}
	return q
}

// WhereInt - Add where condition to DB query with int args
func (q *Query) WhereInt32(index string, condition int, keys ...int32) *Query {

	q.ser.PutVarCUInt(queryCondition).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(condition)
	q.nextOp = opAND

	q.ser.PutVarCUInt(len(keys))
	for _, v := range keys {
		q.ser.PutVarCUInt(valueInt).PutVarInt(int64(v))
	}
	return q
}

// WhereInt64 - Add where condition to DB query with int64 args
func (q *Query) WhereInt64(index string, condition int, keys ...int64) *Query {

	q.ser.PutVarCUInt(queryCondition).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(condition)
	q.nextOp = opAND

	q.ser.PutVarCUInt(len(keys))
	for _, v := range keys {
		q.ser.PutVarCUInt(valueInt64).PutVarInt(v)
	}
	return q
}

// WhereString - Add where condition to DB query with string args
func (q *Query) WhereString(index string, condition int, keys ...string) *Query {

	q.ser.PutVarCUInt(queryCondition).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(condition)
	q.nextOp = opAND

	q.ser.PutVarCUInt(len(keys))
	for _, v := range keys {
		q.ser.PutVarCUInt(valueString).PutVString(v)
	}
	return q
}

// WhereComposite - Add where condition to DB query with interface args for composite indexes
func (q *Query) WhereComposite(index string, condition int, keys ...interface{}) *Query {
	return q.Where(index, condition, keys)
}

// WhereString - Add where condition to DB query with string args
func (q *Query) Match(index string, keys ...string) *Query {

	return q.WhereString(index, EQ, keys...)
}

// WhereString - Add where condition to DB query with bool args
func (q *Query) WhereBool(index string, condition int, keys ...bool) *Query {

	q.ser.PutVarCUInt(queryCondition).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(condition)
	q.nextOp = opAND

	q.ser.PutVarCUInt(len(keys))
	for _, v := range keys {
		q.ser.PutVarCUInt(valueBool)
		if v {
			q.ser.PutVarUInt(1)
		} else {
			q.ser.PutVarUInt(0)
		}
	}
	return q
}

// WhereDouble - Add where condition to DB query with float args
func (q *Query) WhereDouble(index string, condition int, keys ...float64) *Query {

	q.ser.PutVarCUInt(queryCondition).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(condition)
	q.nextOp = opAND

	q.ser.PutVarCUInt(len(keys))
	for _, v := range keys {
		q.ser.PutVarCUInt(valueDouble).PutDouble(v)
	}
	return q
}

// Aggregate - Return aggregation of field
func (q *Query) Aggregate(index string, aggType int) *Query {

	q.ser.PutVarCUInt(queryAggregation).PutVString(index).PutVarCUInt(aggType)
	return q
}

// Sort - Apply sort order to returned from query items
// If values argument specified, then items equal to values, if found will be placed in the top positions
// For composite indexes values must be []interface{}, with value of each subindex
func (q *Query) Sort(sortIndex string, desc bool, values ...interface{}) *Query {

	q.ser.PutVarCUInt(querySortIndex)
	q.ser.PutVString(sortIndex)
	if desc {
		q.ser.PutVarUInt(1)
	} else {
		q.ser.PutVarUInt(0)
	}

	q.ser.PutVarCUInt(len(values))
	for i := 0; i < len(values); i++ {
		q.putValue(reflect.ValueOf(values[i]))
	}

	return q
}

// OR - next condition will added with OR
func (q *Query) Or() *Query {
	q.nextOp = opOR
	return q
}

// Not - next condition will added with NOT AND
func (q *Query) Not() *Query {
	q.nextOp = opNOT
	return q
}

// Distinct - Return only items with uniq value of field
func (q *Query) Distinct(distinctIndex string) *Query {
	q.ser.PutVarCUInt(queryDistinct)
	q.ser.PutVString(distinctIndex)
	return q
}

// ReqTotal Request total items calculation
func (q *Query) ReqTotal(totalNames ...string) *Query {
	q.ser.PutVarCUInt(queryReqTotal)
	q.ser.PutVarCUInt(modeAccurateTotal)
	if len(totalNames) != 0 {
		q.totalName = totalNames[0]
	}
	return q
}

// CachedTotal Request cached total items calculation
func (q *Query) CachedTotal(totalNames ...string) *Query {
	q.ser.PutVarCUInt(queryReqTotal)
	q.ser.PutVarCUInt(modeCachedTotal)
	if len(totalNames) != 0 {
		q.totalName = totalNames[0]
	}
	return q
}

// Limit - Set limit (count) of returned items
func (q *Query) Limit(limitItems int) *Query {
	if limitItems > cInt32Max {
		limitItems = cInt32Max
	}
	q.ser.PutVarCUInt(queryLimit).PutVarCUInt(limitItems)
	return q
}

// Offset - Set start offset of returned items
func (q *Query) Offset(startOffset int) *Query {
	if startOffset > cInt32Max {
		startOffset = cInt32Max
	}
	q.ser.PutVarCUInt(queryOffset).PutVarCUInt(startOffset)
	return q
}

// Debug - Set debug level
func (q *Query) Debug(level int) *Query {
	q.ser.PutVarCUInt(queryDebugLevel).PutVarCUInt(level)
	return q
}

// Explain - Request explain for query
func (q *Query) Explain() *Query {
	q.ser.PutVarCUInt(queryExplain)
	return q
}

// SetContext set interface, which will be passed to Joined interface
func (q *Query) SetContext(ctx interface{}) *Query {
	q.context = ctx
	if q.root != nil {
		q.root.context = ctx
	}

	return q
}

// Exec will execute query, and return slice of items
func (q *Query) Exec() *Iterator {
	if q.root != nil {
		q = q.root
	}
	if q.closed {
		panic(errors.New("Exec call on already closed query. You shoud create new Query"))
	}
	if q.executed {
		panic(errors.New("Exec call on already executed query. You shoud create new Query"))
	}
	q.executed = true

	return q.db.execQuery(q)
}

// ExecAsJson will execute query, and return iterator
func (q *Query) ExecToJson(jsonRoots ...string) *JSONIterator {
	if q.root != nil {
		q = q.root
	}
	if q.closed {
		panic(errors.New("Exec call on already closed query. You shoud create new Query"))
	}
	if q.executed {
		panic(errors.New("Exec call on already executed query. You shoud create new Query"))
	}
	q.executed = true

	jsonRoot := q.Namespace
	if len(jsonRoots) != 0 && len(jsonRoots[0]) != 0 {
		jsonRoot = jsonRoots[0]
	}

	return q.db.execJSONQuery(q, jsonRoot)
}

func (q *Query) close() {
	if q.root != nil {
		q = q.root
	}
	if q.closed {
		panic(errors.New("Close call on already closed query"))
	}

	for i, jq := range q.joinQueries {
		jq.closed = true
		queryPool.Put(jq)
		q.joinQueries[i] = nil
	}

	for i, mq := range q.mergedQueries {
		mq.closed = true
		queryPool.Put(mq)
		q.mergedQueries[i] = nil
	}

	for i := range q.joinHandlers {
		q.joinHandlers[i] = nil
	}

	q.closed = true
	queryPool.Put(q)
}

// Delete will execute query, and delete items, matches query
// On sucess return number of deleted elements
func (q *Query) Delete() (int, error) {
	if q.root != nil || len(q.joinQueries) != 0 {
		return 0, errors.New("Delete does not support joined queries")
	}
	if q.closed {
		panic(errors.New("Delete call on already closed query. You shoud create new Query"))
	}

	defer q.close()
	return q.db.deleteQuery(q)
}

// MustExec will execute query, and return iterator, panic on error
func (q *Query) MustExec() *Iterator {
	it := q.Exec()
	if it.err != nil {
		panic(it.err)
	}
	return it
}

// Get will execute query, and return 1 st item, panic on error
func (q *Query) Get() (item interface{}, found bool) {
	iter := q.Limit(1).MustExec()
	defer iter.Close()
	if iter.Next() {
		return iter.Object(), true
	}
	return nil, false
}

// Get will execute query, and return 1 st item, panic on error
func (q *Query) GetJson() (json []byte, found bool) {
	it := q.Limit(1).ExecToJson()
	defer it.Close()
	if it.Error() != nil {
		panic(it.Error())
	}
	if !it.Next() {
		return nil, false
	}

	return it.JSON(), true
}

// Join joins 2 queries
func (q *Query) join(q2 *Query, field string, joinType int) *Query {
	if q.root != nil {
		q = q.root
	}
	if q2.root != nil {
		panic(errors.New("query.Join call on already joined query. You shoud create new Query"))
	}
	q2.joinType = joinType
	q2.root = q
	q.joinQueries = append(q.joinQueries, q2)
	q.joinToFields = append(q.joinToFields, field)
	q.joinHandlers = append(q.joinHandlers, nil)
	return q2
}

// InnerJoin joins 2 queries
// Items from the 1-st query are filtered by and expanded with the data from the 2-nd query
//
// `field` parameter serves as unique identifier for the join between `q` and `q2`
// One of the conditions below must hold for `field` parameter in order for InnerJoin to work:
// - namespace of `q2` contains `field` as one of its fields marked as `joined`
// - `q` has a join handler (registered via `q.JoinHandler(...)` call) with the same `field` value
func (q *Query) InnerJoin(q2 *Query, field string) *Query {

	if q.nextOp == opOR {
		q.nextOp = opAND
		return q.join(q2, field, orInnerJoin)
	}

	return q.join(q2, field, innerJoin)
}

// Join is an alias for LeftJoin
func (q *Query) Join(q2 *Query, field string) *Query {
	return q.join(q2, field, leftJoin)
}

// LeftJoin joins 2 queries
// Items from the 1-st query are expanded with the data from the 2-nd query
//
// `field` parameter serves as unique identifier for the join between `q` and `q2`
// One of the conditions below must hold for `field` parameter in order for LeftJoin to work:
// - namespace of `q2` contains `field` as one of its fields marked as `joined`
// - `q` has a join handler (registered via `q.JoinHandler(...)` call) with the same `field` value
func (q *Query) LeftJoin(q2 *Query, field string) *Query {
	return q.join(q2, field, leftJoin)
}

// JoinHandler registers join handler that will be called when join, registered on `field` value, finds a match
func (q *Query) JoinHandler(field string, handler JoinHandler) *Query {
	index := -1
	for i := range q.joinToFields {
		if strings.EqualFold(q.joinToFields[i], field) {
			index = i
		}
	}
	if index != -1 {
		q.joinHandlers[index] = handler
	}
	return q
}

// Merge 2 queries
func (q *Query) Merge(q2 *Query) *Query {
	if q.root != nil {
		q = q.root
	}
	if q2.root != nil {
		q2 = q2.root
	}
	q2.root = q
	q.mergedQueries = append(q.mergedQueries, q2)
	return q
}

// On specifies join condition
//
// `index` parameter specifies which field from `q` namespace should be used during join
// `condition` parameter specifies how `q` will be joined with the latest join query issued on `q` (e.g. `EQ`/`GT`/`SET`/...)
// `joinIndex` parameter specifies which field from namespace for the latest join query issued on `q` should be used during join
func (q *Query) On(index string, condition int, joinIndex string) *Query {
	if q.closed {
		panic(errors.New("query.On call on already closed query. You shoud create new Query"))
	}
	if q.root == nil {
		panic(fmt.Errorf("Can't join on root query"))
	}
	q.ser.PutVarCUInt(queryJoinOn)
	q.ser.PutVarCUInt(q.nextOp)
	q.ser.PutVarCUInt(condition)
	q.ser.PutVString(index)
	q.ser.PutVString(joinIndex)
	q.nextOp = opAND

	return q
}

// Select add filter to  fields of result's objects
func (q *Query) Select(fields ...string) *Query {
	for _, field := range fields {
		q.ser.PutVarCUInt(querySelectFilter).PutVString(field)
	}
	return q
}

// FetchCount sets the number of items that will be fetched by one operation
// When n <= 0 query will fetch all results in one operation
func (q *Query) FetchCount(n int) *Query {
	q.fetchCount = n
	return q
}

// Select add filter to  fields of result's objects
func (q *Query) Functions(fields ...string) *Query {
	for _, field := range fields {
		q.ser.PutVarCUInt(QuerySelectFunction).PutVString(field)
	}
	return q
}

// Adds equal position fields to arrays
func (q *Query) EqualPosition(fields ...string) *Query {
	q.ser.PutVarCUInt(QueryEqualPosition)
	q.ser.PutVarCUInt(len(fields))
	for _, field := range fields {
		q.ser.PutVString(field)
	}
	return q
}
