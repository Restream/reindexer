package reindexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/restream/reindexer/v5/bindings"
	"github.com/restream/reindexer/v5/cjson"
)

// Strict modes for queries
type QueryStrictMode int

const (
	queryStrictModeNotSet  QueryStrictMode = bindings.QueryStrictModeNotSet
	QueryStrictModeNone                    = bindings.QueryStrictModeNone    // Allows any fields in conditions but doesn't check actual values for non-existing names
	QueryStrictModeNames                   = bindings.QueryStrictModeNames   // Allows only valid fields and indexes in conditions. Otherwise, query will return error
	QueryStrictModeIndexes                 = bindings.QueryStrictModeIndexes // Allows only indexes in conditions. Otherwise, query will return error
)

// Constants for query serialization
const (
	queryCondition              = bindings.QueryCondition
	querySortIndex              = bindings.QuerySortIndex
	queryJoinOn                 = bindings.QueryJoinOn
	queryLimit                  = bindings.QueryLimit
	queryOffset                 = bindings.QueryOffset
	queryReqTotal               = bindings.QueryReqTotal
	queryDebugLevel             = bindings.QueryDebugLevel
	queryAggregation            = bindings.QueryAggregation
	querySelectFilter           = bindings.QuerySelectFilter
	queryExplain                = bindings.QueryExplain
	querySelectFunction         = bindings.QuerySelectFunction
	queryEqualPosition          = bindings.QueryEqualPosition
	queryUpdateField            = bindings.QueryUpdateField
	queryEnd                    = bindings.QueryEnd
	queryAggregationLimit       = bindings.QueryAggregationLimit
	queryAggregationOffset      = bindings.QueryAggregationOffset
	queryAggregationSort        = bindings.QueryAggregationSort
	queryOpenBracket            = bindings.QueryOpenBracket
	queryCloseBracket           = bindings.QueryCloseBracket
	queryJoinCondition          = bindings.QueryJoinCondition
	queryDropField              = bindings.QueryDropField
	queryUpdateObject           = bindings.QueryUpdateObject
	queryWithRank               = bindings.QueryWithRank
	queryStrictMode             = bindings.QueryStrictMode
	queryUpdateFieldV2          = bindings.QueryUpdateFieldV2
	queryBetweenFieldsCondition = bindings.QueryBetweenFieldsCondition
	queryAlwaysFalseCondition   = bindings.QueryAlwaysFalseCondition
	queryAlwaysTrueCondition    = bindings.QueryAlwaysTrueCondition
	querySubQueryCondition      = bindings.QuerySubQueryCondition
	queryFieldSubQueryCondition = bindings.QueryFieldSubQueryCondition
	queryKnnCondition           = bindings.QueryKnnCondition
	queryKnnConditionExt        = bindings.QueryKnnConditionExt
)

// Constants for KNN query types
const (
	knnQueryTypeBase       = bindings.KnnQueryTypeBase
	knnQueryTypeBruteForce = bindings.KnnQueryTypeBruteForce
	knnQueryTypeHnsw       = bindings.KnnQueryTypeHnsw
	knnQueryTypeIvf        = bindings.KnnQueryTypeIvf

	knnQueryParamsVersion = bindings.KnnQueryParamsVersion

	knnSerializeWithK      = 1
	knnSerializeWithRadius = 1 << 1
)

// Constants for KNN query data formats
const (
	knnQueryDataFormatVector = bindings.KnnQueryDataFormatVector
	knnQueryDataFormatString = bindings.KnnQueryDataFormatString
)

// Constants for calc total
const (
	modeNoCalc        = bindings.ModeNoCalc
	modeCachedTotal   = bindings.ModeCachedTotal
	modeAccurateTotal = bindings.ModeAccurateTotal
)

// Operators
const (
	opAND = bindings.OpAnd
	opOR  = bindings.OpOr
	opNOT = bindings.OpNot
)

// Join types
const (
	innerJoin   = bindings.InnerJoin
	orInnerJoin = bindings.OrInnerJoin
	leftJoin    = bindings.LeftJoin
	merge       = bindings.Merge
)

// Constants for value types
const (
	cInt32Max        = bindings.CInt32Max
	valueInt         = bindings.ValueInt
	valueBool        = bindings.ValueBool
	valueInt64       = bindings.ValueInt64
	valueDouble      = bindings.ValueDouble
	valueString      = bindings.ValueString
	valueComposite   = bindings.ValueComposite
	valueTuple       = bindings.ValueTuple
	valueUuid        = bindings.ValueUuid
	valueFloatVector = bindings.ValueFloatVector
)

const (
	defaultFetchCount = 1000
)

type nsArrayEntry struct {
	*reindexerNamespace
	localCjsonState cjson.State
}

type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// Query represents a database query object
type Query struct {
	noCopy         noCopy
	Namespace      string
	db             *reindexerImpl
	nextOp         int
	ser            cjson.Serializer
	root           *Query
	joinQueries    []*Query
	mergedQueries  []*Query
	joinToFields   []string
	joinHandlers   []JoinHandler
	context        interface{}
	joinType       int
	closed         bool
	initBuf        [256]byte
	nsArray        []nsArrayEntry
	ptVersions     []int32
	iterator       Iterator
	jsonIterator   JSONIterator
	items          []interface{}
	json           []byte
	jsonOffsets    []int
	totalName      string
	executed       bool
	fetchCount     int
	queriesCount   int
	openedBrackets []int
	tx             *Tx
	traceNew       []byte
	traceClose     []byte
}

type KnnSearchParam interface {
	serialize(*cjson.Serializer)
}

type BaseKnnSearchParam struct {
	K      *int
	Radius *float32
}

type IndexBFSearchParam struct {
	BaseKnnSearchParam
}

type IndexHnswSearchParam struct {
	BaseKnnSearchParam
	Ef int
}

type IndexIvfSearchParam struct {
	BaseKnnSearchParam
	NProbe int
}

func NewIndexBFSearchParam(baseParam BaseKnnSearchParam) (IndexBFSearchParam, error) {
	return IndexBFSearchParam{baseParam}, nil
}

func NewIndexHnswSearchParam(ef int, baseParam BaseKnnSearchParam) (IndexHnswSearchParam, error) {
	if baseParam.K != nil && ef < *baseParam.K {
		return IndexHnswSearchParam{}, fmt.Errorf("Ef should not be less than K")
	}
	return IndexHnswSearchParam{baseParam, ef}, nil
}

func NewIndexIvfSearchParam(nprobe int, baseParam BaseKnnSearchParam) (IndexIvfSearchParam, error) {
	if nprobe < 1 {
		return IndexIvfSearchParam{}, fmt.Errorf("Nprobe should not be less than 1")
	}
	return IndexIvfSearchParam{baseParam, nprobe}, nil
}

func serializeKandRadius(p BaseKnnSearchParam, ser *cjson.Serializer) {
	var mask uint8
	if p.K != nil {
		mask |= knnSerializeWithK
	}
	if p.Radius != nil {
		mask |= knnSerializeWithRadius
	}
	ser.PutUInt8(mask)
	if p.K != nil {
		ser.PutVarCUInt(*p.K)
	}
	if p.Radius != nil {
		ser.PutFloat32(*p.Radius)
	}
}

func (p BaseKnnSearchParam) SetK(k int) BaseKnnSearchParam {
	p.K = &k
	return p
}

func (p BaseKnnSearchParam) SetRadius(radius float32) BaseKnnSearchParam {
	p.Radius = &radius
	return p
}

func (p BaseKnnSearchParam) serialize(ser *cjson.Serializer) {
	ser.PutVarCUInt(knnQueryTypeBase)
	ser.PutVarCUInt(knnQueryParamsVersion)
	serializeKandRadius(p, ser)
}

func (p IndexBFSearchParam) serialize(ser *cjson.Serializer) {
	ser.PutVarCUInt(knnQueryTypeBruteForce)
	ser.PutVarCUInt(knnQueryParamsVersion)
	serializeKandRadius(p.BaseKnnSearchParam, ser)
}

func (p IndexHnswSearchParam) serialize(ser *cjson.Serializer) {
	ser.PutVarCUInt(knnQueryTypeHnsw)
	ser.PutVarCUInt(knnQueryParamsVersion)
	serializeKandRadius(p.BaseKnnSearchParam, ser)
	ser.PutVarCUInt(p.Ef)
}

func (p IndexIvfSearchParam) serialize(ser *cjson.Serializer) {
	ser.PutVarCUInt(knnQueryTypeIvf)
	ser.PutVarCUInt(knnQueryParamsVersion)
	serializeKandRadius(p.BaseKnnSearchParam, ser)
	ser.PutVarCUInt(p.NProbe)
}

var queryPool sync.Pool
var enableDebug bool

func init() {
	enableDebug = os.Getenv("REINDEXER_GODEBUG") != ""
}

func mktrace(buf *[]byte) {
	if enableDebug {
		if *buf == nil {
			*buf = make([]byte, 0x4000)
		}
		*buf = (*buf)[0:runtime.Stack((*buf)[0:cap((*buf))], false)]
	}
}

// Create new DB query
func newQuery(db *reindexerImpl, namespace string, tx *Tx) *Query {
	var q *Query
	obj := queryPool.Get()
	if obj != nil {
		q = obj.(*Query)
	}
	if q == nil {
		q = &Query{}
		q.ser = cjson.NewSerializer(q.initBuf[:0])
	} else {
		q.tx = nil
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
		q.queriesCount = 0
		q.openedBrackets = q.openedBrackets[:0]
	}
	mktrace(&q.traceNew)

	q.Namespace = namespace
	q.db = db
	q.nextOp = opAND
	q.fetchCount = defaultFetchCount
	q.tx = tx

	q.ser.PutVString(namespace)
	return q
}

// MakeCopy - create copy of query with same or different DB. Thisresets query context
func (q *Query) MakeCopy(db *Reindexer) *Query {
	return q.makeCopy(db.impl, nil)
}

func (q *Query) makeCopy(db *reindexerImpl, root *Query) *Query {
	var qC *Query
	obj := queryPool.Get()
	if obj != nil {
		qC = obj.(*Query)
	}

	if qC == nil {
		qC = &Query{}
	}
	mktrace(&qC.traceNew)

	qC.ser = cjson.NewSerializer(qC.initBuf[:0])

	qC.db = db
	qC.Namespace = q.Namespace
	qC.nextOp = q.nextOp

	qC.ser.Append(q.ser)

	qC.joinToFields = append(q.joinToFields[:0:0], q.joinToFields...)
	qC.joinHandlers = append(q.joinHandlers[:0:0], q.joinHandlers...)
	//TODO not real copy
	qC.context = q.context
	qC.joinType = q.joinType
	qC.nsArray = append(q.nsArray[:0:0], q.nsArray...)
	qC.ptVersions = append(q.ptVersions[:0:0], q.ptVersions...)
	qC.items = append(q.items[:0:0], q.items...)
	qC.json = append(q.json[:0:0], q.json...)
	qC.jsonOffsets = append(q.jsonOffsets[:0:0], q.jsonOffsets...)
	qC.totalName = q.totalName
	qC.executed = q.executed
	qC.fetchCount = q.fetchCount

	qC.closed = q.closed
	if q.root != nil && root == nil {
		qC.root = q.root.makeCopy(db, nil)
	} else if root != nil {
		qC.root = root
	} else {
		qC.root = nil
	}
	qC.joinQueries = qC.joinQueries[:0]
	for _, qj := range q.joinQueries {
		qC.joinQueries = append(qC.joinQueries, qj.makeCopy(db, qC))
	}
	qC.mergedQueries = qC.mergedQueries[:0]
	for _, qm := range q.mergedQueries {
		qC.mergedQueries = append(qC.mergedQueries, qm.makeCopy(db, qC))
	}
	return qC

}

// Where - Add where condition to DB query
// For composite indexes keys must be []interface{}, with value of each subindex
func (q *Query) Where(index string, condition int, keys interface{}) *Query {
	t := reflect.TypeOf(keys)
	v := reflect.ValueOf(keys)

	if keys != nil && (t == reflect.TypeOf((*Query)(nil)).Elem() || (t.Kind() == reflect.Ptr && t.Elem() == reflect.TypeOf((*Query)(nil)).Elem())) {
		q.ser.PutVarCUInt(queryFieldSubQueryCondition)
		q.ser.PutVarCUInt(q.nextOp)
		q.ser.PutVString(index)
		q.ser.PutVarCUInt(condition)
		if t.Kind() == reflect.Ptr {
			q.ser.PutVBytes(v.Interface().(*Query).ser.Bytes())
		} else {
			subQuery := v.Interface().(Query)
			q.ser.PutVBytes(subQuery.ser.Bytes())
		}
	} else {
		q.ser.PutVarCUInt(queryCondition)
		q.ser.PutVString(index)
		q.ser.PutVarCUInt(q.nextOp)
		q.ser.PutVarCUInt(condition)

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
	}
	q.queriesCount++
	q.nextOp = opAND
	return q
}

// WhereQuery - Add subquery condition to DB query
func (q *Query) WhereQuery(subQuery *Query, condition int, keys interface{}) *Query {
	t := reflect.TypeOf(keys)
	v := reflect.ValueOf(keys)

	q.ser.PutVarCUInt(querySubQueryCondition)
	q.ser.PutVarCUInt(q.nextOp)
	q.ser.PutVBytes(subQuery.ser.Bytes())
	q.ser.PutVarCUInt(condition)
	q.nextOp = opAND
	q.queriesCount++

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

// WhereBetweenFields - Add condition comparing two fields to DB query
func (q *Query) WhereBetweenFields(firstField string, condition int, secondField string) *Query {
	q.ser.PutVarCUInt(queryBetweenFieldsCondition)
	q.ser.PutVarCUInt(q.nextOp)
	q.ser.PutVString(firstField)
	q.ser.PutVarCUInt(condition)
	q.ser.PutVString(secondField)
	q.nextOp = opAND
	q.queriesCount++
	return q
}

// OpenBracket - Open bracket for where condition to DB query
func (q *Query) OpenBracket() *Query {
	q.ser.PutVarCUInt(queryOpenBracket)
	q.ser.PutVarCUInt(q.nextOp)
	q.nextOp = opAND
	q.openedBrackets = append(q.openedBrackets, q.queriesCount)
	q.queriesCount++
	return q
}

// CloseBracket - Close bracket for where condition to DB query
func (q *Query) CloseBracket() *Query {
	if q.nextOp != opAND {
		panic(fmt.Errorf("Operation before close bracket"))
	}
	if len(q.openedBrackets) < 1 {
		panic(fmt.Errorf("Close bracket before open it"))
	}
	q.ser.PutVarCUInt(queryCloseBracket)
	q.openedBrackets = q.openedBrackets[:len(q.openedBrackets)-1]
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

// WhereInt - Add where condition to DB query with int arguments
func (q *Query) WhereInt(index string, condition int, keys ...int) *Query {

	q.ser.PutVarCUInt(queryCondition).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(condition)
	q.nextOp = opAND
	q.queriesCount++

	q.ser.PutVarCUInt(len(keys))
	for _, v := range keys {
		q.ser.PutVarCUInt(valueInt).PutVarInt(int64(v))
	}
	return q
}

// WhereInt - Add where condition to DB query with int32 arguments
func (q *Query) WhereInt32(index string, condition int, keys ...int32) *Query {

	q.ser.PutVarCUInt(queryCondition).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(condition)
	q.nextOp = opAND
	q.queriesCount++

	q.ser.PutVarCUInt(len(keys))
	for _, v := range keys {
		q.ser.PutVarCUInt(valueInt).PutVarInt(int64(v))
	}
	return q
}

// WhereInt64 - Add where condition to DB query with int64 arguments
func (q *Query) WhereInt64(index string, condition int, keys ...int64) *Query {
	q.ser.PutVarCUInt(queryCondition).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(condition)
	q.nextOp = opAND
	q.queriesCount++

	q.ser.PutVarCUInt(len(keys))
	for _, v := range keys {
		q.ser.PutVarCUInt(valueInt64).PutVarInt(v)
	}
	return q
}

// WhereString - Add where condition to DB query with string arguments
func (q *Query) WhereString(index string, condition int, keys ...string) *Query {
	q.ser.PutVarCUInt(queryCondition).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(condition)
	q.nextOp = opAND
	q.queriesCount++

	q.ser.PutVarCUInt(len(keys))
	for _, v := range keys {
		q.ser.PutVarCUInt(valueString).PutVString(v)
	}
	return q
}

// WhereUuid - Add where condition to DB query with UUID arguments.
// This function applies binary encoding to the UUID value.
// 'index' MUST be declared as UUID index in this case
func (q *Query) WhereUuid(index string, condition int, keys ...string) *Query {
	q.ser.PutVarCUInt(queryCondition).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(condition)
	q.nextOp = opAND
	q.queriesCount++

	q.ser.PutVarCUInt(len(keys))
	for _, v := range keys {
		uuid, err := cjson.ParseUuid(v)
		if err != nil {
			q.ser.PutVarCUInt(valueString).PutVString(v)
		} else {
			q.ser.PutVarCUInt(valueUuid).PutUuid(uuid)
		}
	}
	return q
}

// WhereKnn - Add where condition to DB query with float-point vector arguments.
// 'index' MUST be declared as "float_vector" index in this case
func (q *Query) WhereKnn(index string, vec []float32, params KnnSearchParam) *Query {
	q.ser.PutVarCUInt(queryKnnCondition).PutVString(index).PutVarCUInt(q.nextOp).PutFloatVector(vec)
	params.serialize(&q.ser)
	q.nextOp = opAND
	q.queriesCount++
	return q
}

// WhereKnnString - Add where condition to DB query with auto-embedded float-point vector args calculated from 'val'.
// Before this, you need to configure "query_embedder".
// 'index' MUST be declared as "float_vector" index in this case
func (q *Query) WhereKnnString(index string, val string, params KnnSearchParam) *Query {
	q.ser.PutVarCUInt(queryKnnConditionExt).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(knnQueryDataFormatString).PutVString(val)
	params.serialize(&q.ser)
	q.nextOp = opAND
	q.queriesCount++
	return q
}

// WhereComposite - Add where condition to DB query with interface args for composite indexes
func (q *Query) WhereComposite(index string, condition int, keys ...interface{}) *Query {
	return q.Where(index, condition, keys)
}

// Match - Add where string EQ-condition to DB query with string args
func (q *Query) Match(index string, keys ...string) *Query {
	return q.WhereString(index, EQ, keys...)
}

// WhereBool - Add where condition to DB query with bool args
func (q *Query) WhereBool(index string, condition int, keys ...bool) *Query {

	q.ser.PutVarCUInt(queryCondition).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(condition)
	q.nextOp = opAND
	q.queriesCount++

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
	q.queriesCount++

	q.ser.PutVarCUInt(len(keys))
	for _, v := range keys {
		q.ser.PutVarCUInt(valueDouble).PutDouble(v)
	}
	return q
}

// DWithin - Add DWithin condition to DB query
func (q *Query) DWithin(index string, point Point, distance float64) *Query {

	q.ser.PutVarCUInt(queryCondition).PutVString(index).PutVarCUInt(q.nextOp).PutVarCUInt(DWITHIN)
	q.nextOp = opAND
	q.queriesCount++

	q.ser.PutVarCUInt(3)
	q.ser.PutVarCUInt(valueDouble).PutDouble(point[0])
	q.ser.PutVarCUInt(valueDouble).PutDouble(point[1])
	q.ser.PutVarCUInt(valueDouble).PutDouble(distance)
	return q
}

func (q *Query) setAggregate(field string, aggregateType int) *Query {
	q.ser.PutVarCUInt(queryAggregation).PutVarCUInt(aggregateType).PutVarCUInt(1).PutVString(field)
	return q
}
func (q *Query) AggregateSum(field string) *Query { return q.setAggregate(field, AggSum) }
func (q *Query) AggregateAvg(field string) *Query { return q.setAggregate(field, AggAvg) }
func (q *Query) AggregateMin(field string) *Query { return q.setAggregate(field, AggMin) }
func (q *Query) AggregateMax(field string) *Query { return q.setAggregate(field, AggMax) }

type AggregateFacetRequest struct {
	query *Query
}

// fields should not be empty.
func (q *Query) AggregateFacet(fields ...string) *AggregateFacetRequest {
	q.ser.PutVarCUInt(queryAggregation).PutVarCUInt(AggFacet).PutVarCUInt(len(fields))
	for _, f := range fields {
		q.ser.PutVString(f)
	}
	r := AggregateFacetRequest{q}
	return &r
}

func (r *AggregateFacetRequest) setAggregateType(aggregateType int, value int) *AggregateFacetRequest {
	r.query.ser.PutVarCUInt(aggregateType).PutVarCUInt(value)
	return r
}
func (r *AggregateFacetRequest) Limit(limit int) *AggregateFacetRequest {
	return r.setAggregateType(queryAggregationLimit, limit)
}
func (r *AggregateFacetRequest) Offset(offset int) *AggregateFacetRequest {
	return r.setAggregateType(queryAggregationOffset, offset)
}

// Use field 'count' to sort by facet's count value.
func (r *AggregateFacetRequest) Sort(field string, desc bool) *AggregateFacetRequest {
	r.query.ser.PutVarCUInt(queryAggregationSort).PutVString(field)
	if desc {
		r.query.ser.PutVarUInt(1)
	} else {
		r.query.ser.PutVarUInt(0)
	}
	return r
}

// Sort - Apply sort order to returned from query items
// If values argument specified, then items equal to values, if found will be placed in the top positions
// For composite indexes values must be []interface{}, with value of each subindex
// Forced sort is support for the first sorting field only
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

// SortStDistance - wrapper for geometry sorting by shortest distance between geometry field and point (ST_Distance)
func (q *Query) SortStPointDistance(field string, p Point, desc bool) *Query {
	var sb strings.Builder
	sb.Grow(256)
	sb.WriteString("ST_Distance(")
	sb.WriteString(field)
	sb.WriteString(",ST_GeomFromText('point(")
	sb.WriteString(strconv.FormatFloat(p[0], 'f', -1, 64))
	sb.WriteRune(' ')
	sb.WriteString(strconv.FormatFloat(p[1], 'f', -1, 64))
	sb.WriteString(")'))")
	return q.Sort(sb.String(), desc)
}

// SortStDistance - wrapper for geometry sorting by the shortest distance between two geometry fields (ST_Distance)
func (q *Query) SortStFieldDistance(field1 string, field2 string, desc bool) *Query {
	var sb strings.Builder
	sb.Grow(256)
	sb.WriteString("ST_Distance(")
	sb.WriteString(field1)
	sb.WriteRune(',')
	sb.WriteString(field2)
	sb.WriteRune(')')
	return q.Sort(sb.String(), desc)
}

// AND - the next condition will be added with AND.
// This is the default operation for the WHERE statement. It does not have to be called explicitly in user code. Used in DSL conversion.
func (q *Query) And() *Query {
	q.nextOp = opAND
	return q
}

// OR - the next condition will be added with OR.
// Implements short-circuiting:
// if the previous condition is successful, the next will not be evaluated, except for Join conditions.
func (q *Query) Or() *Query {
	q.nextOp = opOR
	return q
}

// Not - the next condition will be added with NOT AND.
// Implements short-circuiting:
// if the previous condition fails, the next will not be evaluated.
func (q *Query) Not() *Query {
	q.nextOp = opNOT
	return q
}

// Distinct - Return only items with uniq value of field
func (q *Query) Distinct(distinctFields ...string) *Query {
	l := len(distinctFields)
	q.ser.PutVarCUInt(queryAggregation).PutVarCUInt(AggDistinct).PutVarCUInt(l)
	for i := 0; i < l; i++ {
		q.ser.PutVString(distinctFields[i])
	}
	return q
}

func (q *Query) reqTotal(accurateMode int, totalNames ...string) *Query {
	q.ser.PutVarCUInt(queryReqTotal)
	q.ser.PutVarCUInt(accurateMode)
	if len(totalNames) != 0 {
		q.totalName = totalNames[0]
	}
	return q
}

// ReqTotal Request total items calculation
func (q *Query) ReqTotal(totalNames ...string) *Query {
	return q.reqTotal(modeAccurateTotal, totalNames...)
}

// CachedTotal Request cached total items calculation
func (q *Query) CachedTotal(totalNames ...string) *Query {
	return q.reqTotal(modeCachedTotal, totalNames...)
}

func (q *Query) setValue(qmode int, value int) *Query {
	if value > cInt32Max {
		value = cInt32Max
	}
	q.ser.PutVarCUInt(qmode).PutVarCUInt(value)
	return q
}

// Limit - Set limit (count) of returned items
func (q *Query) Limit(limitItems int) *Query { return q.setValue(queryLimit, limitItems) }

// Offset - Set start offset of returned items
func (q *Query) Offset(startOffset int) *Query { return q.setValue(queryOffset, startOffset) }

// Debug - Set debug level
func (q *Query) Debug(level int) *Query { return q.setValue(queryDebugLevel, level) }

// Strict - Set query strict mode
func (q *Query) Strict(mode QueryStrictMode) *Query { return q.setValue(queryStrictMode, int(mode)) }

// Explain - Request explain for query
func (q *Query) Explain() *Query {
	q.ser.PutVarCUInt(queryExplain)
	return q
}

// Output fulltex or KNN rank
// Allowed only with fulltext or KNN queries
func (q *Query) WithRank() *Query {
	q.ser.PutVarCUInt(queryWithRank)
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
	return q.ExecCtx(context.Background())
}

// ExecCtx will execute query, and return slice of items
func (q *Query) ExecCtx(ctx context.Context) *Iterator {
	if q.root != nil {
		q = q.root
	}
	if q.closed {
		q.panicTrace("Exec call on already closed query. You should create new Query")
	}
	if q.executed {
		q.panicTrace("Exec call on already executed query. You should create new Query")
	}

	q.executed = true

	return q.db.execQuery(ctx, q)
}

// ExecToJson will execute query, and return iterator
func (q *Query) ExecToJson(jsonRoots ...string) *JSONIterator {
	return q.ExecToJsonCtx(context.Background(), jsonRoots...)
}

// ExecToJsonCtx will execute query, and return iterator
func (q *Query) ExecToJsonCtx(ctx context.Context, jsonRoots ...string) *JSONIterator {
	if q.root != nil {
		q = q.root
	}
	if q.closed {
		q.panicTrace("Exec call on already closed query. You should create new Query")
	}
	if q.executed {
		q.panicTrace("Exec call on already executed query. You should create new Query")
	}

	q.executed = true

	jsonRoot := q.Namespace
	if len(jsonRoots) != 0 && len(jsonRoots[0]) != 0 {
		jsonRoot = jsonRoots[0]
	}

	return q.db.execToJsonQuery(ctx, q, jsonRoot)
}

func (q *Query) close() {
	if q.root != nil {
		q = q.root
	}
	if q.closed {
		q.panicTrace("Close call on already closed query")
	}
	mktrace(&q.traceClose)

	for i, jq := range q.joinQueries {
		jq.closed = true
		mktrace(&jq.traceClose)
		queryPool.Put(jq)
		q.joinQueries[i] = nil
	}

	for i, mq := range q.mergedQueries {
		mq.closed = true
		mktrace(&mq.traceClose)
		queryPool.Put(mq)
		q.mergedQueries[i] = nil
	}

	for i := range q.joinHandlers {
		q.joinHandlers[i] = nil
	}

	q.closed = true
	q.tx = nil
	queryPool.Put(q)
}

func (q *Query) panicTrace(msg string) {
	if !enableDebug {
		fmt.Println("To see query allocation/close traces set REINDEXER_GODEBUG=1 environment variable!")
	} else {
		fmt.Printf("Query allocation trace: %s\n\nQuery close trace %s\n\n", string(q.traceNew), string(q.traceClose))
	}
	panic(errors.New(msg))
}

// Delete will execute query, and delete items, matches query
// On success return number of deleted elements
func (q *Query) Delete() (int, error) {
	return q.DeleteCtx(context.Background())
}

// DeleteCtx will execute query, and delete items, matches query
// On success return number of deleted elements
func (q *Query) DeleteCtx(ctx context.Context) (int, error) {
	if q.root != nil {
		q = q.root
	}

	if q.closed {
		q.panicTrace("Delete call on already closed query. You should create new Query")
	}

	defer q.close()

	if q.tx != nil {
		return q.db.deleteQueryTx(ctx, q, q.tx)
	}

	return q.db.deleteQuery(ctx, q)
}

func getValueJSON(value interface{}) string {
	ok := false
	var err error
	var objectJSON []byte
	t := reflect.TypeOf(value)
	if value == nil {
		objectJSON = []byte("{}")
	} else if t.Kind() == reflect.Struct || t.Kind() == reflect.Map {
		objectJSON, err = json.Marshal(value)
		if err != nil {
			panic(err)
		}
	} else if objectJSON, ok = value.([]byte); !ok {
		panic(errors.New("SetObject doesn't support this type of objects: " + t.Kind().String()))
	}
	return string(objectJSON)
}

// SetObject adds update of object field request for update query
func (q *Query) SetObject(field string, values interface{}) *Query {
	size := 1
	isArray := false
	t := reflect.TypeOf(values)
	v := reflect.ValueOf(values)
	if t != reflect.TypeOf([]byte{}) && (t.Kind() == reflect.Array || t.Kind() == reflect.Slice) {
		size = v.Len()
		isArray = true
	}
	jsonValues := make([]string, size)
	if isArray {
		for i := 0; i < size; i++ {
			jsonValues[i] = getValueJSON(v.Index(i).Interface())
		}
	} else if size > 0 {
		jsonValues[0] = getValueJSON(values)
	}

	q.ser.PutVarCUInt(queryUpdateObject)
	q.ser.PutVString(field)

	// values count
	q.ser.PutVarCUInt(size)
	// is array flag
	if isArray {
		q.ser.PutVarCUInt(1)
	} else {
		q.ser.PutVarCUInt(0)
	}
	for i := 0; i < size; i++ {
		// function/value flag
		q.ser.PutVarUInt(0)
		q.ser.PutVarCUInt(valueString)
		q.ser.PutVString(jsonValues[i])
	}

	return q
}

// Set adds update field request for update query
func (q *Query) Set(field string, values interface{}) *Query {
	t := reflect.TypeOf(values)
	if t.Kind() == reflect.Struct || t.Kind() == reflect.Map {
		return q.SetObject(field, values)
	}
	if (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) && t.Elem().Kind() == reflect.Struct {
		return q.SetObject(field, values)
	}
	v := reflect.ValueOf(values)

	cmd := queryUpdateField
	if (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) && v.Len() <= 1 {
		// If field is slice, with size eq 0 or 1, then old
		// queryUpdateField command cant encode it properly
		cmd = queryUpdateFieldV2
	}

	q.ser.PutVarCUInt(cmd)
	q.ser.PutVString(field)

	if values == nil {
		if cmd == queryUpdateFieldV2 {
			q.ser.PutVarUInt(0) // is array
		}
		q.ser.PutVarUInt(0) // size
	} else if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		if cmd == queryUpdateFieldV2 {
			q.ser.PutVarUInt(1) // is array
		}
		q.ser.PutVarCUInt(v.Len())
		for i := 0; i < v.Len(); i++ {
			// function/value flag
			q.ser.PutVarUInt(0)
			q.putValue(v.Index(i))
		}
	} else {
		if cmd == queryUpdateFieldV2 {
			q.ser.PutVarUInt(0) // is array
		}
		q.ser.PutVarCUInt(1) // size
		// function/value flag
		q.ser.PutVarUInt(0)
		q.putValue(v)
	}
	return q
}

// Drop removes field from item within Update statement
func (q *Query) Drop(field string) *Query {
	q.ser.PutVarCUInt(queryDropField)
	q.ser.PutVString(field)
	return q
}

// SetExpression updates indexed field by arithmetical expression
func (q *Query) SetExpression(field string, value string) *Query {
	q.ser.PutVarCUInt(queryUpdateField)
	q.ser.PutVString(field)

	q.ser.PutVarCUInt(1) // size
	q.ser.PutVarUInt(1)  // is expression
	q.putValue(reflect.ValueOf(value))

	return q
}

// Update will execute query and update fields in items that match this query
// On success, it returns number of updated elements
func (q *Query) Update() *Iterator {
	return q.UpdateCtx(context.Background())
}

// UpdateCtx will execute query and update fields in items that match this query
// On success, it returns number of updated elements
func (q *Query) UpdateCtx(ctx context.Context) *Iterator {
	if q.root != nil {
		q = q.root
	}
	if q.closed {
		q.panicTrace("Update call on already closed query. You should create new Query")
	}

	q.executed = true

	if q.tx != nil {
		return q.db.updateQueryTx(ctx, q, q.tx)
	}

	return q.db.updateQuery(ctx, q)
}

// MustExec will execute query and return iterator. Generates panic on error
func (q *Query) MustExec() *Iterator {
	return q.MustExecCtx(context.Background())
}

// MustExecCtx will execute query and return iterator. Generates panic on error
func (q *Query) MustExecCtx(ctx context.Context) *Iterator {
	it := q.ExecCtx(ctx)
	if it.err != nil {
		panic(it.err)
	}
	return it
}

// Get will execute query and return 1 st item. Generates panic on error
func (q *Query) Get() (item interface{}, found bool) {
	return q.GetCtx(context.Background())
}

// GetCtx will execute query and return first item. Generates panic on error
func (q *Query) GetCtx(ctx context.Context) (item interface{}, found bool) {
	if q.root != nil {
		q = q.root
	}
	iter := q.Limit(1).MustExecCtx(ctx)
	defer iter.Close()
	if iter.Next() {
		return iter.Object(), true
	}
	return nil, false
}

// GetJson will execute query, and return 1 st item. Generates panic on error
func (q *Query) GetJson() (json []byte, found bool) {
	return q.GetJsonCtx(context.Background())
}

// GetJsonCtx will execute query, and return first item. Generates panic on error
func (q *Query) GetJsonCtx(ctx context.Context) (json []byte, found bool) {
	if q.root != nil {
		q = q.root
	}
	it := q.Limit(1).ExecToJsonCtx(ctx)
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
		panic(errors.New("query.Join call on already joined query. You should create new Query"))
	}
	if joinType != leftJoin {
		q.ser.PutVarCUInt(queryJoinCondition)
		q.ser.PutVarCUInt(joinType)
		q.ser.PutVarCUInt(len(q.joinQueries)) // index of join query
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
// Handler will be always set to the main query
func (q *Query) JoinHandler(field string, handler JoinHandler) *Query {
	if q.root != nil {
		// Joined queries can not have JoinHandlers themselves. Routing this call to the root query if current query is joined
		for _, jq := range q.root.joinQueries {
			if q == jq {
				q.root.JoinHandler(field, handler)
				return q
			}
		}
	}
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
		q.panicTrace("query.On call on already closed query. You should create new Query")
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

func (q *Query) addFields(itemType int, fields ...string) *Query {
	for _, field := range fields {
		q.ser.PutVarCUInt(itemType).PutVString(field)
	}
	return q
}

// Select add filter to  fields of result's objects
// The `fields` should be specified in the same case as the jsonpaths corresponding to them.
// Non-existent `fields` and `fields` in the wrong case are ignored.
// If there are no `fields` in this list that meet these conditions, then the filter works as "*".
func (q *Query) Select(fields ...string) *Query {
	return q.addFields(querySelectFilter, fields...)
}

func (q *Query) SelectAllFields() *Query {
	return q.addFields(querySelectFilter, "*", "vectors()")
}

// FetchCount sets the number of items that will be fetched by one operation
// When n <= 0 query will fetch all results in one operation
func (q *Query) FetchCount(n int) *Query {
	q.fetchCount = n
	return q
}

// Functions add optional select functions (e.g highlight or snippet ) to fields of result's objects
func (q *Query) Functions(fields ...string) *Query {
	return q.addFields(querySelectFunction, fields...)
}

// Adds equal position fields to arrays
func (q *Query) EqualPosition(fields ...string) *Query {
	q.ser.PutVarCUInt(queryEqualPosition)
	if len(q.openedBrackets) == 0 {
		q.ser.PutVarCUInt(0)
	} else {
		q.ser.PutVarCUInt(q.openedBrackets[len(q.openedBrackets)-1] + 1)
	}
	q.ser.PutVarCUInt(len(fields))
	for _, field := range fields {
		q.ser.PutVString(field)
	}
	return q
}
