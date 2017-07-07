package reindexer

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/restream/reindexer/bindings"
)

// Constants for query serialization
const (
	queryCondition = bindings.QueryCondition
	queryDistinct  = bindings.QueryDistinct
	querySortIndex = bindings.QuerySortIndex
	queryJoinOn    = bindings.QueryJoinOn
	queryEnd       = bindings.QueryEnd
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
)

// Query to DB object
type Query struct {
	bindings.Query
	db           *Reindexer
	totalCount   int
	nextOp       int
	ser          serializer
	root         *Query
	joinQueries  []*Query
	joinToFields []string
	context      interface{}
	joinType     int
	closed       bool
	initBuf      [256]byte
	iterator     Iterator
	items        []interface{}
	subitems     []interface{}
	copy         bool
	noObjCache   bool
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
		q.ser.buf = q.initBuf[:0:cap(q.initBuf)]
	} else {
		q.totalCount = 0
		q.nextOp = 0
		q.root = nil
		q.joinType = 0
		q.context = nil
		q.joinToFields = q.joinToFields[:0]
		q.joinQueries = q.joinQueries[:0]
		q.ser.buf = q.ser.buf[:0]
		q.closed = false
		q.copy = false
	}

	q.Query = bindings.Query{Namespace: namespace}
	q.db = db
	q.nextOp = opAND

	q.ser.writeString(namespace)
	return q
}

// Where - Add where condition to DB query
func (q *Query) Where(index string, condition int, keys interface{}) *Query {
	t := reflect.TypeOf(keys)
	v := reflect.ValueOf(keys)

	q.ser.writeCInt(queryCondition)
	q.ser.writeString(index)
	q.ser.writeCInt(q.nextOp)
	q.ser.writeCInt(condition)
	q.nextOp = opAND

	if keys == nil {
		q.ser.writeCInt(0)
	} else if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		q.ser.writeCInt(v.Len())
		for i := 0; i < v.Len(); i++ {
			q.ser.writeValue(v.Index(i))
		}
	} else {
		q.ser.writeCInt(1).writeValue(v)
	}
	return q
}

// WhereInt - Add where condition to DB query with int args
func (q *Query) WhereInt(index string, condition int, keys ...int) *Query {

	q.ser.writeCInt(queryCondition).writeString(index).writeCInt(q.nextOp).writeCInt(condition)
	q.nextOp = opAND

	q.ser.writeCInt(len(keys))
	for _, v := range keys {
		q.ser.writeCInt(valueInt).writeCInt(v)
	}
	return q
}

// WhereInt64 - Add where condition to DB query with int64 args
func (q *Query) WhereInt64(index string, condition int, keys ...int64) *Query {

	q.ser.writeCInt(queryCondition).writeString(index).writeCInt(q.nextOp).writeCInt(condition)
	q.nextOp = opAND

	q.ser.writeCInt(len(keys))
	for _, v := range keys {
		q.ser.writeCInt(valueInt64).writeInt64(v)
	}
	return q
}

// WhereString - Add where condition to DB query with string args
func (q *Query) WhereString(index string, condition int, keys ...string) *Query {

	q.ser.writeCInt(queryCondition).writeString(index).writeCInt(q.nextOp).writeCInt(condition)
	q.nextOp = opAND

	q.ser.writeCInt(len(keys))
	for _, v := range keys {
		q.ser.writeCInt(valueString).writeString(v)
	}
	return q
}

// WhereString - Add where condition to DB query with bool args
func (q *Query) WhereBool(index string, condition int, keys ...bool) *Query {

	q.ser.writeCInt(queryCondition).writeString(index).writeCInt(q.nextOp).writeCInt(condition)
	q.nextOp = opAND

	q.ser.writeCInt(len(keys))
	for _, v := range keys {
		q.ser.writeCInt(valueInt).writeBool(v)
	}
	return q
}

// WhereDouble - Add where condition to DB query with float args
func (q *Query) WhereDouble(index string, condition int, keys ...float64) *Query {

	q.ser.writeCInt(queryCondition).writeString(index).writeCInt(q.nextOp).writeCInt(condition)
	q.nextOp = opAND

	q.ser.writeCInt(len(keys))
	for _, v := range keys {
		q.ser.writeCInt(valueDouble).writeDouble(v)
	}
	return q
}

// Sort - Apply sort order to returned from query items
func (q *Query) Sort(sortIndex string, desc bool) *Query {

	q.ser.writeCInt(querySortIndex)
	q.ser.writeString(sortIndex)
	if desc {
		q.ser.writeCInt(1)
	} else {
		q.ser.writeCInt(0)
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
	q.ser.writeCInt(queryDistinct)
	q.ser.writeString(distinctIndex)
	return q
}

// ReqTotal Request total items calculation
func (q *Query) ReqTotal() *Query {
	q.ReqTotalCount = true
	return q
}

// Limit - Set limit (count) of returned items
func (q *Query) Limit(limitItems int) *Query {
	q.LimitItems = limitItems
	return q
}

// Offset - Set start offset of returned items
func (q *Query) Offset(startOffset int) *Query {
	q.StartOffset = startOffset
	return q
}

// GetTotal - return total items in DB for query
func (q *Query) GetTotal() int {
	if q.closed {
		panic(errors.New("query.GetTotal call on already closed query. You shoud create new Query"))
	}
	return q.totalCount
}

// NoObjCache - Disable object cache
func (q *Query) NoObjCache() *Query {
	q.noObjCache = true
	return q
}

// Debug - Set debug level
func (q *Query) Debug(level int) *Query {
	q.DebugLevel = level
	return q
}

// Copy - performs copy of objects. returned slice will contain pointers to object copies
func (q *Query) CopyResults() *Query {
	if q.root != nil {
		q = q.root
	}
	q.copy = true
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
	return q.db.selectQuery(q)
}

func (q *Query) Close() {
	if q.root != nil {
		q = q.root
	}
	if q.closed {
		panic(errors.New("Close call on already closed query."))
	}

	for _, jq := range q.joinQueries {
		jq.closed = true
		queryPool.Put(jq)
	}
	q.joinQueries = q.joinQueries[:0]
	q.closed = true
	queryPool.Put(q)
}

// Delete will execute query, and delete items, matches query
// On sucess return number of deleted elements
func (q *Query) Delete() (int, error) {
	if q.root != nil || len(q.joinQueries) != 0 {
		return 0, errors.New("Delete does not support joined queries")
	}
	if q.LimitItems != 0 || q.StartOffset != 0 || q.ReqTotalCount {
		return 0, errors.New("Delete does not support limit/offset arguments")
	}
	if q.closed {
		panic(errors.New("Delete call on already closed query. You shoud create new Query"))
	}

	defer q.Close()
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
	res, _ := q.MustExec().PtrSlice()
	defer q.Close()
	if len(res) == 0 {
		return nil, false
	}
	return res[0], true
}

// Join joins 2 queries
func (q *Query) join(q2 *Query, field string, joinType int) *Query {
	if q.root != nil {
		q = q.root
	}
	q2.joinType = joinType
	q2.root = q
	q.joinQueries = append(q.joinQueries, q2)
	q.joinToFields = append(q.joinToFields, field)
	return q2
}

// InnerJoin joins 2 queries - items from 1-st query are expanded with data from joined query
func (q *Query) InnerJoin(q2 *Query, field string) *Query {

	if q.nextOp == opOR {
		return q.join(q2, field, orInnerJoin)
	}

	return q.join(q2, field, innerJoin)
}

// Join joins 2 queries, alias to LeftJoin
func (q *Query) Join(q2 *Query, field string) *Query {
	return q.join(q2, field, leftJoin)
}

// LeftJoin joins 2 queries = - items from 1-st query are filtered and expanded with data from 2-nd query
func (q *Query) LeftJoin(q2 *Query, field string) *Query {
	return q.join(q2, field, leftJoin)
}

// On Add Join condition
func (q *Query) On(index string, condition int, joinIndex string) *Query {
	if q.closed {
		panic(errors.New("query.On call on already closed query. You shoud create new Query"))
	}
	if q.root == nil {
		panic(fmt.Errorf("Can't join on root query"))
	}
	q.ser.writeCInt(queryJoinOn)
	q.ser.writeCInt(condition)
	q.ser.writeString(index)
	q.ser.writeString(joinIndex)
	return q
}
