package reindexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/restream/reindexer"
	"github.com/restream/reindexer/bindings"
	"github.com/stretchr/testify/require"
)

const (
	opAND = iota
	opNOT
	opOR
)

type EqualPositions [][]string

var queryNames = map[int]string{
	reindexer.EQ:      "==",
	reindexer.GT:      ">",
	reindexer.LT:      "<",
	reindexer.GE:      ">=",
	reindexer.LE:      "<=",
	reindexer.SET:     "SET",
	reindexer.RANGE:   "RANGE",
	reindexer.ANY:     "ANY",
	reindexer.EMPTY:   "EMPTY",
	reindexer.LIKE:    "LIKE",
	reindexer.DWITHIN: "DWITHIN",
}

const (
	leaf = iota
	tree
)

type queryTestEntryContainer struct {
	op       int
	data     interface{}
	dataType int
}

type queryTestEntryTree struct {
	data        []queryTestEntryContainer
	activeChild int // index in data + 1, if 0 - no active child
}

type queryTestEntry struct {
	index     string
	condition int
	keys      []reflect.Value
	ikeys     interface{}
	fieldIdx  [][]int
}

// Test Query to DB object
type queryTest struct {
	q               *reindexer.Query
	entries         queryTestEntryTree
	distinctIndexes []string
	sortIndex       []string
	sortDesc        bool
	sortValues      map[string][]interface{}
	limitItems      int
	startOffset     int
	reqTotalCount   bool
	db              *ReindexerWrapper
	namespace       string
	nextOp          int
	ns              *testNamespace
	totalCount      int
	equalPositions  EqualPositions
	readOnly        bool
	deepReplEqual   bool
	needVerify      bool
	handClose       bool
}
type testNamespace struct {
	items     map[string]interface{}
	pkIdx     [][]int
	fieldsIdx map[string][][]int
	jsonPaths map[string]string
}

func (tn *testNamespace) getField(field string) ([][]int, bool) {
	value, ok := tn.fieldsIdx[strings.ToLower(field)]
	return value, ok
}

var queryTestPool sync.Pool

// Create new DB query
func newTestQuery(db *ReindexerWrapper, namespace string, needVerify ...bool) *queryTest {
	var qt *queryTest
	obj := queryTestPool.Get()
	if obj != nil {
		qt = obj.(*queryTest)
	}
	if qt == nil {
		qt = &queryTest{}
	} else {
		qt.distinctIndexes = []string{}
		qt.sortIndex = qt.sortIndex[:0]
		qt.entries.data = qt.entries.data[:0]
		qt.entries.activeChild = 0
		qt.equalPositions = qt.equalPositions[:0]
		qt.sortDesc = false
		qt.sortValues = nil
		qt.limitItems = 0
		qt.startOffset = 0
		qt.reqTotalCount = false
	}
	qt.q = db.GetBaseQuery(namespace)
	qt.deepReplEqual = false
	qt.readOnly = true
	qt.namespace = namespace
	qt.db = db
	qt.nextOp = opAND
	qt.handClose = false
	qt.ns = testNamespaces[strings.ToLower(namespace)]
	return qt
}

type txTest struct {
	tx        *reindexer.Tx
	namespace string
	db        *ReindexerWrapper
	ns        *testNamespace
}

func removeTestNamespce(namespace string) {
	delete(testNamespaces, namespace)
}

func newTestNamespace(namespace string, item interface{}) {

	if _, ok := testNamespaces[strings.ToLower(namespace)]; ok {
		return
	}

	ns := &testNamespace{
		items:     make(map[string]interface{}, 1000),
		fieldsIdx: make(map[string][][]int),
	}
	testNamespaces[strings.ToLower(namespace)] = ns
	prepareStruct(ns, reflect.TypeOf(item), []int{}, "")
}

func renameTestNamespace(namespace string, dstName string) {
	ns, ok := testNamespaces[namespace]
	if !ok {
		return
	}
	testNamespaces[strings.ToLower(dstName)] = ns
	delete(testNamespaces, namespace)
}

func newTestTx(db *ReindexerWrapper, namespace string) *txTest {
	return newTestTxCtx(context.Background(), db, namespace)
}

func newTestTxCtx(ctx context.Context, db *ReindexerWrapper, namespace string) *txTest {
	tx := &txTest{namespace: namespace, db: db, ns: testNamespaces[namespace]}
	tx.tx = db.WithContext(ctx).MustBeginTx(namespace)
	return tx
}

func (tx *txTest) Insert(s interface{}) error {
	val := reflect.Indirect(reflect.ValueOf(s))
	tx.ns.items[getPK(tx.ns, val)] = s
	return tx.tx.Insert(s)
}

func (tx *txTest) Update(s interface{}) error {
	val := reflect.Indirect(reflect.ValueOf(s))
	tx.ns.items[getPK(tx.ns, val)] = s
	return tx.tx.Update(s)
}

func (tx *txTest) Upsert(s interface{}) error {
	val := reflect.Indirect(reflect.ValueOf(s))
	tx.ns.items[getPK(tx.ns, val)] = s
	return tx.tx.Upsert(s)
}

func (tx *txTest) UpsertJSON(s interface{}) error {
	val := reflect.Indirect(reflect.ValueOf(s))
	tx.ns.items[getPK(tx.ns, val)] = s
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return tx.tx.UpsertJSON(b)
}

func (tx *txTest) Delete(s interface{}) error {
	val := reflect.Indirect(reflect.ValueOf(s))
	delete(tx.ns.items, getPK(tx.ns, val))
	return tx.tx.Delete(s)
}

func (tx *txTest) InsertAsync(s interface{}, cmpl bindings.Completion) error {
	val := reflect.Indirect(reflect.ValueOf(s))
	tx.ns.items[getPK(tx.ns, val)] = s
	return tx.tx.InsertAsync(s, cmpl)
}

func (tx *txTest) UpdateAsync(s interface{}, cmpl bindings.Completion) error {
	val := reflect.Indirect(reflect.ValueOf(s))
	tx.ns.items[getPK(tx.ns, val)] = s
	return tx.tx.UpdateAsync(s, cmpl)
}

func (tx *txTest) UpsertAsync(s interface{}, cmpl bindings.Completion) error {
	val := reflect.Indirect(reflect.ValueOf(s))
	tx.ns.items[getPK(tx.ns, val)] = s
	return tx.tx.UpsertAsync(s, cmpl)
}

func (tx *txTest) Commit() (int, error) {
	res, err := tx.tx.CommitWithCount()
	tx.db.SetSyncRequired()
	return res, err
}

func (tx *txTest) Rollback() error {
	err := tx.tx.Rollback()
	tx.db.SetSyncRequired()
	return err
}

func (tx *txTest) MustCommit() int {
	res := tx.tx.MustCommit()
	tx.db.SetSyncRequired()
	return res
}

func (tx *txTest) AwaitResults() *txTest {
	tx.tx.AwaitResults()
	return tx
}

func (tx *txTest) Query() *queryTest {
	q := tx.tx.Query()
	return &queryTest{q: q, db: tx.db, ns: tx.ns}

}

func (qt *queryTestEntryTree) toString() (ret string) {
	if len(qt.data) < 1 {
		return ret
	}
	for i, d := range qt.data {
		if i == 0 {
			if d.op == opNOT {
				ret += "NOT "
			}
		} else {
			switch d.op {
			case opNOT:
				ret += " AND NOT "
			case opAND:
				ret += " AND "
			case opOR:
				ret += " OR "
			}
		}
		if d.dataType == tree {
			ret += "(" + d.data.(*queryTestEntryTree).toString() + ")"
		} else {
			entry := d.data.(*queryTestEntry)
			ret += entry.index + " " + queryNames[entry.condition] + " "
			if len(entry.keys) > 1 {
				ret += "("
			}
			for i, c := range entry.keys {
				if i != 0 {
					ret += ","
				}
				ret += fmt.Sprintf("%v", c.Interface())
			}
			if len(entry.keys) > 1 {
				ret += ")"
			}
		}
	}
	return ret
}

func (qt *queryTest) toString() (ret string) {
	ret += " FROM " + qt.q.Namespace
	if len(qt.entries.data) > 0 {
		ret += " WHERE " + qt.entries.toString()
	}
	if len(qt.sortIndex) > 0 {
		ret += " ORDER BY "
	}
	for k := 0; k < len(qt.sortIndex); k++ {
		sortIndex := qt.sortIndex[k]
		ret += sortIndex
		if qt.sortDesc {
			ret += " DESC"
		}
		if qt.sortValues != nil && len(qt.sortValues[sortIndex]) > 0 {
			ret += "( "
			for _, val := range qt.sortValues[sortIndex] {
				ret += fmt.Sprintf("%v ", val)
			}
			ret += ")"
		}
		if k != len(qt.sortIndex) {
			ret += ", "
		}
	}
	if qt.limitItems != 0 {
		ret += " LIMIT " + strconv.Itoa(qt.limitItems)
	}
	if qt.startOffset != 0 {
		ret += " OFFSET " + strconv.Itoa(qt.startOffset)
	}
	for i, dIdx := range qt.distinctIndexes {
		if i != 0 {
			ret += ","
		}
		ret += " DISTINCT(" + dIdx + ")"
	}

	return ret
}

func (qt *queryTestEntryTree) addEntry(entry queryTestEntry, op int) {
	if qt.activeChild > 0 {
		qt.data[qt.activeChild-1].data.(*queryTestEntryTree).addEntry(entry, op)
	} else {
		qt.data = append(qt.data, queryTestEntryContainer{op, &entry, leaf})
	}
}

func (qt *queryTest) Functions(fields ...string) *queryTest {
	qt.q.Functions(fields...)
	return qt
}

func (qt *queryTest) SetExpression(field string, value string) *queryTest {
	qt.q.SetExpression(field, value)
	return qt
}

func (qt *queryTest) DeepReplEqual() *queryTest {
	qt.deepReplEqual = true
	return qt
}

// Where - Add where condition to DB query
func (qt *queryTest) Where(index string, condition int, keys interface{}) *queryTest {
	qte := queryTestEntry{index: index, condition: condition, ikeys: keys}
	qt.q.Where(index, condition, keys)

	if reflect.TypeOf(keys).Kind() == reflect.Slice || reflect.TypeOf(keys).Kind() == reflect.Array {
		for i := 0; i < reflect.ValueOf(keys).Len(); i++ {
			qte.keys = append(qte.keys, reflect.ValueOf(keys).Index(i))
		}
	} else {
		qte.keys = append(qte.keys, reflect.ValueOf(keys))
	}
	qte.fieldIdx, _ = qt.ns.getField(index)
	qt.entries.addEntry(qte, qt.nextOp)
	qt.nextOp = opAND

	return qt
}

// DWithin - Add DWithin condition to DB query
func (qt *queryTest) DWithin(index string, point [2]float64, distance float64) *queryTest {
	keys := make([]reflect.Value, 0)
	keys = append(keys, reflect.ValueOf(point[0]), reflect.ValueOf(point[1]), reflect.ValueOf(distance))
	qte := queryTestEntry{index: index, condition: reindexer.DWITHIN, ikeys: keys}
	qt.q.DWithin(index, point, distance)

	qte.keys = keys
	qte.fieldIdx, _ = qt.ns.getField(index)
	qt.entries.addEntry(qte, qt.nextOp)
	qt.nextOp = opAND

	return qt
}

func (qt *queryTestEntryTree) addTree(op int) {
	if qt.activeChild > 0 {
		qt.data[qt.activeChild-1].data.(*queryTestEntryTree).addTree(op)
	} else {
		qt.data = append(qt.data, queryTestEntryContainer{op, new(queryTestEntryTree), tree})
		qt.activeChild = len(qt.data)
	}
}

// OpenBracket - Open bracket for where condition to DB query
func (qt *queryTest) OpenBracket() *queryTest {
	qt.q.OpenBracket()
	qt.entries.addTree(qt.nextOp)
	qt.nextOp = opAND
	return qt
}

func (qt *queryTestEntryTree) exitTree() {
	if qt.activeChild <= 0 {
		panic(fmt.Errorf("No open bracket"))
	} else {
		t := qt.data[qt.activeChild-1].data.(*queryTestEntryTree)
		if t.activeChild > 0 {
			t.exitTree()
		} else {
			qt.activeChild = 0
		}
	}
}

// CloseBracket - Close bracket for where condition to DB query
func (qt *queryTest) CloseBracket() *queryTest {
	qt.q.CloseBracket()
	qt.entries.exitTree()
	return qt
}

func (qt *queryTest) EqualPosition(fields ...string) *queryTest {
	if len(fields) > 0 {
		qt.q.EqualPosition(fields...)
		qt.equalPositions = append(qt.equalPositions, fields)
	}
	return qt
}

// Sort - Apply sort order to returned from query items
func (qt *queryTest) Sort(sortIndex string, desc bool, values ...interface{}) *queryTest {
	if len(sortIndex) > 0 {
		qt.q.Sort(sortIndex, desc, values...)
		qt.sortIndex = append(qt.sortIndex, sortIndex)
		qt.sortDesc = desc
		if qt.sortValues == nil {
			qt.sortValues = make(map[string][]interface{})
		}
		qt.sortValues[sortIndex] = values
	}
	return qt
}

// OR - next condition will added with OR
func (qt *queryTest) Or() *queryTest {
	qt.q.Or()
	qt.nextOp = opOR
	return qt
}

func (qt *queryTest) Not() *queryTest {
	qt.q.Not()
	qt.nextOp = opNOT
	return qt
}

// Distinct - Return only items with uniq value of field
func (qt *queryTest) Distinct(distinctIndexes []string) *queryTest {
	for _, dIdx := range distinctIndexes {
		qt.q.Distinct(dIdx)
	}
	qt.distinctIndexes = distinctIndexes
	return qt
}

func (qt *queryTest) GetJson() (json []byte, found bool) {
	return qt.q.GetJson()
}

func (qt *queryTest) GetJsonCtx(ctx context.Context) (json []byte, found bool) {
	return qt.q.GetJsonCtx(ctx)
}

func (qt *queryTest) Delete() (int, error) {
	return qt.DeleteCtx(context.Background())
}

func (qt *queryTest) DeleteCtx(ctx context.Context) (int, error) {
	res, err := qt.q.DeleteCtx(ctx)

	qt.db.SetSyncRequired()
	qt.readOnly = false
	return res, err
}

func (qt *queryTest) Drop(field string) *queryTest {
	qt.q.Drop(field)

	qt.db.SetSyncRequired()
	qt.readOnly = false
	return qt
}

func (qt *queryTest) Set(field string, values interface{}) *queryTest {
	qt.q.Set(field, values)

	qt.db.SetSyncRequired()
	qt.readOnly = false
	return qt
}

func (qt *queryTest) SetObject(field string, values interface{}) *queryTest {
	qt.q.SetObject(field, values)

	qt.db.SetSyncRequired()
	qt.readOnly = false
	return qt
}

func (qt *queryTest) Get() (item interface{}, found bool) {
	return qt.q.Get()
}

func (qt *queryTest) GetCtx(ctx context.Context) (item interface{}, found bool) {
	return qt.q.GetCtx(ctx)
}

func (qt *queryTest) ReqTotal(totalNames ...string) *queryTest {
	qt.q.ReqTotal(totalNames...)
	qt.reqTotalCount = true
	return qt
}

func (qt *queryTest) Update() *reindexer.Iterator {
	return qt.UpdateCtx(context.Background())
}

func (qt *queryTest) UpdateCtx(ctx context.Context) *reindexer.Iterator {
	it := qt.q.UpdateCtx(ctx)

	qt.db.SetSyncRequired()
	qt.readOnly = false
	return it
}

// Limit - Set limit (count) of returned items
func (qt *queryTest) Limit(limitItems int) *queryTest {
	qt.q.Limit(limitItems)
	qt.limitItems = limitItems
	return qt
}

// Offset - Set start offset of returned items
func (qt *queryTest) Offset(startOffset int) *queryTest {
	qt.q.Offset(startOffset)
	qt.startOffset = startOffset
	return qt
}

// Debug - set debug level
func (qt *queryTest) Debug(level int) *queryTest {
	qt.q.Debug(level)
	return qt
}

func (qt *queryTest) Explain() *queryTest {
	qt.q.Explain()
	return qt
}

// SelectFilter
func (qt *queryTest) Select(filters ...string) *queryTest {
	qt.q.Select(filters...)
	return qt
}

// Exec will execute query, and return slice of items
func (qt *queryTest) Exec() *reindexer.Iterator {
	return qt.MustExec()
}

// Exec will execute query with context, and return slice of items
func (qt *queryTest) ExecCtx(ctx context.Context) *reindexer.Iterator {
	return qt.MustExecCtx(ctx)
}

// Exec query, and full scan check items returned items
func (qt *queryTest) ExecAndVerify(t *testing.T) *reindexer.Iterator {
	return qt.ExecAndVerifyCtx(t, context.Background())
}

// Exec query with context, and full scan check items returned items
func (qt *queryTest) ExecAndVerifyCtx(t *testing.T, ctx context.Context) *reindexer.Iterator {
	defer qt.close()
	it := qt.ManualClose().ExecCtx(ctx)
	qt.totalCount = it.TotalCount()
	aggregations := it.AggResults()
	it.AllowUnsafe(true)
	defer it.Close()
	items := make([]interface{}, 0, it.Count())
	for it.Next() {
		items = append(items, it.Object())
	}

	if it.Error() != nil {
		panic(it.Error())
	}
	qt.Verify(t, items, aggregations, true)
	//	logger.Printf(reindexer.INFO, "%s -> %d\n", qt.toString(), len(items))
	_ = items

	return it
}

func (qt *queryTest) MustExec(handClose ...bool) *reindexer.Iterator {
	return qt.MustExecCtx(context.Background(), handClose...)
}

func (qt *queryTest) MustExecCtx(ctx context.Context, handClose ...bool) *reindexer.Iterator {
	if !qt.handClose {
		defer qt.close()
	}
	it := qt.db.execQueryCtx(ctx, qt)
	return it
}

func (qt *queryTest) ManualClose() *queryTest {
	qt.handClose = true
	return qt
}

func (qt *queryTest) close() {
	queryTestPool.Put(qt)
}

func (qt *queryTest) InnerJoin(qt2 *queryTest, field string) *queryTest {
	qt2.q = qt.q.InnerJoin(qt2.q, field)
	return qt2
}

func (qt *queryTest) Join(qt2 *queryTest, field string) *queryTest {
	qt2.q = qt.q.Join(qt2.q, field)
	return qt2
}

func (qt *queryTest) LeftJoin(qt2 *queryTest, field string) *queryTest {
	qt2.q = qt.q.LeftJoin(qt2.q, field)
	return qt2
}

func (qt *queryTest) On(index string, condition int, joinIndex string) *queryTest {
	qt.q.On(index, condition, joinIndex)
	return qt

}

func (qt *queryTest) JoinHandler(field string, handler reindexer.JoinHandler) *queryTest {
	qt.q.JoinHandler(field, handler)
	return qt
}

func (qt *queryTest) AggregateSum(field string) {
	qt.q.AggregateSum(field)
}

func (qt *queryTest) AggregateAvg(field string) {
	qt.q.AggregateAvg(field)
}

func (qt *queryTest) AggregateMin(field string) {
	qt.q.AggregateMin(field)
}

func (qt *queryTest) AggregateMax(field string) {
	qt.q.AggregateMax(field)
}

func (qt *queryTest) AggregateFacet(fields ...string) *reindexer.AggregateFacetRequest {
	return qt.q.AggregateFacet(fields...)

}

// Merge 2 queries
func (qt *queryTest) Merge(qt2 *queryTest) *queryTest {
	qt.q.Merge(qt2.q)
	return qt
}

// Exec query, and full scan check items returned items
func (qt *queryTest) ExecToJson(jsonRoots ...string) *reindexer.JSONIterator {
	return qt.q.ExecToJson(jsonRoots...)
}

// Exec query with context, and full scan check items returned items
func (qt *queryTest) ExecToJsonCtx(ctx context.Context, jsonRoots ...string) *reindexer.JSONIterator {
	return qt.q.ExecToJsonCtx(ctx, jsonRoots...)
}

var testNamespaces = make(map[string]*testNamespace, 100)

const (
	containField = iota
	containValue
)

type sortExprValue struct {
	contain   int
	field     [][]int
	fieldName string
	value     float64
}

func convertToDouble(v reflect.Value, sortStr string, fieldName string, item interface{}) float64 {
	switch v.Type().Kind() {
	case reflect.String:
		if result, err := strconv.ParseFloat(v.String(), 64); err == nil {
			return result
		} else {
			panic(err)
		}
	case reflect.Float32, reflect.Float64:
		return v.Float()
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8,
		reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8:
		return float64(v.Int())
	case reflect.Bool:
		if v.Bool() {
			return 1.0
		} else {
			return 0.0
		}
	case reflect.Array, reflect.Slice:
		if v.Len() != 1 {
			panic(fmt.Errorf("Found len(values) != 1 on sort by '%s' index %s in item %+v", sortStr, fieldName, item))
		}
		return convertToDouble(v.Index(0), sortStr, fieldName, item)
	}
	panic(fmt.Errorf("Unknown field type on sort by '%s' index %s in item %+v", sortStr, fieldName, item))
}

func (value *sortExprValue) getValue(item interface{}, sortStr string) float64 {
	if value.contain == containValue {
		return value.value
	}
	vals := getValues(item, value.field)
	if len(vals) != 1 {
		panic(fmt.Errorf("Found len(values) != 1 on sort by '%s' index %s in item %+v", sortStr, value.fieldName, item))
	}
	return convertToDouble(vals[0], sortStr, value.fieldName, item)
}

const (
	opPlus = iota
	opMinus
	opMult
	opDiv
)

type sortExprEntry struct {
	negative  bool
	operation int
	isSubExpr bool
	value     *sortExprValue
	subExpr   []*sortExprEntry
}

func justIndex(sortExpr []*sortExprEntry) bool {
	return len(sortExpr) == 1 && !sortExpr[0].isSubExpr && sortExpr[0].value.contain == containField && sortExpr[0].operation == opPlus && !sortExpr[0].negative
}

func skipSpaces(sortStr string, pos int) int {
	for pos < len(sortStr) {
		if r, w := utf8.DecodeRuneInString(sortStr[pos:]); unicode.IsSpace(r) {
			pos += w
		} else {
			break
		}
	}
	return pos
}

func getSortValueOrIndex(sortStr string, pos int) (string, int) {
	i := pos
	for i < len(sortStr) {
		if r, w := utf8.DecodeRuneInString(sortStr[i:]); !unicode.IsSpace(r) && r != ')' {
			i += w
		} else {
			break
		}
	}
	return sortStr[pos:i], i
}

func parseSortExpr(sortStr string, pos int, ns *testNamespace) ([]*sortExprEntry, int) {
	result := make([]*sortExprEntry, 0)
	expectValue := true
	inSubExpression := false
	lastOperationPlusOrMinus := false
	op := opPlus
	pos = skipSpaces(sortStr, pos)
	for pos < len(sortStr) {
		if expectValue {
			negative := false
			if sortStr[pos] == '-' {
				negative = true
				pos = skipSpaces(sortStr, pos+1)
				if pos >= len(sortStr) {
					panic(fmt.Errorf("Parse of sort expression '%s' failed", sortStr))
				}
			}
			entry := new(sortExprEntry)
			entry.operation = op
			entry.negative = negative
			if sortStr[pos] == '(' {
				var subExpr []*sortExprEntry
				subExpr, pos = parseSortExpr(sortStr, pos+1, ns)
				if pos >= len(sortStr) || sortStr[pos] != ')' {
					panic(fmt.Errorf("Parse of sort expression '%s' failed", sortStr))
				}
				pos++
				entry.isSubExpr = true
				entry.subExpr = subExpr
			} else {
				exprValue := new(sortExprValue)
				var valueStr string
				valueStr, pos = getSortValueOrIndex(sortStr, pos)
				if value, err := strconv.ParseFloat(valueStr, 64); err == nil {
					exprValue.contain = containValue
					exprValue.value = value
				} else {
					exprValue.contain = containField
					exprValue.fieldName = valueStr
					exprValue.field, _ = ns.getField(valueStr)
				}
				entry.isSubExpr = false
				entry.value = exprValue
			}
			if inSubExpression {
				result[len(result)-1].subExpr = append(result[len(result)-1].subExpr, entry)
			} else {
				result = append(result, entry)
			}
			expectValue = false
		} else {
			switch sortStr[pos] {
			case ')':
				return result, pos
			case '+':
				op = opPlus
				lastOperationPlusOrMinus = true
				inSubExpression = false
			case '-':
				op = opMinus
				lastOperationPlusOrMinus = true
				inSubExpression = false
			case '*', '/':
				if op = opMult; sortStr[pos] == '/' {
					op = opDiv
				}
				if lastOperationPlusOrMinus {
					lastEntry := result[len(result)-1]
					newSubExpr := new(sortExprEntry)
					newSubExpr.negative = false
					newSubExpr.operation = lastEntry.operation
					newSubExpr.isSubExpr = true
					newSubExpr.subExpr = make([]*sortExprEntry, 1)
					newSubExpr.subExpr[0] = lastEntry
					lastEntry.operation = opPlus
					result[len(result)-1] = newSubExpr
					lastOperationPlusOrMinus = false
					inSubExpression = true
				}
			default:
				panic(fmt.Errorf("Parse of sort expression '%s' failed, char '%c'", sortStr, sortStr[pos]))
			}
			pos++
			expectValue = true
		}
		pos = skipSpaces(sortStr, pos)
	}
	if expectValue {
		panic(fmt.Errorf("Parse of sort expression '%s' failed", sortStr))
	}
	return result, pos
}

func calculate(sortExpr []*sortExprEntry, item interface{}, sortStr string) float64 {
	var result float64 = 0.0
	for _, sortEntry := range sortExpr {
		var value float64
		if sortEntry.isSubExpr {
			value = calculate(sortEntry.subExpr, item, sortStr)
		} else {
			value = sortEntry.value.getValue(item, sortStr)
		}
		if sortEntry.negative {
			value = -value
		}
		switch sortEntry.operation {
		case opPlus:
			result += value
		case opMinus:
			result -= value
		case opMult:
			result *= value
		case opDiv:
			if value == 0.0 {
				panic(fmt.Errorf("Division by zero on sort by '%s' in item %+v", sortStr, item))
			}
			result /= value
		}
	}
	return result
}

func (qt *queryTest) Verify(t *testing.T, items []interface{}, aggResults []reindexer.AggregationResult, checkEq bool) {
	if len(qt.distinctIndexes) > 1 {
		require.Equal(t, len(items), 0, "Returned items with several distincts")
	}
	// map of found ids
	foundIds := make(map[string]int, len(items))
	var distIndexes [][][]int
	for _, dIdx := range qt.distinctIndexes {
		distIdx, _ := qt.ns.getField(dIdx)
		distIndexes = append(distIndexes, distIdx)
	}
	distinctsByItems := make(map[string]int, 1000)
	distinctsByAggRes := make([]map[string]int, len(distIndexes))
	for i := 0; i < len(distinctsByAggRes); i++ {
		distinctsByAggRes[i] = make(map[string]int, 1000)
	}
	totalItems := 0

	// Check returned items for match query conditions
	for i, item := range items {
		if item == nil {
			panic(fmt.Errorf("Got nil value: %d (%d)", i, len(items)))
		}
		pk := getPK(qt.ns, reflect.Indirect(reflect.ValueOf(item)))

		if _, ok := foundIds[pk]; ok {
			log.Fatalf("Duplicated pkey %s on item %v", pk, item)
		}
		foundIds[pk] = 0

		if checkEq && !reflect.DeepEqual(item, qt.ns.items[pk]) {
			json1, _ := json.Marshal(item)
			json2, _ := json.Marshal(qt.ns.items[pk])

			if string(json1) != string(json2) {
				panic(fmt.Errorf("found item not equal to original \n%#v\n%#v", item, qt.ns.items[pk]))
			}
		}
		if !qt.entries.verifyConditions(t, qt.ns, item) {
			json1, _ := json.Marshal(item)
			log.Fatalf("Found item id=%s, not match condition '%s'\n%+v\n", pk, qt.toString(), string(json1))
		} else {
			totalItems++
		}

		if len(qt.equalPositions) > 0 {
			if !checkEqualPosition(item, qt) {
				log.Fatalf("Equal position check failed")
			}
		}

		// Check distinct
		if len(qt.distinctIndexes) == 1 {
			vals := getValues(item, distIndexes[0])
			require.Equalf(t, len(vals), 1, "Found len(values) != 1 on distinct '%s' in item %#v", qt.distinctIndexes[0], item)
			valStr := fmt.Sprint(vals[0])
			_, ok := distinctsByItems[valStr]
			require.Falsef(t, ok, "Duplicate distinct value '%s' in item %#v", valStr, item)
			distinctsByItems[valStr] = 0
		}
	}
	require.Equal(t, len(aggResults), len(qt.distinctIndexes))
	for i, agg := range aggResults {
		require.Equal(t, agg.Type, "distinct")
		require.Equal(t, len(agg.Fields), 1)
		require.Equal(t, agg.Fields[0], qt.distinctIndexes[i])
		for _, v := range agg.Distincts {
			_, ok := distinctsByAggRes[i][v]
			require.Falsef(t, ok, "Duplicate distinct value '%s' by index '%s'", v, agg.Fields[0])
			distinctsByAggRes[i][v] = 0
		}
	}

	// Check sorting
	sortIdxCount := len(qt.sortIndex)
	var sortIdxs map[int][][]int
	var prevVals map[int]reflect.Value
	var byExpr map[int]bool
	var exprs map[int][]*sortExprEntry
	var res []int
	if sortIdxCount > 0 {
		sortIdxs = make(map[int][][]int)
		byExpr = make(map[int]bool)
		exprs = make(map[int][]*sortExprEntry)
		for k := 0; k < sortIdxCount; k++ {
			sortExpr, _ := parseSortExpr(qt.sortIndex[k], 0, qt.ns)
			if justIndex(sortExpr) {
				sortIdxs[k], _ = qt.ns.getField(qt.sortIndex[k])
				byExpr[k] = false
			} else {
				exprs[k] = sortExpr
				byExpr[k] = true
			}
		}
		prevVals = make(map[int]reflect.Value)
		res = make([]int, sortIdxCount)
	}
	for i := 0; i < len(items); i++ {
		for j := 0; j < len(res); j++ {
			res[j] = -1
		}
		for k := 0; k < sortIdxCount; k++ {
			var val reflect.Value
			if byExpr[k] {
				val = reflect.ValueOf(calculate(exprs[k], items[i], qt.sortIndex[k]))
			} else {
				vals := getValues(items[i], sortIdxs[k])
				if len(vals) != 1 {
					log.Fatalf("Found len(values) != 1 on sort index %s in item %+v", qt.sortIndex[k], items[i])
				}
				val = vals[0]
			}

			if i > 0 {
				needToVerify := true
				if k != 0 {
					for l := k - 1; l >= 0; l-- {
						if res[l] != 0 {
							needToVerify = false
							break
						}
					}
					if needToVerify {
						res[k] = compareValues(prevVals[k], val)
						if (res[k] > 0 && !qt.sortDesc) || (res[k] < 0 && qt.sortDesc) {
							log.Fatalf("Sort error by '%s',desc=%v ... %v ... %v .... ", qt.sortIndex[k], qt.sortDesc, prevVals, val)
						}
					}
				}
			}
			prevVals[k] = val
		}
	}

	// Check all non found items for non match query conditions
	for pk, item := range qt.ns.items {
		if _, ok := foundIds[pk]; !ok {
			if qt.entries.verifyConditions(t, qt.ns, item) {
				// If request with limit or offset - do not check not found items
				if qt.startOffset == 0 && (qt.limitItems == 0 || len(qt.distinctIndexes) <= 1 && len(items) < qt.limitItems) {
					itemJson, _ := json.Marshal(item)
					require.Greaterf(t, len(qt.distinctIndexes), 0, "Not found item pkey=%s, match condition '%s', expected total items=%d, found=%d\n%s", pk, qt.toString(), len(qt.ns.items), len(items), string(itemJson))
					for i, distIdx := range distIndexes {
						vals := getValues(item, distIdx)
						require.Equalf(t, len(vals), 1, "Found len(values) != 1 on distinct %#v in item %s", qt.distinctIndexes, string(itemJson))
						valStr := fmt.Sprint(vals[0])
						_, ok := distinctsByAggRes[i][valStr]
						require.Truef(t, ok, "In query '%s'\nNot present distinct value '%s' by index '%s' of item %s in aggregation results", qt.toString(), valStr, qt.distinctIndexes[i], string(itemJson))
						if len(qt.distinctIndexes) == 1 {
							_, ok := distinctsByItems[valStr]
							require.Truef(t, ok, "In query '%s'\nNot present distinct value '%s' by index '%s' of item %s in aggregation results", qt.toString(), valStr, qt.distinctIndexes[0], string(itemJson))
						}
					}
				}
				totalItems++
			}
		}
	}

	// Check total count
	if qt.reqTotalCount && totalItems != qt.totalCount && len(qt.distinctIndexes) == 0 {
		panic(fmt.Errorf("Total mismatch: %d != %d (%d)", totalItems, qt.totalCount, len(items)))
	}
}

func (qt *queryTest) WhereInt(index string, condition int, keys ...int) *queryTest {
	return qt.Where(index, condition, keys)
}

func (qt *queryTest) WhereInt32(index string, condition int, keys ...int32) *queryTest {
	return qt.Where(index, condition, keys)
}

func (qt *queryTest) WhereInt64(index string, condition int, keys ...int64) *queryTest {
	return qt.Where(index, condition, keys)
}

func (qt *queryTest) WhereString(index string, condition int, keys ...string) *queryTest {
	return qt.Where(index, condition, keys)
}

func (q *queryTest) WhereComposite(index string, condition int, keys ...interface{}) *queryTest {
	return q.Where(index, condition, keys)
}

// WhereString - Add where condition to DB query with string args
func (qt *queryTest) Match(index string, keys ...string) *queryTest {
	qt.q.Match(index, keys...)
	return qt
}

// WhereString - Add where condition to DB query with bool args
func (q *queryTest) WhereBool(index string, condition int, keys ...bool) *queryTest {

	return q.Where(index, condition, keys)

}

// WhereDouble - Add where condition to DB query with float args
func (q *queryTest) WhereDouble(index string, condition int, keys ...float64) *queryTest {

	return q.Where(index, condition, keys)

}

// Get value of items's reindex field by name
func getValues(item interface{}, fieldIdx [][]int) (ret []reflect.Value) {
	vt := reflect.Indirect(reflect.ValueOf(item))

	for _, idx := range fieldIdx {
		v := reflect.Indirect(vt.FieldByIndex(idx))
		if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
			for j := 0; j < v.Len(); j++ {
				ret = append(ret, v.Index(j))
			}
		} else {
			ret = append(ret, v)
		}
	}
	return ret
}

func compareValues(v1 reflect.Value, v2 reflect.Value) int {

	switch v1.Type().Kind() {
	case reflect.String:
		if v1.String() > v2.String() {
			return 1
		} else if v1.String() < v2.String() {
			return -1
		} else {
			return 0
		}
	case reflect.Float32, reflect.Float64:
		if v1.Float() > v2.Float() {
			return 1
		} else if v1.Float() < v2.Float() {
			return -1
		} else {
			return 0
		}
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8,
		reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8:
		if v1.Int() > v2.Int() {
			return 1
		} else if v1.Int() < v2.Int() {
			return -1
		} else {
			return 0
		}
	case reflect.Bool:
		if v1.Bool() == v2.Bool() {
			return 0
		} else {
			return -1
		}
	case reflect.Array, reflect.Slice:
		if v1.Len() != v2.Len() {
			panic("Array sizes are different!")
		}
		for i := 0; i < v1.Len(); i++ {
			res := compareValues(v1.Index(i), v2.Index(i))
			if res > 0 {
				return 1
			}
			if res < 0 {
				return -1
			}
		}
		return 0
	}
	return -1
}

func likeValues(v1 reflect.Value, v2 reflect.Value) bool {
	if v1.Type().Kind() != reflect.String || v2.Type().Kind() != reflect.String {
		panic(fmt.Errorf("Arguments of like must be string"))
	}
	match, err := regexp.MatchString("^"+strings.Replace(strings.Replace(v2.String(), "_", ".", -1), "%", ".*", -1)+"$", v1.String())
	if err != nil {
		panic(err)
	}
	return match
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func checkResult(cmpRes int, cond int) bool {
	result := false
	switch cond {
	case reindexer.EQ:
		result = cmpRes == 0
	case reindexer.GT:
		result = cmpRes > 0
	case reindexer.GE:
		result = cmpRes >= 0
	case reindexer.LT:
		result = cmpRes < 0
	case reindexer.LE:
		result = cmpRes <= 0
	}
	return result
}

func (qt *queryTestEntryTree) getEntryByIndexName(index string) *queryTestEntry {
	for _, d := range qt.data {
		if d.dataType == leaf {
			if entry := d.data.(*queryTestEntry); entry.index == index {
				return entry
			}
		} else {
			t := d.data.(*queryTestEntryTree)
			if found := t.getEntryByIndexName(index); found != nil {
				return found
			}
		}
	}
	return nil
}

func (qt *queryTest) CachedTotal(totalNames ...string) *queryTest {
	qt.q.CachedTotal(totalNames...)
	return qt
}

func (qt *queryTest) SetContext(ctx interface{}) *queryTest {
	qt.q.SetContext(ctx)
	return qt
}

func getValuesForIndex(qt *queryTest, item interface{}, index string) []reflect.Value {
	fields, _ := qt.ns.getField(index)
	return getValues(item, fields)
}

func getEqualPositionMinArrSize(qt *queryTest, ep []string, item interface{}) int {
	arrLen := math.MaxUint32
	for _, index := range ep {
		vals := getValuesForIndex(qt, item, index)
		arrLen = min(len(vals), arrLen)
	}
	return arrLen
}

func checkEqualPosition(item interface{}, qt *queryTest) bool {
	for _, epIndexes := range qt.equalPositions {
		arrIdx := 0
		entry := qt.entries.getEntryByIndexName(epIndexes[0])
		vals := getValuesForIndex(qt, item, epIndexes[0])
		keys := entry.keys
		arrLen := getEqualPositionMinArrSize(qt, epIndexes, item)
		for arrIdx < arrLen && checkResult(compareValues(vals[arrIdx], keys[arrIdx]), entry.condition) == false {
			arrIdx++
		}
		if arrIdx >= arrLen {
			continue
		}
		equal := true
		for fieldIdx := 1; fieldIdx < len(epIndexes); fieldIdx++ {
			entry = qt.entries.getEntryByIndexName(epIndexes[fieldIdx])
			vals = getValuesForIndex(qt, item, epIndexes[fieldIdx])
			keys = entry.keys
			cmpRes := checkResult(compareValues(vals[arrIdx], keys[arrIdx]), entry.condition)
			if cmpRes == false {
				equal = false
				break
			}
		}
		if equal == true {
			return true
		}
	}
	return false
}

func compareComposite(vals []reflect.Value, keyValue interface{}, item interface{}) int {

	if reflect.ValueOf(keyValue).Len() != len(vals) {
		panic("Amount of subindexes and values to compare are different!")
	}
	cmpRes := 0
	for j := 0; j < reflect.ValueOf(keyValue).Len() && cmpRes == 0; j++ {
		subKey := reflect.ValueOf(keyValue).Index(j)
		cmpRes = compareValues(vals[j], reflect.ValueOf(subKey.Interface()))
	}
	return cmpRes
}

func checkCompositeCondition(vals []reflect.Value, cond *queryTestEntry, item interface{}) bool {
	keys := cond.ikeys.([]interface{})
	result := false

	switch cond.condition {
	case reindexer.EQ:
		result = compareComposite(vals, keys[0], item) == 0
	case reindexer.GT:
		result = compareComposite(vals, keys[0], item) > 0
	case reindexer.GE:
		result = compareComposite(vals, keys[0], item) >= 0
	case reindexer.LT:
		result = compareComposite(vals, keys[0], item) < 0
	case reindexer.LE:
		result = compareComposite(vals, keys[0], item) <= 0
	case reindexer.RANGE:
		result = compareComposite(vals, keys[0], item) >= 0 && compareComposite(vals, keys[1], item) <= 0
	case reindexer.SET:
		for i := range keys {
			result = compareComposite(vals, keys[i], item) == 0
			if result {
				break
			}
		}
	}

	return result
}

func checkDWithin(point1 [2]float64, point2 [2]float64, distance float64) bool {
	diffX := point1[0] - point2[0]
	diffY := point1[1] - point2[1]
	return (diffX*diffX + diffY*diffY) <= (distance * distance)
}

func checkCondition(t *testing.T, ns *testNamespace, cond *queryTestEntry, item interface{}) bool {
	vals := getValues(item, cond.fieldIdx)

	switch cond.condition {
	case reindexer.EMPTY:
		return len(vals) == 0
	case reindexer.ANY:
		return len(vals) > 0
	case reindexer.DWITHIN:
		require.Equal(t, 2, len(vals), "Expected point %#v in item %#v", vals, item)
		return checkDWithin([2]float64{vals[0].Float(), vals[1].Float()}, [2]float64{cond.keys[0].Float(), cond.keys[1].Float()}, cond.keys[2].Float())
	}

	found := false

	if len(vals) > 1 && len(cond.fieldIdx) > 1 {
		return checkCompositeCondition(vals, cond, item)
	}

	for _, v := range vals {
		switch cond.condition {
		case reindexer.EQ:
			found = compareValues(v, cond.keys[0]) == 0
		case reindexer.GT:
			found = compareValues(v, cond.keys[0]) > 0
		case reindexer.GE:
			found = compareValues(v, cond.keys[0]) >= 0
		case reindexer.LT:
			found = compareValues(v, cond.keys[0]) < 0
		case reindexer.LE:
			found = compareValues(v, cond.keys[0]) <= 0
		case reindexer.RANGE:
			found = compareValues(v, cond.keys[0]) >= 0 && compareValues(v, cond.keys[1]) <= 0
		case reindexer.SET:
			for _, k := range cond.keys {
				if found = compareValues(v, k) == 0; found {
					break
				}
			}
		case reindexer.LIKE:
			found = likeValues(v, cond.keys[0])
		}
		if found {
			break
		}
	}
	return found
}

func (qt *queryTestEntryTree) verifyConditions(t *testing.T, ns *testNamespace, item interface{}) bool {
	found := true
	for _, cond := range qt.data {
		var curFound bool
		if cond.dataType == leaf {
			curFound = checkCondition(t, ns, cond.data.(*queryTestEntry), item)
		} else {
			tree := cond.data.(*queryTestEntryTree)
			curFound = tree.verifyConditions(t, ns, item)
		}
		switch cond.op {
		case opNOT:
			if curFound {
				return false
			}
		case opAND:
			if !found {
				return false
			}
			found = curFound
		case opOR:
			found = found || curFound
		}
	}
	//fmt.Printf("Verification result: %t\n\n", found)
	return found
}

func prepareStruct(ns *testNamespace, t reflect.Type, basePath []int, reindexBasePath string) {
	if reindexBasePath != "" {
		reindexBasePath += "."
	}

	indexes := make(map[string][]int)
	ns.jsonPaths = make(map[string]string)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tags := strings.SplitN(field.Tag.Get("reindex"), ",", 3)
		jsonPath := strings.Split(field.Tag.Get("json"), ",")[0]

		if len(jsonPath) == 0 && !field.Anonymous {
			jsonPath = field.Name
		}

		idxName := tags[0]
		if idxName == "-" {
			continue
		}

		nonIndexField := bool(len(idxName) == 0 && len(jsonPath) > 0)

		reindexPath := reindexBasePath + idxName
		path := append(basePath, i)
		indexes[idxName] = path
		ns.jsonPaths[idxName] = jsonPath

		tk := field.Type.Kind()
		isPk := len(tags) > 2 && strings.Index(tags[2], "pk") >= 0

		if tk == reflect.Struct {
			if len(idxName) > 0 && len(tags) > 2 && strings.Index(tags[2], "composite") >= 0 {
				subIdxs := strings.Split(idxName, "+")
				ns.fieldsIdx[reindexPath] = make([][]int, len(subIdxs))
				for j, subIdx := range subIdxs {
					ns.fieldsIdx[reindexPath][j] = indexes[subIdx]
					if isPk {
						ns.pkIdx = append(ns.pkIdx, indexes[subIdx])
					}
				}
			} else {
				prepareStruct(ns, field.Type, path, reindexPath)
			}
			continue
		}
		if (tk == reflect.Array || tk == reflect.Slice) && field.Type.Elem().Kind() == reflect.Struct {
			//todo no panic just make support!
			//panic(fmt.Errorf("TestQuery does not supported indexed struct arrays (struct=%s, field=%s)\n", t.Name(), field.Name))
		}

		if isPk {
			ns.pkIdx = append(ns.pkIdx, path)
		}

		if (len(idxName)) > 0 || nonIndexField {
			p := map[bool]string{true: jsonPath, false: reindexPath}
			if _, ok := ns.getField(p[nonIndexField]); !ok {
				ns.fieldsIdx[p[nonIndexField]] = make([][]int, 0, 5)
			}
			ns.fieldsIdx[p[nonIndexField]] = append(ns.fieldsIdx[p[nonIndexField]], path)
		}

	}
}

func getPK(ns *testNamespace, val reflect.Value) string {

	buf := &bytes.Buffer{}

	for _, idx := range ns.pkIdx {
		v := val.FieldByIndex(idx)

		switch v.Kind() {
		case reflect.Bool:
			if v.Bool() {
				buf.WriteByte('1')
			} else {
				buf.WriteByte('0')
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			buf.WriteString(strconv.Itoa(int(v.Int())))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			buf.WriteString(strconv.Itoa(int(v.Uint())))
		case reflect.Float64:
			buf.WriteString(strconv.FormatFloat(v.Float(), 'f', 6, 64))
		case reflect.Float32:
			buf.WriteString(strconv.FormatFloat(v.Float(), 'f', 6, 32))
		case reflect.String:
			buf.WriteString(v.String())
		default:
			panic(errors.New("invalid pk field type"))
		}
		buf.WriteByte('#')
	}

	return buf.String()
}
