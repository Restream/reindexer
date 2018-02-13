package reindexer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/restream/reindexer"
)

const (
	opAND = iota
	opNOT
	opOR
)

var queryNames = map[int]string{
	reindexer.EQ:    "==",
	reindexer.GT:    ">",
	reindexer.LT:    "<",
	reindexer.GE:    ">=",
	reindexer.LE:    "<=",
	reindexer.SET:   "SET",
	reindexer.RANGE: "in",
	reindexer.ANY:   "ANY",
	reindexer.EMPTY: "EMPTY",
}

type queryTestEntry struct {
	index     string
	condition int
	keys      []reflect.Value
	ikeys     interface{}
	op        int
	fieldIdx  [][]int
}

// Test Query to DB object
type queryTest struct {
	q             *reindexer.Query
	iter          *reindexer.Iterator
	entries       []queryTestEntry
	distinctIndex string
	sortIndex     string
	sortDesc      bool
	sortValues    []interface{}
	limitItems    int
	startOffset   int
	reqTotalCount bool
	db            *reindexer.Reindexer
	namespace     string
	nextOp        int
	ns            *testNamespace
}
type testNamespace struct {
	items     map[string]interface{}
	pkIdx     [][]int
	fieldsIdx map[string][][]int
}

// Create new DB query
func newTestQuery(db *reindexer.Reindexer, namespace string) *queryTest {
	return &queryTest{q: db.Query(namespace), namespace: namespace, db: db, nextOp: opAND, ns: testNamespaces[namespace]}
}

type txTest struct {
	tx        *reindexer.Tx
	namespace string
	db        *reindexer.Reindexer
	ns        *testNamespace
}

func newTestNamespace(namespace string, item interface{}) {
	ns := &testNamespace{
		items:     make(map[string]interface{}, 1000),
		fieldsIdx: make(map[string][][]int),
	}
	testNamespaces[namespace] = ns
	prepareStruct(ns, reflect.TypeOf(item), []int{}, "")
}

func newTestTx(db *reindexer.Reindexer, namespace string) *txTest {

	tx := &txTest{namespace: namespace, db: db, ns: testNamespaces[namespace]}
	tx.tx = db.MustBeginTx(namespace)
	return tx
}

func (tx *txTest) Insert(s interface{}) (int, error) {
	val := reflect.Indirect(reflect.ValueOf(s))
	tx.ns.items[getPK(tx.ns, val)] = s
	return tx.tx.Insert(s)
}

func (tx *txTest) Update(s interface{}) (int, error) {
	val := reflect.Indirect(reflect.ValueOf(s))
	tx.ns.items[getPK(tx.ns, val)] = s
	return tx.tx.Update(s)
}

func (tx *txTest) Upsert(s interface{}) error {
	val := reflect.Indirect(reflect.ValueOf(s))
	tx.ns.items[getPK(tx.ns, val)] = s
	return tx.tx.Upsert(s)
}

func (tx *txTest) Delete(s interface{}) error {
	val := reflect.Indirect(reflect.ValueOf(s))
	delete(tx.ns.items, getPK(tx.ns, val))
	return tx.tx.Delete(s)
}

func (tx *txTest) Commit(updatedAt *time.Time) error {
	return tx.tx.Commit(updatedAt)
}

func (tx *txTest) MustCommit(updatedAt *time.Time) {
	tx.tx.MustCommit(updatedAt)
}

func (qt *queryTest) toString() (ret string) {
	if len(qt.entries) > 0 {
		ret += " WHERE "
	}
	for i, qc := range qt.entries {
		if i != 0 {
			switch qc.op {
			case opNOT:
				ret += " AND NOT "
			case opAND:
				ret += " AND "
			case opOR:
				ret += " OR "
			}
		}
		ret += qc.index + " " + queryNames[qc.condition] + " "
		if len(qc.keys) > 1 {
			ret += "("
		}
		for i, c := range qc.keys {
			if i != 0 {
				ret += ","
			}
			ret += fmt.Sprintf("%v", c.Interface())
		}
		if len(qc.keys) > 1 {
			ret += ")"
		}
	}
	if len(qt.sortIndex) > 0 {
		ret += " ORDER BY " + qt.sortIndex
		if qt.sortDesc {
			ret += " DESC"
		}
		if len(qt.sortValues) > 0 {
			ret += "( "
			for _, val := range qt.sortValues {
				ret += fmt.Sprintf("%v ", val)
			}
			ret += ")"
		}
	}
	if qt.limitItems != 0 {
		ret += " LIMIT " + strconv.Itoa(qt.limitItems)
	}
	if qt.startOffset != 0 {
		ret += " OFFSET " + strconv.Itoa(qt.startOffset)
	}
	if len(qt.distinctIndex) > 0 {
		ret += " DISTINCT " + qt.distinctIndex
	}

	return ret
}

// Where - Add where condition to DB query
func (qt *queryTest) Where(index string, condition int, keys interface{}) *queryTest {
	qte := queryTestEntry{index: index, condition: condition, ikeys: keys, op: qt.nextOp}
	qt.q.Where(index, condition, keys)

	if reflect.TypeOf(keys).Kind() == reflect.Slice || reflect.TypeOf(keys).Kind() == reflect.Array {
		for i := 0; i < reflect.ValueOf(keys).Len(); i++ {
			qte.keys = append(qte.keys, reflect.ValueOf(keys).Index(i))
		}
	} else {
		qte.keys = append(qte.keys, reflect.ValueOf(keys))
	}
	qte.fieldIdx = qt.ns.fieldsIdx[index]
	qt.entries = append(qt.entries, qte)
	qt.nextOp = opAND

	return qt
}

// Sort - Apply sort order to returned from query items
func (qt *queryTest) Sort(sortIndex string, desc bool, values ...interface{}) *queryTest {
	if len(sortIndex) > 0 {
		qt.q.Sort(sortIndex, desc, values...)
		qt.sortIndex = sortIndex
		qt.sortDesc = desc
		qt.sortValues = values
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
func (qt *queryTest) Distinct(distinctIndex string) *queryTest {
	if len(distinctIndex) > 0 {
		qt.q.Distinct(distinctIndex)
		qt.distinctIndex = distinctIndex
	}
	return qt
}

// ReqTotal Request total items calculation
func (qt *queryTest) ReqTotal() *queryTest {
	qt.q.ReqTotal()
	qt.reqTotalCount = true
	return qt
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

// SelectFilter
func (qt *queryTest) Select(filters ...string) *queryTest {
	qt.q.Select(filters...)
	return qt
}

// Exec will execute query, and return slice of items
func (qt *queryTest) Exec() *reindexer.Iterator {
	qt.iter = qt.q.Exec()
	return qt.iter
}

// Exec query, and full scan check items returned items
func (qt *queryTest) ExecAndVerify() *reindexer.Iterator {
	it := qt.Exec()
	items, err := it.AllowUnsafe(true).FetchAll()
	if err != nil {
		panic(err)
	}
	qt.Verify(items, true)
	//	logger.Printf(reindexer.INFO, "%s -> %d\n", qt.toString(), len(items))
	_ = items

	return it
}

// Exec query, and full scan check items returned items
func (qt *queryTest) ExecToJson() *reindexer.JSONIterator {
	return qt.q.ExecToJson()
}

var testNamespaces = make(map[string]*testNamespace, 100)

func (qt *queryTest) Verify(items []interface{}, checkEq bool) {
	// map of found ids
	foundIds := make(map[string]int, len(items))
	distincts := make(map[interface{}]int, 100)
	totalItems := 0

	distIdx := qt.ns.fieldsIdx[qt.distinctIndex]
	sortIdx := qt.ns.fieldsIdx[qt.sortIndex]

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

		if !verifyConditions(qt.ns, qt.entries, item) {
			json1, _ := json.Marshal(item)
			log.Fatalf("Found item id=%s, not match condition '%s'\n%+v\n", pk, qt.toString(), string(json1))
		} else {
			totalItems++
		}

		// Check distinct
		if len(qt.distinctIndex) > 0 {
			vals := getValues(item, distIdx)
			if len(vals) != 1 {
				log.Fatalf("Found len(values) != 1 on distinct %s in item %+v", qt.distinctIndex, item)
			}
			intf := vals[0].Interface()
			if _, ok := distincts[intf]; ok {
				log.Fatalf("Duplicate distinct value %+v in item %+v", intf, item)
			}
			distincts[intf] = 0
		}
	}

	// Check sorting
	if len(qt.sortIndex) > 0 {
		var prevVals []reflect.Value
		for i := 0; i < len(items); i++ {
			vals := getValues(items[i], sortIdx)
			if len(vals) != 1 {
				log.Fatalf("Found len(values) != 1 on sort index %s in item %+v", qt.sortIndex, items[i])
			}

			if len(prevVals) > 0 {
				res := compareValues(prevVals[0], vals[0])
				if (res > 0 && !qt.sortDesc) || (res < 0 && qt.sortDesc) {
					log.Fatalf("Sort error on index %s,desc=%v ... %v ... %v .... ", qt.sortIndex, qt.sortDesc, prevVals[0], vals[0])
				}
			}
			prevVals = vals
		}
	}

	// Check all non found items for non match query conditions
	for pk, item := range qt.ns.items {
		if _, ok := foundIds[pk]; !ok {
			if verifyConditions(qt.ns, qt.entries, item) {
				// If request with limit or offset - do not check not found items
				if qt.startOffset == 0 && (qt.limitItems == 0 || len(items) < qt.limitItems) {
					if len(qt.distinctIndex) > 0 {
						vals := getValues(item, distIdx)
						if len(vals) != 1 {
							log.Fatalf("Found len(values) != 1 on distinct %s in item %+v", qt.distinctIndex, item)
						}
						intf := vals[0].Interface()
						if _, ok := distincts[intf]; !ok {
							log.Fatalf("Not present distinct value %+v in item %+v", intf, item)
						}
					} else {
						json1, _ := json.Marshal(item)
						log.Fatalf("Not found item pkey=%s, match condition '%s',expected total items=%d,found=%d\n%s",
							pk, qt.toString(), len(qt.ns.items), len(items), string(json1))
					}
				}
				totalItems++
			}
		}
	}

	// Check total count
	if qt.reqTotalCount && totalItems != qt.iter.TotalCount() && len(qt.distinctIndex) == 0 {
		log.Fatalf("Total mismatch: %d != %d", totalItems, qt.iter.TotalCount())
	}
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
	}
	return -1
}

func checkCondition(ns *testNamespace, cond queryTestEntry, item interface{}) bool {
	vals := getValues(item, cond.fieldIdx)
	found := false

	switch cond.condition {
	case reindexer.EMPTY:
		return len(vals) == 0
	case reindexer.ANY:
		return len(vals) > 0
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
		}
		if found {
			break
		}
	}
	return found
}

func verifyConditions(ns *testNamespace, conditions []queryTestEntry, item interface{}) bool {
	found := true
	for _, cond := range conditions {
		curFound := checkCondition(ns, cond, item)
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
	return found
}

func prepareStruct(ns *testNamespace, t reflect.Type, basePath []int, reindexBasePath string) {
	if reindexBasePath != "" {
		reindexBasePath += "."
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		tags := strings.Split(f.Tag.Get("reindex"), ",")

		idxName := tags[0]

		if idxName == "-" {
			continue
		}
		reindexPath := reindexBasePath + idxName
		path := append(basePath, i)

		tk := f.Type.Kind()
		if tk == reflect.Struct {
			prepareStruct(ns, f.Type, path, reindexPath)
			continue
		}
		if (tk == reflect.Array || tk == reflect.Slice) && f.Type.Elem().Kind() == reflect.Struct {
			panic(fmt.Errorf("TestQuery does not supported indexed struct arrays (struct=%s, field=%s)\n", t.Name(), f.Name))
		}

		if len(tags) > 2 && tags[2] == "pk" {
			ns.pkIdx = append(ns.pkIdx, path)
		}

		if (len(idxName)) > 0 {
			if _, ok := ns.fieldsIdx[reindexPath]; !ok {
				ns.fieldsIdx[reindexPath] = make([][]int, 0, 5)
			}
			ns.fieldsIdx[reindexPath] = append(ns.fieldsIdx[reindexPath], path)
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
