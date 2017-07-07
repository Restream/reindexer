package reindexer

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type queryTestEntry struct {
	index     string
	condition int
	keys      []reflect.Value
	ikeys     interface{}
	op        int
}

// Test Query to DB object
type queryTest struct {
	q             *Query
	entries       []queryTestEntry
	sortIndex     string
	distinctIndex string
	sortDesc      bool
}

// Create new DB query
func newTestQuery(db *Reindexer, namespace string) *queryTest {
	return &queryTest{q: newQuery(db, namespace)}
}

type txTest struct {
	tx *Tx
}

func newTestTx(db *Reindexer, namespace string) *txTest {
	tx := &txTest{}
	tx.tx = db.MustBeginTx(namespace)
	return tx
}

func (tx *txTest) Upsert(s interface{}) error {
	testData[tx.tx.namespace][getPK(reflect.Indirect(reflect.ValueOf(s)), tx.tx.db.ns[tx.tx.namespace].fields)] = s
	return tx.tx.Upsert(s)
}

func (tx *txTest) Delete(s interface{}) error {
	delete(testData[tx.tx.namespace], getPK(reflect.Indirect(reflect.ValueOf(s)), tx.tx.db.ns[tx.tx.namespace].fields))
	return tx.tx.Delete(s)
}

func (tx *txTest) Commit(updatedAt *time.Time) error {
	return tx.tx.Commit(updatedAt)
}

func (tx *txTest) MustCommit(updatedAt *time.Time) {
	tx.tx.MustCommit(updatedAt)
}

func (qt *queryTest) toString() (ret string) {
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
	}
	if qt.q.LimitItems != 0 {
		ret += " LIMIT " + strconv.Itoa(qt.q.LimitItems)
	}
	if qt.q.StartOffset != 0 {
		ret += " OFFSET " + strconv.Itoa(qt.q.StartOffset)
	}
	if len(qt.distinctIndex) > 0 {
		ret += " DISTINCT " + qt.distinctIndex
	}

	return ret
}

// Where - Add where condition to DB query
func (qt *queryTest) Where(index string, condition int, keys interface{}) *queryTest {
	qte := queryTestEntry{index: index, condition: condition, ikeys: keys, op: qt.q.nextOp}
	qt.q.Where(index, condition, keys)

	if reflect.TypeOf(keys).Kind() == reflect.Slice || reflect.TypeOf(keys).Kind() == reflect.Array {
		for i := 0; i < reflect.ValueOf(keys).Len(); i++ {
			qte.keys = append(qte.keys, reflect.ValueOf(keys).Index(i))
		}
	} else {
		qte.keys = append(qte.keys, reflect.ValueOf(keys))
	}
	qt.entries = append(qt.entries, qte)
	return qt
}

// Sort - Apply sort order to returned from query items
func (qt *queryTest) Sort(sortIndex string, desc bool) *queryTest {
	if len(sortIndex) > 0 {
		qt.q.Sort(sortIndex, desc)
		qt.sortIndex = sortIndex
		qt.sortDesc = desc
	}
	return qt
}

// OR - next condition will added with OR
func (qt *queryTest) Or() *queryTest {
	qt.q.Or()
	return qt
}

func (qt *queryTest) Not() *queryTest {
	qt.q.Not()
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
	return qt
}

// Limit - Set limit (count) of returned items
func (qt *queryTest) Limit(limitItems int) *queryTest {
	qt.q.Limit(limitItems)
	return qt
}

// Offset - Set start offset of returned items
func (qt *queryTest) Offset(startOffset int) *queryTest {
	qt.q.Offset(startOffset)
	return qt
}

// GetTotal - return total items in DB for query
func (qt *queryTest) GetTotal() int {
	return qt.q.GetTotal()
}

// Debug - set debug level
func (qt *queryTest) Debug(level int) *queryTest {
	qt.q.Debug(level)
	return qt
}

// Exec will execute query, and return slice of items
func (qt *queryTest) Exec() *Iterator {
	return qt.q.Exec()
}

// Exec query, and full scan check items returned items
func (qt *queryTest) ExecAndVerify() *Iterator {
	it := qt.Exec()
	items, err := it.PtrSlice()
	if err != nil {
		panic(err)
	}
	qt.Verify(items)
	logger.Printf(INFO, "%s -> %d\n", qt.toString(), len(items))

	return it
}

var testData = make(map[string]map[string]interface{}, 100)

func (qt *queryTest) Verify(items []interface{}) {
	// map of found ids
	foundIds := make(map[string]int, 100)
	distincts := make(map[interface{}]int, 100)
	totalItems := 0
	ns, err := qt.q.db.getNS(qt.q.Namespace)
	if err != nil {
		panic(err)
	}

	// Check returned items for match query conditions
	for _, item := range items {
		pk := getPK(reflect.Indirect(reflect.ValueOf(item)), ns.fields)

		if _, ok := foundIds[pk]; ok {
			panic(fmt.Errorf("Duplicated pkey %s on item %v", pk, item))
		}
		foundIds[pk] = 0

		if !verifyConditions(qt.entries, item) {
			panic(fmt.Errorf("Found item id=%s %+v, but it is not match query conditions %+v", pk, item, qt.entries))
		} else {
			totalItems++
		}

		// Check distinct
		if len(qt.distinctIndex) > 0 {
			vals := getValues(item, qt.distinctIndex)
			if len(vals) != 1 {
				panic(fmt.Errorf("Found len(values) != 1 on distinct %s in item %+v", qt.distinctIndex, item))
			}
			intf := vals[0].Interface()
			if _, ok := distincts[intf]; ok {
				panic(fmt.Errorf("Duplicate distinct value %+v in item %+v", intf, item))
			}
			distincts[intf] = 0
		}
	}

	// Check sorting
	if len(qt.sortIndex) > 0 {
		var prevVals []reflect.Value
		for _, item := range items {
			vals := getValues(item, qt.sortIndex)
			if len(vals) != 1 {
				panic(fmt.Errorf("Found len(values) != 1 on sort index %s in item %+v", qt.sortIndex, item))
			}

			if len(prevVals) > 0 {
				res := compareValues(prevVals[0], vals[0])
				if (res > 0 && !qt.sortDesc) || (res < 0 && qt.sortDesc) {
					panic(fmt.Errorf("Sort error on index %s,desc=%v ... %v ... %v .... ", qt.sortIndex, qt.sortDesc, prevVals[0], vals[0]))
				}
			}
			prevVals = vals
		}
	}

	// Check all non found items for non match query conditions
	for _, item := range testData[ns.name] {
		pk := getPK(reflect.Indirect(reflect.ValueOf(item)), ns.fields)
		if _, ok := foundIds[pk]; !ok {
			if verifyConditions(qt.entries, item) {
				// If request with limit or offset - do not check not found items
				if qt.q.StartOffset == 0 && (qt.q.LimitItems == 0 || len(items) < qt.q.LimitItems) {
					if len(qt.distinctIndex) > 0 {
						vals := getValues(item, qt.distinctIndex)
						if len(vals) != 1 {
							panic(fmt.Errorf("Found len(values) != 1 on distinct %s in item %+v", qt.distinctIndex, item))
						}
						intf := vals[0].Interface()
						if _, ok := distincts[intf]; !ok {
							panic(fmt.Errorf("Not present distinct value %+v in item %+v", intf, item))
						}
					} else {
						panic(fmt.Errorf("Not found item pkey=%s,\n%+v, buf it is match query conditions\n%+v,expected total items=%d,found=%d",
							pk, item, qt.entries, len(testData[ns.name]), len(items)))
					}
				}
				totalItems++
			}
		}
	}

	// Check total count
	if qt.q.ReqTotalCount && totalItems != qt.GetTotal() && len(qt.distinctIndex) == 0 {
		panic(fmt.Errorf("Total mismatch: %d != %d", totalItems, qt.GetTotal()))
	}
}

// Get value of items's reindex field by name
func getValues(item interface{}, name string) (ret []reflect.Value) {

	vt := reflect.Indirect(reflect.ValueOf(item))

	for i := 0; i < vt.NumField(); i++ {
		v := reflect.Indirect(vt.Field(i))
		t := vt.Field(i).Type()

		if t.Kind() == reflect.Struct {
			ret = append(ret, getValues(v, name)...)
		} else if (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) && t.Elem().Kind() == reflect.Struct {
			for j := 0; j < v.Len(); j++ {
				ret = append(ret, getValues(v.Index(j).Interface(), name)...)
			}
		} else {
			// Get and parse tags
			tagsSlice := strings.Split(vt.Type().Field(i).Tag.Get("reindex"), ",")
			if len(tagsSlice) > 0 && tagsSlice[0] == name {
				if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
					for j := 0; j < v.Len(); j++ {
						ret = append(ret, v.Index(j))
					}
				} else {
					ret = append(ret, v)
				}
			}
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

func checkCondition(cond queryTestEntry, item interface{}) bool {
	vals := getValues(item, cond.index)
	found := false

	switch cond.condition {
	case EMPTY:
		return len(vals) == 0
	case ANY:
		return len(vals) > 0
	}

	for _, v := range vals {
		switch cond.condition {
		case EQ:
			found = compareValues(v, cond.keys[0]) == 0
		case GT:
			found = compareValues(v, cond.keys[0]) > 0
		case GE:
			found = compareValues(v, cond.keys[0]) >= 0
		case LT:
			found = compareValues(v, cond.keys[0]) < 0
		case LE:
			found = compareValues(v, cond.keys[0]) <= 0
		case RANGE:
			found = compareValues(v, cond.keys[0]) >= 0 && compareValues(v, cond.keys[1]) <= 0
		case SET:
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

func verifyConditions(conditions []queryTestEntry, item interface{}) bool {
	found := true
	for _, cond := range conditions {
		curFound := checkCondition(cond, item)
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

func getPK(val reflect.Value, fields []reindexerField) string {

	ret := ""
	for _, f := range fields {
		v := reflect.Indirect(val.Field(f.fieldIdx))
		if !v.IsValid() {
			continue
		}
		tk := f.fieldType
		if tk == reflect.Struct {
			r := getPK(v, f.subField)
			if len(r) > 0 {
				ret += r + "#"
			}
			continue
		}
		if !f.isPK {
			continue
		}
		// Add simple element
		switch tk {
		case reflect.Bool:
			if v.Bool() {
				ret += strconv.Itoa(1) + "#"
			} else {
				ret += strconv.Itoa(0) + "#"
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			ret += strconv.Itoa(int(v.Int())) + "#"
		case reflect.Float64:
			ret += strconv.FormatFloat(v.Float(), 'f', 6, 64) + "#"
		case reflect.Float32:
			ret += strconv.FormatFloat(v.Float(), 'f', 6, 32) + "#"
		case reflect.String:
			ret += v.String() + "#"
		default:
			panic(errors.New("invalid pk field type"))
		}
	}

	return ret
}
