package reindexer

import (
	"fmt"
	"reflect"

	"github.com/restream/reindexer/bindings"
)

func errIterator(err error) *Iterator {
	return &Iterator{err: err}
}

func errJSONIterator(err error) *JSONIterator {
	return &JSONIterator{err: err}
}

func newIterator(
	q *Query,
	result bindings.RawBuffer,
	nsArray []*reindexerNamespace,
	joinToFields []string,
	joinHandlers []JoinHandler,
	queryContext interface{},
) (it *Iterator) {
	if q != nil {
		it = &q.iterator
		it.query = q
	} else {
		it = &Iterator{}
	}
	it.ser = newSerializer(result.GetBuf())
	it.rawQueryParams = it.ser.readRawQueryParams(func(nsid int) {
		nsArray[nsid].cjsonState.ReadPayloadType(&it.ser.Serializer)
	})
	it.result = result
	it.nsArray = nsArray
	it.joinToFields = joinToFields
	it.joinHandlers = joinHandlers
	it.queryContext = queryContext
	it.ptr = 0
	it.err = nil
	if len(it.joinToFields) > 0 {
		it.current.joinObj = make([][]interface{}, len(it.joinToFields))
	}
	return
}

func newJSONIterator(q *Query, json []byte, jsonOffsets []int) *JSONIterator {
	var ji *JSONIterator
	if q != nil {
		ji = &q.jsonIterator
	} else {
		ji = &JSONIterator{}
	}
	ji.json = json
	ji.jsonOffsets = jsonOffsets
	ji.ptr = -1
	ji.query = q
	ji.err = nil
	return ji
}

// Iterator presents query results
type Iterator struct {
	ser            resultSerializer
	rawQueryParams rawResultQueryParams
	result         bindings.RawBuffer
	nsArray        []*reindexerNamespace
	joinToFields   []string
	joinHandlers   []JoinHandler
	queryContext   interface{}
	query          *Query
	allowUnsafe    bool
	ptr            int
	current        struct {
		obj     interface{}
		joinObj [][]interface{}
		rank    int
	}
	err error
}

// Next moves iterator pointer to the next element.
// Returns bool, that indicates the availability of the next elements.
func (it *Iterator) Next() (hasNext bool) {
	if it.ptr >= it.rawQueryParams.count || it.err != nil {
		return
	}
	it.current.obj, it.current.rank = it.readItem()
	if it.err != nil {
		return
	}
	it.ptr++
	return it.ptr <= it.rawQueryParams.count
}

func (it *Iterator) readItem() (item interface{}, rank int) {
	params := it.ser.readRawtItemParams()
	if it.rawQueryParams.haveProcent {
		rank = params.proc
	}
	subNSRes := int(it.ser.GetVarUInt())
	item, it.err = unpackItem(it.nsArray[params.nsid], params, it.allowUnsafe && (subNSRes == 0))
	if it.err != nil {
		return
	}
	if subNSRes > 0 {
		for nsIndex := 0; nsIndex < subNSRes; nsIndex++ {
			siRes := int(it.ser.GetVarUInt())
			if siRes == 0 {
				continue
			}
			subitems := make([]interface{}, siRes)
			for i := 0; i < siRes; i++ {
				params = it.ser.readRawtItemParams()
				subitems[i], it.err = unpackItem(it.nsArray[nsIndex+1], params, it.allowUnsafe)
				if it.err != nil {
					return
				}
			}
			it.current.joinObj[nsIndex] = subitems
			it.join(nsIndex, item)
		}
	}
	return
}

func (it *Iterator) join(nsIndex int, item interface{}) {
	field := it.joinToFields[nsIndex]
	subitems := it.current.joinObj[nsIndex]
	handler := it.joinHandlers[nsIndex]

	if handler != nil {
		if !handler(field, item, subitems) {
			return
		}
	}

	if joinable, ok := item.(Joinable); ok {
		joinable.Join(field, subitems, it.queryContext)
	} else {

		v := getJoinedField(reflect.ValueOf(item), it.nsArray[0].joined, field)
		if !v.IsValid() {
			panic(fmt.Errorf("Can't find field with tag '%s' in struct '%s' for put join results from '%s'",
				field,
				it.nsArray[0].rtype,
				it.nsArray[nsIndex+1].name))
		}
		if v.IsNil() {
			v.Set(reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(it.nsArray[nsIndex+1].rtype)), 0, len(subitems)))
		}
		for _, subitem := range subitems {
			v.Set(reflect.Append(v, reflect.ValueOf(subitem)))
		}
	}
}

// Object returns current object.
// Will panic when pointer was not moved, Next() must be called before.
func (it *Iterator) Object() interface{} {
	if it.ptr == 0 {
		panic(errIteratorNotReady)
	}
	return it.current.obj
}

// Rank returns current object search rank.
// Will panic when pointer was not moved, Next() must be called before.
func (it *Iterator) Rank() int {
	if it.ptr == 0 {
		panic(errIteratorNotReady)
	}
	return it.current.rank
}

// JoinedObjects returns objects slice, that result of join for the given field
func (it *Iterator) JoinedObjects(field string) (objects []interface{}, err error) {
	if it.ptr == 0 {
		return nil, errIteratorNotReady
	}
	idx := it.findJoinFieldIndex(field)
	if idx == -1 {
		return nil, errJoinUnexpectedField
	}
	return it.current.joinObj[idx], nil
}

// Count returns count if query results
func (it *Iterator) Count() int {
	return it.rawQueryParams.count
}

// TotalCount returns total count of objects (ignoring conditions of limit and offset)
func (it *Iterator) TotalCount() int {
	return it.rawQueryParams.totalcount
}

// AllowUnsafe takes bool, that enable or disable unsafe behavior.
//
// When AllowUnsafe is true and object cache is enabled resulting objects will not be copied for each query.
// That means possible race conditions. But it's good speedup, without overhead for copying.
//
// By default reindexer guarantees that every object its safe to use in multithread.
func (it *Iterator) AllowUnsafe(allow bool) *Iterator {
	it.allowUnsafe = allow
	return it
}

// FetchAll returns all query results as slice []interface{} and closes the iterator.
func (it *Iterator) FetchAll() (items []interface{}, err error) {
	defer it.Close()
	if !it.Next() {
		return nil, it.err
	}
	items = make([]interface{}, it.rawQueryParams.count)
	for i := range items {
		items[i] = it.Object()
		if !it.Next() {
			break
		}
	}
	return
}

// FetchOne returns first element and closes the iterator.
// When it's impossible (count is 0) err will be ErrNotFound.
func (it *Iterator) FetchOne() (item interface{}, err error) {
	defer it.Close()
	if it.Next() {
		return it.Object(), it.err
	}
	if it.err == nil {
		it.err = ErrNotFound
	}
	return nil, it.err
}

// FetchAllWithRank returns resulting slice of objects and slice of objects ranks.
// Closes iterator after use.
func (it *Iterator) FetchAllWithRank() (items []interface{}, ranks []int, err error) {
	defer it.Close()
	if !it.Next() {
		return nil, nil, it.err
	}
	items = make([]interface{}, it.rawQueryParams.count)
	ranks = make([]int, it.rawQueryParams.count)
	for i := range items {
		items[i] = it.Object()
		ranks[i] = it.Rank()
		if !it.Next() {
			break
		}
	}
	if it.err != nil {
		return nil, nil, err
	}
	return
}

// HasRank indicates if this iterator has info about search ranks.
func (it *Iterator) HasRank() bool {
	return it.rawQueryParams.haveProcent
}

// AggResults returns aggregation results (if present)
func (it *Iterator) AggResults() []float64 {
	return it.rawQueryParams.aggResults
}

// GetAggreatedValue - Return aggregation sum of field
func (it *Iterator) GetAggreatedValue(idx int) float64 {
	if idx < 0 || idx >= len(it.rawQueryParams.aggResults) {
		return 0
	}
	return it.rawQueryParams.aggResults[idx]
}

// Error returns query error if it's present.
func (it *Iterator) Error() error {
	return it.err
}

// Close closes the iterator and freed CGO resources
func (it *Iterator) Close() {
	if it.result != nil {
		it.result.Free()
		it.result = nil
		if it.query != nil {
			it.query.close()
		}
	}
	return
}

func (it *Iterator) findJoinFieldIndex(field string) (index int) {
	for index = range it.joinToFields {
		if it.joinToFields[index] == field {
			return
		}
	}
	return -1
}

// JSONIterator its iterator, but results presents as json documents
type JSONIterator struct {
	json        []byte
	jsonOffsets []int
	query       *Query

	err error
	ptr int
}

// Next moves iterator pointer to the next element.
// Returns bool, that indicates the availability of the next elements.
func (it *JSONIterator) Next() bool {
	it.ptr++
	return it.ptr < len(it.jsonOffsets)
}

// FetchAll returns bytes slice it's JSON array with results
func (it *JSONIterator) FetchAll() (json []byte, err error) {
	defer it.Close()
	return it.json, it.err
}

// JSON returns JSON bytes with current document
func (it *JSONIterator) JSON() (json []byte) {
	if it.ptr < 0 {
		panic(errIteratorNotReady)
	}
	o := it.jsonOffsets[it.ptr]
	l := 0
	if it.ptr+1 < len(it.jsonOffsets) {
		l = it.jsonOffsets[it.ptr+1] - 1
	} else {
		l = len(it.json) - 2
	}
	return it.json[o:l]
}

// Count returns count if query results
func (it *JSONIterator) Count() int {
	return len(it.jsonOffsets)
}

// Error returns query error if it's present.
func (it *JSONIterator) Error() error {
	return it.err
}

// Close closes the iterator.
func (it *JSONIterator) Close() {
	if it.query != nil {
		it.query.close()
		it.query = nil
	}
}
