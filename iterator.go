package reindexer

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/restream/reindexer/bindings"
)

type ExplainSelector struct {
	// Field or index name
	Field string `json:"field"`
	// Method, used to process condition
	Method string `json:"method"`
	// Number of uniq keys, processed by this selector (may be incorrect, in case of internal query optimization/caching
	Keys int `json:"keys"`
	// Count of comparators used, for this selector
	Comparators int `json:"comparators"`
	// Cost expectation of this selector
	Cost float64 `json:"cost"`
	// Count of processed documents, matched this selector
	Matched int `json:"matched"`
	// Count of scanned documents by this selector
	Items int `json:"items"`
	// Preselect in joined namespace execution explainings
	ExplainPreselect *ExplainResults `json:"explain_preselect,omitempty"`
	// One of selects in joined namespace execution explainings
	ExplainSelect *ExplainResults   `json:"explain_select,omitempty"`
	Selectors     []ExplainSelector `json:"selectors,omitempty"`
}

// ExplainResults presents query plan
type ExplainResults struct {
	// Total query execution time
	TotalUs int `json:"total_us"`
	// Query prepare and optimize time
	PrepareUs int `json:"prepare_us"`
	// Indexes keys selection time
	IndexesUs int `json:"indexes_us"`
	// Query post process time
	PostprocessUS int `json:"postprocess_us"`
	// Intersection loop time
	LoopUs int `json:"loop_us"`
	// Index, which used for sort results
	SortIndex string `json:"sort_index"`
	// General sort time
	GeneralSortUs int `json:"general_sort_us"`
	// Optimization of sort by uncompleted index has been performed
	SortByUncommittedIndex bool `json:"sort_by_uncommitted_index"`
	// Filter selectors, used to proccess query conditions
	Selectors []ExplainSelector `json:"selectors"`
}

func errIterator(err error) *Iterator {
	return &Iterator{err: err}
}

func errJSONIterator(err error) *JSONIterator {
	return &JSONIterator{err: err}
}

func newIterator(
	userCtx context.Context,
	q *Query,
	result bindings.RawBuffer,
	nsArray []nsArrayEntry,
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
	it.nsArray = nsArray
	it.joinToFields = joinToFields
	it.joinHandlers = joinHandlers
	it.queryContext = queryContext
	it.resPtr = 0
	it.ptr = 0
	it.err = nil
	it.userCtx = userCtx
	it.allowUnsafe = false
	joinObjSize := len(it.joinToFields)
	if q != nil {
		for _, mq := range q.mergedQueries {
			joinSize := len(mq.joinToFields)
			if joinSize > joinObjSize {
				joinObjSize = joinSize
			}
		}
	}
	if joinObjSize > 0 {
		it.current.joinObj = make([][]interface{}, joinObjSize)
	}
	it.setBuffer(result)

	return
}

func newJSONIterator(ctx context.Context, q *Query, json []byte, jsonOffsets []int, explain []byte) *JSONIterator {
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
	ji.explain = explain
	ji.err = nil
	ji.userCtx = ctx

	return ji
}

// Iterator presents query results
type Iterator struct {
	ser            resultSerializer
	rawQueryParams rawResultQueryParams
	result         bindings.RawBuffer
	nsArray        []nsArrayEntry
	joinToFields   []string
	joinHandlers   []JoinHandler
	queryContext   interface{}
	query          *Query
	allowUnsafe    bool
	resPtr         int
	ptr            int
	current        struct {
		obj     interface{}
		joinObj [][]interface{}
		rank    int
	}
	err     error
	userCtx context.Context
}

func (it *Iterator) setBuffer(result bindings.RawBuffer) {
	it.ser = newSerializer(result.GetBuf())
	it.result = result
	it.rawQueryParams = it.ser.readRawQueryParams(func(nsid int) {
		it.nsArray[nsid].localCjsonState = it.nsArray[nsid].cjsonState.ReadPayloadType(&it.ser.Serializer)
	})
}

// Next moves iterator pointer to the next element.
// Returns bool, that indicates the availability of the next elements.
// Decode result to given struct
func (it *Iterator) NextObj(obj interface{}) (hasNext bool) {
	if it.ptr >= it.rawQueryParams.qcount || it.err != nil {
		return
	}
	if it.needMore() {
		it.fetchResults()
		if it.err != nil {
			return
		}
	}
	it.current.obj, it.current.rank = it.readItem(obj)
	if it.err != nil {
		return
	}
	it.resPtr++
	it.ptr++
	return it.ptr <= it.rawQueryParams.qcount
}

func (it *Iterator) Next() (hasNext bool) {
	return it.NextObj(nil)
}

func (it *Iterator) joinedNsIndexOffset(parentNsID int) int {
	if it.query == nil {
		return 1
	}

	// main NS + count of merged ones
	offset := 1 + len(it.query.mergedQueries)

	mergedNsIdx := parentNsID
	if mergedNsIdx > 0 {
		offset += len(it.query.joinQueries)
		// it.query.mergedQueries doesn't store main object joined data
		mergedNsIdx--
	}

	for i := 0; i < mergedNsIdx; i++ {
		offset += len(it.query.mergedQueries[i].joinQueries)
	}
	return offset
}

func (it *Iterator) readItem(toObj interface{}) (item interface{}, rank int) {
	params := it.ser.readRawtItemParams(it.rawQueryParams.shardId)
	if (it.rawQueryParams.flags & bindings.ResultsWithPercents) != 0 {
		rank = params.proc
	}

	subNSRes := 0

	if (it.rawQueryParams.flags & bindings.ResultsWithJoined) != 0 {
		subNSRes = int(it.ser.GetVarUInt())
	}
	item, it.err = unpackItem(&it.nsArray[params.nsid], &params, it.allowUnsafe && (subNSRes == 0), (it.rawQueryParams.flags&bindings.ResultsWithItemID) == 0, toObj)
	if it.err != nil {
		return
	}

	nsIndexOffset := it.joinedNsIndexOffset(params.nsid)

	for nsIndex := 0; nsIndex < subNSRes; nsIndex++ {
		siRes := int(it.ser.GetVarUInt())
		if siRes == 0 {
			continue
		}
		subitems := make([]interface{}, siRes)
		for i := 0; i < siRes; i++ {
			subparams := it.ser.readRawtItemParams(it.rawQueryParams.shardId)
			subitems[i], it.err = unpackItem(&it.nsArray[nsIndex+nsIndexOffset], &subparams, it.allowUnsafe, (it.rawQueryParams.flags&bindings.ResultsWithItemID) == 0, toObj)
			if it.err != nil {
				return
			}
		}

		it.current.joinObj[nsIndex] = subitems
		it.join(nsIndex, nsIndexOffset, params.nsid, item)
	}
	return
}

func (it *Iterator) needMore() bool {
	if it.resPtr >= it.rawQueryParams.count && it.ptr <= it.rawQueryParams.qcount {
		return true
	}
	return false
}

func (it *Iterator) fetchResults() {
	if fetchMore, ok := it.result.(bindings.FetchMore); ok {
		fetchCount := defaultFetchCount
		if it.query != nil {
			fetchCount = it.query.fetchCount
		}

		if it.err = fetchMore.Fetch(it.userCtx, it.ptr, fetchCount, false); it.err != nil {
			return
		}
		it.resPtr = 0
		it.setBuffer(it.result)
	} else {
		panic(fmt.Errorf("unexpected behavior: have the partial query but binding not support that"))
	}
	return
}

func (it *Iterator) join(nsIndex, nsIndexOffset, parentNsID int, item interface{}) {
	var field string
	var handler JoinHandler
	if parentNsID == 0 {
		field = it.joinToFields[nsIndex]
		handler = it.joinHandlers[nsIndex]
	} else {
		field = it.query.mergedQueries[parentNsID-1].joinToFields[nsIndex]
		handler = it.query.mergedQueries[parentNsID-1].joinHandlers[nsIndex]
	}

	subitems := it.current.joinObj[nsIndex]
	if handler != nil {
		if !handler(field, item, subitems) {
			return
		}
	}
	if joinable, ok := item.(Joinable); ok {
		joinable.Join(field, subitems, it.queryContext)
	} else {

		v := getJoinedField(reflect.ValueOf(item), it.nsArray[parentNsID].joined, field)
		if !v.IsValid() {
			panic(fmt.Errorf("Can't find field with tag '%s' in struct '%s' for put join results from '%s'",
				field,
				it.nsArray[0].rtype,
				it.nsArray[nsIndex+nsIndexOffset].name))
		}
		if v.IsNil() {
			v.Set(reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(it.nsArray[nsIndex+nsIndexOffset].rtype)), 0, len(subitems)))
		}
		for _, subitem := range subitems {
			v.Set(reflect.Append(v, reflect.ValueOf(subitem)))
		}
	}
}

// Object returns current object.
// Will panic when pointer was not moved, Next() must be called before.
func (it *Iterator) Object() interface{} {
	if it.resPtr == 0 {
		panic(errIteratorNotReady)
	}
	return it.current.obj
}

// Rank returns current object search rank.
// Will panic when pointer was not moved, Next() must be called before.
func (it *Iterator) Rank() int {
	if it.resPtr == 0 {
		panic(errIteratorNotReady)
	}
	return it.current.rank
}

// JoinedObjects returns objects slice, that result of join for the given field
func (it *Iterator) JoinedObjects(field string) (objects []interface{}, err error) {
	if it.resPtr == 0 {
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
	return it.rawQueryParams.qcount
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
	items = make([]interface{}, it.rawQueryParams.qcount)
	for i := range items {
		items[i] = it.Object()
		if !it.Next() {
			break
		}
	}
	return items, it.err
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
	items = make([]interface{}, it.rawQueryParams.qcount)
	ranks = make([]int, it.rawQueryParams.qcount)
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
	return (it.rawQueryParams.flags & bindings.ResultsWithPercents) != 0
}

// AggResults returns aggregation results (if present)
func (it *Iterator) AggResults() (v []AggregationResult) {
	l := len(it.rawQueryParams.aggResults)
	v = make([]AggregationResult, l)

	for i := 0; i < l; i++ {
		json.Unmarshal(it.rawQueryParams.aggResults[i], &v[i])
	}

	return
}

// GetAggreatedValue - Return aggregation sum of field
func (it *Iterator) GetAggreatedValue(idx int) float64 {
	if idx < 0 || idx >= len(it.rawQueryParams.aggResults) {
		return 0
	}
	res := AggregationResult{}
	json.Unmarshal(it.rawQueryParams.aggResults[idx], &res)

	return res.Value
}

// GetExplainResults returns JSON bytes with explain results
func (it *Iterator) GetExplainResults() (*ExplainResults, error) {
	if len(it.rawQueryParams.explainResults) > 0 {
		explain := &ExplainResults{}
		if err := json.Unmarshal(it.rawQueryParams.explainResults, explain); err != nil {
			return nil, fmt.Errorf("Explain query results is broken: %v", err)
		}
		return explain, nil
	}
	return nil, nil
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
}

// Get namespace's tagsmatcher info
func (it *Iterator) GetTagsMatcherInfo(nsName string) (stateToken int32, version int32) {
	version = -1
	for _, ns := range it.nsArray {
		if nsName == ns.name {
			st := ns.localCjsonState.Copy()
			stateToken = st.StateToken
			version = st.Version
			return
		}
	}
	return
}

func (it *Iterator) findJoinFieldIndex(field string) (index int) {
	for index = range it.joinToFields {
		if strings.EqualFold(it.joinToFields[index], field) {
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
	err         error
	ptr         int
	explain     []byte
	userCtx     context.Context
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

// GetExplainResults returns JSON bytes with explain results
func (it *JSONIterator) GetExplainResults() (*ExplainResults, error) {
	if len(it.explain) > 0 {
		explain := &ExplainResults{}
		if err := json.Unmarshal(it.explain, explain); err != nil {
			return nil, fmt.Errorf("Explain query results is broken: %v", err)
		}
		return explain, nil
	}
	return nil, nil
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
