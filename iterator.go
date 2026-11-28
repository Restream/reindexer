package reindexer

import (
	"context"
	"fmt"
	"reflect"

	"github.com/goccy/go-json"

	"github.com/prometheus/client_golang/prometheus"
	otelattr "go.opentelemetry.io/otel/attribute"

	"github.com/restream/reindexer/v5/bindings"
)

type ExplainSelector struct {
	// Field or index name
	Field string `json:"field,omitempty"`
	// Field type enum: indexed, non-indexed
	FieldType string `json:"field_type,omitempty"`
	// Method, used to process condition
	Method string `json:"method,omitempty"`
	// Number of uniq keys, processed by this selector (may be incorrect, in case of internal query optimization/caching
	Keys int `json:"keys"`
	// Count of comparators used, for this selector
	Comparators int `json:"comparators"`
	// Cost expectation of this selector
	Cost float64 `json:"cost"`
	// Count of processed documents, matched this selector
	Matched int `json:"matched"`
	// Count of scanned documents by this selector
	Items     int    `json:"items"`
	Condition string `json:"condition"`
	// Select iterator type
	Type        string `json:"type,omitempty"`
	Description string `json:"description,omitempty"`
	// Preselect in joined namespace execution explainings
	ExplainPreselect *ExplainResults `json:"explain_preselect,omitempty"`
	// One of selects in joined namespace execution explainings
	ExplainSelect *ExplainResults   `json:"explain_select,omitempty"`
	Selectors     []ExplainSelector `json:"selectors,omitempty"`
}

type ExplainSubQuery struct {
	Namespace string         `json:"namespace"`
	Explain   ExplainResults `json:"explain"`
	Keys      int            `json:"keys,omitempty"`
	Field     string         `json:"field,omitempty"`
}

// ExplainResults represents query plan
type ExplainResults struct {
	SingleQueryExplainResults
	// Detailed execution plans for queries with MERGE (including main query)
	Merged []SingleQueryExplainResults `json:"merged,omitempty"`
}

// SingleQueryExplainResults represents explain plan for single query
type SingleQueryExplainResults struct {
	// Main/merged query namespace name
	Namespace string `json:"namespace,omitempty"`
	// Total query execution time (for MERGE queries includes total_us of all merged queries)
	TotalUs int `json:"total_us"`
	// Query preselect build and select time (for MERGE queries includes preselect_us of all merged queries)
	PreselectUs int `json:"preselect_us"`
	// Query prepare and optimize time (for MERGE queries includes prepare_us of all merged queries)
	PrepareUs int `json:"prepare_us"`
	// Indexes keys selection time (for MERGE queries includes indexes_us of all merged queries)
	IndexesUs int `json:"indexes_us"`
	// Query post process time (for MERGE queries includes postprocess_us of all merged queries)
	PostprocessUS int `json:"postprocess_us"`
	// Intersection loop time (for MERGE queries includes loop_us of all merged queries)
	LoopUs int `json:"loop_us"`
	// Index, which used for sort results
	SortIndex string `json:"sort_index"`
	// General sort time (for MERGE queries includes general_sort_us of all merged queries and post-merge sorting time)
	GeneralSortUs int `json:"general_sort_us"`
	// Optimization of sort by uncompleted index has been performed
	SortByUncommittedIndex bool `json:"sort_by_uncommitted_index"`
	// Filter selectors, used to proccess query conditions
	Selectors []ExplainSelector `json:"selectors,omitempty"`
	// Explaining attempts to inject Join queries ON-conditions into the Main Query WHERE clause
	OnConditionsInsertions []ExplainJoinOnInsertions `json:"on_conditions_insertions,omitempty"`
	// Explaining of subqueries' preselect
	SubQueriesExplains []ExplainSubQuery `json:"subqueries,omitempty"`
}

// Describes the process of a single JOIN-query ON-conditions insertion into the Where clause of a main query
type ExplainJoinOnInsertions struct {
	// joinable ns name
	RightNsName string `json:"namespace"`
	// original ON-conditions clause. SQL-like string
	JoinOnCondition string `json:"on_condition"`
	// total amount of time spent on checking and substituting all conditions
	TotalTimeUs int `json:"total_time_us"`
	// result of insertion attempt
	Succeed bool `json:"success"`
	// optional{succeed==false}. Explains condition insertion failure
	Reason string `json:"reason,omitempty"`
	// by_value or select
	Type string `json:"type"`
	// Inserted condition. SQL-like string
	InsertedCondition string `json:"inserted_condition"`
	// individual conditions processing results
	Conditions []ExplainConditionInsertion `json:"conditions,omitempty"`
}

// Describes an insertion attempt of a single condition from the ON-clause of a JOIN-query
type ExplainConditionInsertion struct {
	// single condition from Join ON section. SQL-like string
	InitialCondition string `json:"condition"`
	// total time elapsed from insertion attempt start till the end of substitution or rejection
	TotalTime int `json:"total_time_us"`
	// optoinal{JoinOnInsertion.type == Select}. Explain raw string from Select subquery
	Explain *ExplainResults `json:"explain_select,omitempty"`
	// Optional. Aggregation type used in subquery
	AggType string `json:"agg_type,omitempty"`
	// result of insertion attempt
	Succeed bool `json:"success"`
	// optional{succeed==false}. Explains condition insertion failure
	Reason string `json:"reason,omitempty"`
	// substituted condition in QueryEntry. SQL-like string
	NewCondition string `json:"new_condition"`
	// resulting size of query values set
	ValuesCount int `json:"values_count"`
}

func errIterator(err error) *Iterator {
	return &Iterator{err: err}
}

func errJSONIterator(err error) *JSONIterator {
	return &JSONIterator{err: err}
}

func newIterator(
	userCtx context.Context,
	db *reindexerImpl,
	namespace string,
	q *Query,
	result bindings.RawBuffer,
	nsArray []nsArrayEntry,
	queryContext any,
) (it *Iterator) {
	if q != nil {
		it = &q.iterator
		it.query = q
	} else {
		it = &Iterator{}
	}
	it.db = db
	it.namespace = namespace
	it.nsArray = nsArray
	it.queryContext = queryContext
	it.resPtr = 0
	it.ptr = 0
	it.err = nil
	it.userCtx = userCtx
	it.allowUnsafe = false
	it.queryFormatVersion = db.binding.QueryFormatVersion()
	if q != nil {
		q.joinsTable = NewQueryJoinsTable(q, nsArray)
		it.joinsTable = q.joinsTable
	} else {
		it.joinsTable = NewQueryJoinsTable(nil, nil)
	}
	if joinedTotal := it.joinsTable.GetJoinQueriesTotal(); joinedTotal > 0 {
		if cap(it.current.joined) < joinedTotal {
			it.current.joined = make([][]any, joinedTotal)
		} else {
			clear(it.current.joined)
			it.current.joined = it.current.joined[:joinedTotal]
		}
	} else {
		clear(it.current.joined)
		it.current.joined = it.current.joined[:0]
	}
	it.setBuffer(result, true)

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
	db                 *reindexerImpl
	namespace          string
	ser                resultSerializer
	rawQueryParams     rawResultQueryParams
	result             bindings.RawBuffer
	nsArray            []nsArrayEntry
	joinsTable         *QueryJoinsTable
	queryFormatVersion int
	queryContext       any
	query              *Query
	allowUnsafe        bool
	resPtr             int
	ptr                int
	current            struct {
		obj    interface{}
		joined [][]any
		rank   float32
	}
	err     error
	userCtx context.Context
}

func (it *Iterator) setBuffer(result bindings.RawBuffer, cleanup bool) {
	it.ser = newSerializer(result.GetBuf())
	it.result = result
	if cleanup {
		nsIncarnationTags := it.rawQueryParams.nsIncarnationTags
		it.rawQueryParams = rawResultQueryParams{nsIncarnationTags: nsIncarnationTags}
		it.ser.readRawQueryParamsResetMissingExtras(&it.rawQueryParams, it.queryFormatVersion, func(nsid int) {
			it.nsArray[nsid].localCjsonState = it.nsArray[nsid].cjsonState.ReadPayloadType(&it.ser.Serializer, it.db.binding, it.nsArray[nsid].name)
		})
	} else {
		it.ser.readRawQueryParamsKeepExtras(&it.rawQueryParams, it.queryFormatVersion, func(nsid int) {
			it.nsArray[nsid].localCjsonState = it.nsArray[nsid].cjsonState.ReadPayloadType(&it.ser.Serializer, it.db.binding, it.nsArray[nsid].name)
		})
	}
}

// Next moves iterator pointer to the next element.
// Returns bool, that indicates the availability of the next elements.
// Decode result to given struct
func (it *Iterator) NextObj(obj any) (hasNext bool) {
	if it.ptr >= it.rawQueryParams.qcount || it.err != nil {
		return
	}
	if it.needMore() {
		it.fetchResults()
		if it.err != nil {
			return
		}
	}
	clear(it.current.joined)
	it.current.obj, it.current.rank, it.err = it.readItem(obj)
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

func (it *Iterator) readItem(toObj interface{}) (item interface{}, rank float32, err error) {
	if it.queryFormatVersion == bindings.QueryFormatV2 {
		return it.readItemImpl(toObj)
	}
	return it.readItemV1(toObj)
}

func (it *Iterator) readItemV1(toObj interface{}) (item interface{}, rank float32, err error) {
	itemParams := it.ser.readRawItemParams(it.rawQueryParams.shardId)
	if (it.rawQueryParams.flags & bindings.ResultsWithRank) != 0 {
		rank = itemParams.rank
	}

	nonCacheable := ((it.rawQueryParams.flags & bindings.ResultsWithItemID) == 0) ||
		len(it.rawQueryParams.nsIncarnationTags) == 0
	hasJoinedFields := (it.rawQueryParams.flags & bindings.ResultsWithJoined) != 0

	item, err = unpackItem(it.db.binding, &it.nsArray[itemParams.nsid], &it.rawQueryParams,
		&itemParams, it.allowUnsafe && !hasJoinedFields, nonCacheable, toObj)
	if err != nil {
		return nil, 0, err
	}

	if hasJoinedFields {
		joinedFields := int(it.ser.GetVarUInt())
		for joinedField := 0; joinedField < joinedFields; joinedField++ {
			itemsCount := int(it.ser.GetVarUInt())
			if itemsCount == 0 {
				it.current.joined[joinedField] = nil
				continue
			}

			joinedNsId := it.joinsTable.GetJoinedNsId(itemParams.nsid, joinedField)
			joinedItems := make([]interface{}, itemsCount)

			for i := 0; i < itemsCount; i++ {
				joinedItems[i], _, err = it.readItemParams(joinedNsId, nil)
				if err != nil {
					return nil, 0, err
				}
			}

			it.current.joined[joinedField] = joinedItems
			it.err = it.join(joinedField, joinedNsId, itemParams.nsid, item)
			if it.err != nil {
				return nil, 0, it.err
			}
		}
	}

	return item, rank, nil
}

func (it *Iterator) readItemImpl(toObj interface{}) (item interface{}, rank float32, err error) {
	itemParams := it.ser.readRawItemParams(it.rawQueryParams.shardId)
	if (it.rawQueryParams.flags & bindings.ResultsWithRank) != 0 {
		rank = itemParams.rank
	}

	nonCacheable := ((it.rawQueryParams.flags & bindings.ResultsWithItemID) == 0) ||
		len(it.rawQueryParams.nsIncarnationTags) == 0
	hasJoinedFields := (it.rawQueryParams.flags & bindings.ResultsWithJoined) != 0

	item, err = unpackItem(it.db.binding, &it.nsArray[itemParams.nsid], &it.rawQueryParams,
		&itemParams, it.allowUnsafe && !hasJoinedFields, nonCacheable, toObj)
	if err != nil {
		return nil, 0, err
	}

	if hasJoinedFields {
		joinedFields := int(it.ser.GetVarUInt())
		for joinedField := 0; joinedField < joinedFields; joinedField++ {
			itemsCount := int(it.ser.GetVarUInt())
			if itemsCount == 0 {
				it.current.joined[joinedField] = nil
				continue
			}

			joinedNsId := it.joinsTable.GetJoinedNsId(itemParams.nsid, joinedField)
			joinedItems := make([]interface{}, itemsCount)

			for i := 0; i < itemsCount; i++ {
				joinedItems[i], _, err = it.readItemImpl(nil)
				if err != nil {
					return nil, 0, err
				}
			}

			it.current.joined[joinedField] = joinedItems
			it.err = it.join(joinedField, joinedNsId, itemParams.nsid, item)
			if it.err != nil {
				return nil, 0, it.err
			}
		}
	}

	return item, rank, nil
}

func (it *Iterator) readItemParams(nsid int, toObj interface{}) (interface{}, float32, error) {
	itemParams := it.ser.readRawItemParams(it.rawQueryParams.shardId)
	if nsid >= 0 {
		itemParams.nsid = nsid
	}

	rank := float32(0)
	if (it.rawQueryParams.flags & bindings.ResultsWithRank) != 0 {
		rank = itemParams.rank
	}

	item, err := unpackItem(it.db.binding, &it.nsArray[itemParams.nsid], &it.rawQueryParams,
		&itemParams, it.allowUnsafe, true, toObj)
	return item, rank, err
}

func (it *Iterator) join(joinedField, joinedNsId, parentNsID int, item interface{}) error {
	field := it.joinsTable.GetField(parentNsID, joinedField)
	handler := it.joinsTable.GetHandler(parentNsID, joinedField)

	subitems := it.current.joined[joinedField]
	if handler != nil {
		if !handler(field, item, subitems) {
			return nil
		}
	}

	if joinable, ok := item.(Joinable); ok {
		joinable.Join(field, subitems, it.queryContext)
	} else if it.query.db.strictJoinHandlers {
		if handler == nil {
			return bindings.NewError(fmt.Sprintf("join handler is missing. Field tag: '%s', struct: '%s', joined namespace: '%s'",
				field, it.nsArray[0].rtype, it.nsArray[joinedNsId].name), ErrCodeStrictMode)
		} else {
			return bindings.NewError(fmt.Sprintf("join handler was found, but returned 'true' and the field was handled via reflection. Field tag: '%s', struct: '%s', joined namespace: '%s'",
				field, it.nsArray[0].rtype, it.nsArray[joinedNsId].name), ErrCodeStrictMode)
		}
	} else {
		var val reflect.Value
		if meta, ok := it.joinsTable.GetFieldMetadata(parentNsID, joinedField); ok {
			val = getJoinedFieldValueByIndex(reflect.ValueOf(item), meta.index)
		} else {
			val = getJoinedFieldValue(reflect.ValueOf(item), it.nsArray[parentNsID].joined, field)
		}
		if !val.IsValid() {
			return bindings.NewError(
				fmt.Sprintf("cannot put join result into '%s.%s': field not found in struct '%s' (joined namespace: '%s')",
					it.nsArray[0].rtype, field, it.nsArray[0].rtype, it.nsArray[joinedNsId].name),
				ErrCodeLogic,
			)
		}
		oldLen := growJoinedSlice(val, len(subitems))
		for _, subitem := range subitems {
			val.Index(oldLen).Set(reflect.ValueOf(subitem))
			oldLen++
		}
	}

	return nil
}

func getJoinedFieldValueByIndex(val reflect.Value, idx []int) reflect.Value {
	return reflect.Indirect(reflect.Indirect(val).FieldByIndex(idx))
}

func growJoinedSlice(v reflect.Value, add int) int {
	oldLen := v.Len()
	newLen := oldLen + add
	if v.IsNil() {
		v.Set(reflect.MakeSlice(v.Type(), newLen, newLen))
	} else if newLen <= v.Cap() {
		v.Set(v.Slice(0, newLen))
	} else {
		newCap := newLen
		if oldCap := v.Cap(); oldCap > 0 {
			newCap = oldCap * 2
			if newCap < newLen {
				newCap = newLen
			}
		}
		nv := reflect.MakeSlice(v.Type(), newLen, newCap)
		reflect.Copy(nv, v)
		v.Set(nv)
	}
	return oldLen
}

func (it *Iterator) needMore() bool {
	if it.resPtr >= it.rawQueryParams.count && it.ptr <= it.rawQueryParams.qcount {
		return true
	}
	return false
}

func (it *Iterator) fetchResults() {
	if it.db.otelTracer != nil {
		defer it.db.startTracingSpan(it.userCtx, "Reindexer.Iterator.FetchResults", otelattr.String("rx.ns", it.namespace)).End()
	}

	if it.db.promMetrics != nil {
		defer prometheus.NewTimer(it.db.promMetrics.clientCallsLatency.WithLabelValues("Iterator.FetchResults", it.namespace)).ObserveDuration()
	}

	if fetchMore, ok := it.result.(bindings.FetchMore); ok {
		fetchCount := defaultFetchCount
		if it.query != nil {
			fetchCount = it.query.fetchCount
		}

		if it.ptr <= it.rawQueryParams.count {
			// Copy aggregation results before the first fetch
			if len(it.rawQueryParams.aggResults) > 0 {
				duplicate := make([][]byte, len(it.rawQueryParams.aggResults))
				for i := range it.rawQueryParams.aggResults {
					duplicate[i] = make([]byte, len(it.rawQueryParams.aggResults[i]))
					copy(duplicate[i], it.rawQueryParams.aggResults[i])
				}
				it.rawQueryParams.aggResults = duplicate
			}
			if len(it.rawQueryParams.explainResults) > 0 {
				duplicate := make([]byte, len(it.rawQueryParams.explainResults))
				copy(duplicate, it.rawQueryParams.explainResults)
				it.rawQueryParams.explainResults = duplicate
			}
		}

		if it.err = fetchMore.Fetch(it.userCtx, it.ptr, fetchCount, false); it.err != nil {
			return
		}
		it.resPtr = 0
		it.setBuffer(it.result, false)
	} else {
		panic(fmt.Errorf("unexpected behavior: have the partial query but binding not support that"))
	}
}

// Object returns current object.
// Will panic when pointer was not moved, Next() must be called before.
func (it *Iterator) Object() any {
	if it.resPtr == 0 {
		panic(errIteratorNotReady)
	}
	return it.current.obj
}

// Rank returns current object search rank.
// Will panic when pointer was not moved, Next() must be called before.
func (it *Iterator) Rank() float32 {
	if it.resPtr == 0 {
		panic(errIteratorNotReady)
	}
	return it.current.rank
}

// JoinedObjects returns joined items slice for root-level join query only.
func (it *Iterator) JoinedObjects(field string) (objects []any, err error) {
	if it.resPtr == 0 {
		return nil, errIteratorNotReady
	}
	idx := it.findJoinFieldIndex(field)
	if idx == -1 {
		return nil, errJoinUnexpectedField
	}
	return it.current.joined[idx], nil
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
func (it *Iterator) FetchAll() (items []any, err error) {
	defer it.Close()
	if !it.Next() {
		return nil, it.err
	}
	items = make([]any, it.rawQueryParams.qcount)
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
func (it *Iterator) FetchOne() (item any, err error) {
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
func (it *Iterator) FetchAllWithRank() (items []any, ranks []float32, err error) {
	defer it.Close()
	if !it.Next() {
		return nil, nil, it.err
	}
	items = make([]any, it.rawQueryParams.qcount)
	ranks = make([]float32, it.rawQueryParams.qcount)
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
	return (it.rawQueryParams.flags & bindings.ResultsWithRank) != 0
}

// AggResults returns aggregation results (if present)
func (it *Iterator) AggResults() (v []AggregationResult) {
	l := len(it.rawQueryParams.aggResults)
	v = make([]AggregationResult, l)
	for i := range l {
		json.Unmarshal(it.rawQueryParams.aggResults[i], &v[i])
	}

	return
}

// GetAggreatedValue - Return aggregation sum of field
func (it *Iterator) GetAggreatedValue(idx int) *float64 {
	if idx < 0 || idx >= len(it.rawQueryParams.aggResults) {
		return nil
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
		it.rawQueryParams.aggResults = nil
		it.rawQueryParams.explainResults = nil
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
	if it.joinsTable == nil {
		return -1
	}
	return it.joinsTable.FindFieldIndex(0, field)
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
