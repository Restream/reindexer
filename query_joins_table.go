package reindexer

import (
	"strings"
	"sync"
)

type joinedFieldMetadata struct {
	index    []int
	hasIndex bool
}

type QueryJoinsTable struct {
	joinQueriesTotal int
	hasNestedJoins   bool
	joinQueriesIDs   [][]int // [parentNsId][joinIndex] = childNsId
	fields           [][]string
	handlers         [][]JoinHandler
	fieldsMetadata   [][]joinedFieldMetadata
}

var (
	queryJoinsTablePool sync.Pool
	emptyJoinsTable     = &QueryJoinsTable{}
)

func NewQueryJoinsTable(q *Query, nsArray []nsArrayEntry) *QueryJoinsTable {
	if q == nil {
		return emptyJoinsTable
	}
	totalNamespaces, totalJoins := calculateJoinQueriesCount(q)
	if totalJoins == 0 {
		return emptyJoinsTable
	}

	table := GetJoinsTableFromPool()
	table.Prepare(q, nsArray, totalNamespaces, totalJoins)
	return table
}

func GetJoinsTableFromPool() *QueryJoinsTable {
	if table, ok := queryJoinsTablePool.Get().(*QueryJoinsTable); ok {
		return table
	}
	return &QueryJoinsTable{}
}

func ReleaseJoinsTable(table *QueryJoinsTable) {
	if table == nil || table == emptyJoinsTable {
		return
	}
	table.clear()
	queryJoinsTablePool.Put(table)
}

func (t *QueryJoinsTable) Prepare(q *Query, nsArray []nsArrayEntry, totalNamespaces int, totalJoins int) {
	t.clear()
	if q == nil || totalJoins == 0 {
		return
	}
	t.joinQueriesTotal = totalJoins

	totalNsIds := totalNamespaces
	t.fields = growSlice(t.fields, totalNsIds)
	t.handlers = growSlice(t.handlers, totalNsIds)
	t.joinQueriesIDs = growSlice(t.joinQueriesIDs, totalNsIds)
	t.fieldsMetadata = growSlice(t.fieldsMetadata, totalNsIds)

	t.buildJoinsOffsetTable(q, nsArray)
}

func (t *QueryJoinsTable) clear() {
	t.joinQueriesTotal = 0
	t.hasNestedJoins = false
	for i := range t.joinQueriesIDs {
		t.joinQueriesIDs[i] = t.joinQueriesIDs[i][:0]
	}
	clear(t.fields)
	clear(t.handlers)
	for i := range t.fieldsMetadata {
		clear(t.fieldsMetadata[i])
		t.fieldsMetadata[i] = t.fieldsMetadata[i][:0]
	}
	t.fields = t.fields[:0]
	t.handlers = t.handlers[:0]
	t.joinQueriesIDs = t.joinQueriesIDs[:0]
	t.fieldsMetadata = t.fieldsMetadata[:0]
}

func growSlice[T any](s []T, size int) []T {
	if cap(s) < size {
		return make([]T, size)
	}
	return s[:size]
}

func (t *QueryJoinsTable) GetJoinedNsId(parentNsId int, joinIndex int) int {
	if parentNsId < 0 || parentNsId >= len(t.joinQueriesIDs) {
		return -1
	}
	lookup := t.joinQueriesIDs[parentNsId]
	if joinIndex < 0 || joinIndex >= len(lookup) {
		return -1
	}
	return lookup[joinIndex]
}

func (t *QueryJoinsTable) GetField(nsId int, joinedField int) string {
	if nsId < len(t.fields) && joinedField < len(t.fields[nsId]) {
		return t.fields[nsId][joinedField]
	}
	return ""
}

func (t *QueryJoinsTable) GetHandler(nsId int, joinedField int) JoinHandler {
	if nsId < len(t.handlers) && joinedField < len(t.handlers[nsId]) {
		return t.handlers[nsId][joinedField]
	}
	return nil
}

func (t *QueryJoinsTable) GetFieldMetadata(nsId int, joinedField int) (joinedFieldMetadata, bool) {
	if nsId < len(t.fieldsMetadata) && joinedField < len(t.fieldsMetadata[nsId]) {
		info := t.fieldsMetadata[nsId][joinedField]
		return info, info.hasIndex
	}
	return joinedFieldMetadata{}, false
}

func (t *QueryJoinsTable) FindFieldIndex(nsId int, field string) int {
	if nsId >= len(t.fields) {
		return -1
	}
	for i, candidate := range t.fields[nsId] {
		if strings.EqualFold(candidate, field) {
			return i
		}
	}
	return -1
}

func (t *QueryJoinsTable) GetJoinQueriesTotal() int {
	return t.joinQueriesTotal
}

func (t *QueryJoinsTable) HasNestedJoins() bool {
	return t.hasNestedJoins
}

func calculateJoinQueriesCount(q *Query) (totalNamespaces int, totalJoins int) {
	if q == nil {
		return 0, 0
	}
	totalNamespaces = 1
	totalJoins = len(q.joinQueries)

	for _, jq := range q.joinQueries {
		jn, jj := calculateJoinQueriesCount(jq)
		totalNamespaces += jn
		totalJoins += jj
	}

	for _, mq := range q.mergedQueries {
		mn, mj := calculateJoinQueriesCount(mq)
		totalNamespaces += mn
		totalJoins += mj
	}

	return totalNamespaces, totalJoins
}

func (t *QueryJoinsTable) buildJoinsOffsetTable(q *Query, nsArray []nsArrayEntry) {
	if q == nil {
		return
	}

	nextNsId := 1 + len(q.mergedQueries)
	t.processQuery(q, 0, &nextNsId, nsArray)

	mergedNsId := 1
	for _, mergedQuery := range q.mergedQueries {
		t.processQuery(mergedQuery, mergedNsId, &nextNsId, nsArray)
		mergedNsId++
	}
}

func (t *QueryJoinsTable) processQuery(query *Query, parentNsId int, nextNsId *int, nsArray []nsArrayEntry) {
	t.fields[parentNsId] = query.joinToFields
	t.handlers[parentNsId] = query.joinHandlers
	t.setFieldsMetadata(parentNsId, query.joinToFields, nsArray)

	joinCount := len(query.joinQueries)
	if joinCount > 0 {
		t.joinQueriesIDs[parentNsId] = growSlice(t.joinQueriesIDs[parentNsId], joinCount)
	}

	for i, joinQuery := range query.joinQueries {
		childNsId := *nextNsId
		(*nextNsId)++
		t.joinQueriesIDs[parentNsId][i] = childNsId
		if len(joinQuery.joinQueries) > 0 {
			t.hasNestedJoins = true
		}
		t.processQuery(joinQuery, childNsId, nextNsId, nsArray)
	}
}

func (t *QueryJoinsTable) setFieldsMetadata(parentNsId int, fields []string, nsArray []nsArrayEntry) {
	metadata := growSlice(t.fieldsMetadata[parentNsId], len(fields))
	var joined map[string][]int
	if parentNsId < len(nsArray) {
		joined = nsArray[parentNsId].joined
	}
	for i, field := range fields {
		metadata[i] = joinedFieldMetadata{}
		if idx, ok := joined[field]; ok {
			metadata[i] = joinedFieldMetadata{index: idx, hasIndex: true}
		}
	}
	t.fieldsMetadata[parentNsId] = metadata
}
