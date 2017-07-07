#include "core/namespace.h"
#include <leveldb/cache.h>
#include <leveldb/comparator.h>
#include <stdio.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include "algoritm/full_text_search/fulltextdumper.h"
#include "tools/errors.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/strings.h"

using std::chrono::duration_cast;
using std::chrono::microseconds;
using std::chrono::high_resolution_clock;
using search_engine::FullTextDumper;

namespace reindexer {

Namespace::Namespace(const Namespace &src)
	: indexesNames_(src.indexesNames_),
	  items_(src.items_),
	  deleted_(src.deleted_),
	  updatedCount_(src.updatedCount_),
	  name(src.name),
	  payloadType_(src.payloadType_),
	  tagsMatcher_(src.tagsMatcher_),
	  db_(src.db_),
	  snapshot_(nullptr),
	  unflushedCount_(0),
	  commitPhasesState_(src.commitPhasesState_),
	  preparedIndexes_(src.preparedIndexes_) {
	for (auto &idxIt : src.indexes_) indexes_.push_back(unique_ptr<Index>(idxIt->Clone()));

	logPrintf(LogTrace, "Namespace::Namespace (clone %s)", name.c_str());
}

Namespace &Namespace::operator=(const Namespace &src) {
	if (this == &src) return *this;

	indexesNames_ = src.indexesNames_;
	items_.assign(src.items_.begin(), src.items_.end());
	deleted_.assign(src.deleted_.begin(), src.deleted_.end());
	updatedCount_ = src.updatedCount_;
	name = src.name;
	payloadType_ = src.payloadType_;
	tagsMatcher_ = src.tagsMatcher_;
	db_ = src.db_;
	snapshot_ = nullptr;
	unflushedCount_ = 0;
	commitPhasesState_ = src.commitPhasesState_;
	preparedIndexes_ = src.preparedIndexes_;
	for (auto &idxIt : src.indexes_) indexes_.push_back(unique_ptr<Index>(idxIt->Clone()));

	return *this;
}

Namespace::Namespace(const string &_name)
	: updatedCount_(0),
	  name(_name),
	  payloadType_(make_shared<PayloadType>(_name)),
	  tagsMatcher_(make_shared<TagsMatcher>(*payloadType_)),
	  snapshot_(nullptr),
	  unflushedCount_(0),
	  commitPhasesState_(0) {
	logPrintf(LogTrace, "Namespace::Namespace (%s)", name.c_str());
	items_.reserve(10000);

	// Add index and payload field for tuple of non indexed fields
	IndexOpts opts{0, 0};
	AddIndex("-tuple", "", IndexStrStore, &opts);
}

Namespace::~Namespace() {
	logPrintf(LogTrace, "Namespace::~Namespace (%s), %d items", name.c_str(), items_.size());
	if (db_ && snapshot_) db_->ReleaseSnapshot(snapshot_);
}

void Namespace::AddIndex(const string &index, const string &jsonPath, IndexType type, const IndexOpts *opts) {
	IndexOpts defaultOpts{0, 0};
	if (!opts) opts = &defaultOpts;

	if (items_.size())
		throw Error(errParams, "Can't add index '%s' in namespace '%s' - ns already contains data", index.c_str(), name.c_str());

	auto idxNameIt = indexesNames_.find(index);
	int idxNo = indexes_.size();

	if (idxNameIt == indexesNames_.end()) {
		// Create new index object
		// Add payload field corresponding to index
		indexesNames_.insert({index, idxNo});
		if (type != IndexComposite && type != IndexCompositeHash)
			indexes_.push_back(unique_ptr<Index>(Index::New(type, index, *opts)));
		else {
			// special handling of composite indexes;
			AddCompositeIndex(index, type, opts);
			return;
		}
	} else if (indexes_[idxNameIt->second]->type != type)
		throw Error(errParams, "Index '%s' is already exists with different type", index.c_str());
	else
		idxNo = idxNameIt->second;
	payloadType_->Add(PayloadFieldType(indexes_[idxNo]->KeyType(), index, jsonPath, opts->IsArray, opts->IsPK));
}

void Namespace::AddCompositeIndex(const string &index, IndexType type, const IndexOpts *opts) {
	FieldsSet ff;
	vector<string> subIdxes;
	bool allPK = true;
	for (auto subIdx : split(index, "+", true, subIdxes)) {
		auto idxNameIt = indexesNames_.find(subIdx);
		if (idxNameIt == indexesNames_.end())
			throw Error(errParams, "Subindex '%s' not found for composite index '%s'", subIdx.c_str(), index.c_str());
		ff.push_back(idxNameIt->second);
		if (!indexes_[idxNameIt->second]->opts_.IsPK) allPK = false;
	}
	if (allPK) {
		logPrintf(LogInfo, "Using composite index instead of single %s", index.c_str());
		for (auto i : ff) {
			indexes_[i]->opts_.IsPK = false;
		}
	}
	IndexOpts o = *opts;
	o.IsPK = allPK;
	indexes_.push_back(unique_ptr<Index>(Index::NewComposite(type, index, o, *payloadType_, ff)));
}

void Namespace::Upsert(Item *item, bool store) {
	// Item to upsert
	ItemImpl *ritem = (ItemImpl *)item;

	auto exists = findByPK(ritem);
	IdType id = exists.first;
	bool doUpdate = exists.second;

	if (!doUpdate) {
		// Check if payload not created yet, create it
		if (deleted_.size()) {
			id = deleted_.back();
			deleted_.pop_back();
			assert(id < (IdType)items_.size());
			assert(items_[id].IsFree());
			items_[id] = PayloadData(ritem->RealSize());
		} else {
			id = items_.size();
			items_.emplace_back(PayloadData(ritem->RealSize()));
		}
	}

	upsert(ritem, id, doUpdate);
	ritem->SetID(id, items_[id].GetVersion());

	if (db_ && store) {
		char pk[512];
		memcpy(pk, "indx", 4);
		ritem->GetPK(pk + 4, sizeof(pk) - 4);
		Slice b = ritem->Serialize();
		batch_.Put(pk, leveldb::Slice(b.data(), b.size()));
		if (++unflushedCount_ > 100000) flushToStorage();
	}
}

void Namespace::Delete(Item *item) {
	ItemImpl *ritem = (ItemImpl *)item;

	auto exists = findByPK(ritem);
	IdType id = exists.first;

	if (!exists.second) {
		//		throw Error(errLogic, "Item with pkey='%s' not found in namespace '%s'", ritem->GetPK().c_str(), name.c_str());
		return;
	}

	ritem->SetID(id, items_[id].GetVersion());
	_delete(id);
}

void Namespace::_delete(IdType id) {
	commitPhasesState_ = 0;
	preparedIndexes_.clear();

	assert(items_.exists(id));

	Payload pl(*payloadType_, &items_[id]);

	if (db_) {
		auto pk = pl.GetPK();
		batch_.Delete(string("indx") + pk);
		if (++unflushedCount_ > 10000) flushToStorage();
	}

	// erase last item
	KeyRefs skrefs;
	int field;
	// erase from composite indexes
	for (field = pl.NumFields(); field < (int)indexes_.size(); ++field) indexes_[field]->Delete(KeyRef(items_[id]), id);

	for (field = 0; field < pl.NumFields(); ++field) {
		auto &index = *indexes_[field];
		pl.Get(field, skrefs);
		// Delete value from index
		for (auto key : skrefs) index.Delete(key, id);
		// If no krefs delete empty value from index
		if (!skrefs.size()) index.Delete(KeyRef(), id);
	}

	// free PayloadData
	items_[id].Free();
}

void Namespace::Delete(const Query &q, QueryResults &result) {
	Select(q, nullptr, result);
	auto tmStart = high_resolution_clock::now();
	for (auto r : result) _delete(r.id);

	if (q.debugLevel >= LogInfo) {
		logPrintf(LogInfo, "Deleted %d items in %d µs", result.size(),
				  duration_cast<microseconds>(high_resolution_clock::now() - tmStart).count());
	}
}

void Namespace::upsert(ItemImpl *ritem, IdType id, bool doUpdate) {
	// Upsert fields to indexes
	assert(items_.exists(id));
	auto &plData = items_[id];

	// Inplace payload
	Payload pl(*payloadType_, &plData);
	if (doUpdate) {
		if (plData.IsUpdated()) {
			commit(NSCommitContext(*this, CommitContext::MakeIdsets));
			upsert(ritem, id, doUpdate);
			return;
		}
		plData.AllocOrClone(pl.RealSize());
		updatedCount_++;
	}
	plData.SetUpdated(true);
	commitPhasesState_ = 0;
	preparedIndexes_.clear();
	KeyRefs krefs, skrefs;

	// Delete from composite indexes first
	int field = 0;
	for (field = ritem->NumFields(); field < (int)indexes_.size(); ++field)
		if (doUpdate) indexes_[field]->Delete(KeyRef(plData), id);

	// Upsert fields to regular indexes
	for (field = 0; field < ritem->NumFields(); ++field) {
		auto &index = *indexes_[field];

		ritem->Get(field, skrefs);
		// Check for update
		if (doUpdate) {
			pl.Get(field, krefs);
			for (auto key : krefs) index.Delete(key, id);
			if (!krefs.size()) index.Delete(KeyRef(), id);
		}
		// Put value to index
		krefs.resize(0);
		for (auto key : skrefs) krefs.push_back(index.Upsert(key, id));

		// Put value to payload
		pl.Set(field, krefs);
		// If no krefs upsert empty value to index
		if (!skrefs.size()) index.Upsert(KeyRef(), id);
	}
	// Upsert to composite indexes
	for (; field < (int)indexes_.size(); ++field) indexes_[field]->Upsert(KeyRef(plData), id);
}

// find id by PK. NOT THREAD SAFE!
pair<IdType, bool> Namespace::findByPK(ItemImpl *ritem) {
	h_vector<IdSetRef, 4> ids;
	h_vector<IdSetRef::iterator, 4> idsIt;

	// It's faster equalent of "select ID from namespace where pk1 = 'item.pk1' and pk2 = 'item.pk2' "
	// Get pkey values from pk fields
	for (int field = 0; field < (int)indexes_.size(); ++field)
		if (indexes_[field]->opts_.IsPK) {
			KeyRefs krefs;
			if (field < ritem->NumFields())
				ritem->Get(field, krefs);
			else
				krefs.push_back(KeyRef(*ritem->Data()));
			assertf(krefs.size() == 1, "Pkey field must contain 1 key, but there '%d' in '%s.%s'", (int)krefs.size(), name.c_str(),
					payloadType_->Field(field).Name().c_str());
			ids.push_back(indexes_[field]->Find(krefs[0]).Sorted(0));
		}
	// Sort idsets. Intersect works faster on sets sorted by size
	std::sort(ids.begin(), ids.end(), [](const IdSetRef &k1, const IdSetRef &k2) { return k1.size() < k2.size(); });
	// Fast intersect found idsets
	if (ids.size() && ids[0].size()) {
		// Setup iterators to begin
		for (size_t i = 0; i < ids.size(); i++) idsIt.push_back(ids[i].begin());
		// Main loop by 1st result
		for (auto cur = ids[0].begin(); cur != ids[0].end(); cur++) {
			// Check if id exists in all other results
			unsigned j;
			for (j = 1; j < ids.size(); j++) {
				idsIt[j] = std::lower_bound(idsIt[j], ids[j].end(), *cur);
				if (idsIt[j] == ids[j].end()) {
					return {-1, false};
				} else if (*idsIt[j] != *cur)
					break;
			}
			if (j == ids.size()) {
				assert(items_.exists(*cur));
				return {*cur, true};
			}
		}
	}
	return {-1, false};
}

void Namespace::commit(const NSCommitContext &ctx) {
	std::lock_guard<mutex> lk(commitLock_);

	if (ctx.phases() & CommitContext::FlushToStorage) flushToStorage();

	// Commit changes
	if (~commitPhasesState_ & ctx.phases() & CommitContext::MakeIdsets) {
		logPrintf(LogTrace, "Namespace::Commit ('%s'),%d items", name.c_str(), items_.size());
		for (auto &idxIt : indexes_) idxIt->Commit(ctx);

		auto dIt = deleted_.begin();
		for (IdType i = 0; i < (IdType)items_.size(); i++) {
			if (items_[i].IsFree()) {
				dIt = std::lower_bound(dIt, deleted_.end(), i);
				if (dIt == deleted_.end() || *dIt != i) dIt = deleted_.insert(dIt, i);
			} else {
				items_[i].SetUpdated(false);
			}
		}

		updatedCount_ = 0;
		items_.shrink_to_fit();
	}

	if (~commitPhasesState_ & ctx.phases() & CommitContext::MakeSortOrders) {
		// Update sort orders and sort_id for each index
		int i = 1;
		for (auto &idxIt : indexes_) {
			if (idxIt->IsSorted()) {
				idxIt->sort_id = i++;
				NSUpdateSortedContext sortCtx(*this, idxIt->sort_id);
				idxIt->MakeSortOrders(sortCtx);
				for (auto &idx_s : indexes_) idx_s->UpdateSortedIds(sortCtx);
			}
		}
	}

	if (ctx.prepareIndexes()) {
		NSCommitContext ctx1(*this, CommitContext::PrepareForSelect, ctx.prepareIndexes());
		for (auto idxNo : *ctx.prepareIndexes())
			if (!preparedIndexes_.contains(idxNo)) {
				assert(idxNo < indexes_.size());
				indexes_[idxNo]->Commit(ctx1);
				preparedIndexes_.push_back(idxNo);
			}
	}
	commitPhasesState_ |= ctx.phases() & (CommitContext::MakeIdsets | CommitContext::MakeSortOrders);
}

void Namespace::Commit() { commit(NSCommitContext(*this, CommitContext::FlushToStorage)); }

bool Namespace::isContainsFullText(const QueryEntries &entries) {
	for (auto &entrie : entries) {
		if (indexes_[entrie.idxNo]->type == IndexFullText || indexes_[entrie.idxNo]->type == IndexNewFullText) {
			return true;
		}
	}
	return false;
}

#define TIMEPOINT(n)                                  \
	std::chrono::high_resolution_clock::time_point n; \
	if (enableTiming) n = high_resolution_clock::now()

// Select ids of items, matches query
// Ids will be appened to result vector as:
// 1-st element: count of items
// 2-st, and next elements items
void Namespace::Select(const Query &q, const IdSet *preResult, QueryResults &result, const JoinedSelectors &joinedSelectors) {
	Index *sortIndex = nullptr;
	bool enableTiming = q.debugLevel >= LogTrace;

	if (q.start < 0) throw Error(errLogic, "Can't handle query with 'offset' < 0");
	if (q.count < 0) throw Error(errLogic, "Can't handle query with 'limit' < 0");

	TIMEPOINT(tmStart);

	QueryEntries whereEntries = lookupQueryIndexes(q.entries);
	substituteCompositeIndexes(whereEntries);
	bool is_ft = isContainsFullText(whereEntries);
	auto &sortBy = (is_ft || q.sortBy.length()) ? q.sortBy : getOptimalSortOrder(whereEntries);

	// Check if commit needed
	if (/*!preResult &&*/ (whereEntries.size() || sortBy.length())) {
		FieldsSet prepareIndexes;
		for (auto &entry : whereEntries) prepareIndexes.push_back(entry.idxNo);
		commit(NSCommitContext(*this, CommitContext::MakeIdsets | (sortBy.length() ? CommitContext::MakeSortOrders : 0), &prepareIndexes));
	}

	if (sortBy.length()) {
		if (is_ft) {
			logPrintf(LogError, "Full text search on '%s.%s' with foreing sort on '%s' is not supported now. sort will be ignored\n",
					  name.c_str(), indexes_[whereEntries[0].idxNo]->name.c_str(), q.sortBy.c_str());
		} else {
			// Query is sorted. Search for sort index
			sortIndex = indexes_[getIndexByName(sortBy)].get();
			if (!sortIndex->sort_id) throw Error(errQueryExec, "Index for '%s' is unordered. Can't sort results", sortBy.c_str());
		}
	}

	RawQueryResult qres;

	// Add preresults with common conditions of join Queres
	if (preResult) {
		SelectKeyResult res;
		res.push_back(SingleSelectKeyResult(preResult));
		qres.push_back(QueryIterator(res, OpAnd, false, "-preresult"));
	}

	TIMEPOINT(tm1);

	selectWhere(whereEntries, qres, sortIndex ? sortIndex->sort_id : 0, is_ft);

	TIMEPOINT(tm2);

	bool haveComparators = false, haveScan = false;
	bool reverse = q.sortDirDesc && sortIndex && !is_ft;

	if (!is_ft && (!qres.size() || qres[0].comparators_.size() || qres[0].op == OpNot)) {
		// special case - no condition or first result have comparator or not
		SelectKeyResult res;
		res.push_back(SingleSelectKeyResult(0, (IdType)(sortIndex ? sortIndex->sort_orders.size() : items_.size())));
		qres.insert(qres.begin(), QueryIterator(res, OpAnd, false, "-scan", true));
		haveScan = sortIndex ? false : true;
	}

	// Get maximum iterations count, for right calculation comparators costs
	int iters = INT_MAX;
	for (auto r = qres.begin(); r != qres.end(); ++r) {
		int cur = r->GetMaxIterations();
		if (!r->comparators_.size() && cur && cur < iters) iters = cur;
	}

	// Sort by cost
	sort(qres.begin(), qres.end(), [iters](const QueryIterator &i1, const QueryIterator &i2) { return i1.Cost(iters) < i2.Cost(iters); });

	// Check NOT or comparator must not be 1st
	for (auto r = qres.begin(); r != qres.end(); ++r) {
		if (r->op != OpNot && !r->comparators_.size()) {
			if (r != qres.begin()) {
				// if NOT found in 1-st position swap it with 1-st non NOT
				auto tmp = *r;
				*r = *qres.begin();
				*qres.begin() = tmp;
			}
			break;
		}
	}

	// Rewing all results iterators
	for (auto &r : qres) {
		r.Start(reverse);
		if (r.comparators_.size()) haveComparators = true;
	}

	// Let iterators choose most effecive algorith
	for (auto r = qres.begin() + 1; r != qres.end(); r++) r->SetExpectMaxIterations(iters);

	TIMEPOINT(tm3);

	if (reverse && haveComparators && haveScan) selectLoop<true, true, true>(q, qres, result, sortIndex, joinedSelectors);
	if (!reverse && haveComparators && haveScan) selectLoop<false, true, true>(q, qres, result, sortIndex, joinedSelectors);
	if (reverse && !haveComparators && haveScan) selectLoop<true, false, true>(q, qres, result, sortIndex, joinedSelectors);
	if (!reverse && !haveComparators && haveScan) selectLoop<false, false, true>(q, qres, result, sortIndex, joinedSelectors);
	if (reverse && haveComparators && !haveScan) selectLoop<true, true, false>(q, qres, result, sortIndex, joinedSelectors);
	if (!reverse && haveComparators && !haveScan) selectLoop<false, true, false>(q, qres, result, sortIndex, joinedSelectors);
	if (reverse && !haveComparators && !haveScan) selectLoop<true, false, false>(q, qres, result, sortIndex, joinedSelectors);
	if (!reverse && !haveComparators && !haveScan) selectLoop<false, false, false>(q, qres, result, sortIndex, joinedSelectors);

	TIMEPOINT(tm4);

	if (q.debugLevel >= LogInfo) {
		q.Dump();
		if (q.debugLevel >= LogTrace) {
			for (auto &r : qres)
				logPrintf(LogInfo, "%s: %d idsets, %d comparators, cost %g, matched %d", r.name.c_str(), r.size(), r.comparators_.size(),
						  r.Cost(iters), r.GetMatchedCount());
			logPrintf(LogInfo, "Got %d items in %d µs [prepare %d µs, select %d µs, postprocess %d µs loop %d µs]", (int)result.size(),
					  duration_cast<microseconds>(tm4 - tmStart).count(), duration_cast<microseconds>(tm1 - tmStart).count(),
					  duration_cast<microseconds>(tm2 - tm1).count(), duration_cast<microseconds>(tm3 - tm2).count(),
					  duration_cast<microseconds>(tm4 - tm3).count());
			result.Dump();
		}
	}
	result.tagsMatcher_ = tagsMatcher_.get();
	result.type_ = payloadType_.get();
}

template <bool reverse, bool haveComparators, bool haveScan>
void Namespace::selectLoop(const Query &q, RawQueryResult &qres, QueryResults &result, Index *sortIndex,
						   const JoinedSelectors &joinedSelectors) {
	int start = q.start;
	int count = q.count ? q.count : INT_MAX;

	bool finish = false;

	// do not calc total by loop, if we have only 1 condition with 1 idset
	bool calcTotal = q.calcTotal && (qres.size() > 1 || haveComparators || qres[0].size() > 1);

	bool haveInnerJoin = false;
	for (auto &joinedSelector : joinedSelectors) haveInnerJoin |= (joinedSelector.type == InnerJoin || joinedSelector.type == OrInnerJoin);

	// TODO: nested conditions support. Like (A  OR B OR C) AND (X OR Z)
	auto &first = *qres.begin();
	IdType val = first.Val();
	while (first.Next(val) && !finish) {
		val = first.Val();
		IdType realVal = val;

		if (haveScan && items_[realVal].IsFree()) continue;
		if (haveComparators && sortIndex != nullptr) realVal = sortIndex->sort_orders[val];

		bool found = true;
		for (auto cur = qres.begin() + 1; cur != qres.end(); cur++) {
			if (!haveComparators || !cur->TryCompare(&items_[realVal], realVal)) {
				while (((reverse && cur->Val() > val) || (!reverse && cur->Val() < val)) && cur->Next(val)) {
				};

				if (cur->End()) {
					finish = true;
					found = false;
				} else if ((reverse && cur->Val() < val) || (!reverse && cur->Val() > val)) {
					found = false;
				}
			}
			bool isNot = cur->op == OpNot;
			if ((isNot && found) || (!isNot && !found)) {
				found = false;
				for (; cur != qres.end(); cur++) {
					if (cur->comparators_.size() || cur->op == OpNot || cur->End()) continue;
					if (reverse && cur->Val() < val) val = cur->Val() + 1;
					if (!reverse && cur->Val() > val) val = cur->Val() - 1;
				}
				break;
			} else if (isNot && !found) {
				found = true;
				finish = false;
			}
		}
		if (found && joinedSelectors.size()) {
			bool match = !start && count;
			if (!haveComparators && sortIndex) realVal = sortIndex->sort_orders[val];

			// inner join process
			if (haveInnerJoin) {
				for (auto &joinedSelector : joinedSelectors) {
					if (joinedSelector.type == InnerJoin) found &= joinedSelector.func(realVal, match);
					if (joinedSelector.type == OrInnerJoin) found |= joinedSelector.func(realVal, match);
				}
			}

			// left join process
			if (match && found)
				for (auto &joinedSelector : joinedSelectors)
					if (joinedSelector.type == LeftJoin) joinedSelector.func(realVal, match);
		}

		if (found) {
			// Check distinct condition:
			// Exclude last sets of id from each query result, so duplicated keys will be removed
			for (auto &r : qres)
				if (r.distinct) r.ExcludeLastSet();

			if (start)
				--start;
			else if (count) {
				if (!haveComparators && sortIndex) realVal = sortIndex->sort_orders[val];
				--count;

				result.push_back({realVal, items_[realVal].GetVersion(), items_[realVal]});
				if (!count && !calcTotal) break;
			}
			if (calcTotal) result.totalCount++;
		}
	}

	// Get total count for simple query with 1 condition and 1 idset
	if (q.calcTotal && !calcTotal) result.totalCount = qres[0].GetMaxIterations();
}

int Namespace::getCompositeIndex(const FieldsSet &fields) {
	for (size_t f = payloadType_->NumFields(); f < indexes_.size(); f++)
		if (indexes_[f]->fields_.contains(fields)) return f;
	return -1;
}

bool Namespace::mergeQueryEntries(QueryEntry *lhs, QueryEntry *rhs) {
	if ((lhs->condition == CondEq || lhs->condition == CondSet) && (rhs->condition == CondEq || rhs->condition == CondSet)) {
		// intersect 2 queryenries on same index
		std::sort(lhs->values.begin(), lhs->values.end());
		lhs->values.erase(std::unique(lhs->values.begin(), lhs->values.end()), lhs->values.end());

		std::sort(rhs->values.begin(), rhs->values.end());
		rhs->values.erase(std::unique(rhs->values.begin(), rhs->values.end()), rhs->values.end());

		auto end =
			std::set_intersection(lhs->values.begin(), lhs->values.end(), rhs->values.begin(), rhs->values.end(), lhs->values.begin());
		lhs->values.resize(end - lhs->values.begin());
		lhs->condition = (lhs->values.size() == 1) ? CondEq : CondSet;
		lhs->distinct |= rhs->distinct;
		return true;
	} else if (rhs->condition == CondAny) {
		lhs->distinct |= rhs->distinct;
		return true;
	} else if (lhs->condition == CondAny) {
		rhs->distinct |= lhs->distinct;
		*lhs = std::move(*rhs);

		return true;
	}

	return false;
}

const string &Namespace::getOptimalSortOrder(const QueryEntries &entries) {
	Index *maxIdx = nullptr;
	static string no = "";
	for (auto c = entries.begin(); c != entries.end(); c++) {
		if ((c->condition == CondGe || c->condition == CondGt || c->condition == CondLe || c->condition == CondLt ||
			 c->condition == CondRange) &&
			!c->distinct && indexes_[c->idxNo]->sort_id) {
			if (!maxIdx || indexes_[c->idxNo]->Size() > maxIdx->Size()) {
				maxIdx = indexes_[c->idxNo].get();
			}
		}
	}

	return maxIdx ? maxIdx->name : no;
}

QueryEntries Namespace::lookupQueryIndexes(const QueryEntries &entries) {
	int iidx[maxIndexes];
	for (auto &i : iidx) i = -1;

	QueryEntries ret;
	for (auto c = entries.begin(); c != entries.end(); c++) {
		QueryEntry cur = *c;
		if (cur.idxNo < 0) cur.idxNo = getIndexByName(cur.index);
		for (auto &key : cur.values) key.convert(indexes_[cur.idxNo]->KeyType());

		// try merge entries with AND opetator
		auto nextc = c;
		nextc++;
		if (cur.op == OpAnd && (nextc == entries.end() || nextc->op == OpAnd)) {
			if (iidx[cur.idxNo] >= 0) {
				if (mergeQueryEntries(&ret[iidx[cur.idxNo]], &cur)) continue;

			} else {
				iidx[cur.idxNo] = ret.size();
			}
		}
		ret.push_back(cur);
	};
	return ret;
}

void Namespace::substituteCompositeIndexes(QueryEntries &entries) {
	FieldsSet fields;
	for (auto cur = entries.begin(), first = entries.begin(); cur != entries.end(); cur++) {
		if (cur->op != OpAnd || (cur->condition != CondEq)) {
			// If query already rewritten, then copy current unmatached part
			first = cur + 1;
			fields.clear();
			continue;
		}
		fields.push_back(cur->idxNo);
		int found = getCompositeIndex(fields);
		if (found >= 0) {
			// composite idx found: replace conditions
			PayloadData d(payloadType_->TotalSize());
			Payload pl(*payloadType_, &d);
			for (auto e = first; e != cur + 1; e++) {
				if (indexes_[found]->fields_.contains(e->idxNo)) {
					KeyRefs kr;
					kr.push_back(KeyRef(e->values[0]));
					pl.Set(e->idxNo, kr);
				} else
					*first++ = *e;
			}
			QueryEntry ce(OpAnd, CondEq, indexes_[found]->name, found);
			ce.values.push_back(KeyValue(d));
			*first++ = ce;
			cur = entries.erase(first, cur + 1);
			first = cur--;
			fields.clear();
		}
	}
}

void Namespace::selectWhere(const QueryEntries &entries, RawQueryResult &result, unsigned sortId, bool is_ft) {
	for (auto &qe : entries) {
		bool is_ft_current = (indexes_[qe.idxNo]->type == IndexFullText || indexes_[qe.idxNo]->type == IndexNewFullText);
		Index::ResultType type = Index::Optimal;
		if (is_ft && qe.distinct) throw Error(errQueryExec, "distinct and full text - can't do it");
		if (is_ft)
			type = Index::ForceComparator;
		else if (qe.distinct)
			type = Index::ForceIdset;

		for (auto res : indexes_[qe.idxNo]->SelectKey(qe.values, qe.condition, sortId, type)) {
			switch (qe.op) {
				case OpOr:
					if (!result.size()) throw Error(errQueryExec, "OR operator in first condition");
					result.back().Append(res);
					result.back().distinct |= qe.distinct;
					result.back().name += " OR " + qe.index;
					break;
				case OpNot:
				case OpAnd: {
					bool forced_first = is_ft_current;
					result.push_back(QueryIterator(res, qe.op, qe.distinct, qe.index, forced_first));
					break;
				}
				default:
					throw Error(errQueryExec, "Unknown operator (code %d) in condition", qe.op);
			}
			result.back().Bind(payloadType_.get(), qe.idxNo);
			if (is_ft_current) {
				result.back().SetUnsorted();
			}
		}
	}
}

bool Namespace::loadIndexesFromStorage() {
	leveldb::ReadOptions opts;
	string def, plds;

	auto status = db_->Get(opts, "indexes", &def);
	if (status.ok() && def.size()) {
		Serializer ser(def.data(), def.size());

		plds = ser.GetString();

		if (indexes_.size() < 2) {
			int cnt = ser.GetInt();
			while (cnt--) {
				string name = ser.GetString();
				string jsonPath = ser.GetString();
				IndexType type = (IndexType)ser.GetInt();
				IndexOpts opts;
				opts.IsArray = ser.GetInt();
				opts.IsPK = ser.GetInt();
				AddIndex(name, jsonPath, type, &opts);
			}
		}
	}

	if (plds != "" && plds != payloadType_->ToString()) {
		logPrintf(LogError, "Strored index structure mismatch for '%s':\n***** NEW *****:\n%s\n***** STORED *****:\n%s", name.c_str(),
				  plds.c_str(), payloadType_->ToString().c_str());
		return false;
	} else
		logPrintf(LogInfo, "Strored index structure for '%s'\n%s", name.c_str(), payloadType_->ToString().c_str());

	return true;
}

void Namespace::saveIndexesToStorage() {
	leveldb::WriteOptions opts;
	WrSerializer ser;

	ser.PutString(payloadType_->ToString());
	ser.PutInt(indexes_.size() - 1);
	for (int f = 1; f < (int)indexes_.size(); f++) {
		ser.PutString(indexes_[f]->name);

		ser.PutString(f < payloadType_->NumFields() ? payloadType_->Field(f).JsonPath() : "");
		ser.PutInt(indexes_[f]->type);
		ser.PutInt(f < payloadType_->NumFields() ? payloadType_->Field(f).IsArray() : 0);
		ser.PutInt(f < payloadType_->NumFields() ? payloadType_->Field(f).IsPK() : 0);
	}

	db_->Put(opts, "indexes", leveldb::Slice((const char *)ser.Buf(), ser.Len()));
}

void Namespace::EnableStorage(const string &path, StorageOpts opts) {
	leveldb::Options options;
	options.create_if_missing = opts.createIfMissing;
	options.max_open_files = 50;
	leveldb::DB *db;

	if (!path.length()) throw Error(errParams, "Storage path is empty - can't enable '%s'", path.c_str());

	string dbpath = path + ((path[path.length() - 1] != '/') ? "/" : "");

	if (opts.createIfMissing) MkDirAll(dbpath);
	dbpath += name;

	if (db_) throw Error(errLogic, "Storage already enabled for namespace '%s' oh path '%s'", name.c_str(), path.c_str());

	while (!db_) {
		leveldb::Status status = leveldb::DB::Open(options, dbpath.c_str(), &db);
		if (!status.ok()) {
			if (opts.dropIfCorrupted) {
				opts.dropIfCorrupted = false;
				leveldb::DestroyDB(dbpath.c_str(), options);
				options.create_if_missing = true;
			} else {
				throw Error(errLogic, "Can't enable storage for namespace '%s' oh path '%s' - %s", name.c_str(), path.c_str(),
							status.ToString().c_str());
			}
		}

		db_ = shared_ptr<leveldb::DB>(db);

		if (!loadIndexesFromStorage()) {
			db_ = nullptr;
			if (opts.dropIfFieldsMismatch) {
				opts.dropIfFieldsMismatch = false;
				leveldb::DestroyDB(dbpath.c_str(), options);
				options.create_if_missing = true;
			} else {
				throw Error(errLogic,
							"Fields for namespace '%s' is different with existing storage '%s'. Set 'dropIfFieldsMismatch' flag for "
							"drop existing storage.",
							name.c_str(), path.c_str());
			}
		}
	}

	string tagsDef;
	leveldb::Status status = db->Get(leveldb::ReadOptions(), "tags", &tagsDef);
	if (status.ok() && tagsDef.size()) {
		Serializer ser(tagsDef.data(), tagsDef.size());
		tagsMatcher_->deserialize(ser);
	} else {
		tagsMatcher_->clear();
	}
	saveIndexesToStorage();
	loadFromStorage();
}

void Namespace::loadFromStorage() {
	leveldb::Options options;
	leveldb::ReadOptions opts;
	opts.fill_cache = false;
	size_t ldcount = 0;

	logPrintf(LogTrace, "Loading items to '%s' from cache", name.c_str());
	auto dbIter = unique_ptr<leveldb::Iterator>(db_->NewIterator(opts));
	ItemImpl item(*payloadType_, tagsMatcher_.get());
	for (dbIter->Seek("indx"); dbIter->Valid() && options.comparator->Compare(dbIter->key(), "indx\xFF") < 0; dbIter->Next()) {
		if (!dbIter->value().empty()) {
			leveldb::Slice dataSlice = dbIter->value();
			item.Deserialize(Slice(dataSlice.data(), dataSlice.size()));

			IdType id = items_.size();
			items_.emplace_back(PayloadData(item.RealSize()));
			upsert(&item, id, false);

			ldcount += dataSlice.size();
		}
	}
	logPrintf(LogTrace, "[%s] Done loading cache. %d items loaded, total size=%dM", name.c_str(), (int)items_.size(),
			  (int)ldcount / (1024 * 1024));
}

void Namespace::flushToStorage() {
	if (db_) {
		if (tagsMatcher_->isUpdated()) {
			WrSerializer ser;
			tagsMatcher_->serialize(ser);
			batch_.Put("tags", leveldb::Slice((const char *)ser.Buf(), ser.Len()));
		}

		auto status = db_->Write(leveldb::WriteOptions(), &batch_);
		if (!status.ok()) throw Error(errLogic, "Error write ns '%s' to storage:", name.c_str(), status.ToString().c_str());
		batch_.Clear();
		unflushedCount_ = 0;
	}
}

void Namespace::NewItem(Item **item) { *item = new ItemImpl(*payloadType_, tagsMatcher_.get()); }

void Namespace::GetItem(IdType id, Item **item) {
	auto ritem = unique_ptr<ItemImpl>(new ItemImpl(GetPayload(id), tagsMatcher_.get()));
	ritem->SetID(id, items_[id].GetVersion());
	*item = ritem.release();
}

// Get meta data from storage by key
string Namespace::GetMeta(const string &key) {
	if (!db_) throw Error(errLogic, "rq: Persistent db is not available for get meta '%s'' in namespace %s", key.c_str(), name.c_str());

	string data;
	leveldb::Status s = db_->Get(leveldb::ReadOptions(), string("meta") + key, &data);

	if (!s.ok()) throw Error(errLogic, "rq: Can't get meta '%s'' in namespace %s", key.c_str(), name.c_str());

	return data;
}
// Put meta data to storage by key
void Namespace::PutMeta(const string &key, const Slice &data) {
	if (!db_) throw Error(errLogic, "rq: Persistent db is not available for get meta '%s'' in namespace %s", key.c_str(), name.c_str());

	leveldb::Status s = db_->Put(leveldb::WriteOptions(), string("meta") + key, leveldb::Slice(data.data(), data.size()));

	if (!s.ok()) throw Error(errLogic, "rq: Can't get meta '%s'' in namespace %s", key.c_str(), name.c_str());
}

// Lock reads to snapshot
void Namespace::LockSnapshot() {
	if (!db_) return;
	if (snapshot_) db_->ReleaseSnapshot(snapshot_);
	snapshot_ = db_->GetSnapshot();
}

ConstPayload Namespace::GetPayload(IdType id) const {
	if (!items_.exists(id)) throw Error(errParams, "Payload for id='%d' not found in namespace '%s'", id, name.c_str());
	return ConstPayload(*payloadType_, &items_[id]);
}

Payload Namespace::GetPayload(IdType id) {
	if (!items_.exists(id)) throw Error(errParams, "Payload for id='%d' not found in namespace '%s'", id, name.c_str());
	return Payload(*payloadType_, &items_[id]);
}

int Namespace::getIndexByName(const string &index) {
	auto idxIt = indexesNames_.find(index);

	if (idxIt == indexesNames_.end()) throw Error(errParams, "Index '%s' not found in '%s'", index.c_str(), name.c_str());

	return idxIt->second;
}

int Namespace::getSortedIdxCount() const {
	int cnt = 0;
	for (auto &it : indexes_)
		if (it->IsSorted()) cnt++;
	return cnt;
}

}  // namespace reindexer