#include "indexrtree.h"

#include "core/rdxcontext.h"
#include "linearsplitter.h"
#include "quadraticsplitter.h"

namespace reindexer {

template <typename KeyEntryT, template <typename, typename, typename, typename, size_t> class Splitter>
SelectKeyResults IndexRTree<KeyEntryT, Splitter>::SelectKey(const VariantArray &keys, CondType condition, SortType sortId,
															Index::SelectOpts opts, BaseFunctionCtx::Ptr funcCtx,
															const RdxContext &rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if (opts.forceComparator) {
		return IndexStore<typename Map::key_type>::SelectKey(keys, condition, sortId, opts, funcCtx, rdxCtx);
	}

	SelectKeyResult res;

	if (condition != CondDWithin) throw Error(errQueryExec, "Only CondDWithin available for RTree index");
	if (keys.size() != 2) throw Error(errQueryExec, "CondDWithin expects two arguments");
	Point point;
	double distance;
	if (keys[0].Type() == KeyValueTuple) {
		point = keys[0].As<Point>();
		distance = keys[1].As<double>();
	} else {
		point = keys[1].As<Point>();
		distance = keys[0].As<double>();
	}
	class Visitor : public Map::Visitor {
	public:
		Visitor(SortType sId, unsigned distinct, unsigned iCountInNs, SelectKeyResult &r)
			: sortId_{sId}, itemsCountInNs_{distinct ? 0u : iCountInNs}, res_{r} {}
		bool operator()(const typename Map::value_type &v) override {
			idsCount_ += v.second.Unsorted().size();
			res_.emplace_back(v.second, sortId_);
			return ScanWin();
		}
		bool ScanWin() const noexcept {
			return itemsCountInNs_ && res_.size() > 1u && (100u * idsCount_ / itemsCountInNs_ > maxSelectivityPercentForIdset());
		}

	private:
		SortType sortId_;
		unsigned itemsCountInNs_;
		SelectKeyResult &res_;
		size_t idsCount_ = 0;
	} visitor{sortId, opts.distinct, opts.itemsCountInNamespace, res};
	this->idx_map.DWithin(point, distance, visitor);
	if (visitor.ScanWin()) {
		// fallback to comparator, due to expensive idset
		return IndexStore<typename Map::key_type>::SelectKey(keys, condition, sortId, opts, funcCtx, rdxCtx);
	}
	return SelectKeyResults(std::move(res));
}

template <typename KeyEntryT, template <typename, typename, typename, typename, size_t> class Splitter>
void IndexRTree<KeyEntryT, Splitter>::Upsert(VariantArray &result, const VariantArray &keys, IdType id, bool needUpsertEmptyValue) {
	if (keys.empty()) {
		if (needUpsertEmptyValue) {
			Upsert(Variant{}, id);
		}
		return;
	}
	// reset cache
	if (this->cache_) this->cache_.reset();
	const Point point = static_cast<Point>(keys);
	typename Map::iterator keyIt = this->idx_map.find(point);
	if (keyIt == this->idx_map.end()) {
		keyIt = this->idx_map.insert({point, typename Map::mapped_type()}).first;
	} else {
		this->delMemStat(keyIt);
	}

	keyIt->second.Unsorted().Add(id, this->opts_.IsPK() ? IdSet::Ordered : IdSet::Auto, this->sortedIdxCount_);
	this->tracker_.markUpdated(this->idx_map, keyIt);

	this->addMemStat(keyIt);

	result = VariantArray{keyIt->first};
}

template <typename KeyEntryT, template <typename, typename, typename, typename, size_t> class Splitter>
void IndexRTree<KeyEntryT, Splitter>::Delete(const VariantArray &keys, IdType id) {
	if (keys.empty()) {
		return Delete(Variant{}, id);
	}
	if (this->cache_) this->cache_.reset();
	int delcnt = 0;
	const Point point = static_cast<Point>(keys);
	typename Map::iterator keyIt = this->idx_map.find(point);
	if (keyIt == this->idx_map.end()) return;

	this->delMemStat(keyIt);
	delcnt = keyIt->second.Unsorted().Erase(id);
	(void)delcnt;
	// TODO: we have to implement removal of composite indexes (doesn't work right now)
	assertf(this->Opts().IsSparse() || delcnt, "Delete unexists id from index '%s' id=%d,key=%s (%s)", this->name_, id,
			Variant(keys).template As<string>(this->payloadType_, this->fields_),
			Variant(keyIt->first).As<string>(this->payloadType_, this->fields_));

	if (keyIt->second.Unsorted().IsEmpty()) {
		this->tracker_.markDeleted(keyIt);
		this->idx_map.template erase<void>(keyIt);
	} else {
		this->addMemStat(keyIt);
		this->tracker_.markUpdated(this->idx_map, keyIt);
	}
}

Index *IndexRTree_New(const IndexDef &idef, const PayloadType &payloadType, const FieldsSet &fields) {
	if (idef.opts_.IsRTreeLinear()) {
		if (idef.opts_.IsPK() || idef.opts_.IsDense()) {
			return new IndexRTree<Index::KeyEntryPlain, LinearSplitter>(idef, payloadType, fields);
		} else {
			return new IndexRTree<Index::KeyEntry, LinearSplitter>(idef, payloadType, fields);
		}
	} else {
		if (idef.opts_.IsPK() || idef.opts_.IsDense()) {
			return new IndexRTree<Index::KeyEntryPlain, QuadraticSplitter>(idef, payloadType, fields);
		} else {
			return new IndexRTree<Index::KeyEntry, QuadraticSplitter>(idef, payloadType, fields);
		}
	}
}

template class IndexRTree<Index::KeyEntry, QuadraticSplitter>;
template class IndexRTree<Index::KeyEntryPlain, QuadraticSplitter>;
template class IndexRTree<Index::KeyEntry, LinearSplitter>;
template class IndexRTree<Index::KeyEntryPlain, LinearSplitter>;

}  // namespace reindexer
