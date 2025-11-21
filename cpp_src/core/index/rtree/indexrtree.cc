#include "indexrtree.h"

#include "core/rdxcontext.h"
#include "greenesplitter.h"
#include "linearsplitter.h"
#include "quadraticsplitter.h"
#include "rstarsplitter.h"

namespace reindexer {

template <typename KeyEntryT, template <typename, typename, typename, typename, size_t, size_t> class Splitter, size_t MaxEntries,
		  size_t MinEntries>
SelectKeyResults IndexRTree<KeyEntryT, Splitter, MaxEntries, MinEntries>::SelectKey(const VariantArray& keys, CondType condition,
																					SortType sortId, const Index::SelectContext& selectCtx,
																					const RdxContext& rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if (selectCtx.opts.forceComparator) {
		return IndexStore<typename Map::key_type>::SelectKey(keys, condition, sortId, selectCtx, rdxCtx);
	}

	SelectKeyResult res;

	if (condition != CondDWithin) {
		throw Error(errQueryExec, "Only DWithin condition is available for RTree index");
	}
	if (keys.size() != 2) {
		throw Error(errQueryExec, "DWithin condition expects two arguments");
	}
	Point point;
	double distance;
	if (keys[0].Type().Is<KeyValueType::Tuple>()) {
		point = keys[0].As<Point>();
		distance = keys[1].As<double>();
	} else {
		point = keys[1].As<Point>();
		distance = keys[0].As<double>();
	}
	class [[nodiscard]] Visitor : public Map::Visitor {
	public:
		Visitor(SortType sId, unsigned distinct, unsigned iCountInNs, SelectKeyResult& r)
			: sortId_{sId}, itemsCountInNs_{distinct ? 0u : iCountInNs}, res_{r} {}
		bool operator()(const typename Map::value_type& v) override {
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
		SelectKeyResult& res_;
		size_t idsCount_ = 0;
	} visitor{sortId, selectCtx.opts.distinct, selectCtx.opts.itemsCountInNamespace, res};
	this->idx_map.DWithin(point, distance, visitor);
	if (visitor.ScanWin()) {
		// fallback to comparator, due to expensive idset
		return IndexStore<typename Map::key_type>::SelectKey(keys, condition, sortId, selectCtx, rdxCtx);
	}
	return SelectKeyResults(std::move(res));
}

template <typename KeyEntryT, template <typename, typename, typename, typename, size_t, size_t> class Splitter, size_t MaxEntries,
		  size_t MinEntries>
void IndexRTree<KeyEntryT, Splitter, MaxEntries, MinEntries>::Upsert(VariantArray& result, const VariantArray& keys, IdType id,
																	 bool& clearCache) {
	if (keys.empty() || keys.IsNullValue()) {
		std::ignore = Upsert(Variant{}, id, clearCache);
		return;
	}
	if (keys.size() != 2) [[unlikely]] {
		[[maybe_unused]] Point p{keys};
	}
	const Point point{keys[0].IsNullValue() ? 0.0 : keys[0].As<double>(), keys[1].IsNullValue() ? 0.0 : keys[1].As<double>()};
	typename Map::iterator keyIt = this->idx_map.find(point);
	if (keyIt == this->idx_map.end()) {
		keyIt = this->idx_map.insert_without_test({point, typename Map::mapped_type()});
	} else {
		this->delMemStat(keyIt);
	}

	if (keyIt->second.Unsorted().Add(id, this->opts_.IsPK() ? IdSet::Ordered : IdSet::Auto, this->sortedIdxCount_)) {
		this->isBuilt_ = false;
		// reset cache
		this->cache_.ResetImpl();
		clearCache = true;
	}
	this->tracker_.markUpdated(this->idx_map, keyIt);

	this->addMemStat(keyIt);

	result = VariantArray(keyIt->first);
}

template <typename KeyEntryT, template <typename, typename, typename, typename, size_t, size_t> class Splitter, size_t MaxEntries,
		  size_t MinEntries>
void IndexRTree<KeyEntryT, Splitter, MaxEntries, MinEntries>::Delete(const VariantArray& keys, IdType id, MustExist mustExist,
																	 StringsHolder& strHolder, bool& clearCache) {
	if (keys.empty() || keys.IsNullValue()) {
		return Delete(Variant{}, id, mustExist, strHolder, clearCache);
	}
	int delcnt = 0;
	const Point point = static_cast<Point>(keys);
	typename Map::iterator keyIt = this->idx_map.find(point);
	if (keyIt == this->idx_map.end()) {
		return;
	}
	this->cache_.ResetImpl();
	clearCache = true;
	this->isBuilt_ = false;

	this->delMemStat(keyIt);
	delcnt = keyIt->second.Unsorted().Erase(id);
	(void)delcnt;
	// TODO: we have to implement removal of composite indexes (doesn't work right now)
	assertf(!mustExist || this->Opts().IsSparse() || delcnt, "Delete non-existent id from index '{}' id={}, key={} ({})", this->name_, id,
			Variant(keys).template As<std::string>(this->payloadType_, this->Fields()),
			Variant(keyIt->first).As<std::string>(this->payloadType_, this->Fields()));

	if (keyIt->second.Unsorted().IsEmpty()) {
		this->tracker_.markDeleted(keyIt);
		this->idx_map.template erase<void>(keyIt);
	} else {
		this->addMemStat(keyIt);
		this->tracker_.markUpdated(this->idx_map, keyIt);
	}
}

std::unique_ptr<Index> IndexRTree_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
									  const NamespaceCacheConfigData& cacheCfg) {
	switch (idef.Opts().RTreeType()) {
		case IndexOpts::Linear:
			if (idef.Opts().IsPK() || idef.Opts().IsDense()) {
				return std::make_unique<IndexRTree<Index::KeyEntryPlain, LinearSplitter, 32, 4>>(idef, std::move(payloadType),
																								 std::move(fields), cacheCfg);
			} else {
				return std::make_unique<IndexRTree<Index::KeyEntry, LinearSplitter, 32, 4>>(idef, std::move(payloadType), std::move(fields),
																							cacheCfg);
			}
		case IndexOpts::Quadratic:
			if (idef.Opts().IsPK() || idef.Opts().IsDense()) {
				return std::make_unique<IndexRTree<Index::KeyEntryPlain, QuadraticSplitter, 32, 4>>(idef, std::move(payloadType),
																									std::move(fields), cacheCfg);
			}
			return std::make_unique<IndexRTree<Index::KeyEntry, QuadraticSplitter, 32, 4>>(idef, std::move(payloadType), std::move(fields),
																						   cacheCfg);
		case IndexOpts::Greene:
			if (idef.Opts().IsPK() || idef.Opts().IsDense()) {
				return std::make_unique<IndexRTree<Index::KeyEntryPlain, GreeneSplitter, 16, 4>>(idef, std::move(payloadType),
																								 std::move(fields), cacheCfg);
			} else {
				return std::make_unique<IndexRTree<Index::KeyEntry, GreeneSplitter, 16, 4>>(idef, std::move(payloadType), std::move(fields),
																							cacheCfg);
			}
		case IndexOpts::RStar:
			if (idef.Opts().IsPK() || idef.Opts().IsDense()) {
				return std::make_unique<IndexRTree<Index::KeyEntryPlain, RStarSplitter, 32, 4>>(idef, std::move(payloadType),
																								std::move(fields), cacheCfg);
			} else {
				return std::make_unique<IndexRTree<Index::KeyEntry, RStarSplitter, 32, 4>>(idef, std::move(payloadType), std::move(fields),
																						   cacheCfg);
			}
	}

	assertrx(0);
	std::abort();
}

template class IndexRTree<Index::KeyEntry, LinearSplitter, 32, 4>;
template class IndexRTree<Index::KeyEntryPlain, LinearSplitter, 32, 4>;
template class IndexRTree<Index::KeyEntry, QuadraticSplitter, 32, 4>;
template class IndexRTree<Index::KeyEntryPlain, QuadraticSplitter, 32, 4>;
template class IndexRTree<Index::KeyEntry, GreeneSplitter, 16, 4>;
template class IndexRTree<Index::KeyEntryPlain, GreeneSplitter, 16, 4>;
template class IndexRTree<Index::KeyEntry, RStarSplitter, 32, 4>;
template class IndexRTree<Index::KeyEntryPlain, RStarSplitter, 32, 4>;

}  // namespace reindexer
