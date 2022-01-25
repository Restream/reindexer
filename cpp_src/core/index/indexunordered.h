#pragma once

#include <functional>
#include <type_traits>
#include "core/idsetcache.h"
#include "core/index/indexstore.h"
#include "core/index/updatetracker.h"
#include "estl/atomic_unique_ptr.h"

namespace reindexer {

template <typename T>
class IndexUnordered : public IndexStore<typename T::key_type> {
	using Base = IndexStore<typename T::key_type>;

public:
	using ref_type =
		typename std::conditional<std::is_same<typename T::key_type, key_string>::value, std::string_view, typename T::key_type>::type;

	IndexUnordered(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields);
	IndexUnordered(const IndexUnordered &other);

	Variant Upsert(const Variant &key, IdType id, bool &chearCache) override;
	void Delete(const Variant &key, IdType id, StringsHolder &, bool &chearCache) override;
	SelectKeyResults SelectKey(const VariantArray &keys, CondType cond, SortType stype, Index::SelectOpts opts, BaseFunctionCtx::Ptr ctx,
							   const RdxContext &) override;
	void Commit() override;
	void UpdateSortedIds(const UpdateSortedContext &) override;
	std::unique_ptr<Index> Clone() override;
	IndexMemStat GetMemStat() override;
	size_t Size() const override final { return idx_map.size(); }
	void SetSortedIdxCount(int sortedIdxCount) override;
	bool HoldsStrings() const noexcept override;
	void ClearCache() override { cache_.reset(); }
	void ClearCache(const std::bitset<64> &s) override {
		if (cache_) cache_->ClearSorted(s);
	}

	void Dump(std::ostream &os, std::string_view step = "  ", std::string_view offset = "") const override;

protected:
	bool tryIdsetCache(const VariantArray &keys, CondType condition, SortType sortId, std::function<bool(SelectKeyResult &)> selector,
					   SelectKeyResult &res);
	void addMemStat(typename T::iterator it);
	void delMemStat(typename T::iterator it);

	// Index map
	T idx_map;
	// Merged idsets cache
	atomic_unique_ptr<IdSetCache> cache_;
	// Empty ids
	Index::KeyEntry empty_ids_;
	// Tracker of updates
	UpdateTracker<T> tracker_;

private:
	template <typename S>
	void dump(S &os, std::string_view step, std::string_view offset) const;
};

constexpr inline unsigned maxSelectivityPercentForIdset() noexcept { return 25u; }

std::unique_ptr<Index> IndexUnordered_New(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields);

}  // namespace reindexer
