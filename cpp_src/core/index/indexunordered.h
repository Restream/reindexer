#pragma once

#include <functional>
#include <type_traits>
#include "core/idsetcache.h"
#include "core/index/indexstore.h"
#include "core/index/updatetracker.h"
#include "estl/atomic_unique_ptr.h"

namespace reindexer {

template <typename T>
using StoreIndexKeyType = typename std::conditional<
	std::is_same_v<typename T::key_type, PayloadValueWithHash>, PayloadValue,
	typename std::conditional<std::is_same_v<typename T::key_type, key_string_with_hash>, key_string, typename T::key_type>::type>::type;

template <typename T>
class IndexUnordered : public IndexStore<StoreIndexKeyType<T>> {
	using Base = IndexStore<StoreIndexKeyType<T>>;

public:
	using ref_type = typename std::conditional<
		std::is_same<typename T::key_type, PayloadValueWithHash>::value, PayloadValue,
		typename std::conditional<std::is_same_v<typename T::key_type, key_string>, std::string_view,
								  typename std::conditional<std::is_same_v<typename T::key_type, key_string_with_hash>, std::string_view,
															typename T::key_type>::type>::type>::type;
	using key_type = StoreIndexKeyType<T>;

	IndexUnordered(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields);
	IndexUnordered(const IndexUnordered &other);

	Variant Upsert(const Variant &key, IdType id, bool &chearCache) override;
	void Delete(const Variant &key, IdType id, StringsHolder &, bool &chearCache) override;
	SelectKeyResults SelectKey(const VariantArray &keys, CondType cond, SortType stype, Index::SelectOpts opts,
							   const BaseFunctionCtx::Ptr &ctx, const RdxContext &) override;
	void Commit() override;
	void UpdateSortedIds(const UpdateSortedContext &) override;
	std::unique_ptr<Index> Clone() const override { return std::unique_ptr<Index>{new IndexUnordered<T>(*this)}; }
	IndexMemStat GetMemStat(const RdxContext &) override;
	size_t Size() const noexcept override final { return idx_map.size(); }
	void SetSortedIdxCount(int sortedIdxCount) override;
	bool HoldsStrings() const noexcept override;
	void ClearCache() override { cache_.reset(); }
	void ClearCache(const std::bitset<kMaxIndexes> &s) override {
		if (cache_) cache_->ClearSorted(s);
	}
	void Dump(std::ostream &os, std::string_view step = "  ", std::string_view offset = "") const override { dump(os, step, offset); }
	void EnableUpdatesCountingMode(bool val) noexcept override { tracker_.enableCountingMode(val); }

	void AddDestroyTask(tsl::detail_sparse_hash::ThreadTaskQueue &) override;
	bool IsDestroyPartSupported() const noexcept override { return true; }

protected:
	bool tryIdsetCache(const VariantArray &keys, CondType condition, SortType sortId,
					   const std::function<bool(SelectKeyResult &, size_t &)> &selector, SelectKeyResult &res);
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

constexpr inline unsigned maxSelectivityPercentForIdset() noexcept { return 30u; }

std::unique_ptr<Index> IndexUnordered_New(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields);

}  // namespace reindexer
