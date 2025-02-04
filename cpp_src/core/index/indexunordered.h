#pragma once

#include <functional>
#include <type_traits>
#include "core/idsetcache.h"
#include "core/index/indexstore.h"
#include "core/index/updatetracker.h"

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

	IndexUnordered(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg);
	IndexUnordered(const IndexUnordered& other);

	Variant Upsert(const Variant& key, IdType id, bool& clearCache) override;
	void Delete(const Variant& key, IdType id, StringsHolder&, bool& clearCache) override;
	SelectKeyResults SelectKey(const VariantArray& keys, CondType cond, SortType stype, Index::SelectOpts opts,
							   const BaseFunctionCtx::Ptr& ctx, const RdxContext&) override;
	void Commit() override;
	void UpdateSortedIds(const UpdateSortedContext&) override;
	std::unique_ptr<Index> Clone() const override { return std::make_unique<IndexUnordered<T>>(*this); }
	IndexMemStat GetMemStat(const RdxContext&) override;
	size_t Size() const noexcept override final { return idx_map.size(); }
	void SetSortedIdxCount(int sortedIdxCount) override;
	IndexPerfStat GetIndexPerfStat() override;
	void ResetIndexPerfStat() override;
	bool HoldsStrings() const noexcept override;
	void DestroyCache() override { cache_.ResetImpl(); }
	void ClearCache() override { cache_.Clear(); }
	void ClearCache(const std::bitset<kMaxIndexes>& s) override { cache_.ClearSorted(s); }
	void Dump(std::ostream& os, std::string_view step = "  ", std::string_view offset = "") const override { dump(os, step, offset); }
	void EnableUpdatesCountingMode(bool val) noexcept override { tracker_.enableCountingMode(val); }

	void AddDestroyTask(tsl::detail_sparse_hash::ThreadTaskQueue&) override;
	void ReconfigureCache(const NamespaceCacheConfigData& cacheCfg) override;

protected:
	bool tryIdsetCache(const VariantArray& keys, CondType condition, SortType sortId,
					   const std::function<bool(SelectKeyResult&, size_t&)>& selector, SelectKeyResult& res);
	void addMemStat(typename T::iterator it);
	void delMemStat(typename T::iterator it);

	// Index map
	T idx_map;
	// Merged idsets cache
	IdSetCache cache_;
	size_t cacheMaxSize_;
	uint32_t hitsToCache_;
	// Empty ids
	Index::KeyEntry empty_ids_;
	// Tracker of updates
	UpdateTracker<T> tracker_;

private:
	template <typename S>
	void dump(S& os, std::string_view step, std::string_view offset) const;
};

constexpr unsigned maxSelectivityPercentForIdset() noexcept { return 30u; }

std::unique_ptr<Index> IndexUnordered_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
										  const NamespaceCacheConfigData& cacheCfg);

}  // namespace reindexer
