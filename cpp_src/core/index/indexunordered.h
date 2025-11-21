#pragma once

#include <functional>
#include <type_traits>
#include "core/idsetcache.h"
#include "core/index/indexstore.h"
#include "core/index/updatetracker.h"

namespace reindexer {

template <typename Map>
using StoreIndexKeyType = std::conditional_t<
	std::is_same_v<typename Map::key_type, PayloadValueWithHash>, PayloadValue,
	std::conditional_t<std::is_same_v<typename Map::key_type, key_string_with_hash>, key_string, typename Map::key_type>>;

template <typename Map>
class [[nodiscard]] IndexUnordered : public IndexStore<StoreIndexKeyType<Map>> {
	using Base = IndexStore<StoreIndexKeyType<Map>>;

public:
	using ref_type = std::conditional_t<std::is_same<typename Map::key_type, PayloadValueWithHash>::value, const PayloadValue&,
										std::conditional_t<std::is_same_v<typename Map::key_type, key_string>, std::string_view,
														   std::conditional_t<std::is_same_v<typename Map::key_type, key_string_with_hash>,
																			  std::string_view, typename Map::key_type>>>;
	using key_type = StoreIndexKeyType<Map>;

	IndexUnordered(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg);

	Variant Upsert(const Variant& key, IdType id, bool& clearCache) override;
	void Delete(const Variant& key, IdType id, MustExist mustExist, StringsHolder&, bool& clearCache) override;
	SelectKeyResults SelectKey(const VariantArray& keys, CondType cond, SortType stype, const Index::SelectContext&,
							   const RdxContext&) override;
	void Commit() override;
	void UpdateSortedIds(const IUpdateSortedContext&) override;
	bool IsSupportSortedIdsBuild() const noexcept override { return true; }
	std::unique_ptr<Index> Clone(size_t /*newCapacity*/) const override { return std::unique_ptr<Index>(new IndexUnordered<Map>(*this)); }
	IndexMemStat GetMemStat(const RdxContext&) override;
	size_t Size() const noexcept override final { return idx_map.size(); }
	void SetSortedIdxCount(int sortedIdxCount) override;
	IndexPerfStat GetIndexPerfStat() override;
	void ResetIndexPerfStat() override;
	bool HoldsStrings() const noexcept override;
	void DestroyCache() override { cache_.ResetImpl(); }
	void ClearCache() override { cache_.Clear(); }
	void Dump(std::ostream& os, std::string_view step = "  ", std::string_view offset = "") const override { dump(os, step, offset); }
	void EnableUpdatesCountingMode(bool val) noexcept override { tracker_.enableCountingMode(val); }

	void AddDestroyTask(tsl::detail_sparse_hash::ThreadTaskQueue&) override;
	void ReconfigureCache(const NamespaceCacheConfigData& cacheCfg) override;

protected:
	IndexUnordered(const IndexUnordered& other);

	bool tryIdsetCache(const VariantArray& keys, CondType condition, SortType sortId,
					   const std::function<bool(SelectKeyResult&, size_t&)>& selector, SelectKeyResult& res);
	void addMemStat(typename Map::iterator it);
	void delMemStat(typename Map::iterator it);

	// Index map
	Map idx_map;
	// Merged idsets cache
	IdSetCache cache_;
	size_t cacheMaxSize_;
	uint32_t hitsToCache_;
	// Empty ids
	Index::KeyEntry empty_ids_;
	// Tracker of updates
	UpdateTracker<Map> tracker_;

private:
	template <typename S>
	void dump(S& os, std::string_view step, std::string_view offset) const;
};

constexpr unsigned maxSelectivityPercentForIdset() noexcept { return 30u; }

std::unique_ptr<Index> IndexUnordered_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
										  const NamespaceCacheConfigData& cacheCfg);

}  // namespace reindexer
