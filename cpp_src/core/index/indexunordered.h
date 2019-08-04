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
public:
	using ref_type =
		typename std::conditional<std::is_same<typename T::key_type, key_string>::value, string_view, typename T::key_type>::type;

	IndexUnordered(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields);
	IndexUnordered(const IndexUnordered &other);

	Variant Upsert(const Variant &key, IdType id) override;
	void Delete(const Variant &key, IdType id) override;
	SelectKeyResults SelectKey(const VariantArray &keys, CondType cond, SortType stype, Index::SelectOpts opts, BaseFunctionCtx::Ptr ctx,
							   const RdxContext &) override;
	void Commit() override;
	void UpdateSortedIds(const UpdateSortedContext &) override;
	Index *Clone() override;
	IndexMemStat GetMemStat() override;
	size_t Size() const override final { return idx_map.size(); }
	void SetSortedIdxCount(int sortedIdxCount) override;

protected:
	void tryIdsetCache(const VariantArray &keys, CondType condition, SortType sortId, std::function<void(SelectKeyResult &)> selector,
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
};

Index *IndexUnordered_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields);

}  // namespace reindexer
