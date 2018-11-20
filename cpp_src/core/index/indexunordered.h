#pragma once

#include <functional>
#include <type_traits>
#include "core/idsetcache.h"
#include "core/index/indexstore.h"
#include "core/index/payload_map.h"
#include "core/index/string_map.h"
#include "core/index/updatetracker.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

template <typename T>
class IndexUnordered : public IndexStore<typename T::key_type> {
public:
	// Constructor specialization for any map or unordered_map
	template <typename U = T>
	IndexUnordered(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields,
				   typename std::enable_if<!is_string_unord_map_key<U>::value && !is_string_map_key<U>::value &&
										   !is_payload_unord_map_key<U>::value && !is_payload_map_key<U>::value>::type * = 0)
		: IndexStore<typename T::key_type>(idef, payloadType, fields) {}

	// Constructor specialization for payload_unordered_map
	template <typename U = T>
	IndexUnordered(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields,
				   typename std::enable_if<is_payload_unord_map_key<U>::value>::type * = 0)
		: IndexStore<typename T::key_type>(idef, payloadType, fields),
		  idx_map(1000, hash_composite(payloadType, fields), equal_composite(payloadType, fields)) {}

	// Constructor specialization for payload_map
	template <typename U = T>
	IndexUnordered(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields,
				   typename std::enable_if<is_payload_map_key<U>::value>::type * = 0)
		: IndexStore<typename T::key_type>(idef, payloadType, fields), idx_map(less_composite(payloadType, fields)) {}

	// Constructor specialization for unordered_str_map
	template <typename U = T>
	IndexUnordered(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields,
				   typename std::enable_if<is_string_unord_map_key<U>::value>::type * = 0)
		: IndexStore<typename T::key_type>(idef, payloadType, fields),
		  idx_map(1000, hash_sptr(idef.opts_.GetCollateMode()), equal_sptr(idef.opts_.collateOpts_)) {}

	// Constructor specialization for str_map
	template <typename U = T>
	IndexUnordered(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields,
				   typename std::enable_if<is_string_map_key<U>::value>::type * = 0)
		: IndexStore<typename T::key_type>(idef, payloadType, fields), idx_map(comparator_sptr(idef.opts_.collateOpts_)) {}

	Variant Upsert(const Variant &key, IdType id) override;
	void Delete(const Variant &key, IdType id) override;
	void DumpKeys() override;
	SelectKeyResults SelectKey(const VariantArray &keys, CondType condition, SortType stype, Index::ResultType res_type,
							   BaseFunctionCtx::Ptr ctx) override;
	void Commit() override;
	void UpdateSortedIds(const UpdateSortedContext &) override;
	Index *Clone() override;
	IndexMemStat GetMemStat() override;
	size_t Size() const override final { return idx_map.size(); }
	IdSetRef Find(const Variant &key) override final;
	void SetSortedIdxCount(int sortedIdxCount) override {
		if (this->sortedIdxCount_ != sortedIdxCount) {
			this->sortedIdxCount_ = sortedIdxCount;
			for (auto &keyIt : idx_map) keyIt.second.Unsorted().ReserveForSorted(this->sortedIdxCount_);
		}
	}

protected:
	void markUpdated(typename T::value_type *key);
	void tryIdsetCache(const VariantArray &keys, CondType condition, SortType sortId, std::function<void(SelectKeyResult &)> selector,
					   SelectKeyResult &res);

	template <typename U = T, typename std::enable_if<is_string_map_key<U>::value || is_string_unord_map_key<T>::value>::type * = nullptr>
	typename T::iterator find(const Variant &key);
	template <typename U = T, typename std::enable_if<!is_string_map_key<U>::value && !is_string_unord_map_key<T>::value>::type * = nullptr>
	typename T::iterator find(const Variant &key);

	template <typename U = T, typename std::enable_if<is_string_map_key<U>::value || is_string_unord_map_key<T>::value>::type * = nullptr>
	void getMemStat(IndexMemStat &) const;
	template <typename U = T, typename std::enable_if<!is_string_map_key<U>::value && !is_string_unord_map_key<T>::value>::type * = nullptr>
	void getMemStat(IndexMemStat &) const;

	// Index map
	T idx_map;
	// Merged idsets cache
	shared_ptr<IdSetCache> cache_ = std::make_shared<IdSetCache>();
	// Empty ids
	Index::KeyEntry empty_ids_;
	// Tracker of updates
	UpdateTracker<T> tracker_;
};

Index *IndexUnordered_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields);

}  // namespace reindexer
