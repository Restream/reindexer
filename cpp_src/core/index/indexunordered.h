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
	IndexUnordered(IndexType _type, const string &_name, const IndexOpts &opts,
				   typename std::enable_if<!is_string_unord_map_key<U>::value && !is_string_map_key<U>::value>::type * = 0)
		: IndexStore<typename T::key_type>(_type, _name, opts) {}

	// Constructor specialization for payload_unordered_map
	template <typename U = T>
	IndexUnordered(IndexType _type, const string &_name, const IndexOpts &opts, const PayloadType payloadType, const FieldsSet &fields,
				   typename std::enable_if<is_payload_unord_map_key<U>::value>::type * = 0)
		: IndexStore<typename T::key_type>(_type, _name, opts, payloadType, fields),
		  idx_map(1000, hash_composite(payloadType, fields), equal_composite(payloadType, fields)) {}

	// Constructor specialization for payload_map
	template <typename U = T>
	IndexUnordered(IndexType _type, const string &_name, const IndexOpts &opts, const PayloadType payloadType, const FieldsSet &fields,
				   typename std::enable_if<is_payload_map_key<U>::value>::type * = 0)
		: IndexStore<typename T::key_type>(_type, _name, opts, payloadType, fields), idx_map(less_composite(payloadType, fields)) {}

	// Constructor specialization for unordered_str_map
	template <typename U = T>
	IndexUnordered(IndexType _type, const string &_name, const IndexOpts &opts,
				   typename std::enable_if<is_string_unord_map_key<U>::value>::type * = 0)
		: IndexStore<typename T::key_type>(_type, _name, opts),
		  idx_map(1000, hash_sptr(opts.GetCollateMode()), equal_sptr(opts.collateOpts_)) {}

	// Constructor specialization for str_map
	template <typename U = T>
	IndexUnordered(IndexType _type, const string &_name, const IndexOpts &opts,
				   typename std::enable_if<is_string_map_key<U>::value>::type * = 0)
		: IndexStore<typename T::key_type>(_type, _name, opts), idx_map(comparator_sptr(opts.collateOpts_)) {}

	KeyRef Upsert(const KeyRef &key, IdType id) override;
	void Delete(const KeyRef &key, IdType id) override;
	void DumpKeys() override;
	SelectKeyResults SelectKey(const KeyValues &keys, CondType condition, SortType stype, Index::ResultType res_type,
							   BaseFunctionCtx::Ptr ctx) override;
	bool Commit(const CommitContext &ctx) override;
	void UpdateSortedIds(const UpdateSortedContext &) override;
	Index *Clone() override;
	IndexMemStat GetMemStat() override;
	size_t Size() const override final { return idx_map.size(); }
	IdSetRef Find(const KeyRef &key) override final;

protected:
	void markUpdated(typename T::value_type *key);
	void tryIdsetCache(const KeyValues &keys, CondType condition, SortType sortId, std::function<void(SelectKeyResult &)> selector,
					   SelectKeyResult &res);

	template <typename U = T, typename std::enable_if<is_string_map_key<U>::value || is_string_unord_map_key<T>::value>::type * = nullptr>
	typename T::iterator find(const KeyRef &key);
	template <typename U = T, typename std::enable_if<!is_string_map_key<U>::value && !is_string_unord_map_key<T>::value>::type * = nullptr>
	typename T::iterator find(const KeyRef &key);

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

Index *IndexUnordered_New(IndexType type, const string &_name, const IndexOpts &opts, const PayloadType payloadType,
						  const FieldsSet &fields_);

}  // namespace reindexer
