#pragma once

#include <functional>
#include <map>
#include <type_traits>
#include <unordered_map>
#include "core/idsetcache.h"
#include "core/indexstore.h"

using std::map;
using std::unordered_map;

namespace reindexer {

template <typename T>
struct is_payload_unord_map_key : std::false_type {};
template <typename... Args>
struct is_payload_unord_map_key<unordered_map<PayloadData, Args...>> : std::true_type {};
template <typename T>
struct is_payload_map_key : std::false_type {};
template <typename... Args>
struct is_payload_map_key<map<PayloadData, Args...>> : std::true_type {};

struct equal_composite {
	explicit equal_composite() : fields_() {}
	equal_composite(const PayloadType *type_, const FieldsSet &fields) : type_(type_), fields_(fields) {}
	bool operator()(const PayloadData &lhs, const PayloadData &rhs) const {
		assert(type_);
		return ConstPayload(*type_, &lhs).IsEQ(&rhs, fields_);
	}
	const PayloadType *type_ = nullptr;
	const FieldsSet fields_;
};
struct hash_composite {
	explicit hash_composite() : fields_() {}
	hash_composite(const PayloadType *type_, const FieldsSet &fields) : type_(type_), fields_(fields) {}
	size_t operator()(const PayloadData &s) const {
		assert(type_);
		return ConstPayload(*type_, &s).GetHash(fields_);
	}
	const PayloadType *type_ = nullptr;
	const FieldsSet fields_;
};

struct less_composite {
	explicit less_composite() : fields_() {}
	less_composite(const PayloadType *type_, const FieldsSet &fields) : type_(type_), fields_(fields) {}
	bool operator()(const PayloadData &lhs, const PayloadData &rhs) const {
		assert(type_);
		return ConstPayload(*type_, &lhs).Less(&rhs, fields_);
	}
	const PayloadType *type_ = nullptr;
	const FieldsSet fields_;
};

template <typename T>
class IndexUnordered : public IndexStore<typename T::key_type> {
public:
	IndexUnordered(IndexType _type, const string &_name, const IndexOpts &opts = {0, 0})
		: IndexStore<typename T::key_type>(_type, _name, opts, FieldsSet()) {}

	template <typename U = T>
	IndexUnordered(IndexType _type, const string &_name, const IndexOpts &opts, const PayloadType &payloadType, const FieldsSet &fields,
				   typename std::enable_if<is_payload_unord_map_key<U>::value>::type * = 0)
		: IndexStore<typename T::key_type>(_type, _name, opts, fields),
		  idx_map(1000, hash_composite(&payloadType, fields), equal_composite(&payloadType, fields)) {}

	template <typename U = T>
	IndexUnordered(IndexType _type, const string &_name, const IndexOpts &opts, const PayloadType &payloadType, const FieldsSet &fields,
				   typename std::enable_if<is_payload_map_key<U>::value>::type * = 0)
		: IndexStore<typename T::key_type>(_type, _name, opts, fields), idx_map(less_composite(&payloadType, fields)) {}

	~IndexUnordered();

	KeyRef Upsert(const KeyRef &key, IdType id) override;
	void Delete(const KeyRef &key, IdType id) override;
	void DumpKeys() override;
	SelectKeyResults SelectKey(const KeyValues &keys, CondType condition, SortType stype, Index::ResultType res_type) override;
	void Commit(const CommitContext &ctx) override;
	void UpdateSortedIds(const UpdateSortedContext &) override;
	Index *Clone() override;
	size_t Size() const override { return idx_map.size(); }

	typename T::mapped_type &Find(const KeyRef &key) override {
		auto res = this->find(key);
		static typename T::mapped_type noEntry;
		return (res != idx_map.end()) ? res->second : noEntry;
	}

protected:
	void tryIdsetCache(const KeyValues &keys, CondType condition, SortType sortId, std::function<void(SelectKeyResult &)> selector,
					   SelectKeyResult &res);

	typename T::iterator find(const KeyRef &key);
	T idx_map;
	// Merged idsets cache
	shared_ptr<IdSetCache> cache_;

	// Empty ids
	Index::KeyEntry empty_ids_;
};

}  // namespace reindexer
