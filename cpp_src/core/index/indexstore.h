#pragma once

#include "core/index/index.h"
#include "core/index/string_map.h"

namespace reindexer {

template <typename T>
class IndexStore : public Index {
public:
	using Index::Index;

	KeyRef Upsert(const KeyRef &key, IdType id) override;
	void Delete(const KeyRef &key, IdType id) override;
	void DumpKeys() override {}
	SelectKeyResults SelectKey(const KeyValues &keys, CondType condition, SortType stype, ResultType res_type,
							   BaseFunctionCtx::Ptr ctx) override;
	void Commit(const CommitContext &) override;
	void UpdateSortedIds(const UpdateSortedContext & /*ctx*/) override {}
	Index *Clone() override;
	IdSetRef Find(const KeyRef & /*key*/) override {
		throw Error(errLogic, "IndexStore::Find of '%s' is not implemented. Do not use '-' index as pk?", this->name_.c_str());
	}
	KeyValueType KeyType() override final {
		static T a;
		return KeyRef(a).Type();
	}

protected:
	unordered_str_map<int>::iterator find(const KeyRef &key);
	unordered_str_map<int> str_map;
	vector<T> idx_data;

	key_string tmpKeyVal_ = make_key_string();
};

Index *IndexStore_New(IndexType type, const string &_name, const IndexOpts &opts, const PayloadType payloadType, const FieldsSet &fields_);

}  // namespace reindexer
