#pragma once

#include "core/index/index.h"
#include "core/index/string_map.h"

namespace reindexer {

template <typename T>
class IndexStore : public Index {
public:
	IndexStore(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields) : Index(idef, payloadType, fields) {
		static T a;
		keyType_ = Variant(a).Type();
		selectKeyType_ = Variant(a).Type();
	}

	Variant Upsert(const Variant &key, IdType id) override;
	void Delete(const Variant &key, IdType id) override;
	void DumpKeys() override {}
	SelectKeyResults SelectKey(const VariantArray &keys, CondType condition, SortType stype, ResultType res_type,
							   BaseFunctionCtx::Ptr ctx) override;
	void Commit() override;
	void UpdateSortedIds(const UpdateSortedContext & /*ctx*/) override {}
	Index *Clone() override;
	IndexMemStat GetMemStat() override;

	IdSetRef Find(const Variant & /*key*/) override {
		throw Error(errLogic, "IndexStore::Find of '%s' is not implemented. Do not use '-' index as pk!", this->name_.c_str());
	}

protected:
	unordered_str_map<int>::iterator find(const Variant &key);
	unordered_str_map<int> str_map;
	h_vector<T> idx_data;

	key_string tmpKeyVal_ = make_key_string();
};

Index *IndexStore_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields_);

}  // namespace reindexer
