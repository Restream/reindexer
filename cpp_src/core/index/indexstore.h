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
	SelectKeyResults SelectKey(const VariantArray &keys, CondType condition, SortType stype, Index::SelectOpts res_type,
							   BaseFunctionCtx::Ptr ctx, const RdxContext &) override;
	void Commit() override;
	void UpdateSortedIds(const UpdateSortedContext & /*ctx*/) override {}
	Index *Clone() override;
	IndexMemStat GetMemStat() override;

protected:
	unordered_str_map<int> str_map;
	h_vector<T> idx_data;

	IndexMemStat memStat_;
};

Index *IndexStore_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields_);

}  // namespace reindexer
