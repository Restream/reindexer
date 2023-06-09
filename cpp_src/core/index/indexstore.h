#pragma once

#include "core/index/index.h"
#include "core/index/string_map.h"

namespace reindexer {

template <typename T>
class IndexStore : public Index {
public:
	IndexStore(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields) : Index(idef, std::move(payloadType), fields) {
		static T a;
		keyType_ = selectKeyType_ = Variant(a).Type();
	}

	Variant Upsert(const Variant &key, IdType id, bool &clearCache) override;
	void Upsert(VariantArray &result, const VariantArray &keys, IdType id, bool &clearCache) override;
	void Delete(const Variant &key, IdType id, StringsHolder &, bool &clearCache) override;
	void Delete(const VariantArray &keys, IdType id, StringsHolder &, bool &clearCache) override;
	SelectKeyResults SelectKey(const VariantArray &keys, CondType condition, SortType stype, Index::SelectOpts res_type,
							   const BaseFunctionCtx::Ptr &ctx, const RdxContext &) override;
	void Commit() override;
	void UpdateSortedIds(const UpdateSortedContext & /*ctx*/) override {}
	std::unique_ptr<Index> Clone() const override { return std::unique_ptr<Index>{new IndexStore<T>(*this)}; }
	IndexMemStat GetMemStat(const RdxContext &) override;
	bool HoldsStrings() const noexcept override { return std::is_same_v<T, key_string> || std::is_same_v<T, key_string_with_hash>; }
	void Dump(std::ostream &os, std::string_view step = "  ", std::string_view offset = "") const override { dump(os, step, offset); }
	virtual void AddDestroyTask(tsl::detail_sparse_hash::ThreadTaskQueue &) override;
	virtual bool IsDestroyPartSupported() const noexcept override { return true; }

	template <typename, typename = void>
	struct HasAddTask : std::false_type {};
	template <typename H>
	struct HasAddTask<H, std::void_t<decltype(std::declval<H>().add_destroy_task(nullptr))>> : public std::true_type {};

protected:
	unordered_str_map<int> str_map;
	h_vector<T> idx_data;

	IndexMemStat memStat_;

private:
	template <typename S>
	void dump(S &os, std::string_view step, std::string_view offset) const;
};

template <>
IndexStore<Point>::IndexStore(const IndexDef &, PayloadType, const FieldsSet &);

std::unique_ptr<Index> IndexStore_New(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields_);

}  // namespace reindexer
