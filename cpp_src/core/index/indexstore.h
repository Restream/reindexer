#pragma once

#include "core/index/index.h"

namespace reindexer {

template <typename T>
class [[nodiscard]] IndexStore : public Index {
public:
	IndexStore(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields)
		: Index(idef, std::move(payloadType), std::move(fields)) {
		static T a;
		keyType_ = selectKeyType_ = Variant(a).Type();
	}

	Variant Upsert(const Variant& key, IdType id, bool& clearCache) override;
	void Upsert(VariantArray& result, const VariantArray& keys, IdType id, bool& clearCache) override;
	void Delete(const Variant& key, IdType id, MustExist mustExist, StringsHolder&, bool& clearCache) override;
	void Delete(const VariantArray& keys, IdType id, MustExist mustExist, StringsHolder&, bool& clearCache) override;
	SelectKeyResults SelectKey(const VariantArray& keys, CondType condition, SortType stype, const Index::SelectContext&,
							   const RdxContext&) override;
	void Commit() override;
	void UpdateSortedIds(const IUpdateSortedContext& /*ctx*/) override { assertrx_dbg(!IsSupportSortedIdsBuild()); }
	bool IsSupportSortedIdsBuild() const noexcept override { return false; }
	std::unique_ptr<Index> Clone(size_t /*newCapacity*/) const override { return std::unique_ptr<Index>(new IndexStore<T>(*this)); }
	IndexMemStat GetMemStat(const RdxContext&) override;
	bool HoldsStrings() const noexcept override { return std::is_same_v<T, key_string> || std::is_same_v<T, key_string_with_hash>; }
	void Dump(std::ostream& os, std::string_view step = "  ", std::string_view offset = "") const override { dump(os, step, offset); }
	virtual void AddDestroyTask(tsl::detail_sparse_hash::ThreadTaskQueue&) override;
	virtual bool IsDestroyPartSupported() const noexcept override final { return true; }
	virtual bool IsUuid() const noexcept override final { return std::is_same_v<T, Uuid>; }
	virtual void ReconfigureCache(const NamespaceCacheConfigData&) override {}
	const void* ColumnData() const noexcept override final { return idx_data.size() ? idx_data.data() : nullptr; }

	bool IsColumnIndexDisabled() const noexcept { return opts_.IsArray() || opts_.IsSparse() || opts_.IsNoIndexColumn(); }

	template <typename, typename = void>
	struct [[nodiscard]] HasAddTask : std::false_type {};
	template <typename H>
	struct [[nodiscard]] HasAddTask<H, std::void_t<decltype(std::declval<H>().add_destroy_task(nullptr))>> : public std::true_type {};

protected:
	IndexStore(const IndexStore&) = default;

	unordered_str_map<int> str_map;

	using IdxDataT =
		std::conditional_t<std::is_same_v<T, bool>, unsigned char, std::conditional_t<std::is_same_v<T, key_string>, std::string_view, T>>;
	h_vector<IdxDataT> idx_data;

	IndexMemStat memStat_;

private:
	bool shouldHoldValueInStrMap() const noexcept;

	template <typename S>
	void dump(S& os, std::string_view step, std::string_view offset) const;
};

template <>
IndexStore<Point>::IndexStore(const IndexDef&, PayloadType&&, FieldsSet&&);

std::unique_ptr<Index> IndexStore_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&&);

}  // namespace reindexer
