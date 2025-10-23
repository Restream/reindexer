#pragma once

#include "indexunordered.h"

namespace reindexer {

template <typename T>
class [[nodiscard]] IndexOrdered : public IndexUnordered<T> {
public:
	using ref_type = typename IndexUnordered<T>::ref_type;
	using key_type = typename IndexUnordered<T>::key_type;

	IndexOrdered(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg)
		: IndexUnordered<T>(idef, std::move(payloadType), std::move(fields), cacheCfg) {}

	SelectKeyResults SelectKey(const VariantArray& keys, CondType condition, SortType stype, const Index::SelectContext&,
							   const RdxContext&) override;
	Variant Upsert(const Variant& key, IdType id, bool& clearCache) override;
	void MakeSortOrders(IUpdateSortedContext& ctx) override;
	IndexIterator::Ptr CreateIterator() const override;
	std::unique_ptr<Index> Clone(size_t /*newCapacity*/) const override { return std::unique_ptr<Index>(new IndexOrdered<T>(*this)); }
	bool IsOrdered() const noexcept override { return true; }

protected:
	IndexOrdered(const IndexOrdered&) = default;
};

std::unique_ptr<Index> IndexOrdered_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
										const NamespaceCacheConfigData& cacheCfg);

}  // namespace reindexer
