#pragma once

#include "indexunordered.h"

namespace reindexer {

template <typename T>
class IndexOrdered : public IndexUnordered<T> {
public:
	using ref_type = typename IndexUnordered<T>::ref_type;
	using key_type = typename IndexUnordered<T>::key_type;

	IndexOrdered(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields)
		: IndexUnordered<T>(idef, std::move(payloadType), fields) {}

	SelectKeyResults SelectKey(const VariantArray &keys, CondType condition, SortType stype, Index::SelectOpts opts,
							   const BaseFunctionCtx::Ptr &ctx, const RdxContext &) override;
	Variant Upsert(const Variant &key, IdType id, bool &clearCache) override;
	void MakeSortOrders(UpdateSortedContext &ctx) override;
	IndexIterator::Ptr CreateIterator() const override;
	std::unique_ptr<Index> Clone() const override { return std::unique_ptr<Index>{new IndexOrdered<T>(*this)}; }
	bool IsOrdered() const noexcept override { return true; }
};

std::unique_ptr<Index> IndexOrdered_New(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields);

}  // namespace reindexer
