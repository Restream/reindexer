
#pragma once

#include "indexunordered.h"
namespace reindexer {

template <typename T>
class IndexOrdered : public IndexUnordered<T> {
public:
	using ref_type = typename IndexUnordered<T>::ref_type;

	IndexOrdered(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields)
		: IndexUnordered<T>(idef, std::move(payloadType), fields) {}

	SelectKeyResults SelectKey(const VariantArray &keys, CondType condition, SortType stype, Index::SelectOpts opts,
							   BaseFunctionCtx::Ptr ctx, const RdxContext &) override;
	Variant Upsert(const Variant &key, IdType id) override;
	void MakeSortOrders(UpdateSortedContext &ctx) override;
	IndexIterator::Ptr CreateIterator() const override;
	std::unique_ptr<Index> Clone() override;
	bool IsOrdered() const override;
};

std::unique_ptr<Index> IndexOrdered_New(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields);

}  // namespace reindexer
