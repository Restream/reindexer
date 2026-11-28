#pragma once

#include <span>
#include "core/namespace/namespace_metadata_view.h"
#include "core/payload/fieldsset.h"
#include "sortingcontext.h"

namespace reindexer {

struct SelectCtx;
class ItemRef;

class [[nodiscard]] ItemComparator {
public:
	// Borrows data from the same namespace; its lock must be held throughout sorting.
	struct [[nodiscard]] Context {
		NamespaceMetadataView metadata;
		std::span<const PayloadValue> items;
	};

	ItemComparator(Context data, const SelectCtx& ctx, const joins::NamespaceResults* jr) noexcept
		: data_{data}, payloadType_{data.metadata.GetPayloadType()}, ctx_(ctx), joinResults_(jr) {}
	ItemComparator(const ItemComparator&) = delete;
	ItemComparator(ItemComparator&&) = delete;
	ItemComparator& operator=(const ItemComparator&) = delete;
	ItemComparator& operator=(ItemComparator&&) = delete;

	bool operator()(const ItemRef& lhs, const ItemRef& rhs) const;

	void BindForForcedSort();
	void BindForGeneralSort();

private:
	template <typename Inserter>
	void bindOne(const SortingContext::Entry& sortingCtx, Inserter insert);
	ComparationResult compareFields(IdType lId, IdType rId, size_t& firstDifferentFieldIdx) const;
	const void* columnData(int fieldIdx) const noexcept { return data_.metadata.GetIndexColumnData(fieldIdx); }

	class BackInserter;
	class FrontInserter;
	struct [[nodiscard]] CompareByField {
		Desc desc;
	};
	struct [[nodiscard]] CompareByJoinedField {
		size_t joinedNs;
		Desc desc;
	};
	struct [[nodiscard]] CompareByExpression {
		Desc desc;
	};
	struct [[nodiscard]] Joined {
		const joins::ItemsProcessor* joinItemsProcessor{nullptr};
		const PayloadType* payloadType{nullptr};
		FieldsSet fields;
		h_vector<const CollateOpts*, 1> collateOpts;
	};

	Context data_;
	const PayloadType& payloadType_;
	const SelectCtx& ctx_;
	const joins::NamespaceResults* joinResults_;
	FieldsSet fields_;
	h_vector<SortingContext::RawDataParams> rawData_;
	Joined joined_;
	h_vector<const CollateOpts*, 1> collateOpts_;
	h_vector<std::variant<CompareByField, CompareByJoinedField, CompareByExpression>, 4> comparators_;
};

}  // namespace reindexer
