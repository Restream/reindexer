#pragma once

#include "core/payload/fieldsset.h"
#include "sortingcontext.h"

namespace reindexer {

class NamespaceImpl;
struct SelectCtx;
class ItemRef;

class ItemComparator {
public:
	ItemComparator(const NamespaceImpl& ns, const SelectCtx& ctx, const joins::NamespaceResults* jr) noexcept
		: ns_(ns), ctx_(ctx), joinResults_(jr) {}
	ItemComparator(const ItemComparator&) = delete;
	ItemComparator(ItemComparator&&) = delete;
	ItemComparator& operator=(const ItemComparator&) = delete;
	ItemComparator& operator=(ItemComparator&&) = delete;

	[[nodiscard]] bool operator()(const ItemRef& lhs, const ItemRef& rhs) const;

	void BindForForcedSort();
	void BindForGeneralSort();

private:
	template <typename Inserter>
	void bindOne(const SortingContext::Entry& sortingCtx, Inserter insert);
	ComparationResult compareFields(IdType lId, IdType rId, size_t& firstDifferentFieldIdx) const;

	class BackInserter;
	class FrontInserter;
	struct CompareByField {
		Desc desc;
	};
	struct CompareByJoinedField {
		size_t joinedNs;
		Desc desc;
	};
	struct CompareByExpression {
		Desc desc;
	};
	struct Joined {
		const JoinedSelector* joinedSelector{nullptr};
		FieldsSet fields;
		h_vector<const CollateOpts*, 1> collateOpts;
	};

	const NamespaceImpl& ns_;
	const SelectCtx& ctx_;
	const joins::NamespaceResults* joinResults_;
	FieldsSet fields_;
	h_vector<SortingContext::RawDataParams> rawData_;
	Joined joined_;
	h_vector<const CollateOpts*, 1> collateOpts_;
	h_vector<std::variant<CompareByField, CompareByJoinedField, CompareByExpression>, 4> comparators_;
};

}  // namespace reindexer
