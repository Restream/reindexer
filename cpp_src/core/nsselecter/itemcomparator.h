#include "core/payload/fieldsset.h"
#include "sortingcontext.h"

namespace reindexer {

struct ItemComparatorState {
	FieldsSet fields_;
	h_vector<const CollateOpts *, 1> collateOpts_;
	vector<pair<size_t, bool>> byExpr_;
	vector<pair<size_t, bool>> byIndex_;
};

class NamespaceImpl;
struct SelectCtx;
class ItemRef;

class ItemComparator {
public:
	ItemComparator(const NamespaceImpl &ns, const SelectCtx &ctx, ItemComparatorState &state)
		: ns_(ns), ctx_(ctx), fields_(state.fields_), collateOpts_(state.collateOpts_), byExpr_(state.byExpr_), byIndex_(state.byIndex_) {}

	bool operator()(const ItemRef &lhs, const ItemRef &rhs) const;

	void BindForForcedSort();
	void BindForGeneralSort();

private:
	template <typename Inserter>
	void bindOne(size_t index, const SortingContext::Entry &sortingCtx, Inserter insert, bool multiSort);

	class BackInserter;
	class FrontInserter;

	const NamespaceImpl &ns_;
	const SelectCtx &ctx_;
	FieldsSet &fields_;
	h_vector<const CollateOpts *, 1> &collateOpts_;
	vector<pair<size_t, bool>> &byExpr_;
	vector<pair<size_t, bool>> &byIndex_;
};

}  // namespace reindexer
