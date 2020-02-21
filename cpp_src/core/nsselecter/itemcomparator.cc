#include "itemcomparator.h"
#include "core/namespace/namespaceimpl.h"
#include "nsselecter.h"

namespace reindexer {

bool ItemComparator::operator()(const ItemRef &lhs, const ItemRef &rhs) const {
	size_t firstDifferentExprIdx = 0;
	int exprCmpRes = 0;
	for (const auto &eR : ctx_.sortingContext.exprResults) {
		if (eR[lhs.SortExprResultsIdx()] != eR[rhs.SortExprResultsIdx()]) {
			exprCmpRes = (eR[lhs.SortExprResultsIdx()] > eR[rhs.SortExprResultsIdx()]) ? 1 : -1;
			break;
		}
		++firstDifferentExprIdx;
	}
	assert(exprCmpRes == 0 || firstDifferentExprIdx < byExpr_.size());

	size_t firstDifferentFieldIdx = 0;
	int fieldsCmpRes = 0;
	// do not perform comparation by indexes if not necessary
	if (exprCmpRes == 0 || byExpr_[firstDifferentExprIdx].first != 0) {
		fieldsCmpRes = ConstPayload(ns_.payloadType_, ns_.items_[lhs.Id()])
						   .Compare(ns_.items_[rhs.Id()], fields_, firstDifferentFieldIdx, collateOpts_);
		assertf(fieldsCmpRes == 0 || firstDifferentFieldIdx < byIndex_.size(), "%d < %d\n", int(firstDifferentFieldIdx),
				int(byIndex_.size()));
	}

	int cmpRes;
	bool desc;
	if (exprCmpRes != 0) {
		if (fieldsCmpRes == 0 || byExpr_[firstDifferentExprIdx].first < byIndex_[firstDifferentFieldIdx].first) {
			cmpRes = exprCmpRes;
			desc = byExpr_[firstDifferentExprIdx].second;
		} else {
			cmpRes = fieldsCmpRes;
			desc = byIndex_[firstDifferentFieldIdx].second;
		}
	} else if (fieldsCmpRes != 0) {
		cmpRes = fieldsCmpRes;
		desc = byIndex_[firstDifferentFieldIdx].second;
	} else {
		// If values are equal, then sort by row ID, to give consistent results
		cmpRes = (lhs.Id() > rhs.Id()) ? 1 : ((lhs.Id() < rhs.Id()) ? -1 : 0);
		desc = ctx_.sortingContext.entries[0].data->desc;
	}

	if (desc) {
		return (cmpRes > 0);
	} else {
		return (cmpRes < 0);
	}
}

class ItemComparator::BackInserter {
public:
	explicit BackInserter(ItemComparator &comparator) : comparator_(comparator) {}
	void expr(size_t i, bool desc) { comparator_.byExpr_.emplace_back(i, desc); }
	void fields(const TagsPath &tp) { comparator_.fields_.push_back(std::move(tp)); }
	void fields(int fieldIdx) { comparator_.fields_.push_back(fieldIdx); }
	void index(size_t i, bool desc) { comparator_.byIndex_.emplace_back(i, desc); }
	void collateOpts(const CollateOpts *opts) { comparator_.collateOpts_.emplace_back(opts); }

private:
	ItemComparator &comparator_;
};

class ItemComparator::FrontInserter {
public:
	FrontInserter(ItemComparator &comparator) : comparator_(comparator) {}
	void expr(size_t i, bool desc) { comparator_.byExpr_.emplace(comparator_.byExpr_.begin(), i, desc); }
	void fields(const TagsPath &tp) {
		FieldsSet tmp;
		tmp.push_back(tp);
		for (const auto &f : comparator_.fields_) tmp.push_back(f);
		std::swap(tmp, comparator_.fields_);
	}
	void fields(int fieldIdx) {
		FieldsSet tmp;
		tmp.push_back(fieldIdx);
		for (const auto &f : comparator_.fields_) tmp.push_back(f);
		std::swap(tmp, comparator_.fields_);
	}
	void index(size_t i, bool desc) { comparator_.byIndex_.emplace(comparator_.byIndex_.begin(), i, desc); }
	void collateOpts(const CollateOpts *opts) { comparator_.collateOpts_.emplace(comparator_.collateOpts_.begin(), opts); }

private:
	ItemComparator &comparator_;
};

template <typename Inserter>
void ItemComparator::bindOne(size_t index, const SortingContext::Entry &sortingCtx, Inserter insert, bool multiSort) {
	if (sortingCtx.expression != SortingContext::Entry::NoExpression) {
		insert.expr(index, sortingCtx.data->desc);
		return;
	}
	int fieldIdx = sortingCtx.data->index;
	if (fieldIdx == IndexValueType::SetByJsonPath || ns_.indexes_[fieldIdx]->Opts().IsSparse()) {
		TagsPath tagsPath;
		if (fieldIdx != IndexValueType::SetByJsonPath) {
			const FieldsSet &fs = ns_.indexes_[fieldIdx]->Fields();
			assert(fs.getTagsPathsLength() > 0);
			tagsPath = fs.getTagsPath(0);
		} else {
			tagsPath = ns_.tagsMatcher_.path2tag(sortingCtx.data->expression);
		}
		if (fields_.contains(tagsPath)) {
			throw Error(errQueryExec, "Can't sort by 2 equal indexes: %s", sortingCtx.data->expression);
		}
		insert.fields(tagsPath);
		insert.index(index, sortingCtx.data->desc);
	} else {
		if (ns_.indexes_[fieldIdx]->Opts().IsArray()) {
			throw Error(errQueryExec, "Sorting cannot be applied to array field.");
		}
		if (fieldIdx >= ns_.indexes_.firstCompositePos()) {
			if (multiSort) {
				throw Error(errQueryExec, "Multicolumn sorting cannot be applied to composite fields: %s", sortingCtx.data->expression);
			}
			fields_ = ns_.indexes_[fieldIdx]->Fields();
			assert(byIndex_.empty());
			byIndex_.resize(fields_.size(), {index, sortingCtx.data->desc});
		} else {
			if (fields_.contains(fieldIdx)) {
				throw Error(errQueryExec, "You cannot sort by 2 same indexes: %s", sortingCtx.data->expression);
			}
			insert.fields(fieldIdx);
			insert.index(index, sortingCtx.data->desc);
		}
	}
	insert.collateOpts(sortingCtx.opts);
}

void ItemComparator::BindForForcedSort() {
	const auto &entries = ctx_.sortingContext.entries;
	const auto &exprResults = ctx_.sortingContext.exprResults;
	assert(entries.size() >= exprResults.size());
	byExpr_.reserve(exprResults.size());
	byIndex_.reserve(entries.size() - exprResults.size());
	const bool multiSort = entries.size() > 1;
	for (size_t i = 1; i < entries.size(); ++i) {
		bindOne(i, entries[i], BackInserter{*this}, multiSort);
	}
	assert(byIndex_.size() == fields_.size());
}

void ItemComparator::BindForGeneralSort() {
	const auto &entries = ctx_.sortingContext.entries;
	const auto &exprResults = ctx_.sortingContext.exprResults;
	assert(entries.size() >= exprResults.size());
	const bool multiSort = entries.size() > 1;
	if (byExpr_.empty() && byIndex_.empty()) {
		byExpr_.reserve(exprResults.size());
		byIndex_.reserve(entries.size() - exprResults.size());
		for (size_t i = 0; i < entries.size(); ++i) {
			bindOne(i, entries[i], BackInserter{*this}, multiSort);
		}
	} else if (!entries.empty()) {
		bindOne(0, entries[0], FrontInserter{*this}, multiSort);
	}
	assert(byExpr_.size() == exprResults.size());
	assert(byIndex_.size() == fields_.size());
}

}  // namespace reindexer
