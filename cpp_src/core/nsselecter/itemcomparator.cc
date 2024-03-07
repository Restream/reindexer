#include "itemcomparator.h"
#include "core/namespace/namespaceimpl.h"
#include "core/queryresults/joinresults.h"
#include "nsselecter.h"

namespace {

constexpr static size_t kNotComputed{std::numeric_limits<size_t>::max()};

struct FieldsCompRes {
	size_t firstDifferentFieldIdx{kNotComputed};
	int fieldsCmpRes;
	[[nodiscard]] int GetResult(bool desc) noexcept {
		if (firstDifferentFieldIdx == 0) {
			return desc ? -fieldsCmpRes : fieldsCmpRes;
		} else {
			--firstDifferentFieldIdx;
			return 0;
		}
	}
};

}  // namespace

namespace reindexer {

bool ItemComparator::operator()(const ItemRef &lhs, const ItemRef &rhs) const {
	size_t expressionIndex{0};
	FieldsCompRes mainNsRes;
	std::vector<FieldsCompRes> joinedNsRes(joined_.size());
	for (const auto &comp : comparators_) {
		const int res = std::visit(
			overloaded{[&](CompareByExpression c) noexcept {
						   assertrx_throw(expressionIndex < ctx_.sortingContext.exprResults.size());
						   const auto &eR{ctx_.sortingContext.exprResults[expressionIndex++]};
						   const auto lR{eR[lhs.SortExprResultsIdx()]};
						   const auto rR{eR[rhs.SortExprResultsIdx()]};
						   if (lR == rR) {
							   return 0;
						   } else if (lR > rR) {
							   return c.desc ? -1 : 1;
						   } else {
							   return c.desc ? 1 : -1;
						   }
					   },
					   [&](CompareByJoinedField c) {
						   assertrx_throw(c.joinedNs < joinedNsRes.size());
						   auto &res = joinedNsRes[c.joinedNs];
						   if (res.firstDifferentFieldIdx == kNotComputed) {
							   assertrx_throw(joined_.size() > c.joinedNs);
							   const auto &jNs = joined_[c.joinedNs];
							   const auto &joinedSelector = *jNs.joinedSelector;
							   const joins::ItemIterator ljIt{joinResults_, lhs.Id()};
							   const joins::ItemIterator rjIt{joinResults_, rhs.Id()};
							   const auto ljfIt = ljIt.at(c.joinedNs);
							   const auto rjfIt = rjIt.at(c.joinedNs);
							   if (ljfIt == ljIt.end() || ljfIt.ItemsCount() == 0 || rjfIt == rjIt.end() || rjfIt.ItemsCount() == 0) {
								   throw Error(errQueryExec, "Not found value joined from ns %s", joinedSelector.RightNsName());
							   }
							   if (ljfIt.ItemsCount() > 1 || rjfIt.ItemsCount() > 1) {
								   throw Error(errQueryExec, "Found more than 1 value joined from ns %s", joinedSelector.RightNsName());
							   }
							   res.fieldsCmpRes =
								   ConstPayload{joinedSelector.RightNs()->payloadType_, ljfIt[0].Value()}.Compare<WithString::No>(
									   rjfIt[0].Value(), jNs.fields, res.firstDifferentFieldIdx, jNs.collateOpts);
						   }
						   return res.GetResult(c.desc);
					   },
					   [&](CompareByField c) {
						   if (mainNsRes.firstDifferentFieldIdx == kNotComputed) {
							   mainNsRes.fieldsCmpRes = ConstPayload(ns_.payloadType_, ns_.items_[lhs.Id()])
															.Compare<WithString::No>(ns_.items_[rhs.Id()], fields_,
																					 mainNsRes.firstDifferentFieldIdx, collateOpts_);
						   }
						   return mainNsRes.GetResult(c.desc);
					   }},
			comp);
		if (res != 0) return res < 0;
	}
	// If values are equal, then sort by row ID, to give consistent results
	return std::visit([&](const auto &e) noexcept { return e.data.desc ? lhs.Id() > rhs.Id() : lhs.Id() < rhs.Id(); },
					  ctx_.sortingContext.entries[0]);
}

class ItemComparator::BackInserter {
public:
	explicit BackInserter(ItemComparator &comparator) : comparator_(comparator) {}
	void expr(bool desc) { comparator_.comparators_.emplace_back(CompareByExpression{desc}); }
	void fields(TagsPath &&tp) { comparator_.fields_.push_back(std::move(tp)); }
	void fields(Joined &joined, TagsPath &&tp) { joined.fields.push_back(std::move(tp)); }
	void fields(int fieldIdx) { comparator_.fields_.push_back(fieldIdx); }
	void fields(Joined &joined, int fieldIdx) { joined.fields.push_back(fieldIdx); }
	void index(bool desc) { comparator_.comparators_.emplace_back(CompareByField{desc}); }
	void joined(size_t nsIdx, bool desc) { comparator_.comparators_.emplace_back(CompareByJoinedField{nsIdx, desc}); }
	void collateOpts(const CollateOpts *opts) { comparator_.collateOpts_.emplace_back(opts); }
	void collateOpts(Joined &joined, const CollateOpts *opts) { joined.collateOpts.emplace_back(opts); }

private:
	ItemComparator &comparator_;
};

class ItemComparator::FrontInserter {
public:
	FrontInserter(ItemComparator &comparator) : comparator_(comparator) {}
	void expr(bool desc) { comparator_.comparators_.emplace(comparator_.comparators_.begin(), CompareByExpression{desc}); }
	void fields(TagsPath &&tp) { comparator_.fields_.push_front(std::move(tp)); }
	void fields(Joined &joined, TagsPath &&tp) { joined.fields.push_front(std::move(tp)); }
	void fields(int fieldIdx) { comparator_.fields_.push_front(fieldIdx); }
	void fields(Joined &joined, int fieldIdx) { joined.fields.push_front(fieldIdx); }
	void index(bool desc) { comparator_.comparators_.emplace(comparator_.comparators_.begin(), CompareByField{desc}); }
	void joined(size_t nsIdx, bool desc) {
		comparator_.comparators_.emplace(comparator_.comparators_.begin(), CompareByJoinedField{nsIdx, desc});
	}
	void collateOpts(const CollateOpts *opts) { comparator_.collateOpts_.emplace(comparator_.collateOpts_.begin(), opts); }
	void collateOpts(Joined &joined, const CollateOpts *opts) { joined.collateOpts.emplace(joined.collateOpts.begin(), opts); }

private:
	ItemComparator &comparator_;
};

template <typename Inserter>
void ItemComparator::bindOne(const SortingContext::Entry &sortingEntry, Inserter insert, bool multiSort) {
	std::visit(
		overloaded{[&](const SortingContext::ExpressionEntry &e) { insert.expr(e.data.desc); },
				   [&](const SortingContext::JoinedFieldEntry &e) {
					   if (joined_.size() <= e.nsIdx) {
						   joined_.resize(e.nsIdx + 1);
					   }
					   if (joined_[e.nsIdx].joinedSelector == nullptr) {
						   assertrx_throw(ctx_.joinedSelectors);
						   assertrx_throw(ctx_.joinedSelectors->size() > e.nsIdx);
						   joined_[e.nsIdx].joinedSelector = &(*ctx_.joinedSelectors)[e.nsIdx];
					   }
					   auto &jns = joined_[e.nsIdx];
					   assertrx_throw(!std::holds_alternative<JoinPreResult::Values>(jns.joinedSelector->PreResult()->preselectedPayload));
					   const auto &ns = *jns.joinedSelector->RightNs();
					   const int fieldIdx = e.index;
					   if (fieldIdx == IndexValueType::SetByJsonPath || ns.indexes_[fieldIdx]->Opts().IsSparse()) {
						   TagsPath tagsPath;
						   if (fieldIdx != IndexValueType::SetByJsonPath) {
							   const FieldsSet &fs = ns.indexes_[fieldIdx]->Fields();
							   assertrx_throw(fs.getTagsPathsLength() > 0);
							   tagsPath = fs.getTagsPath(0);
						   } else {
							   tagsPath = ns.tagsMatcher_.path2tag(e.field);
						   }
						   if (jns.fields.contains(tagsPath)) {
							   throw Error(errQueryExec, "You cannot sort by the same indexes twice: %s", e.data.expression);
						   }
						   insert.fields(jns, std::move(tagsPath));
						   insert.joined(e.nsIdx, e.data.desc);
						   if (fieldIdx != IndexValueType::SetByJsonPath) {
							   insert.collateOpts(jns, &ns.indexes_[fieldIdx]->Opts().collateOpts_);
						   } else {
							   insert.collateOpts(jns, nullptr);
						   }
					   } else {
						   const auto &idx = *ns.indexes_[fieldIdx];
						   if (idx.Opts().IsArray()) {
							   throw Error(errQueryExec, "Sorting cannot be applied to array field.");
						   }
						   if (fieldIdx >= ns.indexes_.firstCompositePos()) {
							   if (multiSort) {
								   throw Error(errQueryExec, "Multicolumn sorting cannot be applied to composite fields: %s", e.field);
							   }
							   jns.fields = idx.Fields();
							   assertrx_throw(comparators_.empty());
							   comparators_.reserve(jns.fields.size());
							   for (size_t i = 0, s = jns.fields.size(); i < s; ++i) {
								   comparators_.emplace_back(CompareByJoinedField{e.nsIdx, e.data.desc});
							   }
						   } else {
							   if (jns.fields.contains(fieldIdx)) {
								   throw Error(errQueryExec, "You cannot sort by the same indexes twice: %s", e.field);
							   }
							   insert.fields(jns, fieldIdx);
							   insert.joined(e.nsIdx, e.data.desc);
						   }
						   insert.collateOpts(jns, &idx.Opts().collateOpts_);
					   }
				   },
				   [&](const SortingContext::FieldEntry &e) {
					   const int fieldIdx = e.data.index;
					   if (fieldIdx == IndexValueType::SetByJsonPath || ns_.indexes_[fieldIdx]->Opts().IsSparse()) {
						   TagsPath tagsPath;
						   if (fieldIdx != IndexValueType::SetByJsonPath) {
							   const FieldsSet &fs = ns_.indexes_[fieldIdx]->Fields();
							   assertrx_throw(fs.getTagsPathsLength() > 0);
							   tagsPath = fs.getTagsPath(0);
						   } else {
							   tagsPath = ns_.tagsMatcher_.path2tag(e.data.expression);
						   }
						   if (fields_.contains(tagsPath)) {
							   throw Error(errQueryExec, "You cannot sort by the same indexes twice: %s", e.data.expression);
						   }
						   insert.fields(std::move(tagsPath));
						   insert.index(e.data.desc);
					   } else {
						   if (ns_.indexes_[fieldIdx]->Opts().IsArray()) {
							   throw Error(errQueryExec, "Sorting cannot be applied to array field.");
						   }
						   if (fieldIdx >= ns_.indexes_.firstCompositePos()) {
							   if (multiSort) {
								   throw Error(errQueryExec, "Multicolumn sorting cannot be applied to composite fields: %s",
											   e.data.expression);
							   }
							   fields_ = ns_.indexes_[fieldIdx]->Fields();
							   assertrx_throw(comparators_.empty());
							   comparators_.reserve(fields_.size());
							   for (size_t i = 0, s = fields_.size(); i < s; ++i) {
								   comparators_.emplace_back(CompareByField{e.data.desc});
							   }
						   } else {
							   if (fields_.contains(fieldIdx)) {
								   throw Error(errQueryExec, "You cannot sort by the same indexes twice: %s", e.data.expression);
							   }
							   insert.fields(fieldIdx);
							   insert.index(e.data.desc);
						   }
					   }
					   insert.collateOpts(e.opts);
				   }},
		sortingEntry);
}

void ItemComparator::BindForForcedSort() {
	const auto &entries = ctx_.sortingContext.entries;
	[[maybe_unused]] const auto &exprResults = ctx_.sortingContext.exprResults;
	assertrx_throw(entries.size() >= exprResults.size());
	comparators_.reserve(entries.size());
	const bool multiSort = entries.size() > 1;
	for (size_t i = 1, s = entries.size(); i < s; ++i) {
		bindOne(entries[i], BackInserter{*this}, multiSort);
	}
}

void ItemComparator::BindForGeneralSort() {
	const auto &entries = ctx_.sortingContext.entries;
	[[maybe_unused]] const auto &exprResults = ctx_.sortingContext.exprResults;
	assertrx_throw(entries.size() >= exprResults.size());
	const bool multiSort = entries.size() > 1;
	if (comparators_.empty()) {
		comparators_.reserve(entries.size());
		for (const auto &e : entries) {
			bindOne(e, BackInserter{*this}, multiSort);
		}
	} else if (!entries.empty()) {
		bindOne(entries[0], FrontInserter{*this}, multiSort);
	}
}

}  // namespace reindexer
