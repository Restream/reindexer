#include "itemcomparator.h"
#include "core/namespace/namespaceimpl.h"
#include "core/queryresults/joinresults.h"
#include "nsselecter.h"

namespace {

constexpr size_t kNotComputed{std::numeric_limits<size_t>::max()};

struct [[nodiscard]] FieldsCompRes {
	size_t firstDifferentFieldIdx{kNotComputed};
	reindexer::ComparationResult fieldsCmpRes{reindexer::ComparationResult::NotComparable};

	reindexer::ComparationResult GetResult(reindexer::Desc desc) noexcept {
		if (firstDifferentFieldIdx == 0) {
			return desc ? -fieldsCmpRes : fieldsCmpRes;
		} else {
			--firstDifferentFieldIdx;
			return reindexer::ComparationResult::Eq;
		}
	}
};

}  // namespace

namespace reindexer {

bool ItemComparator::operator()(const ItemRef& lhs, const ItemRef& rhs) const {
	size_t expressionIndex{0};
	FieldsCompRes mainNsRes;
	FieldsCompRes joinedNsRes;
	for (const auto& comp : comparators_) {
		const ComparationResult res = std::visit(
			overloaded{[&](CompareByExpression c) noexcept {
						   assertrx_throw(expressionIndex < ctx_.sortingContext.exprResults.size());
						   const auto& eR{ctx_.sortingContext.exprResults[expressionIndex++]};
						   const auto lR{eR[lhs.SortExprResultsIdx()]};
						   const auto rR{eR[rhs.SortExprResultsIdx()]};
						   static_assert(std::is_floating_point_v<std::decay_t<decltype(lR)>>);
						   if (fp::ExactlyEqual(lR, rR)) {
							   return ComparationResult::Eq;
						   } else if (lR > rR) {
							   return c.desc ? ComparationResult::Lt : ComparationResult::Gt;
						   } else {
							   return c.desc ? ComparationResult::Gt : ComparationResult::Lt;
						   }
					   },
					   [&](CompareByJoinedField c) {
						   if (joinedNsRes.firstDifferentFieldIdx == kNotComputed) {
							   const auto& jNs = joined_;
							   const auto& joinedSelector = *jNs.joinedSelector;
							   const joins::ItemIterator ljIt{joinResults_, lhs.Id()};
							   const joins::ItemIterator rjIt{joinResults_, rhs.Id()};
							   const auto ljfIt = ljIt.at(c.joinedNs);
							   const auto rjfIt = rjIt.at(c.joinedNs);
							   if (ljfIt == ljIt.end() || ljfIt.ItemsCount() == 0 || rjfIt == rjIt.end() || rjfIt.ItemsCount() == 0) {
								   throw Error(errQueryExec, "Not found value joined from ns {}", joinedSelector.RightNsName());
							   }
							   if (ljfIt.ItemsCount() > 1 || rjfIt.ItemsCount() > 1) {
								   throw Error(errQueryExec, "Found more than 1 value joined from ns {}", joinedSelector.RightNsName());
							   }
							   joinedNsRes.fieldsCmpRes =
								   ConstPayload{joinedSelector.RightNs()->payloadType_, ljfIt[0].Value()}
									   .Compare<WithString::No, NotComparable::Throw, kDefaultNullsHandling>(
										   rjfIt[0].Value(), jNs.fields, joinedNsRes.firstDifferentFieldIdx, jNs.collateOpts);
						   }
						   return joinedNsRes.GetResult(c.desc);
					   },
					   [&](CompareByField c) {
						   if (mainNsRes.firstDifferentFieldIdx == kNotComputed) {
							   mainNsRes.fieldsCmpRes = compareFields(lhs.Id(), rhs.Id(), mainNsRes.firstDifferentFieldIdx);
						   }
						   return mainNsRes.GetResult(c.desc);
					   }},
			comp);
		if (res != ComparationResult::Eq) {
			return res == ComparationResult::Lt;
		}
	}
	// If values are equal, then sort by row ID, to give consistent results
	return std::visit([&](const auto& e) noexcept { return e.data.desc ? lhs.Id() > rhs.Id() : lhs.Id() < rhs.Id(); },
					  ctx_.sortingContext.entries[0].AsVariant());
}

class [[nodiscard]] ItemComparator::BackInserter {
public:
	explicit BackInserter(ItemComparator& comparator) noexcept : comparator_(comparator) {}
	void expr(Desc desc) { comparator_.comparators_.emplace_back(CompareByExpression{desc}); }
	void fields(TagsPath&& tp) { comparator_.fields_.push_back(std::move(tp)); }
	void fields(Joined& joined, TagsPath&& tp) { joined.fields.push_back(std::move(tp)); }
	void fields(int fieldIdx) {
		if (fieldIdx != SetByJsonPath && !comparator_.fields_.contains(fieldIdx)) {
			comparator_.fields_.push_back(fieldIdx);
			auto& rawDataRef = comparator_.rawData_.emplace_back();
			if (auto rawData = comparator_.ns_.indexes_[fieldIdx]->ColumnData(); rawData) {
				rawDataRef.ptr = rawData;
				rawDataRef.type = comparator_.ns_.payloadType_.Field(fieldIdx).Type();
			}
		}
	}
	void fields(Joined& joined, int fieldIdx) { joined.fields.push_back(fieldIdx); }
	void index(Desc desc) { comparator_.comparators_.emplace_back(CompareByField{desc}); }
	void joined(size_t nsIdx, Desc desc) { comparator_.comparators_.emplace_back(CompareByJoinedField{nsIdx, desc}); }
	void collateOpts(const CollateOpts* opts) { comparator_.collateOpts_.emplace_back(opts); }
	void collateOpts(Joined& joined, const CollateOpts* opts) { joined.collateOpts.emplace_back(opts); }

private:
	ItemComparator& comparator_;
};

class [[nodiscard]] ItemComparator::FrontInserter {
public:
	explicit FrontInserter(ItemComparator& comparator) noexcept : comparator_(comparator) {}
	void expr(Desc desc) { comparator_.comparators_.emplace(comparator_.comparators_.cbegin(), CompareByExpression{desc}); }
	void fields(TagsPath&& tp) { comparator_.fields_.push_front(std::move(tp)); }
	void fields(Joined& joined, TagsPath&& tp) { joined.fields.push_front(std::move(tp)); }
	void fields(int fieldIdx) {
		if (fieldIdx != SetByJsonPath && !comparator_.fields_.contains(fieldIdx)) {
			comparator_.fields_.push_front(fieldIdx);
			auto rawDataIt = comparator_.rawData_.insert(comparator_.rawData_.cbegin(), SortingContext::RawDataParams());
			if (auto rawData = comparator_.ns_.indexes_[fieldIdx]->ColumnData(); rawData) {
				rawDataIt->ptr = rawData;
				rawDataIt->type = comparator_.ns_.payloadType_.Field(fieldIdx).Type();
			}
		}
	}
	void fields(Joined& joined, int fieldIdx) { joined.fields.push_front(fieldIdx); }
	void index(Desc desc) { comparator_.comparators_.emplace(comparator_.comparators_.cbegin(), CompareByField{desc}); }
	void joined(size_t nsIdx, Desc desc) {
		comparator_.comparators_.emplace(comparator_.comparators_.cbegin(), CompareByJoinedField{nsIdx, desc});
	}
	void collateOpts(const CollateOpts* opts) { comparator_.collateOpts_.emplace(comparator_.collateOpts_.cbegin(), opts); }
	void collateOpts(Joined& joined, const CollateOpts* opts) { joined.collateOpts.emplace(joined.collateOpts.cbegin(), opts); }

private:
	ItemComparator& comparator_;
};

template <typename Inserter>
void ItemComparator::bindOne(const SortingContext::Entry& sortingEntry, Inserter insert) {
	std::visit(
		overloaded{[&](const SortingContext::ExpressionEntry& e) { insert.expr(e.data.desc); },
				   [&](const SortingContext::JoinedFieldEntry& e) {
					   auto& jns = joined_;
					   if (jns.joinedSelector == nullptr) {
						   assertrx_throw(ctx_.joinedSelectors);
						   assertrx_throw(ctx_.joinedSelectors->size() > e.nsIdx);
						   jns.joinedSelector = &(*ctx_.joinedSelectors)[e.nsIdx];
					   } else {
						   assertrx_dbg(&(*ctx_.joinedSelectors)[e.nsIdx] == jns.joinedSelector);
					   }
					   assertrx_dbg(!std::holds_alternative<JoinPreResult::Values>(jns.joinedSelector->PreResult().payload));
					   const auto& ns = *jns.joinedSelector->RightNs();
					   const int fieldIdx = e.index;
					   if (fieldIdx == IndexValueType::SetByJsonPath || ns.indexes_[fieldIdx]->Opts().IsSparse()) {
						   TagsPath tagsPath;
						   if (fieldIdx != IndexValueType::SetByJsonPath) {
							   const FieldsSet& fs = ns.indexes_[fieldIdx]->Fields();
							   assertrx_throw(fs.getTagsPathsLength() > 0);
							   tagsPath = fs.getTagsPath(0);
						   } else {
							   tagsPath = ns.tagsMatcher_.path2tag(e.field);
						   }
						   if (jns.fields.contains(tagsPath)) {
							   throw Error(errQueryExec, "You cannot sort by the same indexes twice: {}", e.data.expression);
						   }
						   insert.fields(jns, std::move(tagsPath));
						   insert.joined(e.nsIdx, e.data.desc);
						   insert.collateOpts(
							   jns, (fieldIdx == IndexValueType::SetByJsonPath) ? nullptr : &ns.indexes_[fieldIdx]->Opts().collateOpts_);
					   } else {
						   const auto& idx = *ns.indexes_[fieldIdx];
						   if (fieldIdx >= ns.indexes_.firstCompositePos()) {
							   unsigned jsonPathsIndex = 0;
							   const auto& fields = idx.Fields();
							   for (unsigned i = 0, s = fields.size(); i < s; ++i) {
								   const auto f = fields[i];
								   if (f != IndexValueType::SetByJsonPath) {
									   if (jns.fields.contains(f)) {
										   throw Error(errQueryExec, "You cannot sort by the same indexes twice: {}", e.data.expression);
									   }
									   insert.fields(jns, f);
								   } else {
									   TagsPath tagsPath = jns.fields.getTagsPath(jsonPathsIndex++);
									   if (jns.fields.contains(tagsPath)) {
										   throw Error(errQueryExec, "You cannot sort by the same indexes twice: {}", e.data.expression);
									   }
									   insert.fields(jns, std::move(tagsPath));
								   }
								   insert.joined(e.nsIdx, e.data.desc);
								   insert.collateOpts(jns, &idx.Opts().collateOpts_);
							   }
						   } else {
							   if (jns.fields.contains(fieldIdx)) {
								   throw Error(errQueryExec, "You cannot sort by the same indexes twice: {}", e.field);
							   }
							   insert.fields(jns, fieldIdx);
							   insert.joined(e.nsIdx, e.data.desc);
							   insert.collateOpts(jns, &idx.Opts().collateOpts_);
						   }
					   }
				   },
				   [&](const SortingContext::FieldEntry& e) {
					   const int fieldIdx = e.data.index;
					   if (fieldIdx == IndexValueType::SetByJsonPath || ns_.indexes_[fieldIdx]->Opts().IsSparse()) {
						   TagsPath tagsPath;
						   if (fieldIdx != IndexValueType::SetByJsonPath) {
							   const FieldsSet& fs = ns_.indexes_[fieldIdx]->Fields();
							   assertrx_throw(fs.getTagsPathsLength() > 0);
							   tagsPath = fs.getTagsPath(0);
						   } else {
							   tagsPath = ns_.tagsMatcher_.path2tag(e.data.expression);
						   }
						   if (fields_.contains(tagsPath)) {
							   throw Error(errQueryExec, "You cannot sort by the same indexes twice: {}", e.data.expression);
						   }
						   insert.fields(std::move(tagsPath));
						   insert.index(e.data.desc);
						   insert.collateOpts(e.opts);
					   } else {
						   if (fieldIdx >= ns_.indexes_.firstCompositePos()) {
							   const auto& fields = ns_.indexes_[fieldIdx]->Fields();
							   for (unsigned i = 0, s = fields.size(); i < s; ++i) {
								   const auto field(fields[i]);
								   assertrx_dbg(field != SetByJsonPath);
								   if (fields_.contains(field)) {
									   throw Error(errQueryExec, "You cannot sort by the same indexes twice: {}", e.data.expression);
								   }
								   insert.fields(field);
								   insert.index(e.data.desc);
								   insert.collateOpts(e.opts);
							   }
						   } else {
							   if (fields_.contains(fieldIdx)) {
								   throw Error(errQueryExec, "You cannot sort by the same indexes twice: {}", e.data.expression);
							   }
							   insert.fields(fieldIdx);
							   insert.index(e.data.desc);
							   insert.collateOpts(e.opts);
						   }
					   }
				   }},
		sortingEntry.AsVariant());
}

void ItemComparator::BindForForcedSort() {
	const auto& entries = ctx_.sortingContext.entries;
	[[maybe_unused]] const auto& exprResults = ctx_.sortingContext.exprResults;
	assertrx_throw(entries.size() >= exprResults.size());
	comparators_.reserve(entries.size());
	for (size_t i = 1; i < entries.size(); ++i) {
		bindOne(entries[i], BackInserter{*this});
	}
}

void ItemComparator::BindForGeneralSort() {
	const auto& entries = ctx_.sortingContext.entries;
	[[maybe_unused]] const auto& exprResults = ctx_.sortingContext.exprResults;
	assertrx_throw(entries.size() >= exprResults.size());
	if (comparators_.empty()) {
		comparators_.reserve(entries.size());
		for (const auto& e : entries) {
			bindOne(e, BackInserter{*this});
		}
	} else if (!entries.empty()) {
		bindOne(entries[0], FrontInserter{*this});
	}
}

ComparationResult ItemComparator::compareFields(IdType lId, IdType rId, size_t& firstDifferentFieldIdx) const {
	const bool commonOpts = (collateOpts_.size() == 1);
	size_t tagPathIdx = 0;
	size_t rawDataIdx = 0;
	for (size_t i = 0, sz = fields_.size(); i < sz; ++i) {
		const CollateOpts* opts(commonOpts ? collateOpts_[0] : collateOpts_[i]);
		ComparationResult cmpRes;
		const auto field(fields_[i]);
		if (field != SetByJsonPath && rawData_[rawDataIdx].ptr) {
			const auto& rd = rawData_[rawDataIdx];
			++rawDataIdx;
			const auto rawData = rd.ptr;
			auto values = rd.type.EvaluateOneOf(
				[rawData, lId, rId](KeyValueType::Bool) noexcept {
					return std::make_pair(Variant(*(static_cast<const bool*>(rawData) + lId)),
										  Variant(*(static_cast<const bool*>(rawData) + rId)));
				},
				[rawData, lId, rId](KeyValueType::Int) noexcept {
					return std::make_pair(Variant(*(static_cast<const int*>(rawData) + lId)),
										  Variant(*(static_cast<const int*>(rawData) + rId)));
				},
				[rawData, lId, rId](KeyValueType::Int64) noexcept {
					return std::make_pair(Variant(*(static_cast<const int64_t*>(rawData) + lId)),
										  Variant(*(static_cast<const int64_t*>(rawData) + rId)));
				},
				[rawData, lId, rId](KeyValueType::Double) noexcept {
					return std::make_pair(Variant(*(static_cast<const double*>(rawData) + lId)),
										  Variant(*(static_cast<const double*>(rawData) + rId)));
				},
				[rawData, lId, rId](KeyValueType::String) noexcept {
					return std::make_pair(Variant(p_string(static_cast<const std::string_view*>(rawData) + lId), Variant::noHold),
										  Variant(p_string(static_cast<const std::string_view*>(rawData) + rId), Variant::noHold));
				},
				[rawData, lId, rId](KeyValueType::Uuid) noexcept {
					return std::make_pair(Variant(*(static_cast<const Uuid*>(rawData) + lId)),
										  Variant(*(static_cast<const Uuid*>(rawData) + rId)));
				},
				[](KeyValueType::Float) noexcept -> std::pair<Variant, Variant> {
					// Indexed fields can not contain float
					throw_as_assert;
				},
				[](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null,
								   KeyValueType::FloatVector> auto) -> std::pair<Variant, Variant> { throw_as_assert; });
			cmpRes =
				values.first.template Compare<NotComparable::Throw, kDefaultNullsHandling>(values.second, opts ? *opts : CollateOpts());
		} else {
			cmpRes = ConstPayload(ns_.payloadType_, ns_.items_[lId])
						 .CompareField<WithString::No, NotComparable::Throw, kDefaultNullsHandling>(
							 ns_.items_[rId], field, fields_, tagPathIdx, opts ? *opts : CollateOpts());
		}
		if (cmpRes != ComparationResult::Eq) {
			firstDifferentFieldIdx = i;
			return cmpRes;
		}
	}
	return ComparationResult::Eq;
}

}  // namespace reindexer
