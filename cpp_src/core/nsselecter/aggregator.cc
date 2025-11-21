#include "aggregator.h"
#include <algorithm>
#include "core/queryresults/aggregationresult.h"
#include "estl/overloaded.h"

namespace {

static int toSigned(reindexer::ComparationResult compRes) noexcept {
	using UnderlyingType = std::underlying_type_t<reindexer::ComparationResult>;
	const auto res = static_cast<UnderlyingType>(compRes);
	return static_cast<int>(res & (static_cast<UnderlyingType>(reindexer::ComparationResult::Lt) |
								   static_cast<UnderlyingType>(reindexer::ComparationResult::Gt))) *
		   (static_cast<int>(res & static_cast<UnderlyingType>(reindexer::ComparationResult::Gt)) - 1);
}

}  // namespace

namespace reindexer {

template <typename It>
static void moveFrames(It& begin, It& end, size_t size, size_t offset, size_t limit) {
	if (offset > QueryEntry::kDefaultOffset) {
		std::advance(begin, offset);
	}
	if (limit != QueryEntry::kDefaultLimit && offset + limit < size) {
		end = begin;
		std::advance(end, limit);
	}
}

template <typename It>
static void copy(It begin, It end, std::vector<FacetResult>& facets, const FieldsSet& fields, const PayloadType& payloadType) {
	for (; begin != end; ++begin) {
		facets.push_back({{}, begin->second});
		int tagPathIdx = 0;
		for (size_t i = 0; i < fields.size(); ++i) {
			ConstPayload pl(payloadType, begin->first);
			VariantArray va;
			if (fields[i] == IndexValueType::SetByJsonPath) {
				if (fields.isTagsPathIndexed(tagPathIdx)) {
					const IndexedTagsPath& tagsPath = fields.getIndexedTagsPath(tagPathIdx++);
					pl.GetByJsonPath(tagsPath, va, KeyValueType::Undefined{});
				} else {
					const TagsPath& tagsPath = fields.getTagsPath(tagPathIdx++);
					pl.GetByJsonPath(tagsPath, va, KeyValueType::Undefined{});
				}
				if (va.IsObjectValue()) {
					throw Error(errQueryExec, "Cannot aggregate object field");
				}
			} else {
				pl.Get(fields[i], va);
			}
			facets.back().values.push_back(va.empty() ? std::string() : va.front().As<std::string>());
		}
	}
}

template <typename It>
static void copy(It begin, It end, std::vector<FacetResult>& facets) {
	for (; begin != end; ++begin) {
		facets.push_back({{begin->first.template As<std::string>()}, begin->second});
	}
}

class [[nodiscard]] Aggregator::MultifieldComparator {
public:
	MultifieldComparator(const h_vector<SortingEntry, 1>&, const FieldsSet&, const PayloadType&);
	bool HaveCompareByCount() const { return haveCompareByCount; }
	bool operator()(const PayloadValue& lhs, const PayloadValue& rhs) const;
	bool operator()(const std::pair<PayloadValue, int>& lhs, const std::pair<PayloadValue, int>& rhs) const;

private:
	struct [[nodiscard]] CompOpts {
		CompOpts() : direction{Direction::Asc} {}
		CompOpts(const FieldsSet& fs, Direction d) : fields{fs}, direction{d} {}
		FieldsSet fields;  // if empty - compare by count
		Direction direction = Direction::Asc;
	};
	h_vector<CompOpts, 2> compOpts_;
	PayloadType type_;
	bool haveCompareByCount = false;

	void insertField(size_t toIdx, const FieldsSet& from, size_t fromIdx, int& tagsPathIdx);
};

class [[nodiscard]] Aggregator::SinglefieldComparator {
	enum CompareBy { ByValue, ByCount };

public:
	SinglefieldComparator(const h_vector<SortingEntry, 1>&);
	bool HaveCompareByCount() const { return haveCompareByCount; }
	bool operator()(const Variant& lhs, const Variant& rhs) const {
		return toSigned(lhs.Compare<NotComparable::Throw, kDefaultNullsHandling>(rhs)) * int(valueCompareDirection_) < 0;
	}
	bool operator()(const std::pair<Variant, int>& lhs, const std::pair<Variant, int>& rhs) const;

private:
	struct [[nodiscard]] CompOpts {
		CompOpts() = default;
		CompOpts(CompareBy compBy, Direction direc) : compareBy(compBy), direction(direc) {}
		CompareBy compareBy;
		Direction direction;
	};
	h_vector<CompOpts, 1> compOpts_;
	Direction valueCompareDirection_;
	bool haveCompareByCount = false;
};

Aggregator::MultifieldComparator::MultifieldComparator(const h_vector<SortingEntry, 1>& sortingEntries, const FieldsSet& fields,
													   const PayloadType& type)
	: compOpts_{}, type_{type}, haveCompareByCount{false} {
	assertrx_throw(type_);
	if (sortingEntries.empty()) {
		compOpts_.emplace_back(fields, Direction::Asc);
		return;
	}

	compOpts_.resize(sortingEntries.size() + 1);
	int tagsPathIdx = 0;
	for (size_t i = 0; i < fields.size(); ++i) {
		size_t j = 0;
		for (; j < sortingEntries.size(); ++j) {
			if (static_cast<int>(i) == sortingEntries[j].field) {
				insertField(j, fields, i, tagsPathIdx);
				compOpts_[j].direction = sortingEntries[j].desc ? Direction::Desc : Direction::Asc;
				break;
			}
		}
		if (j == sortingEntries.size()) {
			insertField(j, fields, i, tagsPathIdx);
		}
	}
	if (compOpts_.size() > 1 && compOpts_.back().fields.empty()) {
		auto end = compOpts_.end();
		std::ignore = compOpts_.erase(--end);
	}
	for (const auto& opt : compOpts_) {
		if (opt.fields.empty()) {
			haveCompareByCount = true;
			break;
		}
	}
}

bool Aggregator::MultifieldComparator::operator()(const PayloadValue& lhs, const PayloadValue& rhs) const {
	assertrx_throw(!lhs.IsFree());
	assertrx_throw(!rhs.IsFree());
	for (const auto& opt : compOpts_) {
		if (opt.fields.empty()) {
			continue;
		}
		const auto less = ConstPayload(type_, lhs).Compare<WithString::No, NotComparable::Throw, kDefaultNullsHandling>(rhs, opt.fields);
		if (less == ComparationResult::Eq) {
			continue;
		}
		return toSigned(less) * int(opt.direction) < 0;
	}
	return false;
}

bool Aggregator::MultifieldComparator::operator()(const std::pair<PayloadValue, int>& lhs, const std::pair<PayloadValue, int>& rhs) const {
	assertrx_throw(!lhs.first.IsFree());
	assertrx_throw(!rhs.first.IsFree());
	for (const auto& opt : compOpts_) {
		if (opt.fields.empty()) {
			if (lhs.second == rhs.second) {
				continue;
			}
			return int(opt.direction) * (lhs.second - rhs.second) < 0;
		}
		const auto less =
			ConstPayload(type_, lhs.first).Compare<WithString::No, NotComparable::Throw, kDefaultNullsHandling>(rhs.first, opt.fields);
		if (less == ComparationResult::Eq) {
			continue;
		}
		return toSigned(less) * int(opt.direction) < 0;
	}
	return false;
}

void Aggregator::MultifieldComparator::insertField(size_t toIdx, const FieldsSet& from, size_t fromIdx, int& tagsPathIdx) {
	compOpts_[toIdx].fields.push_back(from[fromIdx]);
	if (from[fromIdx] == IndexValueType::SetByJsonPath) {
		compOpts_[toIdx].fields.push_back(from.getTagsPath(tagsPathIdx++));
	}
}

struct [[nodiscard]] Aggregator::MultifieldOrderedMap : public btree::btree_map<PayloadValue, int, Aggregator::MultifieldComparator> {
	using Base = btree::btree_map<PayloadValue, int, MultifieldComparator>;
	using Base::Base;
	MultifieldOrderedMap() = delete;
	MultifieldOrderedMap(MultifieldOrderedMap&&) noexcept = default;
};

Aggregator::SinglefieldComparator::SinglefieldComparator(const h_vector<SortingEntry, 1>& sortingEntries)
	: valueCompareDirection_(Direction::Asc), haveCompareByCount(false) {
	bool haveCompareByValue = false;
	for (const SortingEntry& sortEntry : sortingEntries) {
		CompareBy compareBy = ByValue;
		Direction direc = sortEntry.desc ? Direction::Desc : Direction::Asc;
		if (sortEntry.field == SortingEntry::Count) {
			compareBy = ByCount;
			haveCompareByCount = true;
		} else {
			valueCompareDirection_ = direc;
			haveCompareByValue = true;
		}
		compOpts_.emplace_back(compareBy, direc);
	}
	if (!haveCompareByValue) {
		compOpts_.emplace_back(ByValue, valueCompareDirection_);
	}
}

bool Aggregator::SinglefieldComparator::operator()(const std::pair<Variant, int>& lhs, const std::pair<Variant, int>& rhs) const {
	for (const CompOpts& opt : compOpts_) {
		int less;
		if (opt.compareBy == ByValue) {
			less = toSigned(lhs.first.Compare<NotComparable::Throw, kDefaultNullsHandling>(rhs.first));
		} else {
			less = lhs.second - rhs.second;
		}
		if (less != 0) {
			return less * int(opt.direction) < 0;
		}
	}
	return false;
}

Aggregator::Aggregator(Aggregator&&) noexcept = default;
Aggregator::~Aggregator() = default;

Aggregator::Aggregator(const PayloadType& payloadType, const FieldsSet& fields, AggType aggType, const h_vector<std::string, 1>& names,
					   const h_vector<SortingEntry, 1>& sort, size_t limit, size_t offset, bool compositeIndexFields)
	: payloadType_(payloadType),
	  fields_(fields),
	  aggType_(aggType),
	  names_(names),
	  limit_(limit),
	  offset_(offset),
	  compositeIndexFields_(compositeIndexFields) {
	switch (aggType_) {
		case AggFacet:
			if (fields_.size() == 1) {
				if (sort.empty()) {
					facets_ = std::make_unique<Facets>(SinglefieldUnorderedMap{});
				} else {
					facets_ = std::make_unique<Facets>(SinglefieldOrderedMap{SinglefieldComparator{sort}});
				}
			} else {
				if (sort.empty()) {
					facets_ = std::make_unique<Facets>(MultifieldUnorderedMap{PayloadType{payloadType_}, FieldsSet{fields_}});
				} else {
					facets_ = std::make_unique<Facets>(MultifieldOrderedMap{MultifieldComparator{sort, fields_, payloadType_}});
				}
			}
			break;
		case AggDistinct:
			distincts_.reset(new HashSetVariantRelax(
				16, DistinctHelpers::DistinctHasher<DistinctHelpers::IsCompositeSupported::Yes>(payloadType, fields),
				DistinctHelpers::CompareVariantVector<DistinctHelpers::IsCompositeSupported::Yes>(payloadType, fields),
				DistinctHelpers::LessDistinctVector<DistinctHelpers::IsCompositeSupported::Yes>(payloadType, fields)));
			break;
		case AggMin:
		case AggMax:
		case AggAvg:
		case AggSum:
			break;
		case AggCount:
		case AggCountCached:
		case AggUnknown:
			throw Error(errParams, "Unknown aggregation type {}", int(aggType_));
	}
}

template <typename FacetMap, typename... Args>
static void fillOrderedFacetResult(std::vector<FacetResult>& result, const FacetMap& facets, size_t offset, size_t limit,
								   const Args&... args) {
	if (offset >= static_cast<size_t>(facets.size())) {
		return;
	}
	result.reserve(std::min(limit, facets.size() - offset));
	const auto& comparator = facets.key_comp();
	if (comparator.HaveCompareByCount()) {
		std::vector<std::pair<typename FacetMap::key_type, int>> tmpFacets(facets.begin(), facets.end());
		auto begin = tmpFacets.begin();
		auto end = tmpFacets.end();
		moveFrames(begin, end, tmpFacets.size(), offset, limit);
		std::nth_element(tmpFacets.begin(), begin, tmpFacets.end(), comparator);
		std::partial_sort(begin, end, tmpFacets.end(), comparator);
		copy(begin, end, result, args...);
	} else {
		auto begin = facets.begin();
		auto end = facets.end();
		moveFrames(begin, end, facets.size(), offset, limit);
		copy(begin, end, result, args...);
	}
}

template <typename FacetMap, typename... Args>
static void fillUnorderedFacetResult(std::vector<FacetResult>& result, const FacetMap& facets, size_t offset, size_t limit,
									 const Args&... args) {
	if (offset >= static_cast<size_t>(facets.size())) {
		return;
	}
	result.reserve(std::min(limit, facets.size() - offset));
	auto begin = facets.begin();
	auto end = facets.end();
	moveFrames(begin, end, facets.size(), offset, limit);
	copy(begin, end, result, args...);
}

AggregationResult Aggregator::MoveResult() && {
	assertrx_throw(isValid_);
	isValid_ = false;
	switch (aggType_) {
		case AggAvg:
			if (result_) {
				return AggregationResult{aggType_, std::move(names_), double(hitCount_ == 0 ? 0 : (*result_ / hitCount_))};
			}
			break;
		case AggSum:
		case AggMin:
		case AggMax:
			if (result_) {
				return AggregationResult{aggType_, std::move(names_), *result_};
			}
			break;
		case AggFacet: {
			std::vector<FacetResult> facets;
			std::visit(overloaded{[&](const SinglefieldOrderedMap& fm) { fillOrderedFacetResult(facets, fm, offset_, limit_); },
								  [&](const SinglefieldUnorderedMap& fm) { fillUnorderedFacetResult(facets, fm, offset_, limit_); },
								  [&](const MultifieldOrderedMap& fm) {
									  fillOrderedFacetResult(facets, fm, offset_, limit_, fields_, payloadType_);
								  },
								  [&](const MultifieldUnorderedMap& fm) {
									  fillUnorderedFacetResult(facets, fm, offset_, limit_, fields_, payloadType_);
								  }},
					   *facets_);
			return AggregationResult{aggType_, std::move(names_), std::move(facets)};
		}
		case AggDistinct: {
			assertrx_dbg(distincts_);
			std::vector<Variant> d;
			if (!distincts_->empty()) {
				size_t columnCount = names_.size();
				d.reserve(distincts_->size() * columnCount);
				for (const auto& r : *distincts_) {
					assertf_dbg(r.size() == columnCount, "Incorrect column count size={} columnCount={}", r.size(), columnCount);
					for (unsigned int k = 0; k < columnCount; k++) {
						d.emplace_back(r[k]);
					}
				}
			}
			return AggregationResult{aggType_, std::move(names_), std::move(payloadType_), std::move(fields_), std::move(d)};
		}
		case AggCount:
		case AggCountCached:
		case AggUnknown:
			throw_as_assert;
	}
	return AggregationResult{aggType_, std::move(names_)};
}

void Aggregator::Aggregate(const PayloadValue& data) {
	if (aggType_ == AggFacet) {
		const bool done =
			std::visit(overloaded{[&data](MultifieldUnorderedMap& fm) {
									  ++fm[data];
									  return true;
								  },
								  [&data](MultifieldOrderedMap& fm) {
									  ++fm[data];
									  return true;
								  },
								  [](SinglefieldOrderedMap&) { return false; }, [](SinglefieldUnorderedMap&) { return false; }},
					   *facets_);
		if (done) {
			return;
		}
	}
	if (aggType_ == AggDistinct && compositeIndexFields_) {
		aggregate(Variant(data));
		return;
	}

	if (fields_.size() == 1) {
		if (fields_[0] == IndexValueType::SetByJsonPath) {
			ConstPayload pl(payloadType_, data);
			VariantArray va;
			const TagsPath& tagsPath = fields_.getTagsPath(0);
			pl.GetByJsonPath(tagsPath, va, KeyValueType::Undefined{});
			if (va.IsObjectValue()) {
				throw Error(errQueryExec, "Cannot aggregate object field");
			}
			for (const Variant& v : va) {
				aggregate(v);
			}
			return;
		}

		const auto& fieldType = payloadType_.Field(fields_[0]);
		if (!fieldType.IsArray()) {
			aggregate(PayloadFieldValue(fieldType, data.Ptr() + fieldType.Offset()).Get());
		} else {
			PayloadFieldValue::Array* arr = reinterpret_cast<PayloadFieldValue::Array*>(data.Ptr() + fieldType.Offset());
			uint8_t* ptr = data.Ptr() + arr->offset;
			for (int i = 0; i < arr->len; i++, ptr += fieldType.ElemSizeof()) {
				aggregate(PayloadFieldValue(fieldType, ptr).Get());
			}
		}
	} else {
		assertrx_throw(aggType_ == AggDistinct);
		size_t maxIndex = 0;
		getData(data, distinctDataVector_, maxIndex);
		for (unsigned int i = 0; i < maxIndex; i++) {
			DistinctHelpers::FieldsValue values;
			if (!DistinctHelpers::GetMultiFieldValue(distinctDataVector_, i, fields_.size(), values)) {
				distincts_->insert(std::move(values));
			}
		}
	}
}

void Aggregator::aggregate(const Variant& v) {
	switch (aggType_) {
		case AggSum:
		case AggAvg:
			result_ = (result_ ? *result_ : 0) + v.As<double>();
			hitCount_++;
			break;
		case AggMin:
			result_ = result_ ? std::min(v.As<double>(), *result_) : v.As<double>();
			break;
		case AggMax:
			result_ = result_ ? std::max(v.As<double>(), *result_) : v.As<double>();
			break;
		case AggFacet:
			std::visit(overloaded{[&v](SinglefieldUnorderedMap& fm) { ++fm[v]; }, [&v](SinglefieldOrderedMap& fm) { ++fm[v]; },
								  [](MultifieldUnorderedMap&) { throw_as_assert; }, [](MultifieldOrderedMap&) { throw_as_assert; }},
					   *facets_);
			break;
		case AggDistinct:
			assertrx_dbg(distincts_);
			distincts_->insert({v});
			break;
		case AggUnknown:
		case AggCount:
		case AggCountCached:
			break;
	}
}

}  // namespace reindexer
