#include "core/aggregator.h"
#include <algorithm>
#include <limits>
#include "core/queryresults/queryresults.h"

namespace reindexer {

template <typename It>
static void moveFrames(It &begin, It &end, size_t size, size_t offset, size_t limit) {
	if (offset > 0) {
		std::advance(begin, offset);
	}
	if (limit != UINT_MAX && offset + limit < size) {
		end = begin;
		std::advance(end, limit);
	}
}

template <typename It>
static void copy(It begin, It end, h_vector<FacetResult, 1> &facets, const FieldsSet &fields, const PayloadType &payloadType) {
	for (; begin != end; ++begin) {
		facets.push_back({{}, begin->second});
		int tagPathIdx = 0;
		for (size_t i = 0; i < fields.size(); ++i) {
			ConstPayload pl(payloadType, begin->first);
			VariantArray va;
			if (fields[i] == IndexValueType::SetByJsonPath) {
				const TagsPath &tagsPath = fields.getTagsPath(tagPathIdx++);
				pl.GetByJsonPath(tagsPath, va, KeyValueUndefined);
			} else {
				pl.Get(fields[i], va);
			}
			facets.back().values.push_back(va.empty() ? string() : va.front().As<string>());
		}
	}
}

template <typename It>
static void copy(It begin, It end, h_vector<FacetResult, 1> &facets) {
	for (; begin != end; ++begin) {
		facets.push_back({{begin->first.template As<string>()}, begin->second});
	}
}

class Aggregator::MultifieldComparator {
public:
	MultifieldComparator(const h_vector<SortingEntry, 1> &, const FieldsSet &, const PayloadType &);
	bool HaveCompareByCount() const { return haveCompareByCount; }
	bool operator()(const PayloadValue &lhs, const PayloadValue &rhs) const;
	bool operator()(const pair<PayloadValue, int> &lhs, const pair<PayloadValue, int> &rhs) const;

private:
	struct CompOpts {
		CompOpts() : direction{Asc} {}
		CompOpts(const FieldsSet &fs, Direction d) : fields{fs}, direction{d} {}
		FieldsSet fields;  // if empty - compare by count
		Direction direction = Asc;
	};
	h_vector<CompOpts, 2> compOpts_;
	PayloadType type_;
	bool haveCompareByCount = false;

	void insertField(size_t toIdx, const FieldsSet &from, size_t fromIdx, int &tagsPathIdx);
};

class Aggregator::SinglefieldComparator {
	enum CompareBy { ByValue, ByCount };

public:
	SinglefieldComparator(const h_vector<SortingEntry, 1> &);
	bool HaveCompareByCount() const { return haveCompareByCount; }
	bool operator()(const Variant &lhs, const Variant &rhs) const { return lhs.Compare(rhs) * valueCompareDirection_ < 0; }
	bool operator()(const pair<Variant, int> &lhs, const pair<Variant, int> &rhs) const;

private:
	struct CompOpts {
		CompOpts() = default;
		CompOpts(CompareBy compBy, Direction direc) : compareBy(compBy), direction(direc) {}
		CompareBy compareBy;
		Direction direction;
	};
	h_vector<CompOpts, 1> compOpts_;
	Direction valueCompareDirection_;
	bool haveCompareByCount = false;
};

Aggregator::MultifieldComparator::MultifieldComparator(const h_vector<SortingEntry, 1> &sortingEntries, const FieldsSet &fields,
													   const PayloadType &type)
	: compOpts_{}, type_{type}, haveCompareByCount{false} {
	if (sortingEntries.empty()) {
		compOpts_.emplace_back(fields, Asc);
		return;
	}

	compOpts_.resize(sortingEntries.size() + 1);
	int tagsPathIdx = 0;
	for (size_t i = 0; i < fields.size(); ++i) {
		size_t j = 0;
		for (; j < sortingEntries.size(); ++j) {
			if (static_cast<int>(i) == sortingEntries[j].field) {
				insertField(j, fields, i, tagsPathIdx);
				compOpts_[j].direction = sortingEntries[j].desc ? Desc : Asc;
				break;
			}
		}
		if (j == sortingEntries.size()) {
			insertField(j, fields, i, tagsPathIdx);
		}
	}
	if (compOpts_.size() > 1 && compOpts_.back().fields.empty()) {
		auto end = compOpts_.end();
		compOpts_.erase(--end);
	}
	for (const auto &opt : compOpts_) {
		if (opt.fields.empty()) {
			haveCompareByCount = true;
			break;
		}
	}
}

bool Aggregator::MultifieldComparator::operator()(const PayloadValue &lhs, const PayloadValue &rhs) const {
	for (const auto &opt : compOpts_) {
		if (opt.fields.empty()) continue;
		assert(type_);
		assert(!lhs.IsFree());
		assert(!rhs.IsFree());
		int less = ConstPayload(type_, lhs).Compare(rhs, opt.fields);
		if (less == 0) continue;
		return less * opt.direction < 0;
	}
	return false;
}

bool Aggregator::MultifieldComparator::operator()(const pair<PayloadValue, int> &lhs, const pair<PayloadValue, int> &rhs) const {
	for (const auto &opt : compOpts_) {
		if (opt.fields.empty()) {
			if (lhs.second == rhs.second) continue;
			return opt.direction * (lhs.second - rhs.second) < 0;
		}
		assert(type_);
		assert(!lhs.first.IsFree());
		assert(!rhs.first.IsFree());
		int less = ConstPayload(type_, lhs.first).Compare(rhs.first, opt.fields);
		if (less == 0) continue;
		return less * opt.direction < 0;
	}
	return false;
}

void Aggregator::MultifieldComparator::insertField(size_t toIdx, const FieldsSet &from, size_t fromIdx, int &tagsPathIdx) {
	compOpts_[toIdx].fields.push_back(from[fromIdx]);
	if (from[fromIdx] == IndexValueType::SetByJsonPath) {
		compOpts_[toIdx].fields.push_back(from.getTagsPath(tagsPathIdx++));
	}
}

Aggregator::SinglefieldComparator::SinglefieldComparator(const h_vector<SortingEntry, 1> &sortingEntries)
	: valueCompareDirection_(Asc), haveCompareByCount(false) {
	bool haveCompareByValue = false;
	for (const SortingEntry &sortEntry : sortingEntries) {
		CompareBy compareBy = ByValue;
		Direction direc = sortEntry.desc ? Desc : Asc;
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

bool Aggregator::SinglefieldComparator::operator()(const pair<Variant, int> &lhs, const pair<Variant, int> &rhs) const {
	for (const CompOpts &opt : compOpts_) {
		int less;
		if (opt.compareBy == ByValue) {
			less = lhs.first.Compare(rhs.first);
		} else {
			less = lhs.second - rhs.second;
		}
		if (less != 0) return less * opt.direction < 0;
	}
	return false;
}

Aggregator::Aggregator() = default;
Aggregator::Aggregator(Aggregator &&) = default;
Aggregator::~Aggregator() = default;

Aggregator::Aggregator(const PayloadType &payloadType, const FieldsSet &fields, AggType aggType, const h_vector<string, 1> &names,
					   const h_vector<SortingEntry, 1> &sort, size_t limit, size_t offset)
	: payloadType_(payloadType), fields_(fields), aggType_(aggType), names_(names), limit_(limit), offset_(offset) {
	switch (aggType_) {
		case AggFacet:
			if (fields_.size() == 1) {
				singlefieldFacets_.reset(new SinglefieldMap{SinglefieldComparator{sort}});
			} else {
				multifieldFacets_.reset(new MultifieldMap{MultifieldComparator{sort, fields_, payloadType_}});
			}
			break;
		case AggMin:
			result_ = std::numeric_limits<double>::max();
			break;
		case AggMax:
			result_ = std::numeric_limits<double>::min();
			break;
		case AggAvg:
		case AggSum:
			break;
		default:
			throw Error(errParams, "Unknown aggregation type %d", aggType_);
	}
}

template <typename FacetMap, typename... Args>
static void fillFacetResult(h_vector<FacetResult, 1> &result, const FacetMap &facets, size_t offset, size_t limit, const Args &... args) {
	if (offset >= static_cast<size_t>(facets.size())) return;
	result.reserve(std::min(limit, facets.size() - offset));
	const auto &comparator = facets.key_comp();
	if (comparator.HaveCompareByCount()) {
		vector<pair<typename FacetMap::key_type, int>> tmpFacets(facets.begin(), facets.end());
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

AggregationResult Aggregator::GetResult() const {
	AggregationResult ret;
	ret.fields = names_;
	ret.type = aggType_;

	switch (aggType_) {
		case AggAvg:
			ret.value = double(hitCount_ == 0 ? 0 : (result_ / hitCount_));
			break;
		case AggSum:
		case AggMin:
		case AggMax:
			ret.value = result_;
			break;
		case AggFacet:
			if (multifieldFacets_) {
				fillFacetResult(ret.facets, *multifieldFacets_, offset_, limit_, fields_, payloadType_);
			} else {
				assert(singlefieldFacets_);
				fillFacetResult(ret.facets, *singlefieldFacets_, offset_, limit_);
			}
			break;
		default:
			abort();
	}
	return ret;
}

void Aggregator::Aggregate(const PayloadValue &data) {
	if (aggType_ == AggFacet && multifieldFacets_) {
		++(*multifieldFacets_)[data];
		return;
	}
	assert(fields_.size() == 1);
	if (fields_[0] == IndexValueType::SetByJsonPath) {
		ConstPayload pl(payloadType_, data);
		VariantArray va;
		const TagsPath &tagsPath = fields_.getTagsPath(0);
		pl.GetByJsonPath(tagsPath, va, KeyValueUndefined);
		for (const Variant &v : va) aggregate(v);
		return;
	}

	const auto &fieldType = payloadType_.Field(fields_[0]);
	if (!fieldType.IsArray()) {
		aggregate(PayloadFieldValue(fieldType, data.Ptr() + fieldType.Offset()).Get());
		return;
	}

	PayloadFieldValue::Array *arr = reinterpret_cast<PayloadFieldValue::Array *>(data.Ptr() + fieldType.Offset());

	uint8_t *ptr = data.Ptr() + arr->offset;
	for (int i = 0; i < arr->len; i++, ptr += fieldType.ElemSizeof()) {
		aggregate(PayloadFieldValue(fieldType, ptr).Get());
	}
}

void Aggregator::aggregate(const Variant &v) {
	switch (aggType_) {
		case AggSum:
		case AggAvg:
			result_ += v.As<double>();
			hitCount_++;
			break;
		case AggMin:
			result_ = std::min(v.As<double>(), result_);
			break;
		case AggMax:
			result_ = std::max(v.As<double>(), result_);
			break;
		case AggFacet:
			assert(singlefieldFacets_);
			++(*singlefieldFacets_)[v];
			break;
		case AggUnknown:
			break;
	};
}

}  // namespace reindexer
