#include "core/aggregator.h"
#include <algorithm>
#include <limits>
#include "core/query/queryresults.h"

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

class Aggregator::OrderBy {
private:
	enum Direction { Desc = -1, Asc = 1 };

public:
	OrderBy(const FieldsSet &fields, const PayloadType &type) : compOpts_{CompOpts{fields, Asc}}, type_{type}, haveCompareByCount{false} {}

	OrderBy(const h_vector<SortingEntry, 1> &sortingEntries, const FieldsSet &fields, const PayloadType &type)
		: compOpts_{}, type_{type}, haveCompareByCount{false} {
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

	bool HaveCompareByCount() const { return haveCompareByCount; }

	bool operator()(const PayloadValue &lhs, const PayloadValue &rhs) const {
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

	bool operator()(const pair<PayloadValue, int> &lhs, const pair<PayloadValue, int> &rhs) const {
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

	void insertField(size_t toIdx, const FieldsSet &from, size_t fromIdx, int &tagsPathIdx) {
		compOpts_[toIdx].fields.push_back(from[fromIdx]);
		if (from[fromIdx] == IndexValueType::SetByJsonPath) {
			compOpts_[toIdx].fields.push_back(from.getTagsPath(tagsPathIdx++));
		}
	}
};

Aggregator::Aggregator() = default;
Aggregator::Aggregator(Aggregator &&) = default;
Aggregator &Aggregator::operator=(Aggregator &&) = default;
Aggregator::~Aggregator() = default;

Aggregator::Aggregator(const PayloadType &payloadType, const FieldsSet &fields, AggType aggType, const h_vector<string, 1> &names,
					   const h_vector<SortingEntry, 1> &sort, size_t limit, size_t offset)
	: payloadType_(payloadType), fields_(fields), aggType_(aggType), names_(names), limit_(limit), offset_(offset) {
	switch (aggType_) {
		case AggFacet:
			if (!sort.empty()) {
				comparator_.reset(new OrderBy(sort, fields_, payloadType_));
			} else {
				comparator_.reset(new OrderBy(fields_, payloadType_));
			}
			facets_.reset(new FacetMap(*comparator_));
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
		case AggFacet: {
			if (offset_ >= static_cast<size_t>(facets_->size())) break;
			ret.facets.reserve(std::min(limit_, facets_->size() - offset_));
			if (comparator_->HaveCompareByCount()) {
				vector<pair<PayloadValue, int>> tmpFacets(facets_->begin(), facets_->end());
				auto begin = tmpFacets.begin();
				auto end = tmpFacets.end();
				moveFrames(begin, end, tmpFacets.size(), offset_, limit_);
				std::nth_element(tmpFacets.begin(), begin, tmpFacets.end(), *comparator_);
				std::partial_sort(begin, end, tmpFacets.end(), *comparator_);
				copy(begin, end, ret.facets, fields_, payloadType_);
			} else {
				auto begin = facets_->begin();
				auto end = facets_->end();
				moveFrames(begin, end, facets_->size(), offset_, limit_);
				copy(begin, end, ret.facets, fields_, payloadType_);
			}
			break;
		}
		default:
			abort();
	}
	return ret;
}

void Aggregator::Aggregate(const PayloadValue &data) {
	if (aggType_ == AggFacet) {
		++(*facets_)[data];
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
	for (int i = 0; i < arr->len; i++, ptr += fieldType.Sizeof()) {
		aggregate(PayloadFieldValue(fieldType, ptr).Get());
	}
}

void Aggregator::aggregate(const Variant &v) {
	assert(aggType_ != AggFacet);
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
		case AggUnknown:
			break;
	};
}

}  // namespace reindexer
