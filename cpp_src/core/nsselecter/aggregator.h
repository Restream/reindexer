#pragma once

#include <optional>
#include "core/index/payload_map.h"
#include "core/query/queryentry.h"
#include "distincthelpers.h"
#include "estl/fast_hash_set.h"
#include "vendor/cpp-btree/btree_map.h"

namespace reindexer {

class AggregationResult;

class [[nodiscard]] Aggregator {
public:
	struct [[nodiscard]] SortingEntry {
		SortingEntry(int fld, Desc d) noexcept : field{fld}, desc{d} {}
		int field;
		Desc desc;
		enum { Count = -1 };
	};

	Aggregator(const PayloadType&, const FieldsSet&, AggType aggType, const h_vector<std::string, 1>& names,
			   const h_vector<SortingEntry, 1>& sort = {}, size_t limit = QueryEntry::kDefaultLimit,
			   size_t offset = QueryEntry::kDefaultOffset, bool compositeIndexFields = false);
	Aggregator(Aggregator&&) noexcept;
	~Aggregator();

	void Aggregate(const PayloadValue& lhs);
	AggregationResult MoveResult() &&;

	Aggregator(const Aggregator&) = delete;
	Aggregator& operator=(const Aggregator&) = delete;
	Aggregator& operator=(Aggregator&&) = delete;

	AggType Type() const noexcept { return aggType_; }
	const h_vector<std::string, 1>& Names() const& noexcept {
		assertrx_throw(isValid_);
		return names_;
	}
	auto Names() const&& = delete;

	const FieldsSet& GetFieldSet() const& noexcept {
		assertrx_throw(isValid_);
		return fields_;
	}
	auto GetFieldSet() const&& = delete;

private:
	enum class [[nodiscard]] Direction { Desc = -1, Asc = 1 };
	class MultifieldComparator;
	class SinglefieldComparator;
	struct MultifieldOrderedMap;
	using MultifieldUnorderedMap = unordered_payload_map<int, false>;
	using SinglefieldOrderedMap = btree::btree_map<Variant, int, SinglefieldComparator>;
	using SinglefieldUnorderedMap = fast_hash_map<Variant, int>;
	using Facets = std::variant<MultifieldOrderedMap, MultifieldUnorderedMap, SinglefieldOrderedMap, SinglefieldUnorderedMap>;

	void aggregate(const Variant& variant);

	PayloadType payloadType_;
	FieldsSet fields_;
	std::optional<double> result_ = std::nullopt;
	int hitCount_ = 0;
	AggType aggType_;
	h_vector<std::string, 1> names_;
	size_t limit_ = QueryEntry::kDefaultLimit;
	size_t offset_ = QueryEntry::kDefaultOffset;

	std::unique_ptr<Facets> facets_;

	std::vector<DistinctHelpers::DataType> distinctDataVector_;
	bool isValid_ = true;

	void getData(const PayloadValue& item, std::vector<DistinctHelpers::DataType>& data, size_t& maxIndex) {
		data.resize(0);
		data.reserve(fields_.size());
		ConstPayload pv{payloadType_, item};
		maxIndex = 0;
		size_t tagPathIndx = 0;
		for (size_t i = 0; i < fields_.size(); i++) {
			VariantArray b;
			if (fields_[i] == IndexValueType::SetByJsonPath) {
				const TagsPath& tagsPath = fields_.getTagsPath(tagPathIndx);
				tagPathIndx++;
				pv.GetByJsonPath(tagsPath, b, KeyValueType::Undefined{});
			} else {
				pv.Get(fields_[i], b);
			}
			maxIndex = std::max(maxIndex, size_t(b.size()));
			data.emplace_back(std::move(b), IsArray(b.IsArrayValue()));
		}
	}
	using HashSetVariantRelax =
		fast_hash_set<DistinctHelpers::FieldsValue, DistinctHelpers::DistinctHasher<DistinctHelpers::IsCompositeSupported::Yes>,
					  DistinctHelpers::CompareVariantVector<DistinctHelpers::IsCompositeSupported::Yes>,
					  DistinctHelpers::LessDistinctVector<DistinctHelpers::IsCompositeSupported::Yes>>;
	std::unique_ptr<HashSetVariantRelax> distincts_;
	const bool compositeIndexFields_;
};

}  // namespace reindexer
