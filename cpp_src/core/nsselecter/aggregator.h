#pragma once

#include <optional>
#include <unordered_set>
#include "core/index/payload_map.h"
#include "core/query/queryentry.h"
#include "estl/one_of.h"
#include "vendor/cpp-btree/btree_map.h"

namespace reindexer {

struct AggregationResult;

class Aggregator {
public:
	struct SortingEntry {
		int field;
		bool desc;
		enum { Count = -1 };
	};

	Aggregator(const PayloadType&, const FieldsSet&, AggType aggType, const h_vector<std::string, 1>& names,
			   const h_vector<SortingEntry, 1>& sort = {}, size_t limit = QueryEntry::kDefaultLimit,
			   size_t offset = QueryEntry::kDefaultOffset, bool compositeIndexFields = false);
	Aggregator(Aggregator&&) noexcept;
	~Aggregator();

	void Aggregate(const PayloadValue& lhs);
	AggregationResult GetResult() const;

	Aggregator(const Aggregator&) = delete;
	Aggregator& operator=(const Aggregator&) = delete;
	Aggregator& operator=(Aggregator&&) = delete;

	AggType Type() const noexcept { return aggType_; }
	const h_vector<std::string, 1>& Names() const noexcept { return names_; }

	bool DistinctChanged() noexcept { return distinctChecker_(); }

	void ResetDistinctSet() {
		assertrx_throw(this->Type() == AggType::AggDistinct);
		distincts_->clear();
	}

private:
	enum Direction { Desc = -1, Asc = 1 };
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

	class RelaxVariantCompare {
	public:
		RelaxVariantCompare(const PayloadType& type, const FieldsSet& fields) : type_(type), fields_(fields) {}
		bool operator()(const Variant& v1, const Variant& v2) const {
			if (!v1.Type().IsSame(v2.Type())) {
				return false;
			}
			return v1.Type().EvaluateOneOf(
				[&](OneOf<KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float, KeyValueType::String, KeyValueType::Bool,
						  KeyValueType::Int, KeyValueType::Uuid>) {
					return v1.Compare<NotComparable::Return>(v2) == ComparationResult::Eq;
				},
				[&](KeyValueType::Composite) {
					return ConstPayload(type_, static_cast<const PayloadValue&>(v1)).IsEQ(static_cast<const PayloadValue&>(v2), fields_);
				},
				[](OneOf<KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::FloatVector>) -> bool {
					throw_as_assert;
				});
		}

	private:
		PayloadType type_;
		FieldsSet fields_;
	};
	struct DistinctHasher {
		DistinctHasher(const PayloadType& type, const FieldsSet& fields) : type_(type), fields_(fields) {}
		size_t operator()(const Variant& v) const {
			return v.Type().EvaluateOneOf(
				[&](OneOf<KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float, KeyValueType::String, KeyValueType::Bool,
						  KeyValueType::Int, KeyValueType::Uuid>) noexcept { return v.Hash(); },
				[&](KeyValueType::Composite) { return ConstPayload(type_, static_cast<const PayloadValue&>(v)).GetHash(fields_); },
				[](OneOf<KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::FloatVector>) -> size_t {
					throw_as_assert;
				});
		}

	private:
		PayloadType type_;
		FieldsSet fields_;
	};

	class DistinctChangeChecker {
	public:
		DistinctChangeChecker(const Aggregator& aggregator) noexcept : aggregator_(aggregator) {}
		[[nodiscard]] bool operator()() noexcept {
			assertrx_dbg(aggregator_.Type() == AggType::AggDistinct);
			assertrx_dbg(aggregator_.distincts_);
			size_t prev = lastCheckSize_;
			lastCheckSize_ = aggregator_.distincts_->size();
			return aggregator_.distincts_->size() > prev;
		}

	private:
		const Aggregator& aggregator_;
		size_t lastCheckSize_ = 0;
	} distinctChecker_;

	typedef std::unordered_set<Variant, DistinctHasher, RelaxVariantCompare> HashSetVariantRelax;
	std::unique_ptr<HashSetVariantRelax> distincts_;
	bool compositeIndexFields_;
};

}  // namespace reindexer
