#pragma once

#include <optional>
#include <unordered_set>
#include "core/index/payload_map.h"
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

	Aggregator(const PayloadType &, const FieldsSet &, AggType aggType, const h_vector<std::string, 1> &names,
			   const h_vector<SortingEntry, 1> &sort = {}, size_t limit = UINT_MAX, size_t offset = 0, bool compositeIndexFields = false);
	Aggregator();
	Aggregator(Aggregator &&) noexcept;
	~Aggregator();

	void Aggregate(const PayloadValue &lhs);
	AggregationResult GetResult() const;

	Aggregator(const Aggregator &) = delete;
	Aggregator &operator=(const Aggregator &) = delete;
	Aggregator &operator=(Aggregator &&) = delete;

	AggType Type() const noexcept { return aggType_; }
	const h_vector<std::string, 1> &Names() const noexcept { return names_; }

private:
	enum Direction { Desc = -1, Asc = 1 };
	class MultifieldComparator;
	class SinglefieldComparator;
	struct MultifieldOrderedMap;
	using MultifieldUnorderedMap = unordered_payload_map<int, false>;
	using SinglefieldOrderedMap = btree::btree_map<Variant, int, SinglefieldComparator>;
	using SinglefieldUnorderedMap = fast_hash_map<Variant, int>;
	using Facets = std::variant<MultifieldOrderedMap, MultifieldUnorderedMap, SinglefieldOrderedMap, SinglefieldUnorderedMap>;

	void aggregate(const Variant &variant);

	PayloadType payloadType_;
	FieldsSet fields_;
	std::optional<double> result_ = std::nullopt;
	int hitCount_ = 0;
	AggType aggType_;
	h_vector<std::string, 1> names_;
	size_t limit_ = UINT_MAX;
	size_t offset_ = 0;

	std::unique_ptr<Facets> facets_;

	class RelaxVariantCompare {
	public:
		RelaxVariantCompare(const PayloadType &type, const FieldsSet &fields) : type_(type), fields_(fields) {}
		bool operator()(const Variant &v1, const Variant &v2) const {
			if (!v1.Type().IsSame(v2.Type())) return false;
			return v1.Type().EvaluateOneOf(
				[&](OneOf<KeyValueType::Int64, KeyValueType::Double, KeyValueType::String, KeyValueType::Bool, KeyValueType::Int,
						  KeyValueType::Uuid>) { return v1.Compare(v2) == 0; },
				[&](KeyValueType::Composite) {
					return ConstPayload(type_, static_cast<const PayloadValue &>(v1)).IsEQ(static_cast<const PayloadValue &>(v2), fields_);
				},
				[](OneOf<KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Undefined>) noexcept -> bool { abort(); });
		}

	private:
		PayloadType type_;
		FieldsSet fields_;
	};
	struct DistinctHasher {
		DistinctHasher(const PayloadType &type, const FieldsSet &fields) : type_(type), fields_(fields) {}
		size_t operator()(const Variant &v) const {
			return v.Type().EvaluateOneOf(
				[&](OneOf<KeyValueType::Int64, KeyValueType::Double, KeyValueType::String, KeyValueType::Bool, KeyValueType::Int,
						  KeyValueType::Uuid>) noexcept { return v.Hash(); },
				[&](KeyValueType::Composite) { return ConstPayload(type_, static_cast<const PayloadValue &>(v)).GetHash(fields_); },
				[](OneOf<KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Undefined>) noexcept -> size_t { abort(); });
			assertrx(type_);
			return 0;
		}

	private:
		PayloadType type_;
		FieldsSet fields_;
	};

	typedef std::unordered_set<Variant, DistinctHasher, RelaxVariantCompare> HashSetVariantRelax;
	std::unique_ptr<HashSetVariantRelax> distincts_;
	bool compositeIndexFields_;
};

}  // namespace reindexer
