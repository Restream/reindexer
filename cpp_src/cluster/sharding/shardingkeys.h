#pragma once

#include <functional>
#include <variant>
#include "core/keyvalue/variant.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "estl/overloaded.h"
#include "tools/stringstools.h"

namespace reindexer {

namespace cluster {
struct ShardingConfig;
}

namespace sharding {

constexpr size_t kHvectorConnStack = 9;
using ShardIDsContainer = h_vector<int, kHvectorConnStack>;

class [[nodiscard]] ShardingKeys {
public:
	struct [[nodiscard]] Variant4Segment : Variant {
		bool isRightBound = false;
	};

	struct [[nodiscard]] BaseCompare {
		bool IsInt(const Variant& lhs, const Variant& rhs) const noexcept {
			return (lhs.Type().Is<KeyValueType::Int64>() && rhs.Type().Is<KeyValueType::Int>()) ||
				   (lhs.Type().Is<KeyValueType::Int>() && rhs.Type().Is<KeyValueType::Int64>());
		}
		void ValidateTypes(const Variant& lhs, const Variant& rhs) const {
			if (!lhs.Type().IsSame(rhs.Type())) {
				throw Error(errLogic, "Comparator internal error. Different compared types. Left - {}. Right - {}", lhs.Type().Name(),
							rhs.Type().Name());
			}
		}
	};

	struct [[nodiscard]] FastHashMapCompare : BaseCompare {
		bool operator()(const Variant& lhs, const Variant& rhs) const { return compare(lhs, rhs); }

	private:
		bool compare(const Variant& lhs, const Variant& rhs) const {
			if (IsInt(lhs, rhs)) {
				return lhs.RelaxCompare<WithString::Yes, NotComparable::Throw, NullsHandling::NotComparable>(rhs) == ComparationResult::Eq;
			}

			ValidateTypes(lhs, rhs);
			return lhs == rhs;
		}
	};

	struct [[nodiscard]] MapCompare : BaseCompare {
		using is_transparent = void;

		bool operator()(const Variant4Segment& lhs, const Variant4Segment& rhs) const { return compare(lhs, rhs); }
		bool operator()(const Variant4Segment& lhs, const Variant& rhs) const { return compare(lhs, rhs); }
		bool operator()(const Variant& lhs, const Variant4Segment& rhs) const { return compare(lhs, rhs); }

	private:
		bool compare(const Variant& lhs, const Variant& rhs) const {
			if (IsInt(lhs, rhs)) {
				return lhs.RelaxCompare<WithString::Yes, NotComparable::Throw, NullsHandling::NotComparable>(rhs) == ComparationResult::Lt;
			}

			ValidateTypes(lhs, rhs);
			return lhs < rhs;
		}
	};

	using VariantHashMap = fast_hash_map<Variant, int, std::hash<Variant>, FastHashMapCompare>;
	using Variant4SegmentMap = std::map<Variant4Segment, int, MapCompare>;

	using ValuesData = std::variant<VariantHashMap, Variant4SegmentMap>;

	struct [[nodiscard]] ShardIndexWithValues {
		std::string_view name;
		const ValuesData* values;
	};

	explicit ShardingKeys(const reindexer::cluster::ShardingConfig& config);
	ShardIndexWithValues GetIndex(std::string_view nsName) const;
	int GetDefaultHost(std::string_view nsName) const;
	bool IsShardIndex(std::string_view ns, std::string_view index) const;
	int GetShardId(std::string_view ns, std::string_view index, const VariantArray& v, bool& isShardKey) const;
	int GetShardId(std::string_view ns, const Variant& v) const;
	ShardIDsContainer GetShardsIds(std::string_view ns) const;
	ShardIDsContainer GetShardsIds() const;
	bool IsSharded(std::string_view ns) const noexcept { return keys_.find(ns) != keys_.end(); }

	template <typename ValuesDataType>
	void FillUniqueIds(fast_hash_set<int>& uniqueIds, const ValuesDataType& keysToShard) const {
		for (auto itValuesData = keysToShard.begin(); itValuesData != keysToShard.end(); ++itValuesData) {
			uniqueIds.insert(itValuesData->second);
		}
	}

private:
	using NsName = std::string_view;
	struct [[nodiscard]] NsData {
		std::string_view indexName;
		ValuesData keysToShard;
		int defaultShard;

		int GetShardId(const Variant& val) const {
			return std::visit(
				overloaded{[&val, this](const VariantHashMap& values) {
							   auto it = values.find(val);
							   return (it == values.end()) ? defaultShard : it->second;
						   },
						   [&val, this](const Variant4SegmentMap& values) {
							   auto it = values.lower_bound(val);
							   if (it == values.end() ||
								   (!it->first.isRightBound &&
									it->first.RelaxCompare<WithString::Yes, NotComparable::Throw, NullsHandling::NotComparable>(val) !=
										ComparationResult::Eq)) {
								   return defaultShard;
							   }
							   return it->second;
						   }},
				keysToShard);
		}
	};
	fast_hash_set<int> getShardsIds(const NsData& ns) const;

	fast_hash_map<NsName, NsData, nocase_hash_str, nocase_equal_str, nocase_less_str> keys_;
	// ns
	//   value_i1 - shardNodeId1
	//   value_i2 - shardNodeId2
};

}  // namespace sharding
}  // namespace reindexer
