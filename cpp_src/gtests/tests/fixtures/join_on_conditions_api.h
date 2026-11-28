#pragma once

#include "core/keyvalue/condition_comparison.h"
#include "join_selects_api.h"

namespace reindexer_tests {

class [[nodiscard]] JoinOnConditionsApi : public JoinSelectsApi {
public:
	void SetUp() override { JoinSelectsApi::Init(reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "join_on_conditions_test")); }

	void CreateCondSetTable(const std::string& leftNs, const std::string& rightNs, const std::vector<int>& leftNsData,
							const std::vector<std::vector<int>>& rightNsData) {
		using reindexer::IndexOpts;

		rt.OpenNamespace(leftNs);
		DefineNamespaceDataset(leftNs, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});
		rt.OpenNamespace(rightNs);
		DefineNamespaceDataset(rightNs, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});

		for (auto id : leftNsData) {
			reindexer::WrSerializer ser;
			reindexer::JsonBuilder builder(ser);
			builder.Put("id", id);
			builder.End();
			rt.InsertJSON(leftNs, ser.Slice());
		}

		for (unsigned int i = 0; i < rightNsData.size(); i++) {
			reindexer::WrSerializer ser;
			reindexer::JsonBuilder builder(ser);
			builder.Put("id", i + 10);
			{
				reindexer::JsonBuilder node = builder.Array("set");
				for (auto d : rightNsData[i]) {
					node.Put(reindexer::TagName::Empty(), d);
				}
			}
			builder.End();
			rt.InsertJSON(rightNs, ser.Slice());
		}
	}

	static bool CompareVariants(const Variant& v1, const Variant& v2, CondType condType) {
		const auto result = reindexer::CompareForCondition<reindexer::WithString::No, reindexer::NotComparable::Return,
														   reindexer::kWhereCompareNullHandling>(v1, v2);
		switch (condType) {
			case CondLt:
				return result == reindexer::ComparationResult::Lt;
			case CondLe:
				return result & reindexer::ComparationResult::Le;
			case CondGt:
				return result == reindexer::ComparationResult::Gt;
			case CondGe:
				return result & reindexer::ComparationResult::Ge;
			case CondSet:
			case CondEq:
				return result == reindexer::ComparationResult::Eq;
			case CondAny:
			case CondRange:
			case CondAllSet:
			case CondEmpty:
			case CondLike:
			case CondDWithin:
			case CondKnn:
			default:
				throw Error(errLogic, "Not supported condition!");
		}
	}

	static std::string GetSql(const std::string& sql, CondType condType) {
		switch (condType) {
			case CondLt:
				return fmt::format(fmt::runtime(sql), "<");
			case CondLe:
				return fmt::format(fmt::runtime(sql), "<=");
			case CondGt:
				return fmt::format(fmt::runtime(sql), ">");
			case CondGe:
				return fmt::format(fmt::runtime(sql), ">=");
			case CondEq:
				return fmt::format(fmt::runtime(sql), "=");
			case CondSet:
				return fmt::format(fmt::runtime(sql), "in");
			case CondAny:
			case CondRange:
			case CondAllSet:
			case CondEmpty:
			case CondLike:
			case CondDWithin:
			case CondKnn:
			default:
				throw Error(errLogic, "Not supported condition!");
		}
	}
};

}  // namespace reindexer_tests
