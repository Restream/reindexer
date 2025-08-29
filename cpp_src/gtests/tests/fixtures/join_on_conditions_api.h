#pragma once

#include "join_selects_api.h"

class [[nodiscard]] JoinOnConditionsApi : public JoinSelectsApi {
public:
	void SetUp() override { JoinSelectsApi::Init(reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "join_on_conditions_test")); }

	void CreateCondSetTable(const std::string& leftNs, const std::string& rightNs, const std::vector<int>& leftNsData,
							const std::vector<std::vector<int>>& rightNsData) {
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
		switch (condType) {
			case CondLt:
				return (v1 < v2);
			case CondLe:
				return (v1 < v2) || (v1 == v2);
			case CondGt:
				return (v1 > v2);
			case CondGe:
				return (v1 > v2) || (v1 == v2);
			case CondSet:
			case CondEq:
				return (v1 == v2);
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
