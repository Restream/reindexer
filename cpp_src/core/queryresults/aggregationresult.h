#pragma once

#include <string>
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "estl/span.h"
#include "estl/string_view.h"
#include "tools/errors.h"

struct msgpack_object;

namespace reindexer {
class WrSerializer;

const string_view kParamValue = "value";
const string_view kParamType = "type";
const string_view kParamFacets = "facets";
const string_view kParamCount = "count";
const string_view kParamValues = "values";
const string_view kParamDistincts = "distincts";
const string_view kParamFields = "fields";

struct FacetResult {
	FacetResult(const h_vector<std::string, 1> &v, int c) : values(v), count(c) {}
	FacetResult() : count(0) {}
	h_vector<std::string, 1> values;
	int count;
};

struct AggregationResult {
	void GetJSON(WrSerializer &ser) const;
	void GetMsgPack(WrSerializer &wrser) const;
	Error FromJSON(span<char> json);
	Error FromMsgPack(span<char> msgpack);
	AggType type = AggSum;
	h_vector<std::string, 1> fields;
	double value = 0;
	std::vector<FacetResult> facets;
	std::vector<std::string> distincts;

	static AggType strToAggType(string_view type);
	static string_view aggTypeToStr(AggType type);

	template <typename Node>
	void from(Node root) {
		value = root[kParamValue].template As<double>();
		type = strToAggType(root[kParamType].template As<std::string>());

		for (auto subElem : root[kParamFields]) {
			fields.push_back(subElem.template As<std::string>());
		}

		for (auto facetNode : root[kParamFacets]) {
			FacetResult facet;
			facet.count = facetNode[kParamCount].template As<int>();
			for (auto subElem : facetNode[kParamValues]) {
				facet.values.push_back(subElem.template As<std::string>());
			}
			facets.push_back(facet);
		}

		for (auto distinctNode : root[kParamDistincts]) {
			distincts.emplace_back(distinctNode.template As<std::string>());
		}
	}

	template <typename Builder>
	void get(Builder &builder) const {
		if (value != 0) builder.Put(kParamValue, value);
		builder.Put(kParamType, aggTypeToStr(type));
		if (!facets.empty()) {
			auto facetsArray = builder.Array(kParamFacets, facets.size());
			for (auto &facet : facets) {
				auto facetObj = facetsArray.Object(nullptr, 2);
				facetObj.Put(kParamCount, facet.count);
				auto valuesArray = facetObj.Array(kParamValues, facet.values.size());
				for (const auto &v : facet.values) valuesArray.Put(nullptr, v);
			}
		}

		if (!distincts.empty()) {
			auto distinctsArray = builder.Array(kParamDistincts, distincts.size());
			for (const std::string &v : distincts) distinctsArray.Put(nullptr, v);
		}

		auto fieldsArray = builder.Array(kParamFields, fields.size());
		for (auto &v : fields) fieldsArray.Put(nullptr, v);
		fieldsArray.End();
	}
};

};	// namespace reindexer
