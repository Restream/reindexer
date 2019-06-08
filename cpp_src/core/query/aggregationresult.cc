
#include "aggregationresult.h"
#include "core/cjson/jsonbuilder.h"
#include "gason/gason.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"

namespace reindexer {

string_view AggregationResult::aggTypeToStr(AggType type) {
	switch (type) {
		case AggMax:
			return "max"_sv;
		case AggMin:
			return "min"_sv;
		case AggSum:
			return "sum"_sv;
		case AggFacet:
			return "facet"_sv;
		case AggAvg:
			return "avg"_sv;
		default:
			return "?"_sv;
	}
}

AggType AggregationResult::strToAggType(string_view type) {
	if (type == "avg"_sv) {
		return AggAvg;
	} else if (type == "facet"_sv) {
		return AggFacet;
	} else if (type == "sum"_sv) {
		return AggSum;
	} else if (type == "min"_sv) {
		return AggMin;
	} else if (type == "max"_sv) {
		return AggMax;
	}
	return AggUnknown;
}

void AggregationResult::GetJSON(WrSerializer &ser) const {
	JsonBuilder builder(ser);

	if (value != 0) builder.Put("value", value);
	builder.Put("type", aggTypeToStr(type));

	if (facets.size()) {
		auto arrNode = builder.Array("facets");
		for (auto &facet : facets) {
			auto objNode = arrNode.Object();
			objNode.Put("count", facet.count);
			auto arrNode = objNode.Array("values");
			for (const auto &v : facet.values) {
				arrNode.Put(nullptr, v);
			}
		}
	}

	auto fldNode = builder.Array("fields");
	for (auto &field : fields) {
		fldNode.Put(nullptr, field);
	}
}

Error AggregationResult::FromJSON(span<char> json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);

		value = root["value"].As<double>();
		type = strToAggType(root["type"].As<string>());

		for (auto &subElem : root["fields"]) {
			fields.push_back(subElem.As<string>());
		}

		for (auto &facetNode : root["facets"]) {
			FacetResult facet;
			facet.count = facetNode["count"].As<int>();
			for (auto &subElem : facetNode["values"]) {
				facet.values.push_back(subElem.As<string>());
			}
			facets.push_back(facet);
		}

	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "AggregationResult: %s", ex.what());
	}
	return errOK;
}

}  // namespace reindexer
