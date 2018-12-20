
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
	if (!field.empty()) builder.Put("field", field);
	builder.Put("type", aggTypeToStr(type));

	if (facets.size()) {
		auto arrNode = builder.Array("facets");
		for (auto &facet : facets) {
			auto objNode = arrNode.Object();
			objNode.Put("value", facet.value);
			objNode.Put("count", facet.count);
		}
	}
}

Error AggregationResult::FromJSON(char *json) {
	JsonAllocator jalloc;
	JsonValue jvalue;
	char *endp;

	int status = jsonParse(json, &endp, &jvalue, jalloc);
	if (status != JSON_OK) return Error(errParseJson, "Malformed JSON with aggregation results");
	if (jvalue.getTag() != JSON_OBJECT) return Error(errParseJson, "Expected json object in 'aggregation' key");

	try {
		for (auto elem : jvalue) {
			parseJsonField("value", value, elem);
			parseJsonField("field", field, elem);
			if ("type"_sv == elem->key && elem->value.getTag() == JSON_STRING) {
				type = strToAggType(elem->value.toString());
			}
			if ("facets"_sv == elem->key) {
				if (elem->value.getTag() != JSON_ARRAY) return Error(errParseJson, "Expected json array in 'facets' key");
				for (auto subElem : elem->value) {
					if (subElem->value.getTag() != JSON_OBJECT) return Error(errParseJson, "Expected json object in array of 'facets'");
					FacetResult facet;
					for (auto objElem : subElem->value) {
						parseJsonField("value", facet.value, objElem);
						parseJsonField("count", facet.count, objElem);
					}
					facets.push_back(facet);
				}
			}
		}
	} catch (const Error &error) {
		return error;
	}
	return errOK;
}

}  // namespace reindexer
