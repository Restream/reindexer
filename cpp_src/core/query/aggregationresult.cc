
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
			if ("fields"_sv == elem->key) {
				if (elem->value.getTag() != JSON_ARRAY) return Error(errParseJson, "Expected json array in 'fields' key");
				for (auto subElem : elem->value) {
					if (subElem->value.getTag() != JSON_STRING) return Error(errParseJson, "Expected string in array of 'fields'");
					fields.push_back(subElem->value.toString());
				}
			}
			if ("type"_sv == elem->key && elem->value.getTag() == JSON_STRING) {
				type = strToAggType(elem->value.toString());
			}
			if ("facets"_sv == elem->key) {
				if (elem->value.getTag() != JSON_ARRAY) return Error(errParseJson, "Expected json array in 'facets' key");
				for (auto subElem : elem->value) {
					if (subElem->value.getTag() != JSON_OBJECT) return Error(errParseJson, "Expected json object in array of 'facets'");
					FacetResult facet;
					for (auto objElem : subElem->value) {
						if ("values"_sv == objElem->key) {
							if (objElem->value.getTag() != JSON_ARRAY) return Error(errParseJson, "Expected json array in 'facets' key");
							for (auto subElem : objElem->value) {
								if (subElem->value.getTag() != JSON_STRING)
									return Error(errParseJson, "Expected string in array of 'facets'");
								facet.values.push_back(subElem->value.toString());
							}
						}
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
