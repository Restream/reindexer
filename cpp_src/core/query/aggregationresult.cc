
#include "aggregationresult.h"
#include "core/cjson/jsonbuilder.h"
#include "gason/gason.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"

namespace reindexer {
void AggregationResult::GetJSON(WrSerializer &ser) const {
	JsonBuilder builder(ser);

	if (value != 0) builder.Put("value", value);
	if (!name.empty()) builder.Put("name", name);
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
			parseJsonField("name", name, elem);
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
