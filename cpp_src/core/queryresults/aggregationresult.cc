
#include "aggregationresult.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "gason/gason.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"
#include "vendor/msgpack/msgpackparser.h"

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
		case AggDistinct:
			return "distinct"_sv;
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
	} else if (type == "distinct"_sv) {
		return AggDistinct;
	}
	return AggUnknown;
}

void AggregationResult::GetJSON(WrSerializer &ser) const {
	JsonBuilder builder(ser);
	return get(builder);
}

void AggregationResult::GetMsgPack(WrSerializer &wrser) const {
	int elements = 2;
	if (value != 0) ++elements;
	if (!facets.empty()) ++elements;
	if (!distincts.empty()) ++elements;
	MsgPackBuilder msgpackBuilder(wrser, ObjType::TypeObject, elements);
	get(msgpackBuilder);
}

Error AggregationResult::FromMsgPack(span<char> msgpack) {
	try {
		size_t offset = 0;
		MsgPackParser parser;
		MsgPackValue root = parser.Parse(msgpack, offset);
		if (!root.p) {
			return Error(errLogic, "Error unpacking aggregation data in msgpack");
		}
		from(root);
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error AggregationResult::FromJSON(span<char> json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		from(root);
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "AggregationResult: %s", ex.what());
	}
	return errOK;
}

}  // namespace reindexer
