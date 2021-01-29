
#include "aggregationresult.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/protobufbuilder.h"
#include "core/cjson/protobufschemabuilder.h"
#include "core/schema.h"
#include "gason/gason.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"
#include "vendor/msgpack/msgpackparser.h"

#include <unordered_map>

namespace reindexer {

string_view Parameters::Value() noexcept { return "value"; }
string_view Parameters::Type() noexcept { return "type"; }
string_view Parameters::Facets() noexcept { return "facets"; }
string_view Parameters::Count() noexcept { return "count"; }
string_view Parameters::Values() noexcept { return "values"; }
string_view Parameters::Distincts() noexcept { return "distincts"; }
string_view Parameters::Fields() noexcept { return "fields"; }

using ParametersFieldsNumbers = const std::unordered_map<string_view, int>;
ParametersFieldsNumbers kParametersFieldNumbers = {{Parameters::Value(), 1},  {Parameters::Type(), 2},	 {Parameters::Count(), 1},
												   {Parameters::Values(), 2}, {Parameters::Facets(), 3}, {Parameters::Distincts(), 4},
												   {Parameters::Fields(), 5}};

struct ParameterFieldGetter {
	string_view at(string_view field) const { return field; }
};

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
		case AggCount:
			return "count"_sv;
		case AggCountCached:
			return "count_cached"_sv;
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
	} else if (type == "count"_sv) {
		return AggCount;
	} else if (type == "count_cached"_sv) {
		return AggCountCached;
	}
	return AggUnknown;
}

void AggregationResult::GetJSON(WrSerializer &ser) const {
	JsonBuilder builder(ser);
	ParameterFieldGetter fieldsGetter;
	get(builder, ParametersFields<ParameterFieldGetter, string_view>(fieldsGetter));
}

void AggregationResult::GetMsgPack(WrSerializer &wrser) const {
	int elements = 2;
	if (value != 0) ++elements;
	if (!facets.empty()) ++elements;
	if (!distincts.empty()) ++elements;
	MsgPackBuilder msgpackBuilder(wrser, ObjType::TypeObject, elements);
	ParameterFieldGetter fieldsGetter;
	get(msgpackBuilder, ParametersFields<ParameterFieldGetter, string_view>(fieldsGetter));
}

void AggregationResult::GetProtobuf(WrSerializer &wrser) const {
	ProtobufBuilder builder(&wrser, ObjType::TypePlain);
	get(builder, ParametersFields<ParametersFieldsNumbers, int>(kParametersFieldNumbers));
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

void AggregationResult::GetProtobufSchema(ProtobufSchemaBuilder &builder) {
	ParametersFields<ParametersFieldsNumbers, int> fields(kParametersFieldNumbers);
	ProtobufSchemaBuilder results = builder.Object(0, "AggregationResults");
	results.Field(Parameters::Value(), fields.Value(), FieldProps{KeyValueDouble});
	results.Field(Parameters::Type(), fields.Type(), FieldProps{KeyValueString});
	{
		ProtobufSchemaBuilder facets = results.Object(fields.Facets(), "Facets");
		facets.Field(Parameters::Count(), fields.Count(), FieldProps{KeyValueInt});
		facets.Field(Parameters::Values(), fields.Values(), FieldProps{KeyValueString, true});
	}
	results.Field(Parameters::Facets(), fields.Facets(), FieldProps{KeyValueTuple, true, false, false, "Facets"});
	results.Field(Parameters::Distincts(), fields.Distincts(), FieldProps{KeyValueString, true});
	results.Field(Parameters::Fields(), fields.Fields(), FieldProps{KeyValueString, true});
	results.End();
}

}  // namespace reindexer
