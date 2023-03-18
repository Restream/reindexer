
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

using namespace std::string_view_literals;

std::string_view Parameters::Value() noexcept { return "value"; }
std::string_view Parameters::Type() noexcept { return "type"; }
std::string_view Parameters::Facets() noexcept { return "facets"; }
std::string_view Parameters::Count() noexcept { return "count"; }
std::string_view Parameters::Values() noexcept { return "values"; }
std::string_view Parameters::Distincts() noexcept { return "distincts"; }
std::string_view Parameters::Fields() noexcept { return "fields"; }

using ParametersFieldsNumbers = const std::unordered_map<std::string_view, int>;
ParametersFieldsNumbers kParametersFieldNumbers = {{Parameters::Value(), 1},  {Parameters::Type(), 2},	 {Parameters::Count(), 1},
												   {Parameters::Values(), 2}, {Parameters::Facets(), 3}, {Parameters::Distincts(), 4},
												   {Parameters::Fields(), 5}};

struct ParameterFieldGetter {
	std::string_view at(std::string_view field) const { return field; }
};

std::string_view AggregationResult::aggTypeToStr(AggType type) {
	switch (type) {
		case AggMax:
			return "max"sv;
		case AggMin:
			return "min"sv;
		case AggSum:
			return "sum"sv;
		case AggFacet:
			return "facet"sv;
		case AggAvg:
			return "avg"sv;
		case AggDistinct:
			return "distinct"sv;
		case AggCount:
			return "count"sv;
		case AggCountCached:
			return "count_cached"sv;
		default:
			return "?"sv;
	}
}

AggType AggregationResult::strToAggType(std::string_view type) {
	if (type == "avg"sv) {
		return AggAvg;
	} else if (type == "facet"sv) {
		return AggFacet;
	} else if (type == "sum"sv) {
		return AggSum;
	} else if (type == "min"sv) {
		return AggMin;
	} else if (type == "max"sv) {
		return AggMax;
	} else if (type == "distinct"sv) {
		return AggDistinct;
	} else if (type == "count"sv) {
		return AggCount;
	} else if (type == "count_cached"sv) {
		return AggCountCached;
	}
	return AggUnknown;
}

void AggregationResult::GetJSON(WrSerializer &ser) const {
	JsonBuilder builder(ser);
	ParameterFieldGetter fieldsGetter;
	get(builder, ParametersFields<ParameterFieldGetter, std::string_view>(fieldsGetter));
}

void AggregationResult::GetMsgPack(WrSerializer &wrser) const {
	int elements = 2;
	if (value_) ++elements;
	if (!facets.empty()) ++elements;
	if (!distincts.empty()) ++elements;
	MsgPackBuilder msgpackBuilder(wrser, ObjType::TypeObject, elements);
	ParameterFieldGetter fieldsGetter;
	get(msgpackBuilder, ParametersFields<ParameterFieldGetter, std::string_view>(fieldsGetter));
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
	results.Field(Parameters::Value(), fields.Value(), FieldProps{KeyValueType::Double{}});
	results.Field(Parameters::Type(), fields.Type(), FieldProps{KeyValueType::String{}});
	{
		ProtobufSchemaBuilder facets = results.Object(fields.Facets(), "Facets");
		facets.Field(Parameters::Count(), fields.Count(), FieldProps{KeyValueType::Int{}});
		facets.Field(Parameters::Values(), fields.Values(), FieldProps{KeyValueType::String{}, true});
	}
	results.Field(Parameters::Facets(), fields.Facets(), FieldProps{KeyValueType::Tuple{}, true, false, false, "Facets"});
	results.Field(Parameters::Distincts(), fields.Distincts(), FieldProps{KeyValueType::String{}, true});
	results.Field(Parameters::Fields(), fields.Fields(), FieldProps{KeyValueType::String{}, true});
	results.End();
}

}  // namespace reindexer
