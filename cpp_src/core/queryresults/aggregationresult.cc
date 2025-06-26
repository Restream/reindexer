#include "aggregationresult.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/protobufbuilder.h"
#include "core/cjson/protobufschemabuilder.h"
#include "core/schema.h"
#include "gason/gason.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"
#include "vendor/frozen/string.h"
#include "vendor/frozen/unordered_map.h"
#include "vendor/msgpack/msgpackparser.h"

namespace reindexer {

using namespace std::string_view_literals;

constexpr std::string_view Parameters::Value() noexcept { return "value"sv; }
constexpr std::string_view Parameters::Type() noexcept { return "type"sv; }
constexpr std::string_view Parameters::Facets() noexcept { return "facets"sv; }
constexpr std::string_view Parameters::Count() noexcept { return "count"sv; }
constexpr std::string_view Parameters::Values() noexcept { return "values"sv; }
constexpr std::string_view Parameters::Distincts() noexcept { return "distincts"sv; }
constexpr std::string_view Parameters::Fields() noexcept { return "fields"sv; }

// clang-format off
constexpr auto kParametersFieldNumbers = frozen::make_unordered_map<frozen::string, TagName>(
															{{Parameters::Value(),  1_Tag}, {Parameters::Type(),      2_Tag},
															 {Parameters::Count(),  1_Tag}, {Parameters::Values(),    2_Tag},
															 {Parameters::Facets(), 3_Tag}, {Parameters::Distincts(), 4_Tag},
															 {Parameters::Fields(), 5_Tag}});
// clang-format on
using ParametersFieldsNumbers = decltype(kParametersFieldNumbers);

struct ParameterFieldGetter {
	std::string_view at(std::string_view field) const { return field; }
};

AggType AggregationResult::StrToAggType(std::string_view type) {
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

AggregationResult::AggregationResult() noexcept = default;

AggregationResult::AggregationResult(AggType tp, h_vector<std::string, 1>&& names) noexcept : type_(tp), fields_(std::move(names)) {}

AggregationResult::AggregationResult(AggType tp, h_vector<std::string, 1>&& names, double val) noexcept
	: type_(tp), fields_(std::move(names)), value_(val) {}

AggregationResult::AggregationResult(AggType tp, h_vector<std::string, 1>&& names, PayloadType&& pt, FieldsSet&& fset,
									 std::vector<Variant>&& _distincts) noexcept
	: type_(tp),
	  fields_(std::move(names)),
	  distincts_(std::move(_distincts)),
	  distinctsFields_(std::move(fset)),
	  payloadType_(std::move(pt)) {}

AggregationResult::AggregationResult(AggType tp, h_vector<std::string, 1>&& names, std::vector<FacetResult>&& facets) noexcept
	: type_(tp), fields_(std::move(names)), facets_(std::move(facets)) {}

AggregationResult::~AggregationResult() = default;

void AggregationResult::GetJSON(WrSerializer& ser) const {
	JsonBuilder builder(ser);
	ParameterFieldGetter fieldsGetter;
	get(builder, ParametersFields<ParameterFieldGetter, std::string_view>(fieldsGetter));
}

void AggregationResult::GetMsgPack(WrSerializer& wrser) const {
	int elements = 2;
	if (value_) {
		++elements;
	}
	if (!facets_.empty()) {
		++elements;
	}
	if (!distincts_.empty()) {
		++elements;
	}
	MsgPackBuilder msgpackBuilder(wrser, ObjType::TypeObject, elements);
	ParameterFieldGetter fieldsGetter;
	get(msgpackBuilder, ParametersFields<ParameterFieldGetter, std::string_view>(fieldsGetter));
}

void AggregationResult::GetProtobuf(WrSerializer& wrser) const {
	ProtobufBuilder builder(&wrser, ObjType::TypePlain);
	get(builder, ParametersFields<ParametersFieldsNumbers, TagName>(kParametersFieldNumbers));
}

template <concepts::OneOf<gason::JsonNode, MsgPackValue> Node>
AggregationResult AggregationResult::from(Node root) {
	const Node& node = root[Parameters::Value()];
	bool isValid = false;
	if constexpr (std::is_same_v<MsgPackValue, Node>) {
		isValid = node.isValid();
	}
	if constexpr (std::is_same_v<gason::JsonNode, Node>) {
		isValid = !node.empty();
	}
	AggregationResult ret;
	if (isValid) {
		ret.value_ = node.template As<double>();
	}

	ret.type_ = StrToAggType(root[Parameters::Type()].template As<std::string>());

	for (const auto& subElem : root[Parameters::Fields()]) {
		ret.fields_.emplace_back(subElem.template As<std::string>());
	}

	for (const auto& facetNode : root[Parameters::Facets()]) {
		FacetResult facet;
		facet.count = facetNode[Parameters::Count()].template As<int>();
		for (const auto& subElem : facetNode[Parameters::Values()]) {
			facet.values.emplace_back(subElem.template As<std::string>());
		}
		ret.facets_.emplace_back(std::move(facet));
	}

	for (const auto& distinctNode : root[Parameters::Distincts()]) {
		bool isArray = false;
		if constexpr (std::is_same_v<gason::JsonNode, Node>) {
			isArray = distinctNode.value.getTag() == gason::JsonTag::ARRAY ? true : false;
		} else if constexpr (std::is_same_v<MsgPackValue, Node>) {
			isArray = distinctNode.getTag() == MsgPackTag::MSGPACK_ARRAY ? true : false;
		}
		if (isArray) {
			unsigned column = 0;
			for (const auto& v : distinctNode) {
				ret.distincts_.emplace_back(v.template As<std::string>());
				column++;
			}
			if (column != ret.fields_.size()) {
				throw Error(errParseJson, "Incorrect distinct column count {} ({})", column, ret.fields_.size());
			}
		} else {
			ret.distincts_.emplace_back(distinctNode.template As<std::string>());
		}
	}
	return ret;
}

Expected<AggregationResult> AggregationResult::FromMsgPack(std::string_view msgpack) {
	try {
		size_t offset = 0;
		MsgPackParser parser;
		MsgPackValue root = parser.Parse(msgpack, offset);
		if (!root.p) {
			return Unexpected{Error{errLogic, "Error unpacking aggregation data in msgpack"}};
		}
		return from(root);
	} catch (const Error& err) {
		return Unexpected(err);
	}
}

template <typename T>
Expected<AggregationResult> AggregationResult::FromJSON(T json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		return from(root);
	} catch (const gason::Exception& ex) {
		return Unexpected{Error{errParseJson, "AggregationResult: {}", ex.what()}};
	}
}

template Expected<AggregationResult> AggregationResult::FromJSON<std::string_view>(std::string_view json);
template Expected<AggregationResult> AggregationResult::FromJSON<std::span<char>>(std::span<char> json);

void AggregationResult::GetProtobufSchema(ProtobufSchemaBuilder& builder) {
	ParametersFields<ParametersFieldsNumbers, TagName> fields(kParametersFieldNumbers);
	ProtobufSchemaBuilder results = builder.Object(TagName::Empty(), "AggregationResults");
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

template <typename Builder, typename Fields>
void AggregationResult::get(Builder& builder, const Fields& parametersFields) const {
	if (value_) {
		builder.Put(parametersFields.Value(), *value_);
	}
	builder.Put(parametersFields.Type(), AggTypeToStr(type_));
	if (!facets_.empty()) {
		auto facetsArray = builder.Array(parametersFields.Facets(), facets_.size());
		for (auto& facet : facets_) {
			auto facetObj = facetsArray.Object(TagName::Empty(), 2);
			facetObj.Put(parametersFields.Count(), facet.count);
			auto valuesArray = facetObj.Array(parametersFields.Values(), facet.values.size());
			for (const auto& v : facet.values) {
				valuesArray.Put(TagName::Empty(), v);
			}
		}
	}

	if (!distincts_.empty()) {
		if (std::is_same_v<ProtobufBuilder, Builder> && GetDistinctColumnCount() > 1) {
			throw Error(errParseProtobuf, "protobuf is not supported for distinct with multiple fields");
		}
		auto distinctsArray = builder.Array(parametersFields.Distincts(), GetDistinctRowCount());
		const unsigned dc = GetDistinctRowCount();

		if (GetDistinctColumnCount() == 1) {
			for (unsigned i = 0; i < dc; i++) {
				std::span<const Variant> row = GetDistinctRow(i);
				distinctsArray.Put(TagName::Empty(), row[0].As<std::string>(payloadType_, distinctsFields_));
			}
		} else {
			for (unsigned i = 0; i < dc; i++) {
				std::span<const Variant> row = GetDistinctRow(i);
				auto distinctsSubArray = distinctsArray.Array(std::string_view{}, row.size());
				for (const auto& vv : row) {
					distinctsSubArray.Put(TagName::Empty(), vv.As<std::string>());
				}
			}
		}
	}

	auto fieldsArray = builder.Array(parametersFields.Fields(), fields_.size());
	for (auto& v : fields_) {
		fieldsArray.Put(TagName::Empty(), v);
	}
	fieldsArray.End();
}

}  // namespace reindexer
