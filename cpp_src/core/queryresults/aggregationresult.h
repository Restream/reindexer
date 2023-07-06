#pragma once

#include <optional>
#include <string>
#include <string_view>
#include "core/keyvalue/variant.h"
#include "core/payload/fieldsset.h"
#include "core/payload/payloadtype.h"
#include "core/type_consts.h"
#include "core/type_consts_helpers.h"
#include "estl/h_vector.h"
#include "estl/span.h"
#include "tools/errors.h"

struct msgpack_object;
struct MsgPackValue;
namespace gason {
struct JsonNode;
}

namespace reindexer {
class WrSerializer;
class ProtobufSchemaBuilder;

struct Parameters {
	static std::string_view Value() noexcept;
	static std::string_view Type() noexcept;
	static std::string_view Facets() noexcept;
	static std::string_view Count() noexcept;
	static std::string_view Values() noexcept;
	static std::string_view Distincts() noexcept;
	static std::string_view Fields() noexcept;
};

template <typename T, typename K>
class ParametersFields {
public:
	explicit ParametersFields(const T &fieldsStorage) : fieldsStorage_(fieldsStorage) {}

	K Value() const { return fieldsStorage_.at(Parameters::Value()); }
	K Type() const { return fieldsStorage_.at(Parameters::Type()); }
	K Facets() const { return fieldsStorage_.at(Parameters::Facets()); }
	K Count() const { return fieldsStorage_.at(Parameters::Count()); }
	K Values() const { return fieldsStorage_.at(Parameters::Values()); }
	K Distincts() const { return fieldsStorage_.at(Parameters::Distincts()); }
	K Fields() const { return fieldsStorage_.at(Parameters::Fields()); }

private:
	const T &fieldsStorage_;
};

struct FacetResult {
	FacetResult(const h_vector<std::string, 1> &v, int c) noexcept : values(v), count(c) {}
	FacetResult() noexcept : count(0) {}

	h_vector<std::string, 1> values;
	int count;
};

struct AggregationResult {
	void GetJSON(WrSerializer &ser) const;
	void GetMsgPack(WrSerializer &wrser) const;
	void GetProtobuf(WrSerializer &wrser) const;
	Error FromJSON(span<char> json);
	Error FromMsgPack(span<char> msgpack);
	double GetValueOrZero() const noexcept { return value_ ? *value_ : 0; }
	std::optional<double> GetValue() const noexcept { return value_; }
	void SetValue(double value) { value_ = value; }
	AggType type = AggSum;
	h_vector<std::string, 1> fields;
	std::vector<FacetResult> facets;
	VariantArray distincts;
	FieldsSet distinctsFields;
	PayloadType payloadType;

	static AggType strToAggType(std::string_view type);
	static void GetProtobufSchema(ProtobufSchemaBuilder &);

	template <typename Node>
	void from(Node root) {
		const Node &node = root[Parameters::Value()];
		bool isValid = false;
		if constexpr (std::is_same_v<MsgPackValue, Node>) isValid = node.isValid();
		if constexpr (std::is_same_v<gason::JsonNode, Node>) isValid = !node.empty();
		if (isValid) value_ = node.template As<double>();

		type = strToAggType(root[Parameters::Type()].template As<std::string>());

		for (auto subElem : root[Parameters::Fields()]) {
			fields.push_back(subElem.template As<std::string>());
		}

		for (auto facetNode : root[Parameters::Facets()]) {
			FacetResult facet;
			facet.count = facetNode[Parameters::Count()].template As<int>();
			for (auto subElem : facetNode[Parameters::Values()]) {
				facet.values.push_back(subElem.template As<std::string>());
			}
			facets.push_back(facet);
		}

		for (auto distinctNode : root[Parameters::Distincts()]) {
			distincts.emplace_back(distinctNode.template As<std::string>());
		}
	}

	template <typename Builder, typename Fields>
	void get(Builder &builder, const Fields &parametersFields) const {
		if (value_) builder.Put(parametersFields.Value(), *value_);
		builder.Put(parametersFields.Type(), AggTypeToStr(type));
		if (!facets.empty()) {
			auto facetsArray = builder.Array(parametersFields.Facets(), facets.size());
			for (auto &facet : facets) {
				auto facetObj = facetsArray.Object(0, 2);
				facetObj.Put(parametersFields.Count(), facet.count);
				auto valuesArray = facetObj.Array(parametersFields.Values(), facet.values.size());
				for (const auto &v : facet.values) valuesArray.Put(0, v);
			}
		}

		if (!distincts.empty()) {
			auto distinctsArray = builder.Array(parametersFields.Distincts(), distincts.size());
			for (const Variant &v : distincts) {
				distinctsArray.Put(0, v.As<std::string>(payloadType, distinctsFields));
			}
		}

		auto fieldsArray = builder.Array(parametersFields.Fields(), fields.size());
		for (auto &v : fields) fieldsArray.Put(0, v);
		fieldsArray.End();
	}

private:
	std::optional<double> value_ = std::nullopt;
};

};	// namespace reindexer
