#pragma once

#include <optional>
#include <string>
#include <string_view>
#include "core/keyvalue/variant.h"
#include "core/payload/fieldsset.h"
#include "core/payload/payloadtype.h"
#include "core/type_consts.h"
#include "core/type_consts_helpers.h"
#include "estl/expected.h"
#include "estl/h_vector.h"
#include <span>

struct msgpack_object;
struct MsgPackValue;
namespace gason {
struct JsonNode;
}

namespace reindexer {
class WrSerializer;
class ProtobufSchemaBuilder;

struct Parameters {
	constexpr static std::string_view Value() noexcept;
	constexpr static std::string_view Type() noexcept;
	constexpr static std::string_view Facets() noexcept;
	constexpr static std::string_view Count() noexcept;
	constexpr static std::string_view Values() noexcept;
	constexpr static std::string_view Distincts() noexcept;
	constexpr static std::string_view Fields() noexcept;
};

template <typename T, typename K>
class ParametersFields {
public:
	explicit ParametersFields(const T& fieldsStorage) : fieldsStorage_(fieldsStorage) {}

	K Value() const { return fieldsStorage_.at(Parameters::Value()); }
	K Type() const { return fieldsStorage_.at(Parameters::Type()); }
	K Facets() const { return fieldsStorage_.at(Parameters::Facets()); }
	K Count() const { return fieldsStorage_.at(Parameters::Count()); }
	K Values() const { return fieldsStorage_.at(Parameters::Values()); }
	K Distincts() const { return fieldsStorage_.at(Parameters::Distincts()); }
	K Fields() const { return fieldsStorage_.at(Parameters::Fields()); }

private:
	const T& fieldsStorage_;
};

struct FacetResult {
	FacetResult(const h_vector<std::string, 1>& v, int c) noexcept : values(v), count(c) {}
	FacetResult() noexcept : count(0) {}

	h_vector<std::string, 1> values;
	int count;
};

struct AggregationResult {
	void GetJSON(WrSerializer& ser) const;
	void GetMsgPack(WrSerializer& wrser) const;
	void GetProtobuf(WrSerializer& wrser) const;
	template <typename T>
	static Expected<AggregationResult> FromJSON(T json);
	static Expected<AggregationResult> FromMsgPack(std::string_view msgpack);
	static Expected<AggregationResult> FromMsgPack(std::span<char> msgpack) {
		return FromMsgPack(std::string_view(msgpack.data(), msgpack.size()));
	}
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
	static void GetProtobufSchema(ProtobufSchemaBuilder&);

	template <typename Builder, typename Fields>
	void get(Builder& builder, const Fields& parametersFields) const {
		if (value_) {
			builder.Put(parametersFields.Value(), *value_);
		}
		builder.Put(parametersFields.Type(), AggTypeToStr(type));
		if (!facets.empty()) {
			auto facetsArray = builder.Array(parametersFields.Facets(), facets.size());
			for (auto& facet : facets) {
				auto facetObj = facetsArray.Object(0, 2);
				facetObj.Put(parametersFields.Count(), facet.count);
				auto valuesArray = facetObj.Array(parametersFields.Values(), facet.values.size());
				for (const auto& v : facet.values) {
					valuesArray.Put(0, v);
				}
			}
		}

		if (!distincts.empty()) {
			auto distinctsArray = builder.Array(parametersFields.Distincts(), distincts.size());
			for (const Variant& v : distincts) {
				distinctsArray.Put(0, v.As<std::string>(payloadType, distinctsFields));
			}
		}

		auto fieldsArray = builder.Array(parametersFields.Fields(), fields.size());
		for (auto& v : fields) {
			fieldsArray.Put(0, v);
		}
		fieldsArray.End();
	}
	template <typename S>
	S& DumpFields(S& os) {
		os << '[';
		bool first = true;
		for (const auto& f : fields) {
			if (!first) {
				os << ", ";
			}
			first = false;
			os << f;
		}
		os << ']';
		return os;
	}

private:
	template <typename Node>
	static AggregationResult from(Node root);

	std::optional<double> value_ = std::nullopt;
};

}  // namespace reindexer
