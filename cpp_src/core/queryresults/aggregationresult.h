#pragma once

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include "core/keyvalue/variant.h"
#include "core/payload/fieldsset.h"
#include "core/payload/payloadtype.h"
#include "core/type_consts.h"
#include "estl/concepts.h"
#include "estl/expected.h"
#include "estl/h_vector.h"

struct msgpack_object;
struct MsgPackValue;
namespace gason {
struct JsonNode;
}

namespace reindexer {
class WrSerializer;

namespace builders {
class ProtobufSchemaBuilder;
class ProtobufBuilder;
}  // namespace builders
using builders::ProtobufBuilder;
using builders::ProtobufSchemaBuilder;

struct [[nodiscard]] Parameters {
	constexpr static std::string_view Value() noexcept;
	constexpr static std::string_view Type() noexcept;
	constexpr static std::string_view Facets() noexcept;
	constexpr static std::string_view Count() noexcept;
	constexpr static std::string_view Values() noexcept;
	constexpr static std::string_view Distincts() noexcept;
	constexpr static std::string_view Fields() noexcept;
};

template <typename T, typename K>
class [[nodiscard]] ParametersFields {
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

struct [[nodiscard]] FacetResult {
	h_vector<std::string, 1> values;
	int count = 0;
};

class [[nodiscard]] AggregationResult {
public:
	AggregationResult() noexcept;
	explicit AggregationResult(AggType tp, h_vector<std::string, 1>&& names) noexcept;
	explicit AggregationResult(AggType tp, h_vector<std::string, 1>&& names, double val) noexcept;
	explicit AggregationResult(AggType tp, h_vector<std::string, 1>&& names, PayloadType&& pt, FieldsSet&& fset,
							   std::vector<Variant>&& _distincts) noexcept;
	explicit AggregationResult(AggType tp, h_vector<std::string, 1>&& names, std::vector<FacetResult>&& facets) noexcept;
	~AggregationResult();
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
	AggType GetType() const noexcept { return type_; }
	void UpdateValue(double value) noexcept { value_ = value; }

	const std::vector<FacetResult>& GetFacets() const& noexcept { return facets_; }
	auto GetFacets() const&& = delete;

	static AggType StrToAggType(std::string_view type);
	static void GetProtobufSchema(ProtobufSchemaBuilder&);

	bool IsEquals(const AggregationResult& other) { return (type_ == other.type_ && fields_ == other.fields_); }

	const PayloadType& GetPayloadType() const& noexcept { return payloadType_; }
	auto GetPayloadType() const&& = delete;
	const FieldsSet& GetDistinctFields() const& noexcept { return distinctsFields_; }
	auto GetDistinctFields() const&& = delete;

	std::span<const Variant> GetDistinctRow(unsigned index) const {
		if ((index + 1) * fields_.size() > distincts_.size()) {
			throw Error(errLogic, std::string("Incorrect distinct index ") + std::to_string(index));
		}
		return std::span{distincts_.begin() + index * fields_.size(), fields_.size()};
	}
	unsigned int GetDistinctRowCount() const { return distincts_.size() / fields_.size(); }
	unsigned int GetDistinctColumnCount() const noexcept { return fields_.size(); }

	template <typename T>
	T As(unsigned int row, unsigned int column) const {
		return distincts_[row * fields_.size() + column].As<T>(payloadType_, distinctsFields_);
	}

	const h_vector<std::string, 1>& GetFields() const& noexcept { return fields_; }
	auto GetFields() const&& = delete;

	template <typename S>
	S& DumpFields(S& os) {
		os << '[';
		bool first = true;
		for (const auto& f : fields_) {
			if (!first) {
				os << ", ";
			} else {
				first = false;
			}
			os << f;
		}
		os << ']';
		return os;
	}

private:
	AggType type_ = AggSum;
	h_vector<std::string, 1> fields_;
	std::vector<FacetResult> facets_;
	std::vector<Variant> distincts_;
	FieldsSet distinctsFields_;
	PayloadType payloadType_;

	template <typename Builder, typename Fields>
	void get(Builder& builder, const Fields& parametersFields) const;

	template <concepts::OneOf<gason::JsonNode, MsgPackValue> Node>
	static AggregationResult from(Node root);

	std::optional<double> value_ = std::nullopt;

	template <typename Builder, typename Fields>
	void serialiseDistinct(Builder& builder, const Fields& parametersFields) const;

	template <typename Fields>
	void serialiseDistinct(ProtobufBuilder& builder, const Fields& parametersFields) const;
};

}  // namespace reindexer
