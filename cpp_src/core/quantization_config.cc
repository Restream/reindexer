#include "core/cjson/jsonbuilder.h"
#include "core/index/float_vector/hnswlib/hnswlib.h"
#include "gason/gason.h"
#include "tools/errors.h"
#include "tools/float_comparison.h"

namespace hnswlib {
void QuantizationConfig::FromJSON(const gason::JsonNode& root) {
	quantizationType = decodeQuantizationType(root["quantization_type"].As<std::string_view>());
	if (!root["quantile"].isEmpty()) {
		quantile = root["quantile"].As<float>(1.f);
		if (quantile < 0.95f || quantile > 1.f) {
			throw reindexer::Error(errParams, "The quantile value must be within [0.95; 1.0]");
		}
	}

	sampleSize = root["sample_size"].As<size_t>(kDefaultSampleSize);
	quantizationThreshold = root["quantization_threshold"].As<size_t>(kDefaultQuantizationThreshold);
}

void QuantizationConfig::GetJSON(reindexer::JsonBuilder&& builder) const {
	using namespace std::string_view_literals;
	builder.Put("quantization_type"sv, encodeQuantizationType(quantizationType));
	if (quantile) {
		builder.Put("quantile"sv, *quantile);
	}
	builder.Put("sample_size"sv, sampleSize);
	builder.Put("quantization_threshold"sv, quantizationThreshold);
}

void QuantizationConfig::Deserialize(IReader& reader) {
	quantizationType = QuantizationType(reader.GetVarInt());
	if (auto q = reader.GetFloat(); q >= 0.95f && q <= 1.f) {
		quantile = q;
	} else if (reindexer::fp::EqualWithinULPs(q, 0.f)) {
		quantile = std::nullopt;
	} else {
		throw reindexer::Error(errParams, "Incorrect deserialized quantile value ({}) must be within [0.95; 1.0]", q);
	}
	sampleSize = reader.GetVarUInt();
	quantizationThreshold = reader.GetVarUInt();
}

void QuantizationConfig::Serialize(IWriter& writer) const {
	writer.PutVarInt(int32_t(quantizationType));
	writer.PutFloat(quantile ? *quantile : 0.f);
	writer.PutVarUInt(uint64_t{sampleSize});
	writer.PutVarUInt(uint64_t{quantizationThreshold});
}

bool QuantizationConfig::operator==(const QuantizationConfig& o) const noexcept {
	return quantizationType == o.quantizationType && quantile == o.quantile && sampleSize == o.sampleSize &&
		   quantizationThreshold == o.quantizationThreshold;
}

std::string_view QuantizationConfig::encodeQuantizationType(QuantizationType quantizationType) {
	switch (quantizationType) {
		case QuantizationType::ScalarQuantization8bit:
			return "scalar_quantization_8_bit";
		default:
			throw reindexer::Error(errParams, "Unsupported quantization type - {}", int(quantizationType));
	}
}

QuantizationType QuantizationConfig::decodeQuantizationType(std::string_view quantizationType) {
	if (quantizationType == "scalar_quantization_8_bit") {
		return QuantizationType::ScalarQuantization8bit;
	} else {
		throw reindexer::Error(errParams, "Unsupported quantization type - {}", quantizationType);
	}
}
}  // namespace hnswlib
