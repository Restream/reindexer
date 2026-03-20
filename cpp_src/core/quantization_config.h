#pragma once

#include <optional>
#include <string_view>

namespace gason {
struct JsonNode;
}

namespace reindexer ::builders {
class JsonBuilder;
}  // namespace reindexer::builders

namespace hnswlib {

enum class [[nodiscard]] QuantizationType { ScalarQuantization8bit };
static constexpr size_t kDefaultSampleSize = 20'000;
static constexpr size_t kDefaultQuantizationThreshold = 100'000;

class IReader;
class IWriter;

struct [[nodiscard]] QuantizationConfig {
	void FromJSON(const gason::JsonNode& root);
	void GetJSON(reindexer::builders::JsonBuilder&& builder) const;

	void Deserialize(IReader& reader);
	void Serialize(IWriter& writer) const;

	bool operator==(const QuantizationConfig& o) const noexcept;

	QuantizationType quantizationType = QuantizationType::ScalarQuantization8bit;
	std::optional<float> quantile;
	size_t sampleSize = kDefaultSampleSize;
	size_t quantizationThreshold = kDefaultQuantizationThreshold;

private:
	static std::string_view encodeQuantizationType(QuantizationType quantizationType);
	static QuantizationType decodeQuantizationType(std::string_view quantizationType);
};
}  // namespace hnswlib