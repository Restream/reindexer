#pragma once

#include <string>
#include <vector>
#include "core/indexopts.h"
#include "core/type_consts.h"
#include "estl/span.h"
#include "tools/errors.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

class WrSerializer;

using JsonPaths = std::vector<std::string>;

static const int kIndexJSONWithDescribe = 0x1;

struct IndexDef {
	IndexDef() = default;
	IndexDef(std::string name);
	IndexDef(std::string name, JsonPaths jsonPaths, std::string indexType, std::string fieldType, IndexOpts opts);
	IndexDef(std::string name, JsonPaths jsonPaths, std::string indexType, std::string fieldType, IndexOpts opts, int64_t expireAfter);
	IndexDef(std::string name, std::string indexType, std::string fieldType, IndexOpts opts);
	IndexDef(std::string name, JsonPaths jsonPaths, IndexType type, IndexOpts opts);
	bool IsEqual(const IndexDef &other, IndexComparison cmpType) const;
	IndexType Type() const;
	std::string getCollateMode() const;
	const std::vector<std::string> &Conditions() const;
	void FromType(IndexType type);
	Error FromJSON(span<char> json);
	void FromJSON(const gason::JsonNode &jvalue);
	void GetJSON(WrSerializer &ser, int formatFlags = 0) const;

public:
	std::string name_;
	JsonPaths jsonPaths_;
	std::string indexType_;
	std::string fieldType_;
	IndexOpts opts_;
	int64_t expireAfter_ = 0;
};

[[nodiscard]] bool isSortable(IndexType type);
[[nodiscard]] bool isStore(IndexType type) noexcept;
[[nodiscard]] bool validateIndexName(std::string_view name, IndexType type) noexcept;

}  // namespace reindexer
