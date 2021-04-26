#pragma once

#include <string>
#include <vector>
#include "core/indexopts.h"
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "estl/span.h"
#include "tools/errors.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

class WrSerializer;

using JsonPaths = h_vector<std::string, 0>;

static const int kIndexJSONWithDescribe = 0x1;

struct IndexDef {
	IndexDef();
	IndexDef(const std::string &name);
	IndexDef(const std::string &name, const JsonPaths &jsonPaths, const std::string &indexType, const std::string &fieldType,
			 const IndexOpts opts);
	IndexDef(const std::string &name, const JsonPaths &jsonPaths, const std::string &indexType, const std::string &fieldType,
			 const IndexOpts opts, int64_t expireAfter);
	IndexDef(const std::string &name, const std::string &indexType, const std::string &fieldType, const IndexOpts opts);
	IndexDef(const std::string &name, const JsonPaths &jsonPaths, const IndexType type, const IndexOpts opts);
	bool operator==(const IndexDef &other) const { return IsEqual(other, false); }
	bool operator!=(const IndexDef &other) const { return !IsEqual(other, false); }
	bool IsEqual(const IndexDef &other, bool skipConfig) const;
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

bool isComposite(IndexType type) noexcept;
bool isFullText(IndexType type) noexcept;
bool isSortable(IndexType type);
bool isStore(IndexType type) noexcept;
bool validateIndexName(std::string_view name, IndexType type) noexcept;

}  // namespace reindexer
