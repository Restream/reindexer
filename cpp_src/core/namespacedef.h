#pragma once

#include <string>
#include <string_view>
#include <vector>
#include "core/indexdef.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer {

class WrSerializer;

struct NamespaceDef {
	NamespaceDef() = default;

	NamespaceDef(const std::string &iname, StorageOpts istorage = StorageOpts().Enabled().CreateIfMissing())
		: name(iname), storage(istorage) {}

	NamespaceDef &AddIndex(const std::string &iname, const std::string &indexType, const std::string &fieldType,
						   IndexOpts opts = IndexOpts()) {
		indexes.emplace_back(iname, JsonPaths{iname}, indexType, fieldType, std::move(opts));
		return *this;
	}

	NamespaceDef &AddIndex(const std::string &iname, const JsonPaths &jsonPaths, const std::string &indexType, const std::string &fieldType,
						   IndexOpts opts = IndexOpts()) {
		indexes.emplace_back(iname, jsonPaths, indexType, fieldType, std::move(opts));
		return *this;
	}

	NamespaceDef &AddIndex(const IndexDef &idxDef) {
		indexes.emplace_back(idxDef);
		return *this;
	}

	Error FromJSON(span<char> json);
	void FromJSON(const gason::JsonNode &root);
	void GetJSON(WrSerializer &, int formatFlags = 0) const;

public:
	std::string name;
	StorageOpts storage;
	std::vector<IndexDef> indexes;
	bool isTemporary = false;
	std::string schemaJson = "{}";
};

enum EnumNamespacesOpt {
	kEnumNamespacesWithClosed = 1,
	kEnumNamespacesOnlyNames = 2,
	kEnumNamespacesHideSystem = 4,
};

struct EnumNamespacesOpts {
	EnumNamespacesOpts() : options_(0) {}

	bool IsWithClosed() { return options_ & kEnumNamespacesWithClosed; }
	bool IsOnlyNames() { return options_ & kEnumNamespacesOnlyNames; }
	bool IsHideSystem() { return options_ & kEnumNamespacesHideSystem; }
	bool MatchFilter(std::string_view nsName) {
		return (filter_.empty() || iequals(filter_, nsName)) && (!IsHideSystem() || (!nsName.empty() && nsName[0] != '#'));
	}

	// Add not openened namespaces to enumeration
	EnumNamespacesOpts &WithClosed(bool value = true) {
		options_ = value ? options_ | kEnumNamespacesWithClosed : options_ & ~(kEnumNamespacesWithClosed);
		return *this;
	}

	// Return only namespaces names (faster, and do not try rlock nses)
	EnumNamespacesOpts &OnlyNames(bool value = true) {
		options_ = value ? options_ | kEnumNamespacesOnlyNames : options_ & ~(kEnumNamespacesOnlyNames);
		return *this;
	}

	// Hide system namespaces from enumeration
	EnumNamespacesOpts &HideSystem(bool value = true) {
		options_ = value ? options_ | kEnumNamespacesHideSystem : options_ & ~(kEnumNamespacesHideSystem);
		return *this;
	}
	// Add name filter
	EnumNamespacesOpts &WithFilter(std::string_view flt) {
		filter_ = flt;
		return *this;
	}
	std::string_view filter_;
	uint16_t options_;
};

}  // namespace reindexer
