#pragma once

#include <string>
#include <vector>
#include "core/indexdef.h"
#include "estl/string_view.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer {

using std::string;
using std::vector;

class string_view;
class WrSerializer;

struct NamespaceDef {
	NamespaceDef() {}

	NamespaceDef(const string &iname, StorageOpts istorage = StorageOpts().Enabled().CreateIfMissing()) : name(iname), storage(istorage) {}

	NamespaceDef &AddIndex(const string &iname, const string &indexType, const string &fieldType, IndexOpts opts = IndexOpts()) {
		indexes.push_back({iname, {iname}, indexType, fieldType, opts});
		return *this;
	}

	NamespaceDef &AddIndex(const string &iname, const JsonPaths &jsonPaths, const string &indexType, const string &fieldType,
						   IndexOpts opts = IndexOpts()) {
		indexes.push_back({iname, jsonPaths, indexType, fieldType, opts});
		return *this;
	}

	NamespaceDef &AddIndex(const IndexDef &idxDef) {
		indexes.push_back(idxDef);
		return *this;
	}

	Error FromJSON(span<char> json);
	void FromJSON(const gason::JsonNode &root);
	void GetJSON(WrSerializer &, int formatFlags = 0) const;

public:
	string name;
	StorageOpts storage;
	vector<IndexDef> indexes;
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
	bool MatchFilter(string_view nsName) {
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
	EnumNamespacesOpts &WithFilter(string_view flt) {
		filter_ = flt;
		return *this;
	}
	string_view filter_;
	uint16_t options_;
};

}  // namespace reindexer
