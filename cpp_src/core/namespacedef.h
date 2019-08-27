#pragma once

#include <string>
#include <vector>
#include "core/indexdef.h"
#include "tools/errors.h"

#include "estl/string_view.h"

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
}  // namespace reindexer
