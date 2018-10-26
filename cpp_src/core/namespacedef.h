#pragma once

#include <string>
#include <vector>
#include "core/indexdef.h"
#include "tools/errors.h"

#include "estl/string_view.h"

union JsonValue;

namespace reindexer {

using std::string;
using std::vector;

class string_view;
class WrSerializer;

struct NamespaceDef {
	NamespaceDef() {}

	NamespaceDef(const string &iname, StorageOpts istorage = StorageOpts().Enabled().CreateIfMissing(),
				 CacheMode icacheMode = CacheMode::CacheModeOn)
		: name(iname), storage(istorage), cacheMode(icacheMode) {}

	NamespaceDef &AddIndex(const string &name, const string &indexType, const string &fieldType, IndexOpts opts = IndexOpts()) {
		indexes.push_back({name, {name}, indexType, fieldType, opts});
		return *this;
	}

	NamespaceDef &AddIndex(const string &name, const JsonPaths &jsonPaths, const string &indexType, const string &fieldType,
						   IndexOpts opts = IndexOpts()) {
		indexes.push_back({name, jsonPaths, indexType, fieldType, opts});
		return *this;
	}

	NamespaceDef &AddIndex(const IndexDef &idxDef) {
		indexes.push_back(idxDef);
		return *this;
	}

	Error FromJSON(char *json);
	Error FromJSON(JsonValue &jvalue);
	void GetJSON(WrSerializer &, bool describeCompat = false) const;

public:
	string name;
	StorageOpts storage;
	vector<IndexDef> indexes;
	CacheMode cacheMode;
};
}  // namespace reindexer
