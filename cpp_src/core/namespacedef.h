#pragma once

#include <string>
#include <vector>
#include "core/indexdef.h"
#include "tools/errors.h"

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
	NamespaceDef &AddIndex(const string &name, const string &jsonPath, const string &indexType, const string &fieldType,
						   IndexOpts opts = IndexOpts()) {
		indexes.push_back({name, jsonPath, indexType, fieldType, opts});
		return *this;
	}
	NamespaceDef &AddIndex(const IndexDef &idxDef) {
		indexes.push_back(idxDef);
		return *this;
	}

	Error FromJSON(char *json);
	void GetJSON(WrSerializer &) const;

public:
	string name;
	StorageOpts storage;
	vector<IndexDef> indexes;
	CacheMode cacheMode;
};
}  // namespace reindexer
