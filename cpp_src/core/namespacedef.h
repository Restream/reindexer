#pragma once

#include <string>
#include <vector>
#include "indexdef.h"
#include "tools/errors.h"

namespace reindexer {

using std::string;
using std::vector;

struct Slice;
class WrSerializer;

struct NamespaceDef {
	NamespaceDef(const string &iname, StorageOpts istorage = StorageOpts().Enabled().CreateIfMissing()) : name(iname), storage(istorage) {}
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
	void GetJSON(WrSerializer &);

public:
	string name;
	StorageOpts storage;
	vector<IndexDef> indexes;
};
}  // namespace reindexer
