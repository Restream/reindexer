#pragma once

#include <string>
#include <vector>
#include "tools/errors.h"
#include "type_consts.h"

namespace reindexer {

using std::string;
using std::vector;

struct IndexDef {
	string name;
	string jsonPath;
	string indexType;
	string fieldType;
	IndexOpts opts;
	IndexType Type() const;
};

struct NamespaceDef {
	NamespaceDef(const string &iname, StorageOpts istorage = StorageOpts()) : name(iname), storage(istorage) {}
	NamespaceDef &AddIndex(const string &name, const string &jsonPath, const string &indexType, const string &fieldType,
						   IndexOpts opts = IndexOpts()) {
		indexes.push_back({name, jsonPath, indexType, fieldType, opts});
		return *this;
	}

	string name;
	StorageOpts storage;
	vector<IndexDef> indexes;
	Error Parse(char *json);
};
}  // namespace reindexer
