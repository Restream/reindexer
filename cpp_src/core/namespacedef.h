#pragma once

#include <string>
#include <vector>
#include "tools/errors.h"
#include "type_consts.h"

union JsonValue;

namespace reindexer {

using std::string;
using std::vector;

struct Slice;
class WrSerializer;
struct IndexDef {
	string name;
	string jsonPath;
	string indexType;
	string fieldType;
	IndexOpts opts;
	IndexType Type() const;

	void FromType(IndexType type);
	Error Parse(char *json);
	Error Parse(JsonValue &jvalue);
};

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

	string name;
	StorageOpts storage;
	vector<IndexDef> indexes;
	Error Parse(char *json);
	void Print(WrSerializer &);
	void PrintIndexes(WrSerializer &ser, const char *jsonIndexFieldName = "indexes");
};
}  // namespace reindexer
