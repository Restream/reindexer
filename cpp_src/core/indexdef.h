#pragma once

#include <string>
#include <vector>
#include "indexopts.h"
#include "tools/errors.h"
#include "type_consts.h"

union JsonValue;

namespace reindexer {

using std::string;
using std::vector;

struct Slice;
class WrSerializer;
struct IndexDef {
	IndexType Type() const;
	string getCollateMode() const;
	const vector<string> &Conditions() const;
	void FromType(IndexType type);
	Error FromJSON(char *json);
	Error FromJSON(JsonValue &jvalue);
	void GetJSON(WrSerializer &ser);

public:
	string name;
	string jsonPath;
	string indexType;
	string fieldType;
	IndexOpts opts;
};

bool isComposite(IndexType type);
bool isFullText(IndexType type);
bool isSortable(IndexType type);

}  // namespace reindexer
