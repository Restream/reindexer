#pragma once

#include <string>
#include <vector>
#include "core/indexopts.h"
#include "core/type_consts.h"
#include "tools/errors.h"

union JsonValue;

namespace reindexer {

using std::string;
using std::vector;

class string_view;
class WrSerializer;
struct IndexDef {
	IndexType Type() const;
	string getCollateMode() const;
	const vector<string> &Conditions() const;
	void FromType(IndexType type);
	Error FromJSON(char *json);
	Error FromJSON(JsonValue &jvalue);
	void GetJSON(WrSerializer &ser, bool describeCompat = false) const;

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
