#pragma once

#include <string>
#include <vector>
#include "estl/fast_hash_set.h"
#include "estl/string_view.h"
#include "tools/stringstools.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

using std::vector;
using std::string;

class BaseFTConfig {
public:
	BaseFTConfig();
	virtual ~BaseFTConfig() = default;

	virtual void parse(string_view sv) = 0;

	int mergeLimit = 20000;
	vector<string> stemmers = {"en", "ru"};
	bool enableTranslit = true;
	bool enableKbLayout = true;
	bool enableNumbersSearch = false;
	fast_hash_set<string, hash_str, equal_str> stopWords;
	int logLevel = 0;
	string extraWordSymbols = "-/+";

protected:
	void parseBase(const gason::JsonNode &root);
};

}  // namespace reindexer
