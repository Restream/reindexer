#pragma once

#include <string>
#include <unordered_set>
#include <vector>
#include "gason/gason.h"

namespace reindexer {

using std::vector;
using std::string;
using std::unordered_set;

class BaseFTConfig {
public:
	BaseFTConfig();
	virtual ~BaseFTConfig() = default;

	virtual void parse(char *json) = 0;

	int mergeLimit = 20000;
	vector<string> stemmers = {"en", "ru"};
	bool enableTranslit = true;
	bool enableKbLayout = true;
	bool enableNumbersSearch = false;
	unordered_set<string> stopWords;
	int logLevel = 0;
	string extraWordSymbols = "-/+";

protected:
	void parseBase(const JsonNode *val);
};

}  // namespace reindexer
