#pragma once

#include <string>
#include <vector>
#include "estl/fast_hash_set.h"
#include "gason/gason.h"

namespace reindexer {

using std::vector;
using std::string;

class BaseFTConfig {
public:
	BaseFTConfig();

	virtual void parse(char *json) = 0;

	int mergeLimit = 20000;
	vector<string> stemmers = {"en", "ru"};
	bool enableTranslit = true;
	bool enableKbLayout = true;
	fast_hash_set<string> stopWords;
	int logLevel = 0;

protected:
	void parseBase(const JsonNode *val);
	void parseJsonField(const char *name, bool &ref, const JsonNode *elem);

	template <typename T>
	void parseJsonField(const char *name, T &ref, const JsonNode *elem, double min, double max);
};

}  // namespace reindexer
