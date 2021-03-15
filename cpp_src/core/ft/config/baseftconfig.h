#pragma once

#include <string>
#include <vector>
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "estl/string_view.h"
#include "tools/stringstools.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

class BaseFTConfig {
public:
	struct Synonym {
		vector<string> tokens;
		vector<string> alternatives;
		bool operator==(const Synonym& other) const { return tokens == other.tokens && alternatives == other.alternatives; }
		bool operator!=(const Synonym& other) const { return !(*this == other); }
	};
	BaseFTConfig();
	virtual ~BaseFTConfig() = default;

	virtual void parse(string_view sv, const fast_hash_map<string, int>& fields) = 0;

	int mergeLimit = 20000;
	vector<string> stemmers = {"en", "ru"};
	bool enableTranslit = true;
	bool enableKbLayout = true;
	bool enableNumbersSearch = false;
	fast_hash_set<string, hash_str, equal_str> stopWords;
	vector<Synonym> synonyms;
	int logLevel = 0;
	string extraWordSymbols = "-/+";

protected:
	void parseBase(const gason::JsonNode& root);
};

}  // namespace reindexer
