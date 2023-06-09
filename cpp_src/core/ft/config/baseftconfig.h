#pragma once

#include <string>
#include <string_view>
#include <vector>
#include "core/ft/usingcontainer.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "tools/stringstools.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

static constexpr int kMaxMergeLimitValue = 65000;
static constexpr int kMinMergeLimitValue = 0;

class JsonBuilder;

class BaseFTConfig {
public:
	struct Synonym {
		std::vector<std::string> tokens;
		std::vector<std::string> alternatives;
		bool operator==(const Synonym& other) const { return tokens == other.tokens && alternatives == other.alternatives; }
		bool operator!=(const Synonym& other) const { return !(*this == other); }
	};
	BaseFTConfig();
	virtual ~BaseFTConfig() = default;

	virtual void parse(std::string_view sv, const RHashMap<std::string, int>& fields) = 0;
	virtual std::string GetJson(const fast_hash_map<std::string, int>& fields) const = 0;

	int mergeLimit = 20000;
	std::vector<std::string> stemmers = {"en", "ru"};
	bool enableTranslit = true;
	bool enableKbLayout = true;
	bool enableNumbersSearch = false;
	bool enableWarmupOnNsCopy = false;
	fast_hash_set<std::string, hash_str, equal_str, less_str> stopWords;
	std::vector<Synonym> synonyms;
	int logLevel = 0;
	std::string extraWordSymbols = "-/+";  // word contains symbols (IsAlpa | IsDigit) {IsAlpa | IsDigit | IsExtra}

protected:
	void parseBase(const gason::JsonNode& root);
	void getJson(JsonBuilder&) const;
};

}  // namespace reindexer
