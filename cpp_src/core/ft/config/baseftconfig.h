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

struct StopWord : std::string {
	enum class Type { Stop, Morpheme };
	StopWord(std::string base, Type type = Type::Stop) noexcept : std::string(std::move(base)), type(type) {}
	Type type;
};

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
	virtual std::string GetJSON(const fast_hash_map<std::string, int>& fields) const = 0;

	int mergeLimit = 20000;
	std::vector<std::string> stemmers = {"en", "ru"};
	bool enableTranslit = true;
	bool enableKbLayout = true;
	bool enableNumbersSearch = false;
	bool enableWarmupOnNsCopy = false;

	fast_hash_set<StopWord, hash_str, equal_str, less_str> stopWords;
	std::vector<Synonym> synonyms;
	int logLevel = 0;
	std::string extraWordSymbols = "-/+";  // word contains symbols (IsAlpa | IsDigit) {IsAlpa | IsDigit | IsExtra}
	struct BaseRankingConfig {
		static constexpr int kMinProcAfterPenalty = 1;
		// Relevancy of full word match
		int fullMatch = 100;
		// Mininum relevancy of prefix word match.
		int prefixMin = 50;
		// Mininum relevancy of suffix word match.
		int suffixMin = 10;
		// Base relevancy of typo match
		int typo = 85;
		// Extra penalty for each word's permutation (addition/deletion of the symbol) in typo algorithm
		int typoPenalty = 15;
		// Penalty for the variants, created by stemming
		int stemmerPenalty = 15;
		// Relevancy of the match in incorrect kblayout
		int kblayout = 90;
		// Relevancy of the match in translit
		int translit = 90;
		// Relevancy of the synonym match
		int synonyms = 95;
	};
	BaseRankingConfig rankingConfig;

protected:
	void parseBase(const gason::JsonNode& root);
	void getJson(JsonBuilder&) const;
};

}  // namespace reindexer
