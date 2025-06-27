#pragma once

#include <string_view>
#include <vector>
#include "core/ft/stopwords/types.h"
#include "estl/fast_hash_map.h"
#include "tools/rhashmap.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

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

	uint32_t mergeLimit = 20000;
	std::vector<std::string> stemmers = {"en", "ru"};
	bool enableTranslit = true;
	bool enableKbLayout = true;
	bool enableNumbersSearch = false;

	StopWordsSetT stopWords;
	std::vector<Synonym> synonyms;
	int logLevel = 0;
	std::string extraWordSymbols = "-/+";  // word contains symbols (IsAlpa | IsDigit) {IsAlpa | IsDigit | IsExtra}
	struct BaseRankingConfig {
		static constexpr int kMinProcAfterPenalty = 1;
		// Relevancy of full word match
		int fullMatch = 100;
		// Minimum relevancy of prefix word match.
		int prefixMin = 50;
		// Minimum relevancy of suffix word match.
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

	SymbolTypeMask removeDiacriticsMask = kRemoveAllDiacriticsMask;

protected:
	[[nodiscard]] std::string removeAccentsAndDiacritics(const std::string& str) const;
	void parseBase(const gason::JsonNode& root);
	void getJson(JsonBuilder&) const;
};

}  // namespace reindexer
