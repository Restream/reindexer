#pragma once

#include <string_view>
#include <vector>
#include "core/ft/ftdsl.h"
#include "core/ft/stopwords/types.h"
#include "estl/fast_hash_map.h"
#include "tools/rhashmap.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

using TermsBoostMapT = tsl::hopscotch_map<std::string, float, nocase_hash_str_utf8, nocase_equal_str_utf8>;

class [[nodiscard]] FTRankingConfig {
public:
	float KbLayoutCoeff() const noexcept {
		float res = static_cast<float>(kblayout) / (static_cast<float>(fullMatch) + 0.001f);
		return std::min(res, 1.0f);
	}

	float DelimitedCoeff() const noexcept {
		float res = static_cast<float>(delimited) / (static_cast<float>(fullMatch) + 0.001f);
		return std::min(res, 1.0f);
	}

	float SynonymsCoeff() const noexcept {
		float res = static_cast<float>(synonyms) / (static_cast<float>(fullMatch) + 0.001f);
		return std::min(res, 1.0f);
	}

	float TranslitCoeff() const noexcept {
		float res = static_cast<float>(translit) / (static_cast<float>(fullMatch) + 0.001f);
		return std::min(res, 1.0f);
	}

	float TypoCoeff() const noexcept {
		float res = static_cast<float>(typo) / (static_cast<float>(fullMatch) + 0.001f);
		return std::min(res, 1.0f);
	}

	float StemProc(float proc) const noexcept { return std::max<float>(proc - stemmerPenalty, kMinProcAfterPenalty); }
	void SetFullMatch(int v) noexcept { fullMatch = v; }
	void SetConcat(int v) noexcept { concat = v; }
	void SetPrefixMin(int v) noexcept { prefixMin = v; }
	void SetSuffixMin(int v) noexcept { suffixMin = v; }
	void SetTypo(int v) noexcept { typo = v; }
	void SetTypoPenalty(int v) noexcept { typoPenalty = v; }
	void SetStemmerPenalty(int v) noexcept { stemmerPenalty = v; }
	void SetKblayout(int v) noexcept { kblayout = v; }
	void SetTranslit(int v) noexcept { translit = v; }
	void SetSynonyms(int v) noexcept { synonyms = v; }
	void SetDelimited(int v) noexcept { delimited = v; }

	int FullMatch() const noexcept { return fullMatch; }
	int Concat() const noexcept { return concat; }
	int PrefixMin() const noexcept { return prefixMin; }
	int SuffixMin() const noexcept { return suffixMin; }
	int Typo() const noexcept { return typo; }
	int TypoPenalty() const noexcept { return typoPenalty; }
	int StemmerPenalty() const noexcept { return stemmerPenalty; }

	static constexpr int kMinProcAfterPenalty = 1;

	void parse(const gason::JsonNode& root);
	void getJson(JsonBuilder& jsonBuilder) const;

private:
	static constexpr int kDefaultFullMatch = 100;
	static constexpr int kDefaultConcat = 90;
	static constexpr int kDefaultPrefixMin = 20;
	static constexpr int kDefaultSuffixMin = 10;
	static constexpr int kDefaultTypo = 85;
	static constexpr int kDefaultTypoPenalty = 15;
	static constexpr int kDefaultStemmerPenalty = 15;
	static constexpr int kDefaultKblayout = 90;
	static constexpr int kDefaultTranslit = 90;
	static constexpr int kDefaultSynonyms = 95;
	static constexpr int kDefaultDelimited = 80;

	// Relevancy of full word match
	int fullMatch = kDefaultFullMatch;
	// Relevancy of joined terms match
	int concat = kDefaultConcat;
	// Minimum relevance of prefix word match.
	int prefixMin = kDefaultPrefixMin;
	// Minimum relevance of suffix word match.
	int suffixMin = kDefaultSuffixMin;
	// Base relevance of typo match
	int typo = kDefaultTypo;
	// Extra penalty for each word's permutation (addition/deletion of the symbol) in typo algorithm
	int typoPenalty = kDefaultTypoPenalty;
	// Penalty for the variants, created by stemming
	int stemmerPenalty = kDefaultStemmerPenalty;
	// Relevance of the match in incorrect kblayout
	int kblayout = kDefaultKblayout;
	// Relevance of the match in translit
	int translit = kDefaultTranslit;
	// Relevance of the synonym match
	int synonyms = kDefaultSynonyms;
	// Relevance of the delimited part match
	int delimited = kDefaultDelimited;
};

struct [[nodiscard]] FTFieldConfig {
	double bm25Boost = 1.0;
	double bm25Weight = 0.1;
	double termLenBoost = 1.0;
	double termLenWeight = 0.3;
	double positionBoost = 1.0;
	double positionWeight = 0.1;
	bool operator==(const FTFieldConfig&) const noexcept;

	RX_ALWAYS_INLINE static float pos2rank(unsigned pos) noexcept {
		if (pos <= 10) {
			return 1.0 - (pos / 100.0);
		}
		if (pos <= 100) {
			return 0.9 - (pos / 1000.0);
		}
		if (pos <= 1000) {
			return 0.8 - (pos / 10000.0);
		}
		if (pos <= 10000) {
			return 0.7 - (pos / 100000.0);
		}
		if (pos <= 100000) {
			return 0.6 - (pos / 1000000.0);
		}
		return 0.5;
	}

	RX_ALWAYS_INLINE static float bound(float k, float weight, float boost) noexcept { return (1.0 - weight) + k * boost * weight; }

	RX_ALWAYS_INLINE float calcPositionRank(unsigned pos) noexcept { return bound(pos2rank(pos), positionWeight, positionBoost); }
};

class [[nodiscard]] FTConfig {
public:
	FTConfig(size_t fieldsCount);

	uint32_t mergeLimit = 20000;
	std::vector<std::string> stemmers = {"en", "ru"};
	bool enableTermsConcat = true;
	bool enableTranslit = true;
	bool enableKbLayout = true;
	bool enableNumbersSearch = false;

	StopWordsSetT stopWords;
	TermsBoostMapT termsBoost;

	struct [[nodiscard]] Synonym {
		std::vector<std::string> tokens;
		std::vector<std::string> alternatives;
		bool operator==(const Synonym& other) const { return tokens == other.tokens && alternatives == other.alternatives; }
		bool operator!=(const Synonym& other) const { return !(*this == other); }
	};
	std::vector<Synonym> synonyms;

	int logLevel = 0;

	SplitOptions splitOptions;

	FTRankingConfig rankingConfig;

	double distanceBoost = 1.0;
	double distanceWeight = 0.5;
	double fullMatchBoost = 1.1;
	// Relevancy step of partial match: relevancy = kFullMatchProc - partialMatchDecrease * (non matched symbols) / (matched symbols)
	// For example: partialMatchDecrease: 15, word in index 'terminator', pattern 'termin'. matched: 6 symbols, unmatched: 4. relevancy =
	// 100 - (15*4)/6 = 80
	int partialMatchDecrease = 15;
	int minRank = 5;

	int maxTypos = 2;
	int maxExtraLetters = 2;
	int maxMissingLetters = 2;
	uint8_t maxTypoLen = 15;
	int maxTypoDistance = 0;
	int maxSymbolPermutationDistance = 1;

	int maxRebuildSteps = 50;
	int maxStepSize = 4000;

	struct [[nodiscard]] Bm25Config {
		enum class [[nodiscard]] Bm25Type { classic, rx, wordCount };
		double bm25k1 = 2.0;
		double bm25b = 0.75;
		Bm25Type bm25Type = Bm25Type::rx;
		void getJson(JsonBuilder& jsonBuilder) const;
		void parse(const gason::JsonNode& root);
	};

	Bm25Config bm25Config;

	double summationRanksByFieldsRatio = 0.0;
	int maxAreasInDoc = 5;
	int maxTotalAreasToCache = -1;

	enum class [[nodiscard]] Splitter { Fast, MMSegCN } splitterType = Splitter::Fast;

	h_vector<FTFieldConfig, 8> fieldsCfg;
	enum class [[nodiscard]] Optimization { CPU, Memory } optimization = Optimization::Memory;
	bool enablePreselectBeforeFt = false;
	int MaxTyposInWord() const noexcept { return (maxTypos / 2) + (maxTypos % 2); }
	unsigned MaxExtraLetters() const noexcept { return maxExtraLetters >= 0 ? unsigned(maxExtraLetters) : std::numeric_limits<int>::max(); }
	unsigned MaxMissingLetters() const noexcept {
		return maxMissingLetters >= 0 ? unsigned(maxMissingLetters) : std::numeric_limits<int>::max();
	}
	std::pair<unsigned, bool> MaxSymbolPermutationDistance() const noexcept {
		if (maxSymbolPermutationDistance < 0) {
			return std::make_pair(0u, false);
		}
		return std::make_pair(unsigned(maxSymbolPermutationDistance), true);
	}
	std::pair<unsigned, bool> MaxTypoDistance() const noexcept {
		return maxTypoDistance >= 0 ? std::make_pair(unsigned(maxTypoDistance), true) : std::make_pair(0u, false);
	}

	void parse(std::string_view json, const RHashMap<std::string, FtIndexFieldPros>& fields);
	std::string GetJSON(const fast_hash_map<std::string, int>& fields) const;
};

}  // namespace reindexer
