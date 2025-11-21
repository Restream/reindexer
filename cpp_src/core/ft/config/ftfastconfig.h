#pragma once

#include "baseftconfig.h"

namespace reindexer {

struct [[nodiscard]] FtFastFieldConfig {
	double bm25Boost = 1.0;
	double bm25Weight = 0.1;
	double termLenBoost = 1.0;
	double termLenWeight = 0.3;
	double positionBoost = 1.0;
	double positionWeight = 0.1;
	bool operator==(const FtFastFieldConfig&) const noexcept;

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

struct [[nodiscard]] FtFastConfig : public BaseFTConfig {
	FtFastConfig(size_t fieldsCount) : fieldsCfg(fieldsCount ? fieldsCount : 1) {}
	void parse(std::string_view json, const RHashMap<std::string, FtIndexFieldPros>& fields) override final;
	std::string GetJSON(const fast_hash_map<std::string, int>& fields) const override final;

	double distanceBoost = 1.0;
	double distanceWeight = 0.5;
	double fullMatchBoost = 1.1;
	// Relevancy step of partial match: relevancy = kFullMatchProc - partialMatchDecrease * (non matched symbols) / (matched symbols)
	// For example: partialMatchDecrease: 15, word in index 'terminator', pattern 'termin'. matched: 6 symbols, unmatched: 4. relevancy =
	// 100 - (15*4)/6 = 80
	int partialMatchDecrease = 15;
	double minRelevancy = 0.05;

	int maxTypos = 2;
	int maxExtraLetters = 2;
	int maxMissingLetters = 2;
	int maxTypoLen = 15;
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

	h_vector<FtFastFieldConfig, 8> fieldsCfg;
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
};

}  // namespace reindexer
