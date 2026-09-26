#include <algorithm>
#include <string>
#include "core/ft/config/ftconfig.h"
#include "core/ft/ft_fast/dataholder.h"
#include "core/ft/typos.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

namespace reindexer {

struct [[nodiscard]] WordTypo {
	WordTypo() = default;
	explicit WordTypo(WordIdType w) noexcept : word(w) {}
	explicit WordTypo(WordIdType w, const TyposVec& p) noexcept : word(w), positions(p) { assertrx_dbg(Sorted(p)); }

	static bool Sorted(const TyposVec& positions) noexcept {
		for (size_t i = 1; i < positions.size(); ++i) {
			if (positions[i] <= positions[i - 1]) {
				return false;
			}
		}

		return true;
	}

	WordIdType word;
	TyposVec positions;
};

class [[nodiscard]] TyposHandler {
public:
	TyposHandler(const FTConfig& cfg) noexcept
		: maxMissingLetts_(cfg.MaxMissingLetters()), maxExtraLetts_(cfg.MaxExtraLetters()), logLevel_(cfg.logLevel) {
		const auto maxTypoDist = cfg.MaxTypoDistance();
		maxTypoDist_ = maxTypoDist.first;
		useMaxTypoDist_ = maxTypoDist.second;
		const auto maxLettPermDist = cfg.MaxSymbolPermutationDistance();
		maxLettPermDist_ = maxLettPermDist.first;
		useMaxLettPermDist_ = maxLettPermDist.second;
	}

	template <class IdCont>
	void Process(const std::u16string& pattern, float patternProc, FoundWordsProcsType& fixedVariants, const DataHolder<IdCont>& holder) {
		std::u16string buf;

		struct {
			float patternProc;
			FoundWordsProcsType& fixedVariants;
			const DataHolder<IdCont>& holder;
			int matched, skipped;
		} ctx{patternProc, fixedVariants, holder, 0, 0};

		auto callback = [&ctx, this](std::u16string_view typo, const TyposVec& positions, std::u16string_view typoPattern) {
			size_t maxTypos = ctx.holder.cfg_->maxTypos;

			if (const auto* typoSet = ctx.holder.Typos(typo); typoSet) {
				const auto typoGroupIt = typoSet->find(typo);
				if (typoGroupIt == typoSet->end()) {
					return;
				}
				for (size_t typoKeyIdx = 0, typoKeysCount = TypoKeysCount(*typoGroupIt); typoKeyIdx < typoKeysCount; ++typoKeyIdx) {
					const TypoKey typoKey = GetTypoKey(*typoGroupIt, typoKeyIdx);
					const WordTypo wordTypo = makeWordTypo(typoKey);

					if (wordTypo.positions.size() + positions.size() > maxTypos) {
						continue;
					}

					std::u16string_view word = ctx.holder.GetWord(wordTypo.word);

					if (positions.size() > wordTypo.positions.size() &&
						(positions.size() - wordTypo.positions.size()) > int(maxExtraLetts_)) {
						if (logLevel_ >= LogTrace) [[unlikely]] {
							logFmt(LogInfo, fmt::runtime(" skipping typo '{}' of word '{}': to many extra letters ({})"),
								   utf16_to_utf8(typo), utf16_to_utf8(word), positions.size() - wordTypo.positions.size());
						}
						++ctx.skipped;
						continue;
					}
					if (wordTypo.positions.size() > positions.size() &&
						(wordTypo.positions.size() - positions.size()) > int(maxMissingLetts_)) {
						if (logLevel_ >= LogTrace) [[unlikely]] {
							logFmt(LogInfo, fmt::runtime(" skipping typo '{}' of word '{}': to many missing letters ({})"),
								   utf16_to_utf8(typo), utf16_to_utf8(word), wordTypo.positions.size() - positions.size());
						}
						++ctx.skipped;
						continue;
					}
					if (!checkMaxTyposDist(wordTypo, positions)) {
						const bool needMaxLettPermCheck = useMaxTypoDist_ && (!useMaxLettPermDist_ || maxLettPermDist_ > maxTypoDist_);
						if (!needMaxLettPermCheck || !checkMaxLettPermDist(word, wordTypo, typoPattern, positions)) {
							if (logLevel_ >= LogTrace) [[unlikely]] {
								logFmt(LogInfo, fmt::runtime(" skipping typo '{}' of word '{}' due to max_typos_distance settings"),
									   utf16_to_utf8(typo), utf16_to_utf8(word));
							}
							++ctx.skipped;
							continue;
						}
					}

					const int tcount = std::max(positions.size(), wordTypo.positions.size());  // Each letter switch equals to 1 typo
					const auto& rankingConfig = ctx.holder.cfg_->rankingConfig;
					const float proc =
						std::max<float>(ctx.patternProc * rankingConfig.TypoCoeff() -
											tcount * rankingConfig.TypoPenalty() /
												std::max<float>((word.length() - tcount) / 3.f, FTRankingConfig::kMinProcAfterPenalty),
										1.f);

					const auto [it, emplaced] = ctx.fixedVariants.try_emplace(wordTypo.word, proc);
					if (emplaced) {
						const auto& wordTypoOccurences = ctx.holder.GetWordOccurences(wordTypo.word);
						if (logLevel_ >= LogTrace) [[unlikely]] {
							logFmt(LogInfo, fmt::runtime(" matched typo '{}' of word '{}', {} ids, {}%"), utf16_to_utf8(typo),
								   utf16_to_utf8(word), wordTypoOccurences->size(), proc);
						}
						++ctx.matched;
					} else {
						++ctx.skipped;
						it->second = std::max(it->second, proc);
					}
				}
			}
		};

		mktypos(pattern, holder.cfg_->MaxTyposInWord(), holder.cfg_->maxTypoLen, callback, buf);
		if (holder.cfg_->logLevel >= LogInfo) [[unlikely]] {
			logFmt(LogInfo, "Lookup typos, matched {} typos, skipped {}", ctx.matched, ctx.skipped);
		}
	}

private:
	static WordTypo makeWordTypo(TypoKey key) noexcept {
		const auto wordId = UnpackTypoWordId(key);
		const auto pos0 = UnpackTypoPosition0(key);
		const auto pos1 = UnpackTypoPosition1(key);
		if (pos0 == kTypoMissingPosition) {
			return WordTypo(wordId);
		}
		if (pos1 == kTypoMissingPosition) {
			return WordTypo(wordId, TyposVec(pos0));
		}
		return WordTypo(wordId, TyposVec(pos0, pos1));
	}

	bool checkMaxTyposDist(const WordTypo& found, const TyposVec& current);
	bool checkMaxLettPermDist(std::u16string_view foundWord, const WordTypo& found, std::u16string_view currentWord,
							  const TyposVec& current);

	bool useMaxTypoDist_;
	bool useMaxLettPermDist_;
	unsigned maxTypoDist_;
	unsigned maxLettPermDist_;
	unsigned maxMissingLetts_;
	unsigned maxExtraLetts_;
	int logLevel_;
};

}  // namespace reindexer
