#include "core/ft//config/ftconfig.h"
#include "core/ft/ft_fast/dataholder.h"
#include "core/ft/ft_fast/typosmap.h"
#include "core/ft/typos.h"
#include "tools/logger.h"

namespace reindexer {

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
	void Process(const std::wstring& pattern, float patternProc, FoundWordsProcsType& fixedVariants, const DataHolder<IdCont>& holder) {
		std::wstring buf;

		for (auto& step : holder.steps) {
			struct {
				float patternProc;
				FoundWordsProcsType& fixedVariants;
				const DataHolder<IdCont>& holder;
				const DataHolder<IdCont>::CommitStep& step;
				int matched, skipped;
			} ctx{patternProc, fixedVariants, holder, step, 0, 0};

			auto callback = [&ctx, this](std::wstring_view typo, const TyposVec& positions, std::wstring_view typoPattern) {
				size_t maxTypos = ctx.holder.cfg_->maxTypos;

				const auto typoRng = ctx.step.typos_.TyposRange(TyposMap::CalcHash(typo));
				for (auto typoIt = typoRng.first; typoIt != typoRng.second; ++typoIt) {
					const WordTypo wordTypo = *typoIt;

					if (wordTypo.positions.size() + positions.size() > maxTypos) {
						continue;
					}

					const auto& step = ctx.holder.GetStep(wordTypo.word);
					auto wordIdSfx = ctx.holder.GetWordIdInStep(wordTypo.word, step);
					std::string_view word(step.suffixes_.word_at(wordIdSfx));

					if (!wordTypo.CheckMatch(word, typo)) {
						continue;
					}

					if (positions.size() > wordTypo.positions.size() &&
						(positions.size() - wordTypo.positions.size()) > int(maxExtraLetts_)) {
						if (logLevel_ >= LogTrace) [[unlikely]] {
							logFmt(LogInfo, fmt::runtime(" skipping typo '{}' of word '{}': to many extra letters ({})"),
								   utf16_to_utf8(typo), word, positions.size() - wordTypo.positions.size());
						}
						++ctx.skipped;
						continue;
					}
					if (wordTypo.positions.size() > positions.size() &&
						(wordTypo.positions.size() - positions.size()) > int(maxMissingLetts_)) {
						if (logLevel_ >= LogTrace) [[unlikely]] {
							logFmt(LogInfo, fmt::runtime(" skipping typo '{}' of word '{}': to many missing letters ({})"),
								   utf16_to_utf8(typo), word, wordTypo.positions.size() - positions.size());
						}
						++ctx.skipped;
						continue;
					}
					if (!checkMaxTyposDist(wordTypo, positions)) {
						const bool needMaxLettPermCheck = useMaxTypoDist_ && (!useMaxLettPermDist_ || maxLettPermDist_ > maxTypoDist_);
						if (!needMaxLettPermCheck || !checkMaxLettPermDist(word, wordTypo, typoPattern, positions)) {
							if (logLevel_ >= LogTrace) [[unlikely]] {
								logFmt(LogInfo, fmt::runtime(" skipping typo '{}' of word '{}' due to max_typos_distance settings"),
									   utf16_to_utf8(typo), word);
							}
							++ctx.skipped;
							continue;
						}
					}

					const uint8_t wordLength = step.suffixes_.word_len_at(wordIdSfx);
					const int tcount = std::max(positions.size(), wordTypo.positions.size());  // Each letter switch equals to 1 typo
					const auto& rankingConfig = ctx.holder.cfg_->rankingConfig;
					const float proc =
						std::max<float>(ctx.patternProc * rankingConfig.TypoCoeff() -
											tcount * rankingConfig.TypoPenalty() /
												std::max<float>((wordLength - tcount) / 3.f, FTRankingConfig::kMinProcAfterPenalty),
										1.f);

					const auto [it, emplaced] = ctx.fixedVariants.try_emplace(wordTypo.word, proc);
					if (emplaced) {
						const auto& wordTypoEntry = ctx.holder.GetWordEntry(wordTypo.word);
						std::string typoUTF8 = utf16_to_utf8(typo);
						if (logLevel_ >= LogTrace) [[unlikely]] {
							logFmt(LogInfo, fmt::runtime(" matched typo '{}' of word '{}', {} ids, {}%"), typoUTF8, word,
								   wordTypoEntry.vids.size(), proc);
						}
						++ctx.matched;
					} else {
						++ctx.skipped;
						it->second = std::max(it->second, proc);
					}
				}
			};

			mktypos(pattern, holder.cfg_->MaxTyposInWord(), holder.cfg_->maxTypoLen, callback, buf);
			if (holder.cfg_->logLevel >= LogInfo) [[unlikely]] {
				logFmt(LogInfo, "Lookup typos, matched {} typos, skipped {}", ctx.matched, ctx.skipped);
			}
		}
	}

private:
	bool checkMaxTyposDist(const WordTypo& found, const TyposVec& current);
	bool checkMaxLettPermDist(std::string_view foundWord, const WordTypo& found, std::wstring_view currentWord, const TyposVec& current);

	bool useMaxTypoDist_;
	bool useMaxLettPermDist_;
	unsigned maxTypoDist_;
	unsigned maxLettPermDist_;
	unsigned maxMissingLetts_;
	unsigned maxExtraLetts_;
	int logLevel_;
	std::wstring foundWordUTF16_;
};

}  // namespace reindexer
