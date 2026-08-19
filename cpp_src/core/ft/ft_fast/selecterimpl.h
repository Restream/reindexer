#include "core/ft/variants/typos.h"
#include "mergerimpl.h"
#include "selecter.h"
#include "tools/objects_pool.h"
#include "utf8cpp/utf8.h"

namespace reindexer {

static constexpr uint32_t kMinPartialMatchDenominator = 3;
static constexpr size_t kKbLayoutHeuristicMinWords = 10;
static constexpr size_t kKbLayoutHeuristicMinMergeLimit = 400;

template <typename IdCont>
void Selector<IdCont>::filterStopWordsAndAdd(TermVariants& termVariants, h_vector<TermVariant, 5>& newVariants) const {
	const StopWordsSetT& stopWords = holder_.cfg_->stopWords;
	termVariants.reserve(termVariants.size() + newVariants.size());
	for (auto& v : newVariants) {
		if (stopWords.find(v.PatternUtf8()) == stopWords.end()) {
			termVariants.emplace_back(std::move(v));
		}
	}
}

template <typename IdCont>
bool Selector<IdCont>::exceedsKbLayoutHeuristicThresholds(std::u16string_view pattern, bool pref, bool suff,
														  const FtMergeStatuses::Statuses& docsExcluded, size_t wordsLimit,
														  size_t docsLimit, size_t& words, size_t& docs) const {
	words = 0;
	docs = 0;
	if (pattern.empty()) {
		return false;
	}

	const auto* suffixes = holder_.Suffixes(pattern.front());
	if (!suffixes) {
		return false;
	}

	for (auto wordIt = suffixes->lower_bound(pattern); wordIt != suffixes->end(); ++wordIt) {
		if (!SuffixStartsWith(holder_.GetSuffixData(*wordIt), pattern)) {
			break;
		}

		const auto suffixInfo = holder_.ResolveSuffix(*wordIt);
		const auto& wordOccurences = holder_.GetWordOccurences(suffixInfo.wordId);
		if (allVidsExcluded(docsExcluded, wordOccurences)) {
			continue;
		}

		const size_t lengthBeforePattern = suffixInfo.offset;
		if (!suff && lengthBeforePattern != 0) {
			continue;
		}

		const size_t lengthAfterPattern = suffixInfo.length - pattern.length() - lengthBeforePattern;
		if (!pref && lengthAfterPattern != 0) {
			break;
		}

		++words;
		docs += wordOccurences.size();
		if (words >= wordsLimit || docs > docsLimit) {
			return true;
		}
	}

	return false;
}

template <typename IdCont>
bool Selector<IdCont>::shouldEnableKbLayoutCorrection(const FtDSLEntry& term, const FtMergeStatuses::Statuses& docsExcluded) const {
	using KbLayoutMode = FTConfig::KbLayoutMode;
	if (holder_.cfg_->kbLayoutMode == KbLayoutMode::Disable) {
		return false;
	}
	if (holder_.cfg_->kbLayoutMode == KbLayoutMode::Enable) {
		return true;
	}

	const FtDslOpts& opts = term.Opts();
	if (!opts.pref && !opts.suff) {
		return true;
	}

	const size_t effectiveMergeLimit =
		holder_.cfg_->mergeLimit < kKbLayoutHeuristicMinMergeLimit ? kKbLayoutHeuristicMinMergeLimit : holder_.cfg_->mergeLimit;
	const size_t docsLimit = effectiveMergeLimit / 4;
	size_t words = 0;
	size_t docs = 0;
	if (exceedsKbLayoutHeuristicThresholds(term.Pattern(), opts.pref, opts.suff, docsExcluded, kKbLayoutHeuristicMinWords, docsLimit, words,
										   docs)) {
		if (holder_.cfg_->logLevel >= LogInfo) [[unlikely]] {
			logFmt(LogInfo, "KbLayoutCorrection disabled by heuristic for term '{}': matched words {}, docs {}, limits words>={}, docs>{}",
				   utf16_to_utf8(term.Pattern()), words, docs, kKbLayoutHeuristicMinWords, docsLimit);
		}
		return false;
	}
	return true;
}

template <typename IdCont>
void Selector<IdCont>::tryToCorrectKbLayout(TermVariants& termVariants, bool enable) {
	if (!enable) {
		for (TermVariant& v : termVariants) {
			v.RemovePossibleExtraTermSymbol(splitOptions_);
		}
		return;
	}

	const FTRankingConfig& rankingCfg = holder_.cfg_->rankingConfig;
	newVariants.resize(0);
	newVariants.reserve(termVariants.size());

	__RX_VAR_FROM_POOL__(std::u16string, correctedPattern)

	for (TermVariant& v : termVariants) {
		if (v.pattern.size() <= 3 && (v.pref || v.suff)) {
			v.RemovePossibleExtraTermSymbol(splitOptions_);
			continue;
		}
		// NOLINTNEXTLINE(bugprone-use-after-move)
		holder_.kbLayout_->Transform(v.pattern, correctedPattern);
		if (!correctedPattern.empty() && correctedPattern != v.pattern) {
			float kblayoutProc = v.proc * rankingCfg.KbLayoutCoeff();
			newVariants.emplace_back(std::move(correctedPattern), kblayoutProc, v);
		}

		v.RemovePossibleExtraTermSymbol(splitOptions_);
	}

	filterStopWordsAndAdd(termVariants, newVariants);
}

template <typename IdCont>
void Selector<IdCont>::tryToSplit(TermVariants& termVariants, PhraseTerm phraseTerm) {
	const FTRankingConfig& rankingCfg = holder_.cfg_->rankingConfig;
	if (!holder_.cfg_->splitOptions.HasDelims()) {
		return;
	}

	__RX_VAR_FROM_POOL__(std::u16string, dataWithoutDelims)
	__RX_VAR_FROM_POOL__(std::u16string, nextPart)

	newVariants.resize(0);
	newVariants.reserve(termVariants.size());

	for (const TermVariant& v : termVariants) {
		if (!holder_.cfg_->splitOptions.ContainsDelims(v.pattern)) {
			continue;
		}
		// NOLINTNEXTLINE(bugprone-use-after-move)
		dataWithoutDelims.resize(0);
		// NOLINTNEXTLINE(bugprone-use-after-move)
		nextPart.resize(0);
		const float delimitedProc = v.proc * rankingCfg.DelimitedCoeff();
		size_t numPartsFound = 0;

		for (char16_t symbol : v.pattern) {
			if (!holder_.cfg_->splitOptions.IsWordPartDelimiter(symbol)) {
				dataWithoutDelims.push_back(symbol);
				nextPart.push_back(symbol);
				continue;
			}

			if (!nextPart.empty()) {
				numPartsFound++;
				if (phraseTerm == PhraseTerm_False && nextPart.size() >= holder_.cfg_->splitOptions.GetMinPartSize()) {
					newVariants.emplace_back(std::move(nextPart), delimitedProc, v);
					newVariants.back().pref = false;
					newVariants.back().suff = false;
					newVariants.back().stem = false;
					newVariants.back().synonyms = false;
				}

				// NOLINTNEXTLINE(bugprone-use-after-move)
				nextPart.resize(0);
			}
		}

		if (!nextPart.empty()) {
			numPartsFound++;
			if (phraseTerm == PhraseTerm_False && nextPart.size() >= holder_.cfg_->splitOptions.GetMinPartSize()) {
				newVariants.emplace_back(std::move(nextPart), delimitedProc, v);
				newVariants.back().synonyms = false;
				if (newVariants.back().pattern.size() < kMinSplitVariantStemLen) {
					newVariants.back().stem = false;
				}
			}
		}

		// for word with delimiters e.g. user-friendly we should also search for userfriendly #1863
		if (numPartsFound > 1) {
			newVariants.emplace_back(std::move(dataWithoutDelims), delimitedProc, v);
			if (newVariants.back().pattern.size() < kMinSplitVariantStemLen) {
				newVariants.back().stem = false;
			}
		}
	}

	filterStopWordsAndAdd(termVariants, newVariants);
}

template <typename IdCont>
void Selector<IdCont>::tryToCorrectTypos(TermVariants& termVariants) {
	TyposHandler typosHandler(*holder_.cfg_);

	__RX_VAR_FROM_POOL__(FoundWordsProcsType, fixedVariants)
	__RX_VAR_FROM_POOL__(FoundWordsType, wordsFound)

	newVariants.resize(0);
	newVariants.reserve(termVariants.size());

	for (const TermVariant& v : termVariants) {
		if (!v.typos) {
			continue;
		}
		const bool stripWildcards = v.pattern.size() <= 3;
		fixedVariants.clear();
		typosHandler.Process(v.pattern, v.proc, fixedVariants, holder_);

		for (auto& [wId, proc] : fixedVariants) {
			if (auto wfIt = wordsFound.find(wId); wfIt != wordsFound.end()) {
				newVariants[wfIt->second].Unite(v, proc, stripWildcards);
			} else {
				wordsFound[wId] = newVariants.size();
				newVariants.emplace_back(std::u16string(holder_.GetWord(wId)), proc, v);
				if (stripWildcards) {
					newVariants.back().pref = false;
					newVariants.back().suff = false;
				}
				if (newVariants.back().pattern.size() < kMinTypoVariantStemLen) {
					newVariants.back().stem = false;
				}
			}
		}
	}

	filterStopWordsAndAdd(termVariants, newVariants);
}

template <typename IdCont>
void Selector<IdCont>::transliterate(TermVariants& termVariants) {
	if (!holder_.cfg_->enableTranslit) {
		return;
	}

	const FTRankingConfig& rankingCfg = holder_.cfg_->rankingConfig;
	newVariants.resize(0);
	newVariants.reserve(termVariants.size());

	using TranslitVariantsType = h_vector<std::u16string, 5>;
	__RX_VAR_FROM_POOL__(TranslitVariantsType, translitVariants)

	for (const TermVariant& v : termVariants) {
		translitVariants.resize(0);
		holder_.translit_->Transliterate(v.pattern, translitVariants);
		float translitProc = v.proc * rankingCfg.TranslitCoeff();
		for (auto& patternTransliterated : translitVariants) {
			if (patternTransliterated != v.pattern) {
				newVariants.emplace_back(std::move(patternTransliterated), translitProc, v);
			}
		}
	}

	filterStopWordsAndAdd(termVariants, newVariants);
}

template <typename IdCont>
void Selector<IdCont>::stem(TermVariants& termVariants) {
	const FTRankingConfig& rankingCfg = holder_.cfg_->rankingConfig;

	__RX_VAR_FROM_POOL__(std::string, stemstr)

	newVariants.resize(0);
	newVariants.reserve(termVariants.size());

	for (TermVariant& v : termVariants) {
		if (!v.stem) {
			continue;
		}
		v.stem = false;
		const float stemProc = rankingCfg.StemProc(v.proc);

		if (termVariants.Op() == OpNot && v.suff) {
			// More strict match for negative (excluding) suffix terms
			if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
				logFmt(LogInfo, "Skipping stemming for '{}'", v.FullPattern());
			}
			continue;
		}

		for (auto& lang : holder_.cfg_->stemmers) {
			auto stemIt = holder_.stemmers_.find(lang);
			if (stemIt == holder_.stemmers_.end()) {
				throw Error(errParams, "Stemmer for language {} is not available", lang);
			}
			stemstr.resize(0);
			stemIt->second.stem(v.PatternUtf8(), stemstr);
			if (stemstr == v.PatternUtf8() || stemstr.empty()) {
				continue;
			}

			const int stemLen = getUTF8StringCharactersCount(stemstr);
			if (stemLen <= kMaxStemSkipLen) {
				if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
					logFmt(LogInfo, "Skipping too short stemmer's term '{}{}*'", v.suff ? "*" : "", stemstr);
				}
				continue;
			} else if (stemLen >= kMinStemRelevantLen) {
				std::u16string wStemStr = utf8_to_utf16(stemstr);
				newVariants.emplace_back(std::move(wStemStr), stemProc, v);
				newVariants.back().pref = true;
			} else {
				// low relevant stemmed term
				std::u16string wStemStr = utf8_to_utf16(stemstr);
				newVariants.emplace_back(std::move(wStemStr), stemProc, v);
				newVariants.back().pref = true;
				newVariants.back().lowRelevance = true;
			}
		}
	}

	filterStopWordsAndAdd(termVariants, newVariants);
}

template <typename IdCont>
void Selector<IdCont>::addSynonyms(TermVariants& termVariants) {
	if (termVariants.Op() == OpNot) {
		return;
	}

	const FTRankingConfig& rankingCfg = holder_.cfg_->rankingConfig;
	newVariants.resize(0);
	newVariants.reserve(termVariants.size());

	using SynonymsType = h_vector<std::u16string, 5>;
	__RX_VAR_FROM_POOL__(SynonymsType, synonyms)

	for (const TermVariant& v : termVariants) {
		if (!v.synonyms) {
			continue;
		}

		synonyms.resize(0);
		holder_.synonyms_->FindOne2OneSubstitutions(v.pattern, synonyms);
		float synonymProc = v.proc * rankingCfg.SynonymsCoeff();
		for (auto& synonym : synonyms) {
			newVariants.emplace_back(std::move(synonym), synonymProc, v);
			newVariants.back().synonyms = false;
			newVariants.back().stem = true;
		}
	}

	filterStopWordsAndAdd(termVariants, newVariants);
}

template <class VidsContainer>
static bool allVidsExcluded(const FtMergeStatuses::Statuses& docsExcluded, const VidsContainer& wordOccurences) {
	for (const auto& id : wordOccurences) {
		if (!docsExcluded[id.Id()]) {
			return false;
		}
	}

	return true;
}

// Lookup indexed words for each variant; apply partial prefix/suffix penalty (PartialMatchDecrease,
// PrefixMin/SuffixMin) and terms_boost. Best proc per indexed word is kept.
// See fulltext_ranking.md#how-term-variants-are-scored
template <typename IdCont>
void Selector<IdCont>::processExactTermVariant(TermVariant& variant, ft::TermResults<IdCont>& res, FoundWordsType& wordsFound,
											   size_t& totalVids, const FtMergeStatuses::Statuses& docsExcluded) {
	size_t matched = 0, vids = 0, excludedCnt = 0;
	std::string wordUtf8 = variant.PatternUtf8();
	const size_t wordOrdinal = holder_.FindWordOrdinal(wordUtf8);
	if (wordOrdinal != IDataHolder::kIncorrectWordOrdinal) {
		const WordIdType wordId = holder_.GetWordIdByOrdinal(wordOrdinal);
		const auto& wordOccurences = holder_.GetWordOccurencesByOrdinal(wordOrdinal);
		if (allVidsExcluded(docsExcluded, wordOccurences)) {
			++excludedCnt;
		} else {
			const float boost = std::max(getTermBoost(wordUtf8), variant.boost);
			float proc = variant.proc;
			if (boost > 0.0f) {
				proc *= boost;
			}

			if (auto it = wordsFound.find(wordId); it != wordsFound.end()) {
				res.Subterm(it->second).SetProc(std::max(res.Subterm(it->second).Proc(), proc));
			} else {
				res.AddSubterm(wordOccurences, std::move(wordUtf8), wordId, proc);
				wordsFound[wordId] = res.NumSubterms() - 1;
				++matched;
				totalVids += wordOccurences.size();
				vids += wordOccurences.size();
			}
		}
	}

	if (holder_.cfg_->logLevel >= LogInfo) [[unlikely]] {
		logFmt(LogInfo, "Lookup variant '{}' ({}%), matched {} words, with {} vids, excluded {}", variant.FullPattern(), variant.proc,
			   matched, vids, excludedCnt);
	}
}

template <typename IdCont>
void Selector<IdCont>::processSuffixTermVariant(TermVariant& variant, ft::TermResults<IdCont>& res, FoundWordsType& wordsFound,
												size_t& totalVids, size_t lowRelevanceLimit, const FtMergeStatuses::Statuses& docsExcluded,
												const FTRankingConfig& rankingCfg) {
	size_t matched = 0, vids = 0, excludedCnt = 0;
	const auto& pattern = variant.pattern;
	assertrx(!pattern.empty());

	const size_t singleAffixQueryLimit = 2 * holder_.cfg_->mergeLimit;

	const auto* suffixes = holder_.Suffixes(pattern.front());
	if (suffixes) {
		for (auto wordIt = suffixes->lower_bound(std::u16string_view(pattern)); wordIt != suffixes->end(); ++wordIt) {
			if (!SuffixStartsWith(holder_.GetSuffixData(*wordIt), pattern)) {
				break;
			}

			if (variant.lowRelevance && totalVids >= lowRelevanceLimit) {
				break;
			}
			if (limitSingleAffixQuerySubterms_ && totalVids > singleAffixQueryLimit) {
				break;
			}

			const auto suffixInfo = holder_.ResolveSuffix(*wordIt);
			const WordIdType wordId = suffixInfo.wordId;
			const auto& wordOccurences = holder_.GetWordOccurences(wordId);
			if (allVidsExcluded(docsExcluded, wordOccurences)) {
				++excludedCnt;
				continue;
			}

			const size_t lengthBeforePattern = suffixInfo.offset;
			if (!variant.suff && lengthBeforePattern != 0) {
				continue;
			}

			const size_t wordLen = suffixInfo.length;
			const size_t lengthAfterPattern = wordLen - pattern.length() - lengthBeforePattern;
			if (!variant.pref && lengthAfterPattern != 0) {
				break;
			}

			const size_t unmatchedChars = lengthBeforePattern + lengthAfterPattern;
			const float decreasePenalty = (static_cast<float>(holder_.cfg_->partialMatchDecrease) * static_cast<float>(unmatchedChars)) /
										  static_cast<float>(std::max(pattern.length(), size_t(kMinPartialMatchDenominator)));
			const bool isPrefix = lengthBeforePattern == 0;
			float proc = std::max<float>(variant.proc - decreasePenalty, isPrefix ? rankingCfg.PrefixMin() : rankingCfg.SuffixMin());
			proc = std::min<float>(proc, variant.proc);

			const auto foundWord = wordsFound.find(wordId);
			float boost = variant.boost;
			std::string wordUtf8;
			if (!holder_.stemmedTermsBoost.empty()) {
				std::string_view word;
				if (foundWord != wordsFound.end()) {
					word = res.Subterm(foundWord->second).Pattern();
				} else {
					wordUtf8 = utf16_to_utf8(std::u16string_view(holder_.GetWordData(wordId), wordLen));
					word = wordUtf8;
				}
				boost = std::max(getTermBoost(word), boost);
			}
			if (boost > 0.0f) {
				proc *= boost;
			}

			if (foundWord != wordsFound.end()) {
				res.Subterm(foundWord->second).SetProc(std::max(res.Subterm(foundWord->second).Proc(), proc));
			} else {
				if (wordUtf8.empty()) {
					wordUtf8 = utf16_to_utf8(std::u16string_view(holder_.GetWordData(wordId), wordLen));
				}
				res.AddSubterm(wordOccurences, std::move(wordUtf8), wordId, proc);
				wordsFound[wordId] = res.NumSubterms() - 1;
				++matched;
				totalVids += wordOccurences.size();
				vids += wordOccurences.size();
			}
		}
	}

	if (holder_.cfg_->logLevel >= LogInfo) [[unlikely]] {
		logFmt(LogInfo, "Lookup variant '{}' ({}%), matched {} words, with {} vids, excluded {}", variant.FullPattern(), variant.proc,
			   matched, vids, excludedCnt);
	}
}

template <typename IdCont>
ft::TermResults<IdCont> Selector<IdCont>::buildTermResults(const FtDSLEntry& term, TermVariants& termVariants,
														   const FtMergeStatuses::Statuses& docsExcluded) {
	const FTRankingConfig& rankingCfg = holder_.cfg_->rankingConfig;
	ft::TermResults<IdCont> res(term);

	__RX_VAR_FROM_POOL__(FoundWordsType, wordsFound)
	termVariants.SortByProc();

	size_t totalVids = 0;
	const size_t lowRelevanceLimit = 4 * holder_.cfg_->mergeLimit;
	const size_t singleAffixQueryLimit = 2 * holder_.cfg_->mergeLimit;
	bool singleAffixQueryLimitApplied = false;

	for (auto& variant : termVariants) {
		if (limitSingleAffixQuerySubterms_ && totalVids > singleAffixQueryLimit) {
			singleAffixQueryLimitApplied = true;
			break;
		}
		if (variant.lowRelevance && totalVids >= lowRelevanceLimit) {
			continue;
		}
		assertrx(!variant.pattern.empty());
		if (!variant.pref && !variant.suff) {
			processExactTermVariant(variant, res, wordsFound, totalVids, docsExcluded);
		} else {
			processSuffixTermVariant(variant, res, wordsFound, totalVids, lowRelevanceLimit, docsExcluded, rankingCfg);
			if (limitSingleAffixQuerySubterms_ && totalVids > singleAffixQueryLimit) {
				singleAffixQueryLimitApplied = true;
			}
		}
	}

	if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
		for (const ft::SubtermResults<IdCont>& subterm : res) {
			logFmt(LogInfo, "Matched word '{}', {} vids, {}%", subterm.Pattern(), subterm.Occurences().size(), subterm.Proc());
		}
	}

	if (singleAffixQueryLimitApplied && holder_.cfg_->logLevel >= LogInfo) [[unlikely]] {
		logFmt(LogInfo, "Single affix query subterms limited: total vids {}, limit {}", totalVids, singleAffixQueryLimit);
	}

	return res;
}

template <typename IdCont>
static FtDslOpts calcSubstitutionOptions(const ft::QueryMergeData<IdCont>& queryMergeData, const Synonyms::Substitution& subst) {
	FtDslOpts substOpts = queryMergeData.queryParts[subst.positionsSubstituted[0]].Term().Opts();
	substOpts.pref = false;
	substOpts.suff = false;

	for (size_t i = 1; i < subst.positionsSubstituted.size(); ++i) {
		const FtDslOpts& opts = queryMergeData.queryParts[subst.positionsSubstituted[i]].Term().Opts();

		substOpts.boost += opts.boost;
		substOpts.termLenBoost += opts.termLenBoost;
		assertrx(substOpts.fieldsOpts.size() == opts.fieldsOpts.size());
		for (size_t f = 0; f < opts.fieldsOpts.size(); ++f) {
			substOpts.fieldsOpts[f].boost += opts.fieldsOpts[f].boost;
		}
	}

	substOpts.boost /= subst.positionsSubstituted.size();
	substOpts.termLenBoost /= subst.positionsSubstituted.size();
	for (auto& fOpts : substOpts.fieldsOpts) {
		fOpts.boost /= subst.positionsSubstituted.size();
	}

	return substOpts;
}

template <typename IdCont>
h_vector<size_t, 4> Selector<IdCont>::addSynonymsBySplittingTermVariants(TermVariants& termVariants,
																		 const FtMergeStatuses::Statuses& docsExcluded,
																		 ft::QueryMergeData<IdCont>& queryMergeData) {
	const FTRankingConfig& rankingCfg = holder_.cfg_->rankingConfig;
	const StopWordsSetT& stopWords = holder_.cfg_->stopWords;

	h_vector<size_t, 4> synonymIds;

	auto trySplitAt = [&](TermVariant& tv, std::u16string_view firstSplitPart, std::string_view firstSplitPartUtf8,
						  std::u16string_view secondSplitPart, std::string_view secondSplitPartUtf8) {
		if (!holder_.ContainsWord(firstSplitPartUtf8) || !holder_.ContainsWord(secondSplitPartUtf8) ||
			stopWords.find(firstSplitPartUtf8) != stopWords.end() || stopWords.find(secondSplitPartUtf8) != stopWords.end()) {
			return;
		}

		ft::Synonym<IdCont> synData;
		const FtDslOpts& opts = termVariants.Opts();

		TermVariants firstPartVariants(opts);
		firstPartVariants.emplace_back(std::u16string(firstSplitPart), (tv.proc / 2.0) * rankingCfg.SplitCoeff());
		firstPartVariants.back().suff = false;
		if (firstSplitPart.size() < kMinSplitVariantStemLen) {
			firstPartVariants.back().stem = false;
		}

		transliterate(firstPartVariants);
		stem(firstPartVariants);
		for (auto& v : firstPartVariants) {
			v.boost = getTermBoost(v.PatternUtf8());
		}

		ft::TermResults<IdCont> firstPartTerm =
			buildTermResults(FtDSLEntry(std::u16string(firstSplitPart), opts), firstPartVariants, docsExcluded);
		queryMergeData.totalORVids += firstPartTerm.MaxVDocs();
		synData.AddTerm(std::move(firstPartTerm));

		TermVariants secondPartVariants(opts);
		secondPartVariants.emplace_back(std::u16string(secondSplitPart), (tv.proc / 2.0) * rankingCfg.SplitCoeff());
		secondPartVariants.back().pref = false;
		if (secondSplitPart.size() < kMinSplitVariantStemLen) {
			secondPartVariants.back().stem = false;
		}

		transliterate(secondPartVariants);
		stem(secondPartVariants);
		for (auto& v : secondPartVariants) {
			v.boost = getTermBoost(v.PatternUtf8());
		}

		ft::TermResults<IdCont> secondPartTerm =
			buildTermResults(FtDSLEntry(std::u16string(secondSplitPart), opts), secondPartVariants, docsExcluded);
		queryMergeData.totalORVids += secondPartTerm.MaxVDocs();
		synData.AddTerm(std::move(secondPartTerm));

		queryMergeData.synonyms.emplace_back(std::move(synData));
		synonymIds.push_back(queryMergeData.synonyms.size() - 1);
	};

	for (auto& tv : termVariants) {
		if (!tv.split || tv.pattern.size() < kMinSplitSize + 1 || tv.pattern.size() > kMaxSplitLen) {
			continue;
		}
		const std::string& patternUtf8 = tv.PatternUtf8();
		const bool startsWithSingleDigit = IsDigit(tv.pattern.front()) && !IsDigit(tv.pattern[1]);
		const bool endsWithSingleDigit = IsDigit(tv.pattern.back()) && !IsDigit(tv.pattern[tv.pattern.size() - 2]);

		if (startsWithSingleDigit) {
			trySplitAt(tv, std::u16string_view(tv.pattern.data(), 1), std::string_view(patternUtf8.data(), 1),
					   std::u16string_view(tv.pattern.data() + 1, tv.pattern.size() - 1),
					   std::string_view(patternUtf8.data() + 1, patternUtf8.size() - 1));
		}

		auto splitIt = patternUtf8.begin();
		size_t splitIdx = 0;
		while (splitIdx < kMinSplitSize) {
			std::ignore = utf8::unchecked::next(splitIt);
			++splitIdx;
		}

		while (splitIdx + kMinSplitSize < tv.pattern.size()) {
			trySplitAt(tv, std::u16string_view(tv.pattern.begin(), tv.pattern.begin() + splitIdx),
					   std::string_view(patternUtf8.begin(), splitIt), std::u16string_view(tv.pattern.begin() + splitIdx, tv.pattern.end()),
					   std::string_view(splitIt, patternUtf8.end()));

			std::ignore = utf8::unchecked::next(splitIt);
			++splitIdx;
		}

		if (endsWithSingleDigit) {
			trySplitAt(tv, std::u16string_view(tv.pattern.data(), tv.pattern.size() - 1),
					   std::string_view(patternUtf8.data(), patternUtf8.size() - 1),
					   std::u16string_view(tv.pattern.data() + tv.pattern.size() - 1, 1),
					   std::string_view(patternUtf8.data() + patternUtf8.size() - 1, 1));
		}
	}

	return synonymIds;
}

// FT variant pipeline per query term: kblayout → typos → split → translit → stem → synonyms.
// Sets initial proc (FullMatch / ConcatProc) and builds TermResults for merger.
// See fulltext_ranking.md#how-term-variants-are-scored
template <typename IdCont>
void Selector<IdCont>::buildQueryMergeData(FtDSLQuery&& query, const FtMergeStatuses::Statuses& docsExcluded, bool inTransaction,
										   const RdxContext& rdxCtx, ft::QueryMergeData<IdCont>& queryMergeData) {
	const FTRankingConfig& rankingCfg = holder_.cfg_->rankingConfig;

	limitSingleAffixQuerySubterms_ = false;
	if (query.NumTerms() == 1) {
		const FtDslOpts& opts = query.GetTerm(0).Opts();
		limitSingleAffixQuerySubterms_ = (opts.pref || opts.suff) && !opts.exact && opts.phraseNum == -1;
	}

	int curPhraseNum = -1;
	ft::PhraseResults<IdCont> nextPhrase;

	__RX_VAR_FROM_POOL__(std::vector<TermVariants>, variantsForSubstitution)
	__RX_VAR_FROM_POOL__(std::vector<size_t>, variantsForSubstitutionPositions)

	for (size_t queryTermIdx = 0; queryTermIdx < query.NumTerms(); ++queryTermIdx) {
		if (!inTransaction) {
			ThrowOnCancel(rdxCtx);
		}

		const FtDSLEntry& term = query.GetTerm(queryTermIdx);
		TermVariants termVariants(term.Opts());
		termVariants.emplace_back(term.Pattern(), rankingCfg.FullMatch());

		const bool phraseTerm = term.Opts().phraseNum != -1;
		if (!phraseTerm && nextPhrase.NumTerms()) {
			queryMergeData.queryParts.emplace_back(std::move(nextPhrase));
			// NOLINTNEXTLINE(bugprone-use-after-move)
			nextPhrase.clear();
		}

		const bool exact = term.Opts().exact;
		h_vector<size_t, 4> synonymIds;
		const bool enableKbLayout = shouldEnableKbLayoutCorrection(term, docsExcluded);

		if (phraseTerm) {
			tryToCorrectKbLayout(termVariants, enableKbLayout);
			tryToCorrectTypos(termVariants);
			tryToSplit(termVariants, PhraseTerm_True);
			addSynonyms(termVariants);

			if (!exact) {
				transliterate(termVariants);
				stem(termVariants);
			}
		} else if (exact) {
			tryToCorrectTypos(termVariants);
		} else {
			bool needJoinWithPrevTerm = holder_.cfg_->enableTermsConcat && queryTermIdx > 0;
			if (needJoinWithPrevTerm && term.CanBeJoinedWith(query.GetTerm(queryTermIdx - 1))) {
				FtDSLEntry joinedTerm = term.JoinWithPrevTerm(query.GetTerm(queryTermIdx - 1));
				termVariants.emplace_back(std::move(joinedTerm.Pattern()), rankingCfg.Concat(), joinedTerm.Opts());
				termVariants.back().split = false;
			}

			tryToCorrectKbLayout(termVariants, enableKbLayout);
			if (term.Opts().op == OpOr && holder_.cfg_->enableTermsSplit) {
				synonymIds = addSynonymsBySplittingTermVariants(termVariants, docsExcluded, queryMergeData);
			}

			tryToCorrectTypos(termVariants);
			tryToSplit(termVariants, PhraseTerm_False);
			transliterate(termVariants);
			stem(termVariants);
			addSynonyms(termVariants);
			// stem synonyms
			stem(termVariants);
		}

		for (auto& v : termVariants) {
			v.boost = getTermBoost(v.PatternUtf8());
		}

		ft::TermResults<IdCont> nextTerm = buildTermResults(term, termVariants, docsExcluded);
		queryMergeData.totalORVids += nextTerm.MaxVDocs();
		if (phraseTerm) {
			if (nextPhrase.NumTerms() && curPhraseNum != term.Opts().phraseNum) {
				queryMergeData.queryParts.emplace_back(std::move(nextPhrase));
				// NOLINTNEXTLINE(bugprone-use-after-move)
				nextPhrase.clear();
			}

			curPhraseNum = term.Opts().phraseNum;
			nextPhrase.Add(std::move(nextTerm));
		} else {
			queryMergeData.queryParts.emplace_back(std::move(nextTerm));
			for (size_t synonymId : synonymIds) {
				queryMergeData.queryParts.back().AddSynonymId(synonymId);
			}

			if (termVariants.Op() != OpNot) {
				variantsForSubstitution.emplace_back(std::move(termVariants));
				variantsForSubstitutionPositions.emplace_back(queryMergeData.queryParts.size() - 1);
			}
		}
	}

	if (nextPhrase.NumTerms()) {
		queryMergeData.queryParts.emplace_back(std::move(nextPhrase));
		// NOLINTNEXTLINE(bugprone-use-after-move)
		nextPhrase.clear();
	}

	__RX_VAR_FROM_POOL__(std::vector<Synonyms::Substitution>, substitutions)
	holder_.synonyms_->FindComplexSubstitutions(variantsForSubstitution, substitutions);

	for (Synonyms::Substitution& subst : substitutions) {
		subst.TransformPositions(variantsForSubstitutionPositions);
		assertrx_dbg(subst.positionsSubstituted.size() > 0);
		FtDslOpts substOpts = calcSubstitutionOptions(queryMergeData, subst);

		ft::Synonym<IdCont> synData;
		for (std::u16string& word : subst.substitutionWords) {
			TermVariants termVariants(substOpts);
			termVariants.emplace_back(word, (subst.proc / subst.substitutionWords.size()) * rankingCfg.SynonymsCoeff());

			transliterate(termVariants);
			stem(termVariants);
			for (auto& v : termVariants) {
				v.boost = getTermBoost(v.PatternUtf8());
			}

			ft::TermResults<IdCont> nextTerm = buildTermResults(FtDSLEntry(word, substOpts), termVariants, docsExcluded);
			queryMergeData.totalORVids += nextTerm.MaxVDocs();
			synData.AddTerm(std::move(nextTerm));
		}
		queryMergeData.synonyms.emplace_back(std::move(synData));

		for (size_t i = 0; i < subst.positionsSubstituted.size(); ++i) {
			size_t synonymId = queryMergeData.synonyms.size() - 1;
			queryMergeData.queryParts[subst.positionsSubstituted[i]].AddSynonymId(synonymId);
		}
	}

	queryMergeData.SupressDuplicatesInSynonyms();
}

// Dispatches document-frequency scoring: rx_bm25 / bm25 / word_count.
// See fulltext.md#basic-document-ranking-algorithms
template <typename IdCont>
template <typename MergedOffsetT, typename MergedDataType, typename DocsStatsGetter>
MergedDataType Selector<IdCont>::mergeResults(size_t totalNumDocs, ft::QueryMergeData<IdCont>& queryMergeData, RankSortType rankSortType,
											  FtMergeStatuses::Statuses& docsExcluded, bool inTransaction, const RdxContext& rdxCtx,
											  const DocsStatsGetter& docsStatsGetter) {
	ft::Merger<IdCont, MergedDataType, MergedOffsetT> merger(totalNumDocs, holder_.cfg_, docsExcluded, fieldSize_, maxAreasInDoc_,
															 inTransaction, rdxCtx);
	switch (holder_.cfg_->bm25Config.bm25Type) {
		case FTConfig::Bm25Config::Bm25Type::rx:
			return merger.template Merge<Bm25Rx>(queryMergeData, rankSortType, docsStatsGetter);
		case FTConfig::Bm25Config::Bm25Type::classic:
			return merger.template Merge<Bm25Classic>(queryMergeData, rankSortType, docsStatsGetter);
		case FTConfig::Bm25Config::Bm25Type::wordCount:
			return merger.template Merge<TermCount>(queryMergeData, rankSortType, docsStatsGetter);
		default:
			assertrx_throw(false);
			return MergedDataType();
	}
}

template <typename IdCont>
template <typename MergedDataType, typename DocsStatsGetter>
MergedDataType Selector<IdCont>::Process(size_t totalNumDocs, FtDSLQuery&& query, bool inTransaction, RankSortType rankSortType,
										 FtMergeStatuses::Statuses&& docsExcluded, const RdxContext& rdxCtx,
										 const DocsStatsGetter& docsStatsGetter) {
	ft::QueryMergeData<IdCont> queryMergeData;
	buildQueryMergeData(std::move(query), docsExcluded, inTransaction, rdxCtx, queryMergeData);

	const auto maxMergedSize = std::min<uint32_t>(holder_.cfg_->mergeLimit, queryMergeData.totalORVids);
	assertrx_throw(maxMergedSize < 0xFFFFFFFF);
	if (maxMergedSize < 0xFFFF) {
		return mergeResults<uint16_t, MergedDataType>(totalNumDocs, queryMergeData, rankSortType, docsExcluded, inTransaction, rdxCtx,
													  docsStatsGetter);
	}
	return mergeResults<uint32_t, MergedDataType>(totalNumDocs, queryMergeData, rankSortType, docsExcluded, inTransaction, rdxCtx,
												  docsStatsGetter);
}

}  // namespace reindexer
