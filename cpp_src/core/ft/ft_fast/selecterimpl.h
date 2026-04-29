#include "core/ft/variants/typos.h"
#include "mergerimpl.h"
#include "selecter.h"
#include "tools/objects_pool.h"
#include "tools/scope_guard.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer {

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
void Selector<IdCont>::tryToCorrectKbLayout(TermVariants& termVariants) {
	const FTRankingConfig& rankingCfg = holder_.cfg_->rankingConfig;
	newVariants.resize(0);
	newVariants.reserve(termVariants.size());

	__RX_VAR_FROM_POOL__(std::wstring, correctedPattern)

	for (const TermVariant& v : termVariants) {
		// NOLINTNEXTLINE(bugprone-use-after-move)
		holder_.kbLayout_->Transform(v.pattern, correctedPattern);
		if (!correctedPattern.empty() && correctedPattern != v.pattern) {
			float kblayoutProc = v.proc * rankingCfg.KbLayoutCoeff();
			newVariants.emplace_back(std::move(correctedPattern), kblayoutProc, v);
		}
	}

	filterStopWordsAndAdd(termVariants, newVariants);
}

template <typename IdCont>
void Selector<IdCont>::tryToSplit(TermVariants& termVariants, PhraseTerm phraseTerm) {
	const FTRankingConfig& rankingCfg = holder_.cfg_->rankingConfig;
	if (!holder_.cfg_->splitOptions.HasDelims()) {
		return;
	}

	__RX_VAR_FROM_POOL__(std::wstring, dataWithoutDelims)
	__RX_VAR_FROM_POOL__(std::wstring, nextPart)

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

		for (wchar_t symbol : v.pattern) {
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
		fixedVariants.clear();
		typosHandler.Process(v.pattern, v.proc, fixedVariants, holder_);

		for (auto& [wId, proc] : fixedVariants) {
			if (auto wfIt = wordsFound.find(wId); wfIt != wordsFound.end()) {
				newVariants[wfIt->second].Unite(v, proc);
			} else {
				const auto& step = holder_.GetStep(wId);
				auto wIdInStep = holder_.GetWordIdInStep(wId, step);
				std::string_view word(step.suffixes_.word_at(wIdInStep));
				wordsFound[wId] = newVariants.size();
				newVariants.emplace_back(word, proc, v);
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
	const FTRankingConfig& rankingCfg = holder_.cfg_->rankingConfig;
	newVariants.resize(0);
	newVariants.reserve(termVariants.size());

	using TranslitVariantsType = h_vector<std::wstring, 5>;
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
				std::wstring wStemStr = utf8_to_utf16(stemstr);
				newVariants.emplace_back(std::move(wStemStr), stemProc, v);
				newVariants.back().pref = true;
			} else {
				// low relevant stemmed term
				std::wstring wStemStr = utf8_to_utf16(stemstr);
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

	using SynonymsType = h_vector<std::wstring, 5>;
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
static bool allVidsExcluded(const FtMergeStatuses::Statuses& docsExcluded, const VidsContainer& wordVids) {
	for (const auto& id : wordVids) {
		if (!docsExcluded[id.Id()]) {
			return false;
		}
	}

	return true;
}

template <typename IdCont>
template <FtUseExternStatuses useExternSt>
ft::TermResults<IdCont> Selector<IdCont>::buildTermResults(const FtDSLEntry& term, TermVariants& termVariants,
														   const FtMergeStatuses::Statuses& docsExcluded) {
	const FTRankingConfig& rankingCfg = holder_.cfg_->rankingConfig;
	ft::TermResults<IdCont> res(term);

	__RX_VAR_FROM_POOL__(FoundWordsType, wordsFound)
	termVariants.SortByProc();

	size_t totalVids = 0;
	size_t lowRelevanceLimit = 4 * holder_.cfg_->mergeLimit;

	for (auto& variant : termVariants) {
		size_t matched = 0, vids = 0, excludedCnt = 0;
		for (const auto& step : holder_.steps) {
			if (variant.lowRelevance && totalVids >= lowRelevanceLimit) {
				break;
			}

			auto& suffixes = step.suffixes_;
			bool needStop = false;
			for (auto wordIt = suffixes.lower_bound(variant.PatternUtf8()); wordIt != suffixes.end() && !needStop; ++wordIt) {
				if (variant.lowRelevance && totalVids >= lowRelevanceLimit) {
					break;
				}
				needStop = wordIt.lcp() < int(variant.PatternUtf8().length());

				const char* suffixPtr = wordIt->first;
				const WordIdType wordId = wordIt->second;
				const auto& wordEntry = holder_.GetWordEntry(wordId);

				// ToDo This seems really bad for short suffix search...
				if (useExternSt == FtUseExternStatuses::Yes && allVidsExcluded(docsExcluded, wordEntry.vids)) {
					++excludedCnt;
					continue;
				}

				const uint32_t wordIdInStep = holder_.GetWordIdInStep(wordId, step);
				const char* word = suffixes.word_at(wordIdInStep);
				const size_t wordLength = suffixes.word_len_at(wordIdInStep);

				const size_t wordLengthBeforePattern = suffixPtr - word;
				const bool isPrefix = (wordLengthBeforePattern == 0);
				const size_t wordLengthAfterPattern = wordLength - wordLengthBeforePattern - variant.PatternUtf8().length();
				const bool isSuffix = (wordLengthAfterPattern == 0);

				if (!variant.suff && !isPrefix) {
					continue;
				}
				if (!variant.pref && !isSuffix) {
					break;
				}

				// ToDo fix it (broken for russian utf8 symbols)
				const int matchDif = std::abs(long(wordLength - variant.PatternUtf8().length() + wordLengthBeforePattern));
				const float boost = std::max(getTermBoost(word), variant.boost);
				const float decreasePenalty =
					static_cast<float>(holder_.cfg_->partialMatchDecrease * matchDif) / std::max<float>(variant.PatternUtf8().length(), 3);
				float proc = std::max<float>(variant.proc - decreasePenalty, isPrefix ? rankingCfg.PrefixMin() : rankingCfg.SuffixMin());
				proc = std::min<float>(proc, variant.proc);
				if (boost > 0.0f) {
					proc *= boost;
				}

				if (auto it = wordsFound.find(wordId); it != wordsFound.end()) {
					res.Subterm(it->second).SetProc(std::max(res.Subterm(it->second).Proc(), proc));
				} else {
					res.AddSubterm(wordEntry.vids, word, wordId, proc);
					wordsFound[wordId] = res.NumSubterms() - 1;
					matched++;
					totalVids += wordEntry.vids.size();
					vids += wordEntry.vids.size();

					if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
						logFmt(LogInfo, "Matched word '{}' (variant '{}'), {} vids, {}%", word, variant.FullPattern(),
							   wordEntry.vids.size(), proc);
					}
				}
			}
		}

		if (holder_.cfg_->logLevel >= LogInfo) [[unlikely]] {
			logFmt(LogInfo, "Lookup variant '{}' ({}%), matched {} words, with {} vids, excluded {}", variant.FullPattern(), variant.proc,
				   matched, vids, excludedCnt);
		}
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
template <FtUseExternStatuses useExternSt>
void Selector<IdCont>::buildQueryMergeData(FtDSLQuery&& query, const FtMergeStatuses::Statuses& docsExcluded, bool inTransaction,
										   const RdxContext& rdxCtx, ft::QueryMergeData<IdCont>& queryMergeData) {
	const FTRankingConfig& rankingCfg = holder_.cfg_->rankingConfig;

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

		if (phraseTerm) {
			tryToCorrectKbLayout(termVariants);
			tryToCorrectTypos(termVariants);
			tryToSplit(termVariants, PhraseTerm_True);
			addSynonyms(termVariants);

			if (!exact) {
				transliterate(termVariants);
				stem(termVariants);
			}
		} else if (exact) {
			tryToCorrectKbLayout(termVariants);
			tryToCorrectTypos(termVariants);
		} else {
			bool needJoinWithPrevTerm = holder_.cfg_->enableTermsConcat && queryTermIdx > 0;
			if (needJoinWithPrevTerm && term.CanBeJoinedWith(query.GetTerm(queryTermIdx - 1))) {
				FtDSLEntry joinedTerm = term.JoinWithPrevTerm(query.GetTerm(queryTermIdx - 1));
				termVariants.emplace_back(std::move(joinedTerm.Pattern()), rankingCfg.Concat(), joinedTerm.Opts());
			}

			tryToCorrectKbLayout(termVariants);
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

		ft::TermResults<IdCont> nextTerm = buildTermResults<useExternSt>(term, termVariants, docsExcluded);
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
		for (std::wstring& word : subst.substitutionWords) {
			TermVariants termVariants(substOpts);
			termVariants.emplace_back(word, (subst.proc / subst.substitutionWords.size()) * rankingCfg.SynonymsCoeff());

			transliterate(termVariants);
			stem(termVariants);
			for (auto& v : termVariants) {
				v.boost = getTermBoost(v.PatternUtf8());
			}

			ft::TermResults<IdCont> nextTerm = buildTermResults<useExternSt>(FtDSLEntry(word, substOpts), termVariants, docsExcluded);
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

template <typename IdCont>
template <typename MergedOffsetT, typename MergedDataType>
MergedDataType Selector<IdCont>::mergeResults(ft::QueryMergeData<IdCont>& queryMergeData, RankSortType rankSortType,
											  FtMergeStatuses::Statuses& docsExcluded, bool inTransaction, const RdxContext& rdxCtx) {
	ft::Merger<IdCont, MergedDataType, MergedOffsetT> merger(holder_, docsExcluded, fieldSize_, maxAreasInDoc_, inTransaction, rdxCtx);
	switch (holder_.cfg_->bm25Config.bm25Type) {
		case FTConfig::Bm25Config::Bm25Type::rx:
			return merger.template Merge<Bm25Rx>(queryMergeData, rankSortType);
		case FTConfig::Bm25Config::Bm25Type::classic:
			return merger.template Merge<Bm25Classic>(queryMergeData, rankSortType);
		case FTConfig::Bm25Config::Bm25Type::wordCount:
			return merger.template Merge<TermCount>(queryMergeData, rankSortType);
		default:
			assertrx_throw(false);
			return MergedDataType();
	}
}

template <typename IdCont>
template <FtUseExternStatuses useExternSt, typename MergedDataType>
MergedDataType Selector<IdCont>::Process(FtDSLQuery&& query, bool inTransaction, RankSortType rankSortType,
										 FtMergeStatuses::Statuses&& docsExcluded, const RdxContext& rdxCtx) {
	ft::QueryMergeData<IdCont> queryMergeData;
	buildQueryMergeData<useExternSt>(std::move(query), docsExcluded, inTransaction, rdxCtx, queryMergeData);

	const auto maxMergedSize = std::min<uint32_t>(holder_.cfg_->mergeLimit, queryMergeData.totalORVids);
	assertrx_throw(maxMergedSize < 0xFFFFFFFF);
	if (maxMergedSize < 0xFFFF) {
		return mergeResults<uint16_t, MergedDataType>(queryMergeData, rankSortType, docsExcluded, inTransaction, rdxCtx);
	}
	return mergeResults<uint32_t, MergedDataType>(queryMergeData, rankSortType, docsExcluded, inTransaction, rdxCtx);
}

}  // namespace reindexer
