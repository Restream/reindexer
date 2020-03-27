#include "selecter.h"
#include "core/ft/bm25.h"
#include "core/ft/typos.h"
#include "sort/pdqsort.hpp"
#include "tools/logger.h"

namespace reindexer {
// Relevancy procent of full word match
const int kFullMatchProc = 100;
// Mininum relevancy procent of prefix word match.
const int kPrefixMinProc = 50;
const int kSuffixMinProc = 10;
// Relevancy step of prefix match: relevancy = kFullMatchProc - (non matched symbols) * kPrefixStepProc / (matched symbols/3)
// For example: word in index 'terminator', pattern 'termin'. matched: 6 symbols, unmatched: 4. relevancy = 100 - (4*10)/(6/3) = 80
const int kPrefixStepProc = 5;
// Maximum relevancy procent of typo match
const int kTypoProc = 85;
// Relevancy step of typo match
const int kTypoStepProc = 15;
// Decrease procent of relevancy if pattern found by word stem
const int kStemProcDecrease = 15;

void Selecter::prepareVariants(FtSelectContext &ctx, FtDSLEntry &term, std::vector<string> &langs) {
	ctx.variants.clear();

	vector<pair<std::wstring, int>> variantsUtf16{{term.pattern, kFullMatchProc}};

	if (!holder_.cfg_->enableNumbersSearch || !term.opts.number) {
		// Make translit and kblayout variants
		if (holder_.cfg_->enableTranslit && !term.opts.exact) {
			holder_.translit_->GetVariants(term.pattern, variantsUtf16);
		}
		if (holder_.cfg_->enableKbLayout && !term.opts.exact) {
			holder_.kbLayout_->GetVariants(term.pattern, variantsUtf16);
		}
		// Synonims
		holder_.synonyms_->GetVariants(term.pattern, variantsUtf16);
	}

	// Apply stemmers
	string tmpstr, stemstr;
	for (auto &v : variantsUtf16) {
		utf16_to_utf8(v.first, tmpstr);
		ctx.variants.push_back({tmpstr, term.opts, v.second});
		if (!term.opts.exact) {
			for (auto &lang : langs) {
				auto stemIt = holder_.stemmers_.find(lang);
				if (stemIt == holder_.stemmers_.end()) {
					throw Error(errParams, "Stemmer for language %s is not available", lang);
				}
				stemIt->second.stem(tmpstr, stemstr);
				if (tmpstr != stemstr) {
					FtDslOpts opts = term.opts;
					opts.pref = true;

					if (&v != &variantsUtf16[0]) opts.suff = false;

					ctx.variants.push_back({stemstr, opts, v.second - kStemProcDecrease});
				}
			}
		}
	}
}

Selecter::MergeData Selecter::Process(FtDSLQuery &dsl) {
	FtSelectContext ctx;
	// STEP 2: Search dsl terms for each variant
	for (auto &term : dsl) {
		ctx.rawResults.push_back(TextSearchResults());
		TextSearchResults &res = ctx.rawResults.back();
		res.term = term;

		// Prepare term variants (original + translit + stemmed + kblayout)
		this->prepareVariants(ctx, term, holder_.cfg_->stemmers);

		if (holder_.cfg_->logLevel >= LogInfo) {
			WrSerializer wrSer;
			for (auto &variant : ctx.variants) {
				if (&variant != &*ctx.variants.begin()) wrSer << ", ";
				wrSer << variant.pattern;
			}
			wrSer << "], typos: [";
			typos_context tctx[kMaxTyposInWord];
			if (term.opts.typos)
				mktypos(tctx, term.pattern, holder_.cfg_->maxTyposInWord, holder_.cfg_->maxTypoLen, [&wrSer](string_view typo, int) {
					wrSer << typo;
					wrSer << ", ";
				});
			logPrintf(LogInfo, "Variants: [%s]", wrSer.Slice());
		}

		processVariants(ctx);
		if (term.opts.typos) {
			// Lookup typos from typos_ map and fill results
			processTypos(ctx, term);
		}
	}

	return mergeResults(ctx.rawResults);
}

void Selecter::processStepVariants(FtSelectContext &ctx, DataHolder::CommitStep &step, const FtVariantEntry &variant,
								   TextSearchResults &res) {
	if (variant.opts.op == OpAnd) {
		ctx.foundWords.clear();
	}
	auto &tmpstr = variant.pattern;
	auto &suffixes = step.suffixes_;
	//  Lookup current variant in suffixes array
	auto keyIt = suffixes.lower_bound(tmpstr);

	int matched = 0, skipped = 0, vids = 0;
	bool withPrefixes = variant.opts.pref;
	bool withSuffixes = variant.opts.suff;

	// Walk current variant in suffixes array and fill results
	do {
		if (keyIt == suffixes.end()) break;

		WordIdType glbwordId = keyIt->second;

		uint32_t suffixWordId = holder_.GetSuffixWordId(glbwordId, step);
		const string::value_type *word = suffixes.word_at(suffixWordId);

		int16_t wordLength = suffixes.word_len_at(suffixWordId);

		ptrdiff_t suffixLen = keyIt->first - word;
		int matchLen = tmpstr.length();

		if (!withSuffixes && suffixLen) continue;
		if (!withPrefixes && wordLength != matchLen + suffixLen) break;

		int matchDif = std::abs(long(wordLength - matchLen + suffixLen));
		int proc =
			std::max(variant.proc - matchDif * kPrefixStepProc / std::max(matchLen / 3, 1), suffixLen ? kSuffixMinProc : kPrefixMinProc);

		auto it = ctx.foundWords.find(glbwordId);
		if (it == ctx.foundWords.end() || it->second.first != ctx.rawResults.size() - 1) {
			res.push_back({&holder_.getWordById(glbwordId).vids_, keyIt->first, proc, suffixes.virtual_word_len(suffixWordId)});
			res.idsCnt_ += holder_.getWordById(glbwordId).vids_.size();
			ctx.foundWords[glbwordId] = std::make_pair(ctx.rawResults.size() - 1, res.size() - 1);
			if (holder_.cfg_->logLevel >= LogTrace)
				logPrintf(LogInfo, " matched %s '%s' of word '%s', %d vids, %d%%", suffixLen ? "suffix" : "prefix", keyIt->first, word,
						  holder_.getWordById(glbwordId).vids_.size(), proc);
			matched++;
			vids += holder_.getWordById(glbwordId).vids_.size();
		} else {
			if (ctx.rawResults[it->second.first][it->second.second].proc_ < proc)
				ctx.rawResults[it->second.first][it->second.second].proc_ = proc;
			skipped++;
		}
	} while ((keyIt++).lcp() >= int(tmpstr.length()));
	if (holder_.cfg_->logLevel >= LogInfo)
		logPrintf(LogInfo, "Lookup variant '%s' (%d%%), matched %d suffixes, with %d vids, skiped %d", tmpstr, variant.proc, matched, vids,
				  skipped);
}

void Selecter::processVariants(FtSelectContext &ctx) {
	TextSearchResults &res = ctx.rawResults.back();

	for (const FtVariantEntry &variant : ctx.variants) {
		if (variant.opts.op == OpAnd) {
			ctx.foundWords.clear();
		}
		for (auto &step : holder_.steps) {
			processStepVariants(ctx, step, variant, res);
		}
	}
}

void Selecter::processTypos(FtSelectContext &ctx, FtDSLEntry &term) {
	TextSearchResults &res = ctx.rawResults.back();

	for (auto &step : holder_.steps) {
		typos_context tctx[kMaxTyposInWord];
		auto &typos = step.typos_;
		int matched = 0, skiped = 0, vids = 0;
		mktypos(tctx, term.pattern, holder_.cfg_->maxTyposInWord, holder_.cfg_->maxTypoLen, [&](string_view typo, int tcount) {
			auto typoRng = typos.equal_range(typo);
			tcount = holder_.cfg_->maxTyposInWord - tcount;
			for (auto typoIt = typoRng.first; typoIt != typoRng.second; typoIt++) {
				WordIdType wordIdglb = typoIt->second;
				auto &step = holder_.GetStep(wordIdglb);

				auto wordIdSfx = holder_.GetSuffixWordId(wordIdglb, step);

				// bool virtualWord = suffixes_.is_word_virtual(wordId);
				uint8_t wordLength = step.suffixes_.word_len_at(wordIdSfx);
				int proc = kTypoProc - tcount * kTypoStepProc / std::max((wordLength - tcount) / 3, 1);
				auto it = ctx.foundWords.find(wordIdglb);
				if (it == ctx.foundWords.end()) {
					res.push_back({&holder_.getWordById(wordIdglb).vids_, typoIt->first, proc, step.suffixes_.virtual_word_len(wordIdSfx)});
					res.idsCnt_ += holder_.getWordById(wordIdglb).vids_.size();
					ctx.foundWords.emplace(wordIdglb, std::make_pair(ctx.rawResults.size() - 1, res.size() - 1));

					if (holder_.cfg_->logLevel >= LogTrace)
						logPrintf(LogInfo, " matched typo '%s' of word '%s', %d ids, %d%%", typoIt->first,
								  step.suffixes_.word_at(wordIdSfx), holder_.getWordById(wordIdglb).vids_.size(), proc);
					++matched;
					vids += holder_.getWordById(wordIdglb).vids_.size();
				} else
					++skiped;
			}
		});
		if (holder_.cfg_->logLevel >= LogInfo)
			logPrintf(LogInfo, "Lookup typos, matched %d typos, with %d vids, skiped %d", matched, vids, skiped);
	}
}

double bound(double k, double weight, double boost) { return (1.0 - weight) + k * boost * weight; }

void Selecter::debugMergeStep(const char *msg, int vid, float normBm25, float normDist, int finalRank, int prevRank) {
#ifdef REINDEX_FT_EXTRA_DEBUG
	if (holder_.cfg_->logLevel < LogTrace) return;

	logPrintf(LogInfo, "%s - '%s' (vid %d), bm25 %f, dist %f, rank %d (prev rank %d)", msg, holder_.vdocs_[vid].keyDoc, vid, normBm25,
			  normDist, finalRank, prevRank);
#else
	(void)msg;
	(void)vid;
	(void)normBm25;
	(void)normDist;
	(void)finalRank;
	(void)prevRank;
#endif
}

void Selecter::mergeItaration(TextSearchResults &rawRes, vector<bool> &exists, vector<MergeInfo> &merged, vector<MergedIdRel> &merged_rd,
							  h_vector<int16_t> &idoffsets) {
	auto &vdocs = holder_.vdocs_;

	int totalDocsCount = vdocs.size();
	bool simple = idoffsets.size() == 0;
	auto op = rawRes.term.opts.op;

	vector<bool> curExists((simple && rawRes.size() <= 1) ? 0 : totalDocsCount, false);
	if (simple && rawRes.size() > 1) {
		idoffsets.resize(totalDocsCount);
	}

	for (auto &m_rd : merged_rd) {
		if (m_rd.next.Size()) m_rd.cur = std::move(m_rd.next);
	}

	for (auto &r : rawRes) {
		auto idf = IDF(totalDocsCount, r.vids_->size());
		auto termLenBoost = bound(rawRes.term.opts.termLenBoost, holder_.cfg_->termLenWeight, holder_.cfg_->termLenBoost);
		if (holder_.cfg_->logLevel >= LogTrace) {
			logPrintf(LogInfo, "Pattern %s, idf %f, termLenBoost %f", r.pattern, idf, termLenBoost);
		}

		for (auto &relid : *r.vids_) {
			int vid = relid.Id();

			// Do not calc anithing if
			if (op == OpAnd && !exists[vid]) {
				continue;
			}

			assert(vid < int(exists.size()));

			// Find field with max rank
			int field = 0;
			double normBm25 = 0.0, termRank = 0.0;
			for (uint64_t fieldsMask = relid.UsedFieldsMask(), f = 0; fieldsMask;) {
				while ((fieldsMask & 1) == 0) {
					++f;
					fieldsMask >>= 1;
				}
				assert(f < vdocs[vid].wordsCount.size());
				assert(f < rawRes.term.opts.fieldsBoost.size());
				auto fboost = rawRes.term.opts.fieldsBoost[f];
				if (fboost) {
					// raw bm25
					const double bm25 = idf * bm25score(relid.WordsInField(f), vdocs[vid].mostFreqWordCount[f], vdocs[vid].wordsCount[f],
														holder_.avgWordsCount_[f]);

					// normalized bm25
					const double normBm25Tmp = bound(bm25, holder_.cfg_->bm25Weight, holder_.cfg_->bm25Boost);

					// final term rank calculation
					const double termRankTmp = fboost * r.proc_ * normBm25Tmp * rawRes.term.opts.boost * termLenBoost;
					if (termRankTmp > termRank) {
						field = f;
						normBm25 = normBm25Tmp;
						termRank = termRankTmp;
					}
				}
				++f;
				fieldsMask >>= 1;
			}
			if (!termRank) continue;

			if (!simple) {
				auto moffset = idoffsets[vid];
				if (exists[vid]) {
					assert(relid.Size());
					assert(merged_rd[moffset].cur.Size());

					// match of 2-rd, and next terms
					if (op == OpNot) {
						merged[moffset].proc = 0;
						exists[vid] = false;
					} else {
						// Calculate words distance
						int distance = 0;
						float normDist = 1;

						if (merged_rd[moffset].qpos != rawRes.term.opts.qpos) {
							distance = merged_rd[moffset].cur.Distance(relid, INT_MAX);

							// Normaized distance
							normDist =
								bound(1.0 / double(std::max(distance, 1)), holder_.cfg_->distanceWeight, holder_.cfg_->distanceBoost);
						}
						int finalRank = normDist * termRank;

						if (distance <= rawRes.term.opts.distance && (!curExists[vid] || finalRank > merged_rd[moffset].rank)) {
							// distance and rank is better, than prev. update rank
							if (curExists[vid]) {
								merged[moffset].proc -= merged_rd[moffset].rank;
								debugMergeStep("merged better score ", vid, normBm25, normDist, finalRank, merged_rd[moffset].rank);
							} else {
								debugMergeStep("merged new ", vid, normBm25, normDist, finalRank, merged_rd[moffset].rank);
								merged[moffset].matched++;
							}
							merged[moffset].proc += finalRank;
							if (needArea_) {
								for (auto pos : relid.Pos()) {
									if (!merged[moffset].holder->AddWord(pos.pos(), r.wordLen_, pos.field())) {
										break;
									}
								}
							}
							merged_rd[moffset].rank = finalRank;
							merged_rd[moffset].next = std::move(relid);
							curExists[vid] = true;
						} else {
							debugMergeStep("skiped ", vid, normBm25, normDist, finalRank, merged_rd[moffset].rank);
						}
					}
				}
			}
			if (int(merged.size()) < holder_.cfg_->mergeLimit && op == OpOr) {
				const bool currentlyAddedLessRankedMerge = !curExists.empty() && curExists[vid] && merged[idoffsets[vid]].proc < static_cast<int16_t>(termRank);
				if (exists[vid] && !(simple && currentlyAddedLessRankedMerge)) continue;
				// match of 1-st term
				MergeInfo info;
				info.id = vid;
				info.proc = termRank;
				info.matched = 1;
				info.field = field;
				if (needArea_) {
					info.holder.reset(new AreaHolder);
					info.holder->ReserveField(fieldSize_);
					for (auto pos : relid.Pos()) {
						info.holder->AddWord(pos.pos(), r.wordLen_, pos.field());
					}
				}
				if (exists[vid]) {
					merged[idoffsets[vid]] = std::move(info);
				} else {
					merged.push_back(std::move(info));
					exists[vid] = true;
					if (!curExists.empty()) {
						curExists[vid] = true;
						idoffsets[vid] = merged.size() - 1;
					}
				}
				if (simple) continue;
				// prepare for intersect with next terms
				merged_rd.push_back({IdRelType(std::move(relid)), IdRelType(), int(termRank), rawRes.term.opts.qpos});
			}
		}
	}
	if (op == OpAnd) {
		for (auto &info : merged) {
			auto vid = info.id;
			if (exists[vid] && !curExists[vid]) {
				info.proc = 0;
				exists[vid] = false;
			}
		}
	}
}

Selecter::MergeData Selecter::mergeResults(vector<TextSearchResults> &rawResults) {
	auto &vdocs = holder_.vdocs_;
	MergeData merged;

	if (!rawResults.size() || !vdocs.size()) return merged;

	vector<bool> exists(vdocs.size(), false);
	vector<MergedIdRel> merged_rd;
	h_vector<int16_t> idoffsets;

	int idsMaxCnt = 0;
	for (auto &rawRes : rawResults) {
		boost::sort::pdqsort(rawRes.begin(), rawRes.end(),
							 [](const TextSearchResult &lhs, const TextSearchResult &rhs) { return lhs.proc_ > rhs.proc_; });
		if (rawRes.term.opts.op == OpOr || !idsMaxCnt) idsMaxCnt += rawRes.idsCnt_;
	}

	merged.reserve(std::min(holder_.cfg_->mergeLimit, idsMaxCnt));

	if (rawResults.size() > 1) {
		idoffsets.resize(vdocs.size());
		merged_rd.reserve(std::min(holder_.cfg_->mergeLimit, idsMaxCnt));
	}
	rawResults[0].term.opts.op = OpOr;
	for (auto &rawRes : rawResults) {
		mergeItaration(rawRes, exists, merged, merged_rd, idoffsets);

		if (rawRes.term.opts.op != OpNot) merged.mergeCnt++;
	}
	if (holder_.cfg_->logLevel >= LogInfo) logPrintf(LogInfo, "Complex merge (%d patterns): out %d vids", rawResults.size(), merged.size());

	// Update full match rank
	for (size_t ofs = 0; ofs < merged.size(); ++ofs) {
		auto &m = merged[ofs];
		if (size_t(vdocs[m.id].wordsCount[m.field]) == rawResults.size()) {
			m.proc *= holder_.cfg_->fullMatchBoost;
		}
	}

	boost::sort::pdqsort(merged.begin(), merged.end(), [](const MergeInfo &lhs, const MergeInfo &rhs) { return lhs.proc > rhs.proc; });

	return merged;
}
}  // namespace reindexer
