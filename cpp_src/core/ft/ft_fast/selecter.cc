#include "selecter.h"
#include "core/ft/bm25.h"
#include "core/ft/ft_fuzzy/dataholder/smardeque.h"
#include "core/ft/typos.h"
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

	vector<pair<std::wstring, search_engine::ProcType>> variantsUtf16{{term.pattern, kFullMatchProc}};

	if (!holder_.cfg_->enableNumbersSearch || !term.opts.number) {
		// Make translit and kblayout variants
		if (holder_.cfg_->enableTranslit && holder_.searchers_.size() > 0 && !term.opts.exact) {
			holder_.searchers_[0]->Build(term.pattern.data(), term.pattern.length(), variantsUtf16);
		}
		if (holder_.cfg_->enableKbLayout && holder_.searchers_.size() > 1 && !term.opts.exact) {
			holder_.searchers_[1]->Build(term.pattern.data(), term.pattern.length(), variantsUtf16);
		}
	}

	// Apply stemmers
	string tmpstr;
	for (auto &v : variantsUtf16) {
		utf16_to_utf8(v.first, tmpstr);
		ctx.variants.push_back({tmpstr, term.opts, v.second});
		if (!term.opts.exact) {
			for (auto &lang : langs) {
				auto stemIt = holder_.stemmers_.find(lang);
				if (stemIt == holder_.stemmers_.end()) {
					throw Error(errParams, "Stemmer for language %s is not available", lang.c_str());
				}
				char *stembuf = reinterpret_cast<char *>(alloca(1 + tmpstr.size() * 4));
				stemIt->second.stem(stembuf, 1 + tmpstr.size() * 4, tmpstr.data(), tmpstr.length());
				if (tmpstr != stembuf) {
					FtDslOpts opts = term.opts;
					opts.pref = true;

					if (&v != &variantsUtf16[0]) opts.suff = false;

					ctx.variants.push_back({stembuf, opts, v.second - kStemProcDecrease});
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
			string vars;
			for (auto &variant : ctx.variants) {
				if (&variant != &*ctx.variants.begin()) vars += ", ";
				vars += variant.pattern;
			}
			vars += "], typos: [";
			typos_context tctx[kMaxTyposInWord];
			if (term.opts.typos)
				mktypos(tctx, term.pattern, holder_.cfg_->maxTyposInWord, holder_.cfg_->maxTypoLen, [&vars](const string &typo, int) {
					vars += typo;
					vars += ", ";
				});
			logPrintf(LogInfo, "Variants: [%s]", vars.c_str());
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
	bool withPrefixes = (variant.opts.pref || variant.opts.suff);
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
		if (!withPrefixes && wordLength != matchLen) break;

		int matchDif = std::abs(long(wordLength - matchLen + suffixLen));
		int proc =
			std::max(variant.proc - matchDif * kPrefixStepProc / std::max(matchLen / 3, 1), suffixLen ? kSuffixMinProc : kPrefixMinProc);

		auto it = ctx.foundWords.find(glbwordId);
		if (it == ctx.foundWords.end() || it->second.first != ctx.rawResults.size() - 1) {
			res.push_back({&holder_.getWordById(glbwordId).vids_, keyIt->first, proc, suffixes.virtual_word_len(suffixWordId)});
			res.idsCnt_ += holder_.getWordById(glbwordId).vids_.size();
			ctx.foundWords[glbwordId] = std::make_pair(ctx.rawResults.size() - 1, res.size() - 1);
			if (holder_.cfg_->logLevel >= LogTrace)
				logPrintf(LogTrace, " matched %s '%s' of word '%s', %d vids, %d%%", suffixLen ? "suffix" : "prefix", keyIt->first, word,
						  int(holder_.getWordById(glbwordId).vids_.size()), proc);
			matched++;
			vids += holder_.getWordById(glbwordId).vids_.size();
		} else {
			if (ctx.rawResults[it->second.first][it->second.second].proc_ < proc)
				ctx.rawResults[it->second.first][it->second.second].proc_ = proc;
			skipped++;
		}
	} while ((keyIt++).lcp() >= int(tmpstr.length()));
	if (holder_.cfg_->logLevel >= LogInfo)
		logPrintf(LogInfo, "Lookup variant '%s' (%d%%), matched %d suffixes, with %d vids, skiped %d", tmpstr.c_str(), variant.proc,
				  matched, vids, skipped);
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
		mktypos(tctx, term.pattern, holder_.cfg_->maxTyposInWord, holder_.cfg_->maxTypoLen, [&](const string &typo, int tcount) {
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
						logPrintf(LogTrace, " matched typo '%s' of word '%s', %d ids, %d%%", typoIt->first,
								  step.suffixes_.word_at(wordIdSfx), int(holder_.getWordById(wordIdglb).vids_.size()), proc);
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

	vector<unique_ptr<string>> bufStrs;
	auto fieldStrVec = this->getDocFields(*vdocs[vid].keyDoc, bufStrs);
	string text = fieldStrVec[0].ToString();
	if (text.length() > 48) {
		text = text.substr(0, 48) + "...";
	}

	logPrintf(LogTrace, "%s - '%s' (vid %d), bm25 %f, dist %f, rank %d (prev rank %d)", msg, text.c_str(), vid, normBm25, normDist,
			  finalRank, prevRank);
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

	vector<bool> curExists(simple ? 0 : totalDocsCount, false);

	for (auto &m_rd : merged_rd) {
		if (m_rd.next.pos.size()) m_rd.cur = std::move(m_rd.next);
	}

	for (auto &r : rawRes) {
		auto idf = IDF(totalDocsCount, r.vids_->size());
		auto termLenBoost = bound(rawRes.term.opts.boost, holder_.cfg_->termLenWeight, holder_.cfg_->termLenBoost);
		if (holder_.cfg_->logLevel >= LogTrace) {
			logPrintf(LogTrace, "Pattern %s, idf %f, termLenBoost %f", r.pattern, idf, termLenBoost);
		}

		for (auto &relid : *r.vids_) {
			int vid = relid.id;

			// Do not calc anithing if
			if (op == OpAnd && !exists[vid]) {
				continue;
			}

			assert(vid < int(exists.size()));

			int field = relid.pos[0].field();
			assert(field < int(vdocs[vid].wordsCount.size()));
			assert(field < int(rawRes.term.opts.fieldsBoost.size()));

			auto fboost = rawRes.term.opts.fieldsBoost[field];
			if (!fboost) {
				// TODO: search another fields
				continue;
			};

			// raw bm25
			auto bm25 = idf * bm25score(relid.wordsInField(field), vdocs[vid].mostFreqWordCount[field], vdocs[vid].wordsCount[field],
										holder_.avgWordsCount_[field]);

			// normalized bm25
			auto normBm25 = bound(bm25, holder_.cfg_->bm25Weight, holder_.cfg_->bm25Boost);

			// final term rank calculation
			double termRank = fboost * r.proc_ * normBm25 * rawRes.term.opts.boost * termLenBoost;

			if (!simple) {
				auto moffset = idoffsets[vid];
				if (exists[vid]) {
					assert(relid.pos.size());
					assert(merged_rd[moffset].cur.pos.size());

					// match of 2-rd, and next terms
					if (op == OpNot) {
						merged[moffset].proc = 0;
						exists[vid] = false;
					} else {
						// Calculate words distance
						int distance = 0;
						float normDist = 1;

						if (merged_rd[moffset].qpos != rawRes.term.opts.qpos) {
							distance = merged_rd[moffset].cur.distance(relid, INT_MAX);

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
							}
							merged[moffset].proc += finalRank;
							if (needArea_) {
								for (auto pos : relid.pos) {
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
			if (int(merged.size()) < holder_.cfg_->mergeLimit && op == OpOr && !exists[vid]) {
				// match of 1-st term
				MergeInfo info;
				info.id = vid;
				info.proc = termRank;
				if (needArea_) {
					info.holder.reset(new AreaHolder);
					info.holder->ReserveField(fieldSize_);
					for (auto pos : relid.pos) {
						info.holder->AddWord(pos.pos(), r.wordLen_, pos.field());
					}
				}
				merged.push_back(std::move(info));
				exists[vid] = true;
				if (simple) continue;
				// prepare for intersect with next terms
				merged_rd.push_back({IdRelType(std::move(relid)), IdRelType(), int(termRank), rawRes.term.opts.qpos});
				curExists[vid] = true;
				idoffsets[vid] = merged.size() - 1;
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
		std::sort(rawRes.begin(), rawRes.end(),
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
	if (holder_.cfg_->logLevel >= LogInfo)
		logPrintf(LogInfo, "Complex merge (%d patterns): out %d vids", int(rawResults.size()), int(merged.size()));

	std::sort(merged.begin(), merged.end(), [](const MergeInfo &lhs, const MergeInfo &rhs) { return lhs.proc > rhs.proc; });

	return merged;
}
}  // namespace reindexer
