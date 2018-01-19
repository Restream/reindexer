#include <chrono>
#include <thread>
#include "core/ft/bm25.h"

#include "fasttextsearch.h"

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

using std::thread;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::high_resolution_clock;

template <typename T>
void FastIndexText<T>::buildTyposMap() {
	typos_context tctx[kMaxTyposInWord];
	auto &typos = typos_;
	typos_.clear();
	typos_.reserve(words_.size() * (10 >> (GetConfig()->maxTyposInWord - 1)) / 2,
				   words_.size() * 5 * (10 >> (GetConfig()->maxTyposInWord - 1)));
	for (size_t wordId = 0; wordId < words_.size(); wordId++) {
		mktypos(tctx, suffixes_.word_at(wordId), GetConfig()->maxTyposInWord, GetConfig()->maxTypoLen,
				[&typos, wordId](const string &typo, int) { typos.emplace(typo, wordId); });
	}
	typos_.shrink_to_fit();
}

// Generic implemetation for string index
template <typename T>
h_vector<pair<const string *, int>, 8> FastIndexText<T>::getDocFields(const typename T::key_type &doc, vector<unique_ptr<string>> &) {
	return {{doc.get(), 0}};
}

// Specific implemetation for composite index
template <>
h_vector<pair<const string *, int>, 8> FastIndexText<unordered_payload_map<Index::KeyEntryPlain>>::getDocFields(
	const typename unordered_payload_map<Index::KeyEntryPlain>::key_type &doc, vector<unique_ptr<string>> &strsBuf) {
	ConstPayload pl(this->payloadType_, doc);

	h_vector<pair<const string *, int>, 8> ret;
	int fieldPos = 0;
	for (auto field : fields_) {
		KeyRefs krefs;
		pl.Get(field, krefs);
		for (auto kref : krefs) {
			if (kref.Type() != KeyValueString) {
				strsBuf.emplace_back(unique_ptr<string>(new string(KeyValue(kref).toString())));
				ret.push_back({strsBuf.back().get(), fieldPos});

			} else {
				ret.push_back({(p_string(kref)).getCxxstr(), fieldPos});
			}
		}
		fieldPos++;
	}
	return ret;
}

template <typename T>
void FastIndexText<T>::buildWordsMap(fast_hash_map<string, WordEntry> &words_um) {
	struct context {
		fast_hash_map<string, WordEntry> words_um;
		std::thread thread;
	};
	int maxIndexWorkers = std::thread::hardware_concurrency();
	if (!maxIndexWorkers) maxIndexWorkers = 1;
	unique_ptr<context[]> ctxs(new context[maxIndexWorkers]);

	// buffer strings, for printing non text fields
	vector<unique_ptr<string>> bufStrs;
	// array with pointers to docs fields text
	vector<h_vector<pair<const string *, int>, 8>> vdocsTexts;
	// Prepare vdocs -> addresable array all docs in the index
	this->vdocs_.reserve(this->idx_map.size());
	vdocsTexts.reserve(this->idx_map.size());
	for (auto &doc : this->idx_map) {
		this->vdocs_.push_back({&doc.second, {}, {}});
		vdocsTexts.push_back(getDocFields(doc.first, bufStrs));
	}

	int fieldscount = std::max(1, int(this->fields_.size()));
	auto &vdocs = this->vdocs_;
	auto *cfg = GetConfig();
	// build words map parallel in maxIndexWorkers threads
	for (int i = 0; i < maxIndexWorkers; i++)
		ctxs[i].thread = thread(
			[&ctxs, &vdocsTexts, maxIndexWorkers, &vdocs, fieldscount, &cfg](int i) {
				auto ctx = &ctxs[i];
				string word, str;
				std::wstring utf16str;
				vector<const char *> wrds;
				for (VDocIdType j = i; j < VDocIdType(vdocsTexts.size()); j += maxIndexWorkers) {
					vdocs[j].wordsCount.insert(vdocs[j].wordsCount.begin(), fieldscount, 0.0);
					vdocs[j].mostFreqWordCount.insert(vdocs[j].mostFreqWordCount.begin(), fieldscount, 0.0);

					for (size_t field = 0, pos = 0; field < vdocsTexts[j].size(); ++field) {
						split(*vdocsTexts[j][field].first, utf16str, str, wrds);
						int rfield = vdocsTexts[j][field].second;
						assert(rfield < fieldscount);

						vdocs[j].wordsCount[rfield] = wrds.size();

						for (auto w : wrds) {
							word.assign(w);
							if (!word.length() || cfg->stopWords.find(word) != cfg->stopWords.end()) continue;
							auto idxIt = ctx->words_um.find(word);
							if (idxIt == ctx->words_um.end()) {
								idxIt = ctx->words_um.emplace(word, WordEntry()).first;
								idxIt->second.vids_.reserve(16);
							}

							int mfcnt = idxIt->second.vids_.Add(j, pos++, rfield);
							if (mfcnt > vdocs[j].mostFreqWordCount[rfield]) {
								vdocs[j].mostFreqWordCount[rfield] = mfcnt;
							}
						}
					}
				}
			},
			i);

	// If was 1 build thread. Just return it's build resultes
	if (maxIndexWorkers == 1) {
		ctxs[0].thread.join();
		words_um.swap(ctxs[0].words_um);
		return;
	}
	// Merge results into single map
	for (int i = 0; i < maxIndexWorkers; i++) {
		ctxs[i].thread.join();
		for (auto it = ctxs[i].words_um.begin(); it != ctxs[i].words_um.end(); it++) {
			auto idxIt = words_um.find(it->first);
			if (idxIt == words_um.end()) {
				words_um.emplace(it->first, std::move(it->second));
			} else {
				idxIt->second.vids_.reserve(it->second.vids_.size() + idxIt->second.vids_.size());
				for (auto &r : it->second.vids_) idxIt->second.vids_.push_back(std::move(r));
				it->second.vids_.clear();
			}
		}
	}

	// Calculate avg words count per document for bm25 calculation
	if (this->vdocs_.size()) {
		avgWordsCount_.resize(fieldscount);
		for (int i = 0; i < fieldscount; i++) avgWordsCount_[i] = 0;

		for (auto &vdoc : this->vdocs_) {
			for (int i = 0; i < fieldscount; i++) avgWordsCount_[i] += vdoc.wordsCount[i];
		}
		for (int i = 0; i < fieldscount; i++) avgWordsCount_[i] /= this->vdocs_.size();
	}

	// Check and print potential stop words
	if (GetConfig()->logLevel >= LogInfo) {
		string str;
		for (auto &w : words_um) {
			if (w.second.vids_.size() > this->vdocs_.size() / 5) str += w.first + " ";
		}
		logPrintf(LogInfo, "Potential stop words: %s", str.c_str());
	}
}

template <typename T>
void FastIndexText<T>::prepareVariants(FtSelectContext &ctx, FtDSLEntry &term, std::vector<string> &langs) {
	ctx.variants.clear();

	vector<pair<std::wstring, search_engine::ProcType>> variantsUtf16{{term.pattern, kFullMatchProc}};

	// Make translit and kblayout variants
	if (GetConfig()->enableTranslit && this->searchers_.size() > 0 && !term.opts.exact) {
		this->searchers_[0]->Build(term.pattern.data(), term.pattern.length(), variantsUtf16);
	}
	if (GetConfig()->enableKbLayout && this->searchers_.size() > 1 && !term.opts.exact) {
		this->searchers_[1]->Build(term.pattern.data(), term.pattern.length(), variantsUtf16);
	}

	// Apply stemmers
	string tmpstr;
	for (auto &v : variantsUtf16) {
		utf16_to_utf8(v.first, tmpstr);
		ctx.variants.push_back({tmpstr, term.opts, v.second});
		if (!term.opts.exact) {
			for (auto &lang : langs) {
				auto stemIt = this->stemmers_.find(lang);
				if (stemIt == this->stemmers_.end()) {
					throw Error(errParams, "Stemmer for language %s is not available", lang.c_str());
				}
				char *stembuf = reinterpret_cast<char *>(alloca(1 + tmpstr.size() * 4));
				stemIt->second.stem(stembuf, 1 + tmpstr.size() * 4, tmpstr.data(), tmpstr.length());
				if (tmpstr != stembuf) {
					FtDslOpts opts = term.opts;
					opts.pref = true;
					ctx.variants.push_back({stembuf, opts, v.second - kStemProcDecrease});
				}
			}
		}
	}
}

template <typename T>
void FastIndexText<T>::processVariants(FtSelectContext &ctx) {
	TextSearchResults &res = ctx.rawResults.back();

	for (auto &variant : ctx.variants) {
		auto &tmpstr = variant.pattern;
		//  Lookup current variant in suffixes array
		auto keyIt = suffixes_.lower_bound(tmpstr);

		int matched = 0, skiped = 0, vids = 0;
		bool withPrefixes = (variant.opts.pref || variant.opts.suff);
		bool withSuffixes = variant.opts.suff;

		// Walk current variant in suffixes array and fill results
		do {
			if (keyIt == suffixes_.end()) break;

			auto wordId = keyIt->second;
			assert(wordId < WordIdType(words_.size()));

			ptrdiff_t suffixLen = keyIt->first - suffixes_.word_at(wordId);
			int matchLen = tmpstr.length();

			if (!withSuffixes && suffixLen) continue;
			if (!withPrefixes && suffixes_.word_len_at(wordId) != matchLen) break;

			int proc = int(std::max(int((variant.proc - std::abs(long(suffixes_.word_len_at(wordId) - matchLen + suffixLen)) *
															kPrefixStepProc / std::max(matchLen / 3, 1))),
									suffixLen ? kSuffixMinProc : kPrefixMinProc));

			auto it = ctx.foundWords.find(wordId);
			if (it == ctx.foundWords.end()) {
				res.push_back({&words_[wordId].vids_, proc});
				res.idsCnt_ += words_[wordId].vids_.size();

				ctx.foundWords.emplace(wordId, std::make_pair(ctx.rawResults.size() - 1, res.size() - 1));
				if (GetConfig()->logLevel >= LogTrace)
					logPrintf(LogTrace, " matched %s '%s' of word '%s', %d vids, %d%%", suffixLen ? "suffix" : "prefix", keyIt->first,
							  suffixes_.word_at(wordId), int(words_[wordId].vids_.size()), proc);
				matched++;
				vids += words_[wordId].vids_.size();
			} else {
				if (ctx.rawResults[it->second.first][it->second.second].proc_ < proc)
					ctx.rawResults[it->second.first][it->second.second].proc_ = proc;
				skiped++;
			}
		} while ((keyIt++).lcp() >= int(tmpstr.length()));
		if (GetConfig()->logLevel >= LogInfo)
			logPrintf(LogInfo, "Lookup variant '%s' (%d%%), matched %d suffixes, with %d vids, skiped %d", tmpstr.c_str(), variant.proc,
					  matched, vids, skiped);
	}
}

template <typename T>
void FastIndexText<T>::processTypos(FtSelectContext &ctx, FtDSLEntry &term) {
	TextSearchResults &res = ctx.rawResults.back();

	typos_context tctx[kMaxTyposInWord];
	auto &typos = typos_;
	int matched = 0, skiped = 0, vids = 0;
	mktypos(tctx, term.pattern, GetConfig()->maxTyposInWord, GetConfig()->maxTypoLen, [&](const string &typo, int tcount) {
		auto typoRng = typos.equal_range(typo);
		tcount = GetConfig()->maxTyposInWord - tcount;
		for (auto typoIt = typoRng.first; typoIt != typoRng.second; typoIt++) {
			auto wordId = typoIt->second;
			assert(wordId < WordIdType(words_.size()));
			int proc = kTypoProc - tcount * kTypoStepProc / std::max((suffixes_.word_len_at(wordId) - tcount) / 3, 1);
			auto it = ctx.foundWords.find(wordId);
			if (it == ctx.foundWords.end()) {
				res.push_back({&words_[wordId].vids_, proc});
				res.idsCnt_ += words_[wordId].vids_.size();
				ctx.foundWords.emplace(wordId, std::make_pair(ctx.rawResults.size() - 1, res.size() - 1));

				if (GetConfig()->logLevel >= LogTrace)
					logPrintf(LogTrace, " matched typo '%s' of word '%s', %d ids, %d%%", typoIt->first, suffixes_.word_at(wordId),
							  int(words_[wordId].vids_.size()), proc);
				++matched;
				vids += words_[wordId].vids_.size();
			} else
				++skiped;
		}
	});
	if (GetConfig()->logLevel >= LogInfo)
		logPrintf(LogInfo, "Lookup typos, matched %d typos, with %d vids, skiped %d", matched, vids, skiped);
}

double bound(double k, double weight, double boost) { return (1.0 - weight) + k * boost * weight; }

template <typename T>
void FastIndexText<T>::mergeItaration(TextSearchResults &rawRes, vector<bool> &exists, vector<pair<IdType, int>> &merged,
									  vector<MergedIdRel> &merged_rd, h_vector<int16_t> &idoffsets) {
	int totalDocsCount = this->vdocs_.size();
	bool simple = idoffsets.size() == 0;
	auto op = rawRes.term.opts.op;

	vector<bool> curExists(simple ? 0 : totalDocsCount, false);

	for (auto &m_rd : merged_rd) {
		if (m_rd.next.pos.size()) m_rd.cur = std::move(m_rd.next);
	}

	for (auto &r : rawRes) {
		auto idf = IDF(totalDocsCount, r.vids_->size());
		auto termLenBoost = bound(rawRes.term.opts.boost, GetConfig()->termLenWeight, GetConfig()->termLenBoost);

		for (auto &relid : *r.vids_) {
			int field = relid.pos[0].field();
			int vid = relid.id;
			assert(vid < int(exists.size()));
			assert(field < int(this->vdocs_[vid].wordsCount.size()));
			assert(field < int(rawRes.term.opts.fieldsBoost.size()));

			auto fboost = rawRes.term.opts.fieldsBoost[field];
			if (!fboost) {
				// TODO: search another fields
				continue;
			};

			auto bm25 = idf * bm25score(relid.wordsInField(field), this->vdocs_[vid].mostFreqWordCount[field],
										this->vdocs_[vid].wordsCount[field], avgWordsCount_[field]);

			// final term rank calculation
			double rank =
				fboost * bound(bm25, GetConfig()->bm25Weight, GetConfig()->bm25Boost) * r.proc_ * rawRes.term.opts.boost * termLenBoost;

			if (!simple) {
				auto moffset = idoffsets[vid];
				if (exists[vid] && merged_rd[moffset].qpos != rawRes.term.opts.qpos) {
					assert(relid.pos.size());
					assert(merged_rd[moffset].cur.pos.size());

					// match of 2-rd, and next terms
					if (op == OpNot) {
						merged[moffset].second = 0;
						exists[vid] = false;
					} else {
						// Calculate words distance
						int distance = merged_rd[moffset].cur.distance(relid, INT_MAX);
						int irank =
							rank * bound(1.0 / double(std::max(distance, 1)), GetConfig()->distanceWeight, GetConfig()->distanceBoost);
						if (distance <= rawRes.term.opts.distance && (!curExists[vid] || irank > merged_rd[moffset].rank)) {
							// distance and rank is better, than prev. update rank
							if (curExists[vid]) {
								merged_rd[moffset].rank -= merged_rd[moffset].rank;
							}
							merged[moffset].second += irank;
							merged_rd[moffset].rank = irank;
							merged_rd[moffset].next = std::move(relid);
							curExists[vid] = true;
						}
					}
				}
			}
			if (int(merged.size()) < GetConfig()->mergeLimit && op == OpOr && !exists[relid.id]) {
				// match of 1-st term
				merged.push_back({relid.id, rank});
				exists[vid] = true;
				if (simple) continue;
				// prepare for intersect with next terms
				merged_rd.push_back({IdRelType(std::move(relid)), IdRelType(), int(rank), rawRes.term.opts.qpos});
				curExists[vid] = true;
				idoffsets[vid] = merged.size() - 1;
			}
		}
	}
	if (op == OpAnd) {
		for (size_t id = 0; id < this->vdocs_.size(); id++) {
			if (exists[id] && !curExists[id]) {
				merged[idoffsets[id]].second = 0;
				exists[id] = false;
			}
		}
	}
}

template <typename T>
IdSet::Ptr FastIndexText<T>::mergeResults(vector<TextSearchResults> &rawResults, FullTextCtx::Ptr ctx) {
	if (!rawResults.size() || !this->vdocs_.size()) return std::make_shared<IdSet>();

	vector<bool> exists(this->vdocs_.size(), false);
	vector<pair<IdType, int>> merged;
	vector<MergedIdRel> merged_rd;
	h_vector<int16_t> idoffsets;

	int mergeCnt = 0, idsMaxCnt = 0;
	for (auto &rawRes : rawResults) {
		std::sort(rawRes.begin(), rawRes.end(),
				  [](const TextSearchResult &lhs, const TextSearchResult &rhs) { return lhs.proc_ > rhs.proc_; });
		if (rawRes.term.opts.op == OpOr || !idsMaxCnt) idsMaxCnt += rawRes.idsCnt_;
	}

	merged.reserve(std::min(GetConfig()->mergeLimit, idsMaxCnt));

	if (rawResults.size() > 1) {
		idoffsets.resize(this->vdocs_.size());
		merged_rd.reserve(std::min(GetConfig()->mergeLimit, idsMaxCnt));
	}
	for (auto &rawRes : rawResults) {
		mergeItaration(rawRes, exists, merged, merged_rd, idoffsets);

		if (rawRes.term.opts.op != OpNot) mergeCnt++;
	}
	if (GetConfig()->logLevel >= LogInfo)
		logPrintf(LogInfo, "Complex merge (%d patterns): out %d vids", int(rawResults.size()), int(merged.size()));

	std::sort(merged.begin(), merged.end(),
			  [](const pair<IdType, int> &lhs, const pair<IdType, int> &rhs) { return lhs.second > rhs.second; });

	// convert vids(uniq documents id) to ids (real ids)
	IdSet::Ptr mergedIds = std::make_shared<IdSet>();
	int cnt = 0;
	int minRelevancy = GetConfig()->minRelevancy * 100;
	for (auto &vid : merged) {
		assert(vid.first < int(this->vdocs_.size()));
		if (vid.second <= minRelevancy) break;
		cnt += this->vdocs_[vid.first].keyEntry->Sorted(0).size();
	}

	mergedIds->reserve(cnt);
	ctx->Reserve(cnt);
	for (auto &vid : merged) {
		auto id = vid.first;
		assert(id < IdType(this->vdocs_.size()));

		if (vid.second <= minRelevancy) break;
		int proc = std::min(255, vid.second / mergeCnt);
		ctx->Add(this->vdocs_[id].keyEntry->Sorted(0).begin(), this->vdocs_[id].keyEntry->Sorted(0).end(), proc);
		mergedIds->Append(this->vdocs_[id].keyEntry->Sorted(0).begin(), this->vdocs_[id].keyEntry->Sorted(0).end(), IdSet::Unordered);
	}
	if (GetConfig()->logLevel >= LogInfo) {
		logPrintf(LogInfo, "Total merge out: %d ids", int(mergedIds->size()));

		string str;
		for (size_t i = 0; i < ctx->GetSize();) {
			size_t j = i;
			for (; j < ctx->GetSize() && ctx->Proc(i) == ctx->Proc(j); j++)
				;
			str += std::to_string(ctx->Proc(i)) + "%";
			if (j - i > 1) {
				str += "(";
				str += std::to_string(j - i);
				str += ")";
			}
			str += " ";
			i = j;
		}
		logPrintf(LogInfo, "Relevancy(%d): %s", ctx->GetSize(), str.c_str());
	}

	assert(mergedIds->size() == ctx->GetSize());
	return mergedIds;
}

template <typename T>
Index *FastIndexText<T>::Clone() {
	return new FastIndexText<T>(*this);
}

template <typename T>
void FastIndexText<T>::Commit() {
	words_.clear();
	suffixes_.clear();
	typos_.clear();
	auto tm0 = high_resolution_clock::now();

	// Step 1: parse all documents and build hash map of all uniq words
	fast_hash_map<string, WordEntry> words_um;
	buildWordsMap(words_um);

	// Step 2: Evaluate total size
	size_t szCnt = 0;
	vector<unique_ptr<string>> bufStrs;
	for (auto &doc : this->idx_map) {
		for (auto f : getDocFields(doc.first, bufStrs)) szCnt += f.first->length();
	}

	auto tm2 = high_resolution_clock::now();

	// Step 3: Build words array
	suffixes_.reserve(words_um.size() * 20, words_um.size());
	for (auto keyIt = words_um.begin(); keyIt != words_um.end(); keyIt++) {
		suffixes_.insert(keyIt->first, words_.size());
		keyIt->second.vids_.Commit();
		words_.emplace_back(PackedWordEntry());
	}

	// Step 4: Build suffixes array. It runs in parallel with next step
	auto &suffixes = suffixes_;
	auto tm3 = high_resolution_clock::now(), tm4 = high_resolution_clock::now();
	thread sufBuildThread([&suffixes, &tm3]() {
		suffixes.build();
		tm3 = high_resolution_clock::now();
	});

	// Step 5: Normalize and sort idrelsets. It runs in parallel with next step
	auto &words = words_;
	size_t idsetcnt = 0;
	thread idrelsetCommitThread([&words, &tm4, &idsetcnt, &words_um]() {
		auto wIt = words.begin();
		for (auto keyIt = words_um.begin(); keyIt != words_um.end(); keyIt++, wIt++) {
			// Pack idrelset
			wIt->vids_.insert(wIt->vids_.end(), keyIt->second.vids_.begin(), keyIt->second.vids_.end());
			keyIt->second.vids_.clear();
			idsetcnt += wIt->vids_.real_size();
			wIt->vids_.shrink_to_fit();
		}
		tm4 = high_resolution_clock::now();
	});

	// Wait for suf array build. It is neccessary for typos
	sufBuildThread.join();

	// Step 6: Build typos hash map
	buildTyposMap();
	auto tm5 = high_resolution_clock::now();

	idrelsetCommitThread.join();

	auto tm6 = high_resolution_clock::now();

	logPrintf(LogInfo, "FastIndexText built with [%d uniq words, %d typos, %dKB text size, %dKB suffixarray size, %dKB idrelsets size]",
			  int(words_um.size()), int(typos_.size()), int(szCnt / 1024), int((3 * sizeof(int) + sizeof(char)) * suffixes_.size() / 1024),
			  int(idsetcnt / 1024));

	logPrintf(
		LogInfo,
		"FastIndexText::Commit elapsed %d ms total [ build words %d ms, build typos %d ms | build suffixarry %d ms | sort idrelsets %d ms]",
		duration_cast<milliseconds>(tm6 - tm0), duration_cast<milliseconds>(tm2 - tm0), duration_cast<milliseconds>(tm5 - tm3),
		duration_cast<milliseconds>(tm3 - tm2), duration_cast<milliseconds>(tm4 - tm2));
}

template <typename T>
IdSet::Ptr FastIndexText<T>::Select(FullTextCtx::Ptr fctx, FtDSLQuery &dsl) {
	FtSelectContext ctx;
	// STEP 2: Search dsl terms for each variant
	for (auto &term : dsl) {
		ctx.rawResults.push_back(TextSearchResults());
		TextSearchResults &res = ctx.rawResults.back();
		res.term = term;

		// Prepare term variants (original + translit + stemmed + kblayout)
		this->prepareVariants(ctx, term, GetConfig()->stemmers);

		if (GetConfig()->logLevel >= LogInfo) {
			string vars;
			for (auto &variant : ctx.variants) {
				if (&variant != &*ctx.variants.begin()) vars += ", ";
				vars += variant.pattern;
			}
			vars += "], typos: [";
			typos_context tctx[kMaxTyposInWord];
			if (term.opts.typos)
				mktypos(tctx, term.pattern, GetConfig()->maxTyposInWord, GetConfig()->maxTypoLen, [&vars](const string &typo, int) {
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

	auto mergedIds = mergeResults(ctx.rawResults, fctx);
	return mergedIds;
}
template <typename T>
FtFastConfig *FastIndexText<T>::GetConfig() const {
	return dynamic_cast<FtFastConfig *>(this->cfg_.get());
}
template <typename T>
void FastIndexText<T>::CreateConfig(const FtFastConfig *cfg) {
	if (cfg) {
		this->cfg_.reset(new FtFastConfig(*cfg));
		return;
	}
	this->cfg_.reset(new FtFastConfig());
}
}  // namespace reindexer
