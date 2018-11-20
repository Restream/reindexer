
#include "dataprocessor.h"
#include <chrono>
#include <functional>
#include <thread>
#include "core/ft/numtotext.h"
#include "core/ft/typos.h"

#include "tools/logger.h"
#include "tools/stringstools.h"

using std::chrono::high_resolution_clock;
using std::thread;
using std::placeholders::_1;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::bind;
using std::tie;

namespace reindexer {

const int kDigitUtfSizeof = 1;

void DataProcessor::Process(bool multithread) {
	multithread_ = multithread;

	fast_hash_map<string, WordEntry> words_um;
	auto tm0 = high_resolution_clock::now();
	size_t szCnt = buildWordsMap(words_um);
	auto tm2 = high_resolution_clock::now();
	auto &words = holder_.GetWords();

	holder_.SetWordsOffset(words.size());
	size_t wrdOffset = words.size();

	auto found = BuildSuffix(words_um, holder_);
	auto getWordByIdFunc = bind(&DataHolder::getWordById, &holder_, _1);

	// Step 4: Commit suffixes array. It runs in parallel with next step
	auto &suffixes = holder_.GetSuffix();
	auto tm3 = high_resolution_clock::now(), tm4 = high_resolution_clock::now();
	thread sufBuildThread([&suffixes, &tm3]() {
		suffixes.build();
		tm3 = high_resolution_clock::now();
	});

	// Step 5: Normalize and sort idrelsets. It runs in parallel with next step
	size_t idsetcnt = 0;

	auto wIt = words.begin() + wrdOffset;
	auto status = holder_.status_;

	thread idrelsetCommitThread([&wIt, &found, getWordByIdFunc, &tm4, &idsetcnt, &words_um, status]() {
		uint32_t i = 0;
		for (auto keyIt = words_um.begin(); keyIt != words_um.end(); keyIt++, i++) {
			// Pack idrelset

			PackedWordEntry *word;

			if (found.size() && !found[i].isEmpty()) {
				word = &getWordByIdFunc(found[i]);
			} else {
				word = &(*wIt);
				++wIt;
				idsetcnt += sizeof(*wIt);
			}

			if (status == CreateNew) {
				word->cur_step_pos_ = word->vids_.end().pos();
			}
			word->vids_.insert(word->vids_.end(), keyIt->second.vids_.begin(), keyIt->second.vids_.end());

			if (status == FullRebuild) {
				word->cur_step_pos_ = word->vids_.end().pos();
			}

			word->vids_.shrink_to_fit();

			keyIt->second.vids_.clear();
			idsetcnt += word->vids_.heap_size();
		}
		tm4 = high_resolution_clock::now();
	});

	// Wait for suf array build. It is neccessary for typos
	sufBuildThread.join();

	// std::cout << suffixes.dump() << std::endl;
	idrelsetCommitThread.join();

	// Step 6: Build typos hash map
	buildTyposMap(wrdOffset, found);
	// print(words_um);

	auto tm5 = high_resolution_clock::now();

	logPrintf(LogInfo, "FastIndexText[%d] built with [%d uniq words, %d typos, %dKB text size, %dKB suffixarray size, %dKB idrelsets size]",
			  int(holder_.steps.size()), int(words_um.size()), int(holder_.GetTypos().size()), int(szCnt / 1024),
			  int(suffixes.heap_size() / 1024), int(idsetcnt / 1024));

	logPrintf(LogInfo,
			  "DataProcessor::Process elapsed %d ms total [ build words %d ms, build typos %d ms | build suffixarry %d ms | sort "
			  "idrelsets %d ms]\n",
			  int(duration_cast<milliseconds>(tm5 - tm0).count()), int(duration_cast<milliseconds>(tm2 - tm0).count()),
			  int(duration_cast<milliseconds>(tm5 - tm4).count()), int(duration_cast<milliseconds>(tm3 - tm2).count()),
			  int(duration_cast<milliseconds>(tm4 - tm2).count()));
}

vector<WordIdType> DataProcessor::BuildSuffix(fast_hash_map<std::string, WordEntry> &words_um, DataHolder &holder) {
	auto &words = holder.GetWords();

	auto &suffix = holder.GetSuffix();

	suffix.reserve(words_um.size() * 20, words_um.size());

	vector<WordIdType> found;

	found.reserve(words_um.size());

	for (auto keyIt = words_um.begin(); keyIt != words_um.end(); keyIt++) {
		// if we still haven't whis word we add it to new suffix tree else we will only add info to current word

		auto id = words.size();
		// keyIt->second.vids_.Commit();
		WordIdType pos;
		pos = holder_.findWord(keyIt->first);
		found.push_back(pos);

		if (!pos.isEmpty()) {
			continue;
		}

		words.emplace_back(PackedWordEntry());
		pos = holder_.BuildWordId(id);
		if (holder_.cfg_->enableNumbersSearch && keyIt->second.virtualWord) {
			suffix.insert(keyIt->first, pos, kDigitUtfSizeof);
		} else {
			suffix.insert(keyIt->first, pos);
		}
	}
	return found;
}

size_t DataProcessor::buildWordsMap(fast_hash_map<string, WordEntry> &words_um) {
	// int maxIndexWorkers = !this->opts_.IsDense() ? std::thread::hardware_concurrency() : 0;
	uint32_t maxIndexWorkers = multithread_ ? std::thread::hardware_concurrency() : 0;
	if (!maxIndexWorkers) maxIndexWorkers = 1;
	if (maxIndexWorkers > 8) maxIndexWorkers = 8;
	size_t szCnt = 0;
	struct context {
		fast_hash_map<string, WordEntry> words_um;
		std::thread thread;
	};
	unique_ptr<context[]> ctxs(new context[maxIndexWorkers]);

	auto &cfg = holder_.cfg_;
	auto &vdocsTexts = holder_.vdocsTexts;
	auto &vdocs = holder_.vdocs_;
	// int fieldscount = std::max(1, int(this->fields_.size()));
	int fieldscount = fieldSize_;
	size_t offset = holder_.vodcsOffset_;
	// build words map parallel in maxIndexWorkers threads
	auto worker = [this, &ctxs, &vdocsTexts, offset, maxIndexWorkers, fieldscount, &cfg, &vdocs](int i) {
		auto ctx = &ctxs[i];
		string word, str;
		vector<const char *> wrds;
		std::vector<string> virtualWords;
		for (VDocIdType j = i; j < VDocIdType(vdocsTexts.size()); j += maxIndexWorkers) {
			size_t vdocId = offset + j;
			vdocs[vdocId].wordsCount.insert(vdocs[vdocId].wordsCount.begin(), fieldscount, 0.0);
			vdocs[vdocId].mostFreqWordCount.insert(vdocs[vdocId].mostFreqWordCount.begin(), fieldscount, 0.0);

			for (size_t field = 0; field < vdocsTexts[j].size(); ++field) {
				split(vdocsTexts[j][field].first, str, wrds, cfg->extraWordSymbols);
				int rfield = vdocsTexts[j][field].second;
				assert(rfield < fieldscount);

				vdocs[vdocId].wordsCount[rfield] = wrds.size();

				int insertPos = -1;
				for (auto w : wrds) {
					insertPos++;
					word.assign(w);
					if (!word.length() || cfg->stopWords.find(word) != cfg->stopWords.end()) continue;

					auto idxIt = ctx->words_um.find(word);
					if (idxIt == ctx->words_um.end()) {
						idxIt = ctx->words_um.emplace(word, WordEntry()).first;
						// idxIt->second.vids_.reserve(16);
					}
					int mfcnt = idxIt->second.vids_.Add(vdocId, insertPos, rfield);
					if (mfcnt > vdocs[vdocId].mostFreqWordCount[rfield]) {
						vdocs[vdocId].mostFreqWordCount[rfield] = mfcnt;
					}

					if (cfg->enableNumbersSearch && is_number(word)) {
						buildVirtualWord(word, ctx->words_um, vdocId, field, insertPos, virtualWords);
					}
				}
			}
		}
	};

	// If there was only 1 build thread. Just return it's build results
	if (maxIndexWorkers == 1) {
		worker(0);
		words_um.swap(ctxs[0].words_um);
	} else {
		for (uint32_t t = 0; t < maxIndexWorkers; t++) ctxs[t].thread = thread(worker, t);
		// Merge results into single map
		for (uint32_t i = 0; i < maxIndexWorkers; i++) {
			ctxs[i].thread.join();
			for (auto it = ctxs[i].words_um.begin(); it != ctxs[i].words_um.end(); it++) {
				auto idxIt = words_um.find(it->first);

				if (idxIt == words_um.end()) {
					words_um.emplace(it->first, std::move(it->second));
				} else {
					idxIt->second.vids_.reserve(it->second.vids_.size() + idxIt->second.vids_.size());
					for (auto &r : it->second.vids_) idxIt->second.vids_.push_back(std::move(r));
					it->second.vids_ = IdRelSet();
				}
			}
			fast_hash_map<string, WordEntry>().swap(ctxs[i].words_um);
		}
	}

	// Calculate avg words count per document for bm25 calculation
	if (vdocs.size()) {
		holder_.avgWordsCount_.resize(fieldscount);
		for (int i = 0; i < fieldscount; i++) holder_.avgWordsCount_[i] = 0;

		for (auto &vdoc : vdocs) {
			for (int i = 0; i < fieldscount; i++) holder_.avgWordsCount_[i] += vdoc.wordsCount[i];
		}
		for (int i = 0; i < fieldscount; i++) holder_.avgWordsCount_[i] /= vdocs.size();
	}

	// Check and print potential stop words
	if (holder_.cfg_->logLevel >= LogInfo) {
		string str;
		for (auto &w : words_um) {
			if (w.second.vids_.size() > vdocs.size() / 5) str += w.first + " ";
		}
		logPrintf(LogInfo, "Potential stop words: %s", str.c_str());
	}
	vector<h_vector<pair<string_view, uint32_t>, 8>>().swap(holder_.vdocsTexts);

	vector<unique_ptr<string>>().swap(holder_.bufStrs_);
	return szCnt;
}

void DataProcessor::buildVirtualWord(const string &word, fast_hash_map<string, WordEntry> &words_um, VDocIdType docType, int rfield,
									 size_t insertPos, std::vector<string> &output) {
	auto &vdocs = holder_.vdocs_;

	auto &vdoc(vdocs[docType]);
	NumToText::convert(word, output);
	for (const string &numberWord : output) {
		WordEntry wentry;
		wentry.virtualWord = true;
		auto idxIt = words_um.emplace(numberWord, std::move(wentry)).first;
		int mfcnt = idxIt->second.vids_.Add(docType, insertPos, rfield);
		if (mfcnt > vdoc.mostFreqWordCount[rfield]) {
			vdoc.mostFreqWordCount[rfield] = mfcnt;
		}
		++vdoc.wordsCount[rfield];
		insertPos += kDigitUtfSizeof;
	}
}

void DataProcessor::buildTyposMap(uint32_t startPos, const vector<WordIdType> &found) {
	if (!holder_.cfg_->maxTyposInWord) {
		return;
	}

	typos_context tctx[kMaxTyposInWord];
	auto &typos = holder_.GetTypos();
	auto &words_ = holder_.GetWords();
	size_t wordsSize = !found.empty() ? found.size() : words_.size() - startPos;

	typos.reserve(wordsSize * (10 >> (holder_.cfg_->maxTyposInWord - 1)) / 2, wordsSize * 5 * (10 >> (holder_.cfg_->maxTyposInWord - 1)));

	for (size_t i = 0; i < wordsSize; ++i) {
		if (!found.empty() && !found[i].isEmpty()) {
			continue;
		}

		auto wordId = holder_.BuildWordId(startPos);
		mktypos(tctx, holder_.GetSuffix().word_at(holder_.GetSuffixWordId(wordId)), holder_.cfg_->maxTyposInWord, holder_.cfg_->maxTypoLen,
				[&typos, wordId](const string &typo, int) { typos.emplace(typo, wordId); });
		startPos++;
	}

	typos.shrink_to_fit();
}

}  // namespace reindexer
