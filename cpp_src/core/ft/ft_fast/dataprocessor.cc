
#include "dataprocessor.h"
#include <chrono>
#include <functional>
#include <thread>
#include "core/ft/numtotext.h"
#include "core/ft/typos.h"

#include "tools/hardware_concurrency.h"
#include "tools/logger.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

using std::chrono::high_resolution_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

namespace reindexer {

constexpr int kDigitUtfSizeof = 1;

template <typename IdCont>
void DataProcessor<IdCont>::Process(bool multithread) {
	multithread_ = multithread;

	words_map words_um;
	auto tm0 = high_resolution_clock::now();
	size_t szCnt = buildWordsMap(words_um);
	auto tm2 = high_resolution_clock::now();
	auto &words = holder_.GetWords();

	holder_.SetWordsOffset(words.size());
	size_t wrdOffset = words.size();

	auto found = BuildSuffix(words_um, holder_);
	auto getWordByIdFunc = [this](WordIdType id) -> PackedWordEntry<IdCont> & { return holder_.getWordById(id); };

	// Step 4: Commit suffixes array. It runs in parallel with next step
	auto &suffixes = holder_.GetSuffix();
	auto tm3 = high_resolution_clock::now(), tm4 = high_resolution_clock::now();
	auto sufBuildFun = [&suffixes, &tm3]() {
		suffixes.build();
		tm3 = high_resolution_clock::now();
	};
	std::thread sufBuildThread(sufBuildFun);
	// Step 5: Normalize and sort idrelsets. It runs in parallel with next step
	size_t idsetcnt = 0;

	auto wIt = words.begin() + wrdOffset;

	auto idrelsetCommitFun = [&wIt, &found, &getWordByIdFunc, &tm4, &idsetcnt, &words_um]() {
		uint32_t i = 0;
		for (auto keyIt = words_um.begin(), endIt = words_um.end(); keyIt != endIt; ++keyIt, ++i) {
			// Pack idrelset

			PackedWordEntry<IdCont> *word;

			if (found.size() && !found[i].isEmpty()) {
				word = &getWordByIdFunc(found[i]);
			} else {
				word = &(*wIt);
				++wIt;
				idsetcnt += sizeof(*wIt);
			}

			word->vids_.insert(word->vids_.end(), keyIt->second.vids_.begin(), keyIt->second.vids_.end());
			word->vids_.shrink_to_fit();

			keyIt->second.vids_.clear();
			idsetcnt += word->vids_.heap_size();
		}
		tm4 = high_resolution_clock::now();
	};

	std::thread idrelsetCommitThread(idrelsetCommitFun);

	// Wait for suf array build. It is neccessary for typos
	sufBuildThread.join();

	idrelsetCommitThread.join();

	// Step 6: Build typos hash map
	buildTyposMap(wrdOffset, found);
	// print(words_um);

	auto tm5 = high_resolution_clock::now();

	logPrintf(LogInfo, "FastIndexText[%d] built with [%d uniq words, %d typos, %dKB text size, %dKB suffixarray size, %dKB idrelsets size]",
			  holder_.steps.size(), words_um.size(), holder_.GetTyposHalf().size() + holder_.GetTyposMax().size(), szCnt / 1024,
			  suffixes.heap_size() / 1024, idsetcnt / 1024);

	logPrintf(LogInfo,
			  "DataProcessor::Process elapsed %d ms total [ build words %d ms, build typos %d ms | build suffixarry %d ms | sort "
			  "idrelsets %d ms]",
			  duration_cast<milliseconds>(tm5 - tm0).count(), duration_cast<milliseconds>(tm2 - tm0).count(),
			  duration_cast<milliseconds>(tm5 - tm4).count(), duration_cast<milliseconds>(tm3 - tm2).count(),
			  duration_cast<milliseconds>(tm4 - tm2).count());
}

template <typename IdCont>
std::vector<WordIdType> DataProcessor<IdCont>::BuildSuffix(words_map &words_um, DataHolder<IdCont> &holder) {
	auto &words = holder.GetWords();

	auto &suffix = holder.GetSuffix();

	suffix.reserve(words_um.size() * 20, words_um.size());

	std::vector<WordIdType> found;

	found.reserve(words_um.size());

	for (auto &keyIt : words_um) {
		// if we still haven't whis word we add it to new suffix tree else we will only add info to current word

		auto id = words.size();
		WordIdType pos = found.emplace_back(holder_.findWord(keyIt.first));

		if (!pos.isEmpty()) {
			continue;
		}

		words.emplace_back();
		pos = holder_.BuildWordId(id);
		if (holder_.cfg_->enableNumbersSearch && keyIt.second.virtualWord) {
			suffix.insert(keyIt.first, pos, kDigitUtfSizeof);
		} else {
			suffix.insert(keyIt.first, pos);
		}
	}
	return found;
}

template <typename IdCont>
size_t DataProcessor<IdCont>::buildWordsMap(words_map &words_um) {
	uint32_t maxIndexWorkers = multithread_ ? hardware_concurrency() : 1;
	if (!maxIndexWorkers) {
		maxIndexWorkers = 1;
	} else if (maxIndexWorkers > 8) {
		maxIndexWorkers = 8;
	}
	size_t szCnt = 0;
	struct context {
		words_map words_um;
		std::thread thread;
	};
	std::unique_ptr<context[]> ctxs(new context[maxIndexWorkers]);

	auto &cfg = holder_.cfg_;
	auto &vdocsTexts = holder_.vdocsTexts;
	auto &vdocs = holder_.vdocs_;
	const int fieldscount = fieldSize_;
	size_t offset = holder_.vdocsOffset_;
	auto cycleSize = vdocsTexts.size() / maxIndexWorkers + (vdocsTexts.size() % maxIndexWorkers ? 1 : 0);
	// build words map parallel in maxIndexWorkers threads
	auto worker = [this, &ctxs, &vdocsTexts, offset, cycleSize, fieldscount, &cfg, &vdocs](int i) {
		auto ctx = &ctxs[i];
		std::string word, str;
		std::vector<const char *> wrds;
		std::vector<std::string> virtualWords;
		size_t start = cycleSize * i;
		size_t fin = std::min(cycleSize * (i + 1), vdocsTexts.size());
		for (VDocIdType j = start; j < fin; ++j) {
			const size_t vdocId = offset + j;
			auto &vdoc = vdocs[vdocId];
			vdoc.wordsCount.insert(vdoc.wordsCount.begin(), fieldscount, 0.0);
			vdoc.mostFreqWordCount.insert(vdoc.mostFreqWordCount.begin(), fieldscount, 0.0);

			auto &vdocsText = vdocsTexts[j];
			for (size_t field = 0, sz = vdocsText.size(); field < sz; ++field) {
				split(vdocsText[field].first, str, wrds, cfg->extraWordSymbols);
				const int rfield = vdocsText[field].second;
				assertrx(rfield < fieldscount);

				vdoc.wordsCount[rfield] = wrds.size();

				int insertPos = -1;
				for (auto w : wrds) {
					insertPos++;
					word.assign(w);
					if (!word.length() || cfg->stopWords.find(word) != cfg->stopWords.end()) continue;

					auto [idxIt, emplaced] = ctx->words_um.try_emplace(word, WordEntry());
					(void)emplaced;
					const int mfcnt = idxIt->second.vids_.Add(vdocId, insertPos, rfield);
					if (mfcnt > vdoc.mostFreqWordCount[rfield]) {
						vdoc.mostFreqWordCount[rfield] = mfcnt;
					}

					if (cfg->enableNumbersSearch && is_number(word)) {
						buildVirtualWord(word, ctx->words_um, vdocId, field, insertPos, virtualWords);
					}
				}
			}
		}
	};

	for (uint32_t t = 1; t < maxIndexWorkers; ++t) {
		ctxs[t].thread = std::thread(worker, t);
	}
	// If there was only 1 build thread. Just return it's build results
	worker(0);
	words_um = std::move(ctxs[0].words_um);
	// Merge results into single map
	for (uint32_t i = 1; i < maxIndexWorkers; ++i) {
		try {
			auto &ctx = ctxs[i];
			ctx.thread.join();
			for (auto &it : ctx.words_um) {
#if defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)
				const auto fBeforeMove = it.first;
				const auto sBeforeMove = it.second;
				const auto sCapacityBeforeMove = it.second.vids_.capacity();
#endif	// defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)
				auto [idxIt, emplaced] = words_um.try_emplace(std::move(it.first), std::move(it.second));
				if (!emplaced) {
#if defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)
					// Make sure, that try_emplace did not moved the values
					assertrx(it.first == fBeforeMove);
					assertrx(it.second.virtualWord == sBeforeMove.virtualWord);
					assertrx(it.second.vids_.size() == sBeforeMove.vids_.size());
					assertrx(it.second.vids_.capacity() == sCapacityBeforeMove);
#endif	// defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)
					idxIt->second.vids_.reserve(it.second.vids_.size() + idxIt->second.vids_.size());
					for (auto &&r : it.second.vids_) idxIt->second.vids_.emplace_back(std::move(r));
					it.second.vids_ = IdRelSet();
				}
			}
			words_map().swap(ctx.words_um);
		} catch (const Error &e) {
			logPrintf(LogError, "Exeption in loop with thread.join() error:[%s]", e.what());
		} catch (const std::exception &e) {
			logPrintf(LogError, "Exeption in loop with thread.join() error:[%s]", e.what());
		} catch (...) {
			logPrintf(LogError, "Exeption in loop with thread.join()");
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
	if rx_unlikely (holder_.cfg_->logLevel >= LogInfo) {
		WrSerializer out;
		for (auto &w : words_um) {
			if (w.second.vids_.size() > vdocs.size() / 5 || int64_t(w.second.vids_.size()) > holder_.cfg_->mergeLimit) {
				out << w.first << "(" << w.second.vids_.size() << ") ";
			}
		}
		logPrintf(LogInfo, "Total documents: %d. Potential stop words (with corresponding docs count): %s", vdocs.size(), out.Slice());
	}
	std::vector<RVector<std::pair<std::string_view, uint32_t>, 8>>().swap(holder_.vdocsTexts);

	std::vector<std::unique_ptr<std::string>>().swap(holder_.bufStrs_);
	return szCnt;
}

template <typename IdCont>
void DataProcessor<IdCont>::buildVirtualWord(std::string_view word, words_map &words_um, VDocIdType docType, int rfield, size_t insertPos,
											 std::vector<std::string> &container) {
	auto &vdocs = holder_.vdocs_;

	auto &vdoc(vdocs[docType]);
	NumToText::convert(word, container);
	for (std::string &numberWord : container) {
		WordEntry wentry;
		wentry.virtualWord = true;
		auto idxIt = words_um.emplace(std::move(numberWord), std::move(wentry)).first;
		const int mfcnt = idxIt->second.vids_.Add(docType, insertPos, rfield);
		if (mfcnt > vdoc.mostFreqWordCount[rfield]) {
			vdoc.mostFreqWordCount[rfield] = mfcnt;
		}
		++vdoc.wordsCount[rfield];
		insertPos += kDigitUtfSizeof;
	}
}

template <typename IdCont>
void DataProcessor<IdCont>::buildTyposMap(uint32_t startPos, const std::vector<WordIdType> &found) {
	if (!holder_.cfg_->maxTypos) {
		return;
	}

	typos_context tctx[kMaxTyposInWord];
	auto &typosHalf = holder_.GetTyposHalf();
	auto &typosMax = holder_.GetTyposMax();
	auto &words_ = holder_.GetWords();
	size_t wordsSize = !found.empty() ? found.size() : words_.size() - startPos;

	const auto maxTyposInWord = holder_.cfg_->MaxTyposInWord();
	const auto halfMaxTypos = holder_.cfg_->maxTypos / 2;
	if (maxTyposInWord == halfMaxTypos) {
		assertrx(maxTyposInWord > 0);
		const auto multiplicator = wordsSize * (10 << (maxTyposInWord - 1));
		typosHalf.reserve(multiplicator / 2, multiplicator * 5);
	} else {
		assertrx(maxTyposInWord == halfMaxTypos + 1);
		auto multiplicator = wordsSize * (10 << (halfMaxTypos > 1 ? (halfMaxTypos - 1) : 0));
		typosHalf.reserve(multiplicator / 2, multiplicator * 5);
		multiplicator = wordsSize * (10 << (maxTyposInWord - 1)) - multiplicator;
		typosMax.reserve(multiplicator / 2, multiplicator * 5);
	}

	for (size_t i = 0; i < wordsSize; ++i) {
		if (!found.empty() && !found[i].isEmpty()) {
			continue;
		}

		const auto wordId = holder_.BuildWordId(startPos);
		const std::string_view word = holder_.GetSuffix().word_at(holder_.GetSuffixWordId(wordId));
		mktypos(tctx, word, maxTyposInWord, holder_.cfg_->maxTypoLen,
				maxTyposInWord == halfMaxTypos
					? typos_context::CallBack{[&typosHalf, wordId](std::string_view typo, int, const typos_context::TyposVec &positions) {
						  typosHalf.emplace(typo, WordTypo{wordId, positions});
					  }}
					: typos_context::CallBack{[&](std::string_view typo, int level, const typos_context::TyposVec &positions) {
						  if (level > 1 || typo.size() == word.size()) {
							  typosHalf.emplace(typo, WordTypo{wordId, positions});
						  } else {
							  typosMax.emplace(typo, WordTypo{wordId, positions});
						  }
					  }});
		startPos++;
	}

	typosHalf.shrink_to_fit();
	typosMax.shrink_to_fit();
}

template class DataProcessor<PackedIdRelVec>;
template class DataProcessor<IdRelVec>;

}  // namespace reindexer
