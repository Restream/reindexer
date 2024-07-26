#include "dataprocessor.h"
#include <chrono>
#include "core/ft/numtotext.h"
#include "core/ft/typos.h"

#include "tools/hardware_concurrency.h"
#include "tools/clock.h"
#include "tools/logger.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

using std::chrono::duration_cast;
using std::chrono::milliseconds;

namespace reindexer {

constexpr int kDigitUtfSizeof = 1;

template <typename IdCont>
void DataProcessor<IdCont>::Process(bool multithread) {
	ExceptionPtrWrapper exwr;
	words_map words_um;
	const auto tm0 = system_clock_w::now();
	size_t szCnt = buildWordsMap(words_um, multithread);
	const auto tm1 = system_clock_w::now();
	auto &words = holder_.GetWords();
	const size_t wrdOffset = words.size();
	holder_.SetWordsOffset(wrdOffset);

	const auto preprocWords = insertIntoSuffix(words_um, holder_);
	const auto tm2 = system_clock_w::now();
	// Step 4: Commit suffixes array. It runs in parallel with next step
	auto &suffixes = holder_.GetSuffix();
	auto tm3 = tm2, tm4 = tm2;
	std::thread sufBuildThread = runInThread(exwr, [&suffixes, &tm3] {
		suffixes.build();
		tm3 = system_clock_w::now();
	});
	// Step 5: Normalize and sort idrelsets. It runs in parallel with next step
	size_t idsetcnt = 0;
	std::thread idrelsetCommitThread = runInThread(exwr, [&] {
		idsetcnt = commitIdRelSets(preprocWords, words_um, holder_, wrdOffset);
		tm4 = system_clock_w::now();
	});

	// Step 6: Build typos hash map
	try {
		buildTyposMap(wrdOffset, preprocWords);
	} catch (...) {
		exwr.SetException(std::current_exception());
	}
	const auto tm5 = system_clock_w::now();

	// Step 7: Await threads
	idrelsetCommitThread.join();
	sufBuildThread.join();
	exwr.RethrowException();
	const auto tm6 = system_clock_w::now();

	logPrintf(
		LogInfo,
		"FastIndexText[%d] built with [%d uniq words, %d typos (%d + %d), %dKB text size, %dKB suffixarray size, %dKB idrelsets size]",
		holder_.steps.size(), words_um.size(), holder_.GetTyposHalf().size() + holder_.GetTyposMax().size(), holder_.GetTyposHalf().size(),
		holder_.GetTyposMax().size(), szCnt / 1024, suffixes.heap_size() / 1024, idsetcnt / 1024);

	logPrintf(LogInfo,
			  "DataProcessor::Process elapsed %d ms total [ build words %d ms | suffixes preproc %d ms | build typos %d ms | build "
			  "suffixarry %d ms | sort idrelsets %d ms]",
			  duration_cast<milliseconds>(tm6 - tm0).count(), duration_cast<milliseconds>(tm1 - tm0).count(),
			  duration_cast<milliseconds>(tm2 - tm1).count(), duration_cast<milliseconds>(tm5 - tm2).count(),
			  duration_cast<milliseconds>(tm3 - tm2).count(), duration_cast<milliseconds>(tm4 - tm2).count());
}

template <typename IdCont>
typename DataProcessor<IdCont>::WordsVector DataProcessor<IdCont>::insertIntoSuffix(words_map &words_um, DataHolder<IdCont> &holder) {
	auto &words = holder.GetWords();
	auto &suffix = holder.GetSuffix();

	suffix.reserve(words_um.size() * 20, words_um.size());
	const bool enableNumbersSearch = holder.cfg_->enableNumbersSearch;

	WordsVector found;
	found.reserve(words_um.size());

	for (auto &keyIt : words_um) {
		// if we still haven't whis word we add it to new suffix tree else we will only add info to current word

		auto id = words.size();
		WordIdType pos = holder.findWord(keyIt.first);

		if (!pos.IsEmpty()) {
			found.emplace_back(pos);
			continue;
		}
		found.emplace_back(keyIt.first);

		words.emplace_back();
		pos = holder.BuildWordId(id);
		if (enableNumbersSearch && keyIt.second.virtualWord) {
			suffix.insert(keyIt.first, pos, kDigitUtfSizeof);
		} else {
			suffix.insert(keyIt.first, pos);
		}
	}
	return found;
}

template <typename IdCont>
size_t DataProcessor<IdCont>::commitIdRelSets(const WordsVector &preprocWords, words_map &words_um, DataHolder<IdCont> &holder,
											  size_t wrdOffset) {
	size_t idsetcnt = 0;
	auto wIt = holder.GetWords().begin() + wrdOffset;
	uint32_t i = 0;
	auto preprocWordsSize = preprocWords.size();
	for (auto keyIt = words_um.begin(), endIt = words_um.end(); keyIt != endIt; ++keyIt, ++i) {
		// Pack idrelset
		PackedWordEntry<IdCont> *word = nullptr;
		if (preprocWordsSize > i) {
			if (auto widPtr = std::get_if<WordIdType>(&preprocWords[i]); widPtr) {
				assertrx_dbg(!widPtr->IsEmpty());
				word = &holder.GetWordById(*widPtr);
			}
		}
		if (!word) {
			word = &(*wIt);
			++wIt;
			idsetcnt += sizeof(*wIt);
		}

		word->vids.insert(word->vids.end(), std::make_move_iterator(keyIt->second.vids.begin()),
						  std::make_move_iterator(keyIt->second.vids.end()));
		keyIt->second.vids = IdRelSet();
		word->vids.shrink_to_fit();
		idsetcnt += word->vids.heap_size();
	}
	return idsetcnt;
}

static uint32_t getMaxBuildWorkers(bool multithread) noexcept {
	if (!multithread) {
		return 1;
	}
	// using std's hardware_concurrency instead of reindexer's hardware_concurrency here
	auto maxIndexWorkers = std::thread::hardware_concurrency();
	if (!maxIndexWorkers) {
		return 4;
	} else if (maxIndexWorkers > 32) {
		return 16;
	} else if (maxIndexWorkers > 24) {
		return 12;
	} else if (maxIndexWorkers > 8) {
		return 8;
	}
	return maxIndexWorkers;
}

template <typename ContextT>
void makeDocsDistribution(ContextT *ctxs, size_t ctxsCount, size_t docs) {
	if (ctxsCount == 1) {
		ctxs[0].from = 0;
		ctxs[0].to = docs;
		return;
	}
	if (docs < ctxsCount) {
		for (size_t i = 0; i < docs; ++i) {
			ctxs[i].from = i;
			ctxs[i].to = i + 1;
		}
		for (size_t i = docs; i < ctxsCount; ++i) {
			ctxs[i].from = 0;
			ctxs[i].to = 0;
		}
		return;
	}
	const size_t part = docs / ctxsCount;
	const size_t smallPart = part - (part / 8);
	const size_t largePart = part + (part / 8);
	size_t next = 0;
	for (size_t i = 0; i < ctxsCount / 2; ++i) {
		ctxs[i].from = next;
		next += smallPart;
		ctxs[i].to = next;
	}
	for (size_t i = ctxsCount / 2; i < ctxsCount; ++i) {
		ctxs[i].from = next;
		next += largePart;
		ctxs[i].to = next;
	}
	ctxs[ctxsCount - 1].to = docs;
}

template <typename IdCont>
size_t DataProcessor<IdCont>::buildWordsMap(words_map &words_um, bool multithread) {
	ExceptionPtrWrapper exwr;
	uint32_t maxIndexWorkers = getMaxBuildWorkers(multithread);
	size_t szCnt = 0;
	auto &vdocsTexts = holder_.vdocsTexts;
	struct context {
		words_map words_um;
		std::thread thread;
		size_t from;
		size_t to;

		~context() {
			if (thread.joinable()) {
				thread.join();
			}
		}
	};
	std::unique_ptr<context[]> ctxs(new context[maxIndexWorkers]);
	makeDocsDistribution(ctxs.get(), maxIndexWorkers, vdocsTexts.size());
#ifdef RX_WITH_STDLIB_DEBUG
	size_t to = 0;
	auto printDistribution = [&] {
		std::cerr << "Distribution:\n";
		for (uint32_t i = 0; i < maxIndexWorkers; ++i) {
			std::cerr << fmt::sprintf("%d: { from: %d; to: %d }", i, ctxs[i].from, ctxs[i].to) << std::endl;
		}
	};
	for (uint32_t i = 0; i < maxIndexWorkers; ++i) {
		if (to == vdocsTexts.size() && (ctxs[i].from || ctxs[i].to)) {
			printDistribution();
			assertrx_dbg(!ctxs[i].from);
			assertrx_dbg(!ctxs[i].to);
		} else if (ctxs[i].from > ctxs[i].to || (ctxs[i].from && ctxs[i].from != to)) {
			printDistribution();
			assertrx_dbg(ctxs[i].from <= ctxs[i].to);
			assertrx_dbg(ctxs[i].from == to);
		}
		to = ctxs[i].to ? ctxs[i].to : to;
	}
	assertrx_dbg(ctxs[0].from == 0);
	assertrx_dbg(to == vdocsTexts.size());
#endif	// RX_WITH_STDLIB_DEBUG
	ThreadsContainer bgThreads;

	auto &cfg = holder_.cfg_;
	auto &vdocs = holder_.vdocs_;
	const int fieldscount = fieldSize_;
	size_t offset = holder_.vdocsOffset_;
	// build words map parallel in maxIndexWorkers threads
	auto worker = [this, &ctxs, &vdocsTexts, offset, fieldscount, &cfg, &vdocs](int i) {
		auto ctx = &ctxs[i];
		std::string str;
		std::vector<std::string_view> wrds;
		std::vector<std::string> virtualWords;
		const size_t start = ctx->from;
		const size_t fin = ctx->to;
		const std::string_view extraWordSymbols(cfg->extraWordSymbols);
		const bool enableNumbersSearch = cfg->enableNumbersSearch;
		const word_hash h;
		for (VDocIdType j = start; j < fin; ++j) {
			const size_t vdocId = offset + j;
			auto &vdoc = vdocs[vdocId];
			vdoc.wordsCount.resize(fieldscount, 0.0);
			vdoc.mostFreqWordCount.resize(fieldscount, 0.0);

			auto &vdocsText = vdocsTexts[j];
			for (size_t field = 0, sz = vdocsText.size(); field < sz; ++field) {
				split(vdocsText[field].first, str, wrds, extraWordSymbols);
				const int rfield = vdocsText[field].second;
				assertrx(rfield < fieldscount);

				vdoc.wordsCount[rfield] = wrds.size();

				int insertPos = -1;
				for (auto word : wrds) {
					++insertPos;
					const auto whash = h(word);
					if (!word.length() || cfg->stopWords.find(word, whash) != cfg->stopWords.end()) continue;

					auto [idxIt, emplaced] = ctx->words_um.try_emplace_prehashed(whash, word);
					(void)emplaced;
					const int mfcnt = idxIt->second.vids.Add(vdocId, insertPos, rfield);
					if (mfcnt > vdoc.mostFreqWordCount[rfield]) {
						vdoc.mostFreqWordCount[rfield] = mfcnt;
					}

					if (enableNumbersSearch && is_number(word)) {
						buildVirtualWord(word, ctx->words_um, vdocId, field, insertPos, virtualWords);
					}
				}
			}
		}
	};

	for (uint32_t t = 1; t < maxIndexWorkers; ++t) {
		ctxs[t].thread = runInThread(exwr, worker, t);
	}
	// If there was only 1 build thread. Just return it's build results
	worker(0);
	words_um = std::move(ctxs[0].words_um);
	// Merge results into single map
	for (uint32_t i = 1; i < maxIndexWorkers; ++i) {
		auto &ctx = ctxs[i];
		ctx.thread.join();

		if (exwr.HasException()) {
			continue;
		}
		for (auto &it : ctx.words_um) {
#if defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)
			const auto fBeforeMove = it.first;
			const auto sBeforeMove = it.second.MakeCopy();
			const auto sCapacityBeforeMove = it.second.vids.capacity();
#endif	// defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)
			auto [idxIt, emplaced] = words_um.try_emplace(std::move(it.first), std::move(it.second));
			if (!emplaced) {
#if defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)
				// Make sure, that try_emplace did not moved the values
				assertrx(it.first == fBeforeMove);
				assertrx(it.second.virtualWord == sBeforeMove.virtualWord);
				assertrx(it.second.vids.size() == sBeforeMove.vids.size());
				assertrx(it.second.vids.capacity() == sCapacityBeforeMove);
#endif	// defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)
				auto &resultVids = idxIt->second.vids;
				auto &newVids = it.second.vids;
				resultVids.insert(resultVids.end(), std::make_move_iterator(newVids.begin()), std::make_move_iterator(newVids.end()));
			}
		}
		bgThreads.Add([&ctx]() noexcept {
			try {
				words_map().swap(ctx.words_um);
				// NOLINTBEGIN(bugprone-empty-catch)
			} catch (...) {
			}
			// NOLINTEND(bugprone-empty-catch)
		});
	}
	exwr.RethrowException();

	bgThreads.Add([this]() noexcept { std::vector<RVector<std::pair<std::string_view, uint32_t>, 8>>().swap(holder_.vdocsTexts); });
	bgThreads.Add([this]() noexcept { std::vector<std::unique_ptr<std::string>>().swap(holder_.bufStrs_); });

	// Calculate avg words count per document for bm25 calculation
	if (vdocs.size()) {
		holder_.avgWordsCount_.resize(fieldscount, 0);
		for (int i = 0; i < fieldscount; i++) {
			auto &avgRef = holder_.avgWordsCount_[i];
			for (auto &vdoc : vdocs) avgRef += vdoc.wordsCount[i];
			avgRef /= vdocs.size();
		}
	}
	// Check and print potential stop words
	if (holder_.cfg_->logLevel >= LogInfo) {
		WrSerializer out;
		for (auto &w : words_um) {
			if (w.second.vids.size() > vdocs.size() / 5 || int64_t(w.second.vids.size()) > holder_.cfg_->mergeLimit) {
				out << w.first << "(" << w.second.vids.size() << ") ";
			}
		}
		logPrintf(LogInfo, "Total documents: %d. Potential stop words (with corresponding docs count): %s", vdocs.size(), out.Slice());
	}

	return szCnt;
}

template <typename IdCont>
void DataProcessor<IdCont>::buildVirtualWord(std::string_view word, words_map &words_um, VDocIdType docType, int rfield, size_t insertPos,
											 std::vector<std::string> &container) {
	auto &vdoc(holder_.vdocs_[docType]);
	NumToText::convert(word, container);
	for (std::string &numberWord : container) {
		WordEntry wentry;
		wentry.virtualWord = true;
		auto idxIt = words_um.emplace(std::move(numberWord), std::move(wentry)).first;
		const int mfcnt = idxIt->second.vids.Add(docType, insertPos, rfield);
		if (mfcnt > vdoc.mostFreqWordCount[rfield]) {
			vdoc.mostFreqWordCount[rfield] = mfcnt;
		}
		++vdoc.wordsCount[rfield];
		insertPos += kDigitUtfSizeof;
	}
}

template <typename IdCont>
void DataProcessor<IdCont>::buildTyposMap(uint32_t startPos, const WordsVector &preprocWords) {
	if (!holder_.cfg_->maxTypos) {
		return;
	}
	if (preprocWords.empty()) {
		return;
	}

	auto &typosHalf = holder_.GetTyposHalf();
	auto &typosMax = holder_.GetTyposMax();
	const auto wordsSize = preprocWords.size();
	const auto maxTypoLen = holder_.cfg_->maxTypoLen;
	const auto maxTyposInWord = holder_.cfg_->MaxTyposInWord();
	const auto halfMaxTypos = holder_.cfg_->maxTypos / 2;
	if (maxTyposInWord == halfMaxTypos) {
		assertrx_throw(maxTyposInWord > 0);
		typos_context tctx[kMaxTyposInWord];
		const auto multiplicator = wordsSize * (10 << (maxTyposInWord - 1));
		typosHalf.reserve(multiplicator / 2, multiplicator * 5);
		auto wordPos = startPos;

		for (auto &word : preprocWords) {
			const auto wordString = std::get_if<std::string_view>(&word);
			if (!wordString) {
				continue;
			}
			const auto wordId = holder_.BuildWordId(wordPos++);
			mktypos(tctx, *wordString, maxTyposInWord, maxTypoLen,
					typos_context::CallBack{[&typosHalf, wordId](std::string_view typo, int, const typos_context::TyposVec &positions) {
						typosHalf.emplace(typo, WordTypo{wordId, positions});
					}});
		}
	} else {
		assertrx_throw(maxTyposInWord == halfMaxTypos + 1);

		auto multiplicator = wordsSize * (10 << (halfMaxTypos > 1 ? (halfMaxTypos - 1) : 0));
		ExceptionPtrWrapper exwr;
		std::thread maxTyposTh = runInThread(
			exwr,
			[&](size_t mult) noexcept {
				typos_context tctx[kMaxTyposInWord];
				auto wordPos = startPos;
				mult = wordsSize * (10 << (maxTyposInWord - 1)) - mult;
				typosMax.reserve(multiplicator / 2, multiplicator * 5);
				for (auto &word : preprocWords) {
					const auto wordString = std::get_if<std::string_view>(&word);
					if (!wordString) {
						continue;
					}
					const auto wordId = holder_.BuildWordId(wordPos++);
					mktypos(tctx, *wordString, maxTyposInWord, maxTypoLen,
							typos_context::CallBack{[wordId, &typosMax, wordString](std::string_view typo, int level,
																					const typos_context::TyposVec &positions) {
								if (level <= 1 && typo.size() != wordString->size()) {
									typosMax.emplace(typo, WordTypo{wordId, positions});
								}
							}});
				}
				typosMax.shrink_to_fit();
			},
			multiplicator);

		try {
			auto wordPos = startPos;
			typos_context tctx[kMaxTyposInWord];
			typosHalf.reserve(multiplicator / 2, multiplicator * 5);
			for (auto &word : preprocWords) {
				const auto wordString = std::get_if<std::string_view>(&word);
				if (!wordString) {
					continue;
				}
				const auto wordId = holder_.BuildWordId(wordPos++);
				mktypos(tctx, *wordString, maxTyposInWord, maxTypoLen,
						typos_context::CallBack{
							[wordId, &typosHalf, wordString](std::string_view typo, int level, const typos_context::TyposVec &positions) {
								if (level > 1 || typo.size() == wordString->size()) {
									typosHalf.emplace(typo, WordTypo{wordId, positions});
								}
							}});
			}
		} catch (...) {
			exwr.SetException(std::current_exception());
		}
		maxTyposTh.join();
		exwr.RethrowException();
	}
	typosHalf.shrink_to_fit();
}

template <typename IdCont>
template <typename F, typename... Args>
std::thread DataProcessor<IdCont>::runInThread(ExceptionPtrWrapper &ew, F &&f, Args &&...args) noexcept {
	return std::thread(
		[fw = std::forward<F>(f), &ew](auto &&...largs) noexcept {
			try {
				fw(largs...);
			} catch (...) {
				ew.SetException(std::current_exception());
			}
		},
		std::forward<Args>(args)...);
}

template class DataProcessor<PackedIdRelVec>;
template class DataProcessor<IdRelVec>;

}  // namespace reindexer
