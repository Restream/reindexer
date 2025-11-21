#include "dataprocessor.h"
#include <chrono>
#include "core/ft/numtotext.h"
#include "core/ft/typos.h"

#include "tools/clock.h"
#include "tools/logger.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "tools/thread_exception_wrapper.h"

using std::chrono::duration_cast;
using std::chrono::milliseconds;

namespace reindexer {

template <typename IdCont>
void DataProcessor<IdCont>::Process(bool multithread) {
	ExceptionPtrWrapper exwr;
	words_map words_um;
	const auto tm0 = system_clock_w::now();
	size_t szCnt = buildWordsMap(words_um, multithread, holder_.splitter_);
	const auto tm1 = system_clock_w::now();
	auto& words = holder_.GetWords();
	const size_t wrdOffset = words.size();
	holder_.SetWordsOffset(wrdOffset);

	const auto preprocWords = insertIntoSuffix(words_um, holder_);
	const auto tm2 = system_clock_w::now();
	// Step 4: Commit suffixes array. It runs in parallel with next step
	auto& suffixes = holder_.GetSuffix();
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

	logFmt(LogInfo,
		   "FastIndexText[{}] built with [{} uniq words, {} typos ({} + {}), {}KB text size, {}KB suffixarray size, {}KB idrelsets size]",
		   holder_.steps.size(), words_um.size(), holder_.GetTyposHalf().size() + holder_.GetTyposMax().size(),
		   holder_.GetTyposHalf().size(), holder_.GetTyposMax().size(), szCnt / 1024, suffixes.heap_size() / 1024, idsetcnt / 1024);

	logFmt(LogInfo,
		   "DataProcessor::Process elapsed {} ms total [ build words {} ms | suffixes preproc {} ms | build typos {} ms | build "
		   "suffixarry {} ms | sort idrelsets {} ms]",
		   duration_cast<milliseconds>(tm6 - tm0).count(), duration_cast<milliseconds>(tm1 - tm0).count(),
		   duration_cast<milliseconds>(tm2 - tm1).count(), duration_cast<milliseconds>(tm5 - tm2).count(),
		   duration_cast<milliseconds>(tm3 - tm2).count(), duration_cast<milliseconds>(tm4 - tm2).count());
}

template <typename IdCont>
typename DataProcessor<IdCont>::WordsVector DataProcessor<IdCont>::insertIntoSuffix(words_map& words_um, DataHolder<IdCont>& holder) {
	auto& words = holder.GetWords();
	auto& suffix = holder.GetSuffix();

	suffix.reserve(words_um.size() * 20, words_um.size());

	WordsVector found;
	found.reserve(words_um.size());

	for (auto& keyIt : words_um) {
		// if we still don't have that word, we add it to new suffix tree, otherwise we just add information to current word

		auto id = words.size();
		WordIdType pos = holder.findWord(keyIt.first);

		if (!pos.IsEmpty()) {
			found.emplace_back(pos);
			continue;
		}
		found.emplace_back(keyIt.first);

		words.emplace_back();
		pos = holder.BuildWordId(id);
		suffix.insert(keyIt.first, pos);
	}
	return found;
}

template <typename IdCont>
size_t DataProcessor<IdCont>::commitIdRelSets(const WordsVector& preprocWords, words_map& words_um, DataHolder<IdCont>& holder,
											  size_t wrdOffset) {
	size_t idsetcnt = 0;
	auto wIt = holder.GetWords().begin() + wrdOffset;
	uint32_t i = 0;
	auto preprocWordsSize = preprocWords.size();
	for (auto keyIt = words_um.begin(), endIt = words_um.end(); keyIt != endIt; ++keyIt, ++i) {
		// Pack idrelset
		PackedWordEntry<IdCont>* word = nullptr;
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

		if constexpr (std::is_same_v<IdCont, PackedIdRelVec>) {
			word->vids.insert_back(keyIt->second.vids_.begin(), keyIt->second.vids_.end());
		} else {
			word->vids.insert(word->vids.end(), std::make_move_iterator(keyIt->second.vids_.begin()),
							  std::make_move_iterator(keyIt->second.vids_.end()));
		}
		keyIt->second.vids_ = IdRelSet();
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
void makeDocsDistribution(ContextT* ctxs, size_t ctxsCount, size_t docs) {
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
size_t DataProcessor<IdCont>::buildWordsMap(words_map& words_um, bool multithread, intrusive_ptr<const ISplitter> textSplitter) {
	ExceptionPtrWrapper exwr;
	uint32_t maxIndexWorkers = getMaxBuildWorkers(multithread);
	size_t szCnt = 0;
	auto& vdocsTexts = holder_.vdocsTexts;
	struct [[nodiscard]] context {
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
			std::cerr << fmt::format("{}: {{ from: {}; to: {} }}", i, ctxs[i].from, ctxs[i].to) << std::endl;
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

	auto& cfg = holder_.cfg_;
	auto& vdocs = holder_.vdocs_;
	const size_t fieldscount = fieldSize_;
	size_t offset = holder_.vdocsOffset_;
	// build words map parallel in maxIndexWorkers threads
	auto worker = [this, &ctxs, &vdocsTexts, offset, fieldscount, &cfg, &vdocs, &textSplitter](int i) {
		auto ctx = &ctxs[i];
		std::vector<std::string_view> virtualWords;
		const size_t start = ctx->from;
		const size_t fin = ctx->to;
		const bool enableNumbersSearch = cfg->enableNumbersSearch;
		const word_hash h;
		std::string wordWithoutDelims;
		auto task = textSplitter->CreateTask();
		for (VDocIdType j = start; j < fin; ++j) {
			const size_t vdocId = offset + j;
			auto& vdoc = vdocs[vdocId];
			vdoc.wordsCount.resize(fieldscount, 0.0);
			vdoc.mostFreqWordCount.resize(fieldscount, 0.0);

			auto& vdocsText = vdocsTexts[j];
			for (size_t idx = 0, arrayIdx = 0, sz = vdocsText.size(); idx < sz; ++idx, ++arrayIdx) {
				task->SetText(vdocsText[idx].first);
				const unsigned field = vdocsText[idx].second;
				if (idx > 0 && field != vdocsTexts[j][idx - 1].second) {
					arrayIdx = 0;
				}

				assertrx_throw(field < fieldscount);

				const std::vector<WordWithPos>& entrances = task->GetResults();
				vdoc.wordsCount[field] = entrances.size();

				for (const auto& e : entrances) {
					const auto whash = h(e.word);
					if (e.word.empty() || cfg->stopWords.find(e.word, whash) != cfg->stopWords.end()) {
						continue;
					}

					if (cfg->splitOptions.ContainsDelims(e.word)) {
						cfg->splitOptions.RemoveDelims(e.word, wordWithoutDelims);
						if (cfg->stopWords.find(wordWithoutDelims) != cfg->stopWords.end()) {
							continue;
						}
					}

					auto [idxIt, emplaced] = ctx->words_um.try_emplace_prehashed(whash, e.word);
					(void)emplaced;
					const int mfcnt = idxIt->second.vids_.Add(vdocId, e.pos, field, arrayIdx);
					if (mfcnt > vdoc.mostFreqWordCount[field]) {
						vdoc.mostFreqWordCount[field] = mfcnt;
					}

					if (enableNumbersSearch && is_number(e.word)) {
						buildVirtualWord(e.word, ctx->words_um, vdocId, field, arrayIdx, e.pos, virtualWords);
					}
				}
			}
		}
	};

	for (uint32_t t = 1; t < maxIndexWorkers; ++t) {
		ctxs[t].thread = runInThread(exwr, worker, t);
	}
	// If there was only 1 build thread. Just return build results
	worker(0);
	words_um = std::move(ctxs[0].words_um);
	// Merge results into single map
	for (uint32_t i = 1; i < maxIndexWorkers; ++i) {
		auto& ctx = ctxs[i];
		ctx.thread.join();

		if (exwr.HasException()) {
			continue;
		}
		for (auto& it : ctx.words_um) {
#if (defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)) && !defined(NDEBUG)
			const auto fBeforeMove = it.first;
			const auto sBeforeMove = it.second.MakeCopy();
			const auto sCapacityBeforeMove = it.second.vids_.capacity();
#endif	// (defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)) && !defined(NDEBUG)
			auto [idxIt, emplaced] = words_um.try_emplace(std::move(it.first), std::move(it.second));
			if (!emplaced) {
#if (defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)) && !defined(NDEBUG)
				// Make sure, that try_emplace did not moved the values
				assertrx(it.first == fBeforeMove);
				assertrx(it.second.vids_.size() == sBeforeMove.vids_.size());
				assertrx(it.second.vids_.capacity() == sCapacityBeforeMove);
#endif	// (defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)) && !defined(NDEBUG)
				auto& resultVids = idxIt->second.vids_;
				auto& newVids = it.second.vids_;
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

	bgThreads.Add([this]() noexcept { std::vector<h_vector<std::pair<std::string_view, uint32_t>, 8>>().swap(holder_.vdocsTexts); });
	bgThreads.Add([this]() noexcept { std::vector<std::unique_ptr<std::string>>().swap(holder_.bufStrs_); });

	// Calculate avg words count per document for bm25 calculation
	if (vdocs.size()) {
		holder_.avgWordsCount_.resize(fieldscount, 0);
		for (unsigned i = 0; i < fieldscount; i++) {
			auto& avgRef = holder_.avgWordsCount_[i];
			for (auto& vdoc : vdocs) {
				avgRef += vdoc.wordsCount[i];
			}
			avgRef /= vdocs.size();
		}
	}
	// Check and print potential stop words
	if (holder_.cfg_->logLevel >= LogInfo) {
		WrSerializer out;
		for (auto& w : words_um) {
			if (w.second.vids_.size() > vdocs.size() / 5 || int64_t(w.second.vids_.size()) > holder_.cfg_->mergeLimit) {
				out << w.first << "(" << w.second.vids_.size() << ") ";
			}
		}
		logFmt(LogInfo, "Total documents: {}. Potential stop words (with corresponding docs count): {}", vdocs.size(), out.Slice());
	}

	return szCnt;
}

template <typename IdCont>
void DataProcessor<IdCont>::buildVirtualWord(std::string_view word, words_map& words_um, VDocIdType docType, unsigned field,
											 unsigned arrayIdx, size_t insertPos, std::vector<std::string_view>& container) {
	auto& vdoc(holder_.vdocs_[docType]);
	std::ignore = NumToText::convert(word, container);
	for (const auto numberWord : container) {
		WordEntry wentry;
		auto idxIt = words_um.emplace(numberWord, std::move(wentry)).first;
		const int mfcnt = idxIt->second.vids_.Add(docType, insertPos, field, arrayIdx);
		assertrx_throw(vdoc.mostFreqWordCount.size() > field);
		assertrx_throw(vdoc.wordsCount.size() > field);
		if (mfcnt > vdoc.mostFreqWordCount[field]) {
			vdoc.mostFreqWordCount[field] = mfcnt;
		}
		++vdoc.wordsCount[field];
	}
}

template <typename IdCont>
void DataProcessor<IdCont>::buildTyposMap(uint32_t startPos, const WordsVector& preprocWords) {
	if (!holder_.cfg_->maxTypos) {
		return;
	}
	if (preprocWords.empty()) {
		return;
	}

	auto& typosHalf = holder_.GetTyposHalf();
	auto& typosMax = holder_.GetTyposMax();
	const auto wordsSize = preprocWords.size();
	const auto maxTypoLen = holder_.cfg_->maxTypoLen;
	const auto maxTyposInWord = holder_.cfg_->MaxTyposInWord();
	const auto halfMaxTypos = holder_.cfg_->maxTypos / 2;
	if (maxTyposInWord == halfMaxTypos) {
		assertrx_throw(maxTyposInWord > 0);
		typos_context tctx[kMaxTyposInWord];
		const auto multiplier = wordsSize * (10 << (maxTyposInWord - 1));
		typosHalf.reserve(multiplier / 2, multiplier * 5);
		auto wordPos = startPos;

		for (auto& word : preprocWords) {
			const auto wordString = std::get_if<std::string_view>(&word);
			if (!wordString) {
				continue;
			}
			const auto wordId = holder_.BuildWordId(wordPos++);
			mktypos(tctx, *wordString, maxTyposInWord, maxTypoLen,
					typos_context::CallBack{
						[&typosHalf, wordId](std::string_view typo, int, const typos_context::TyposVec& positions,
											 const std::wstring_view) { typosHalf.emplace(typo, WordTypo{wordId, positions}); }});
		}
	} else {
		assertrx_throw(maxTyposInWord == halfMaxTypos + 1);

		auto multiplier = wordsSize * (10 << (halfMaxTypos > 1 ? (halfMaxTypos - 1) : 0));
		ExceptionPtrWrapper exwr;
		std::thread maxTyposTh = runInThread(
			exwr,
			[&](size_t mult) noexcept {
				try {
					typos_context tctx[kMaxTyposInWord];
					auto wordPos = startPos;
					mult = wordsSize * (10 << (maxTyposInWord - 1)) - mult;
					typosMax.reserve(multiplier / 2, multiplier * 5);
					for (auto& word : preprocWords) {
						const auto wordString = std::get_if<std::string_view>(&word);
						if (!wordString) {
							continue;
						}
						const auto wordId = holder_.BuildWordId(wordPos++);

						struct {
							reindexer::WordIdType wordId_;
							decltype(typosMax)& typosMax_;
							size_t wordStringSize_;
						} callbackCtx{wordId, typosMax, wordString->size()};

						mktypos(tctx, *wordString, maxTyposInWord, maxTypoLen,
								typos_context::CallBack{[&callbackCtx](std::string_view typo, int level,
																	   const typos_context::TyposVec& positions, const std::wstring_view) {
									if (level <= 1 && typo.size() != callbackCtx.wordStringSize_) {
										callbackCtx.typosMax_.emplace(typo, WordTypo{callbackCtx.wordId_, positions});
									}
								}});
					}
					typosMax.shrink_to_fit();
				} catch (...) {
					exwr.SetException(std::current_exception());
				}
			},
			multiplier);

		try {
			auto wordPos = startPos;
			typos_context tctx[kMaxTyposInWord];
			typosHalf.reserve(multiplier / 2, multiplier * 5);
			for (auto& word : preprocWords) {
				const auto wordString = std::get_if<std::string_view>(&word);
				if (!wordString) {
					continue;
				}
				const auto wordId = holder_.BuildWordId(wordPos++);

				struct {
					reindexer::WordIdType wordId_;
					decltype(typosHalf)& typosHalf_;
					size_t wordStringSize_;
				} callbackCtx{wordId, typosHalf, wordString->size()};

				mktypos(tctx, *wordString, maxTyposInWord, maxTypoLen,
						typos_context::CallBack{[&callbackCtx](std::string_view typo, int level, const typos_context::TyposVec& positions,
															   const std::wstring_view) {
							if (level > 1 || typo.size() == callbackCtx.wordStringSize_) {
								callbackCtx.typosHalf_.emplace(typo, WordTypo{callbackCtx.wordId_, positions});
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
std::thread DataProcessor<IdCont>::runInThread(ExceptionPtrWrapper& ew, F&& f, Args&&... args) noexcept {
	return std::thread(
		[fw = std::forward<F>(f), &ew](auto&&... largs) noexcept {
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
