#include "dataprocessor.h"
#include <chrono>
#include <limits>
#include "core/ft/numtotext.h"
#include "core/ft/typos.h"

#include "tools/clock.h"
#include "tools/logger.h"
#include "tools/serilize/wrserializer.h"
#include "tools/stringstools.h"
#include "tools/thread_exception_wrapper.h"

using std::chrono::duration_cast;
using std::chrono::milliseconds;

namespace reindexer {

static constexpr size_t kMaxFtWordBytes = std::numeric_limits<suffix_map<char, WordIdType>::word_len_type>::max();

RX_ALWAYS_INLINE static bool IsFtWordIndexable(std::string_view word) noexcept { return !word.empty() && word.size() <= kMaxFtWordBytes; }

template <typename IdCont>
void DataProcessor<IdCont>::Process(VDocsTexts& vdocsTexts, const std::vector<uint32_t>& vdocsIds, size_t numDocsTotal, bool multithreaded,
									std::vector<h_vector<float, 3>>& wordsCounts) {
	ExceptionPtrWrapper exwr;
	words_map words_um;
	const auto tm0 = system_clock_w::now();
	size_t szCnt = buildWordsMap(vdocsTexts, vdocsIds, numDocsTotal, words_um, multithreaded, holder_.splitter_, wordsCounts);
	const auto tm1 = system_clock_w::now();
	auto& words = holder_.GetWords();
	const size_t wrdOffset = words.size();
	holder_.SetWordsOffset(wrdOffset);

	const auto preprocWords = insertIntoSuffix(words_um, holder_);
	const auto tm2 = system_clock_w::now();
	// Step 4: Commit suffixes array. It runs in parallel with next step
	auto& suffixes = holder_.GetLastStepSuffix();
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
		buildTyposMap(wrdOffset, preprocWords, multithreaded);
	} catch (...) {
		exwr.SetException(std::current_exception());
	}
	const auto tm5 = system_clock_w::now();

	// Step 7: Await threads
	idrelsetCommitThread.join();
	sufBuildThread.join();
	exwr.RethrowException();
	const auto tm6 = system_clock_w::now();

	logFmt(LogInfo, "IndexText[{}] built with [{} uniq words, {} typos, {}KB text size, {}KB suffixarray size, {}KB idrelsets size]",
		   holder_.steps.size(), words_um.size(), holder_.GetLastStepTypos().size(), szCnt / 1024, suffixes.heap_size() / 1024,
		   idsetcnt / 1024);

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
	auto& suffix = holder.GetLastStepSuffix();
	word_hash wh;

	suffix.reserve(words_um.size() * 20, words_um.size());
	holder.ReserveWords(words.size() + words_um.size());

	WordsVector found;
	found.reserve(words_um.size());

	for (auto& keyIt : words_um) {
		// if we still don't have that word, we add it to new suffix tree, otherwise we just add information to current word

		auto id = words.size();
		WordIdType pos = holder.FindWord(keyIt.first, false);

		if (!pos.IsEmpty()) {
			found.emplace_back(pos);
			continue;
		}
		found.emplace_back(keyIt.first);

		const auto charsLen = static_cast<typename DataHolder<IdCont>::WordCharsLenType>(getUTF8StringCharactersCount(keyIt.first));
		words.emplace_back();
		holder.AddWordCharsLen(charsLen);
		pos = holder.BuildWordId(id);
		suffix.insert(keyIt.first, pos);
		holder.lastStepWords_[wh(keyIt.first)].emplace_back(pos);
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
		PackedWordEntry<IdCont>* wordEntry = nullptr;
		if (preprocWordsSize > i) {
			if (auto widPtr = std::get_if<WordIdType>(&preprocWords[i]); widPtr) {
				assertrx_dbg(!widPtr->IsEmpty());
				wordEntry = &holder.GetWordEntry(*widPtr);
			}
		}
		if (!wordEntry) {
			wordEntry = &(*wIt);
			++wIt;
			idsetcnt += sizeof(*wIt);
		}

		if constexpr (std::is_same_v<IdCont, PackedIdRelVec>) {
			wordEntry->vids.insert_back(keyIt->second.begin(), keyIt->second.end());
		} else {
			wordEntry->vids.insert(wordEntry->vids.end(), std::make_move_iterator(keyIt->second.begin()),
								   std::make_move_iterator(keyIt->second.end()));
		}
		keyIt->second = IdRelSet();
		wordEntry->vids.shrink_to_fit();
		idsetcnt += wordEntry->vids.heap_size();
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
size_t DataProcessor<IdCont>::buildWordsMap(VDocsTexts& vdocsTexts, const std::vector<uint32_t>& vdocsIds, size_t numDocsTotal,
											words_map& words_um, bool multithreaded, intrusive_ptr<const ISplitter> textSplitter,
											std::vector<h_vector<float, 3>>& wordsCounts) {
	ExceptionPtrWrapper exwr;
	uint32_t maxIndexWorkers = getMaxBuildWorkers(multithreaded);
	size_t szCnt = 0;
	wordsCounts.resize(vdocsTexts.size());

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

	ThreadsContainer bgThreads;

	auto& cfg = holder_.cfg_;
	const size_t fieldscount = fieldSize_;
	// build words map parallel in maxIndexWorkers threads
	auto worker = [this, &ctxs, &vdocsTexts, &vdocsIds, fieldscount, &cfg, &wordsCounts, &textSplitter](int i) {
		auto ctx = &ctxs[i];
		std::vector<std::string_view> virtualWords;
		const size_t start = ctx->from;
		const size_t fin = ctx->to;
		const bool enableNumbersSearch = cfg->enableNumbersSearch;
		const word_hash h;
		std::string wordWithoutDelims;
		auto task = textSplitter->CreateTask();
		for (VDocIdType j = start; j < fin; ++j) {
			const size_t vdocId = vdocsIds[j];
			auto& vdocsText = vdocsTexts[j];
			wordsCounts[j].resize(0);
			wordsCounts[j].resize(fieldscount, 0.0);

			for (size_t idx = 0, arrayIdx = 0, sz = vdocsText.size(); idx < sz; ++idx, ++arrayIdx) {
				task->SetText(vdocsText[idx].first);
				const unsigned field = vdocsText[idx].second;
				if (idx > 0 && field != vdocsTexts[j][idx - 1].second) {
					arrayIdx = 0;
				}

				assertrx_throw(field < fieldscount);

				const std::vector<WordWithPos>& occurences = task->GetResults();

				for (const auto& occurence : occurences) {
					if (!IsFtWordIndexable(occurence.word)) {
						continue;
					}
					++wordsCounts[j][field];
					const auto whash = h(occurence.word);
					if (cfg->stopWords.find(occurence.word, whash) != cfg->stopWords.end()) {
						continue;
					}

					if (cfg->splitOptions.ContainsDelims(occurence.word)) {
						cfg->splitOptions.RemoveDelims(occurence.word, wordWithoutDelims);
						if (cfg->stopWords.find(wordWithoutDelims) != cfg->stopWords.end()) {
							continue;
						}
					}

					auto [idxIt, emplaced] = ctx->words_um.try_emplace_prehashed(whash, occurence.word);
					(void)emplaced;
					idxIt->second.Add(vdocId, occurence.pos, field, arrayIdx);

					if (enableNumbersSearch && is_number(occurence.word)) {
						buildVirtualWord(occurence.word, ctx->words_um, vdocId, wordsCounts[j], field, arrayIdx, occurence.pos,
										 virtualWords);
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
			auto [idxIt, emplaced] = words_um.try_emplace(std::move(it.first), std::move(it.second));
			if (!emplaced) {
				auto& resultVids = idxIt->second;
				auto& newVids = it.second;
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

	// Check and print potential stop words

	if (holder_.cfg_->logLevel >= LogInfo) {
		WrSerializer out;
		for (auto& w : words_um) {
			if ((numDocsTotal > 1000 && w.second.size() > numDocsTotal / 5) || int64_t(w.second.size()) > holder_.cfg_->mergeLimit) {
				out << w.first << "(" << w.second.size() << ") ";
			}
		}
		logFmt(LogInfo, "Total documents: {}. Potential stop words (with corresponding docs count): {}", numDocsTotal, out.Slice());
	}

	bgThreads.Add([&vdocsTexts]() noexcept { VDocsTexts().swap(vdocsTexts); });

	return szCnt;
}

template <typename IdCont>
void DataProcessor<IdCont>::buildVirtualWord(std::string_view word, words_map& words_um, VDocIdType vdocId,
											 h_vector<float, 3>& vdocWordsCounts, unsigned field, unsigned arrayIdx, size_t insertPos,
											 std::vector<std::string_view>& container) {
	std::ignore = NumToText::convert(word, container);
	for (const auto numberWord : container) {
		if (!IsFtWordIndexable(numberWord)) {
			continue;
		}
		auto idxIt = words_um.emplace(numberWord, IdRelSet()).first;
		idxIt->second.Add(vdocId, insertPos, field, arrayIdx);
		assertrx_dbg(vdocWordsCounts.size() > field);
		++vdocWordsCounts[field];
	}
}

template <typename IdCont>
void DataProcessor<IdCont>::buildTyposMap(uint32_t startPos, const WordsVector& preprocWords, bool multithread) {
	if (!holder_.cfg_->maxTypos || preprocWords.empty()) {
		return;
	}

	const size_t stepNum = holder_.steps.size() - 1;
	const auto maxTypoLen = holder_.cfg_->maxTypoLen;
	const auto maxTyposInWord = holder_.cfg_->MaxTyposInWord();
	assertrx_throw(maxTyposInWord > 0);

	std::vector<std::pair<uint32_t, WordTypo>> typosData;
	const size_t multiplier = preprocWords.size() * (10 << (maxTyposInWord - 1));

	const size_t numThreads = getMaxBuildWorkers(multithread);
	std::vector<std::vector<std::pair<uint32_t, WordTypo>>> threadsTyposDatas(numThreads);

	for (auto& td : threadsTyposDatas) {
		td.reserve(multiplier / numThreads + 1);
	}

	std::vector<std::thread> ths(numThreads);
	ExceptionPtrWrapper exwr;

	for (size_t threadIdx = 0; threadIdx < numThreads; ++threadIdx) {
		ths[threadIdx] =
			runInThread(exwr, [threadIdx, numThreads, &preprocWords, startPos, maxTypoLen, maxTyposInWord, &threadsTyposDatas, this]() {
				auto wordPos = startPos;
				std::wstring wordStringW, buf;
				std::vector<std::pair<uint32_t, WordTypo>>& typosData = threadsTyposDatas[threadIdx];

				for (const auto& word : preprocWords) {
					const auto wordString = std::get_if<std::string_view>(&word);
					if (!wordString) {
						continue;
					}

					if (wordPos % numThreads != threadIdx) {
						wordPos++;
						continue;
					}
					const auto wordId = holder_.BuildWordId(wordPos++);

					auto cb = [&typosData, wordId](std::wstring_view typo, const TyposVec& positions, std::wstring_view) {
						typosData.emplace_back(TyposMap::CalcHash(typo), WordTypo{wordId, positions});
					};

					utf8_to_utf16(*wordString, wordStringW);
					mktypos(wordStringW, maxTyposInWord, maxTypoLen, TyposCallBack{cb}, buf);
				}
			});
	}

	for (auto& th : ths) {
		th.join();
	}

	exwr.RethrowException();
	holder_.GetLastStepTypos().Build(threadsTyposDatas, stepNum, numThreads);
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
