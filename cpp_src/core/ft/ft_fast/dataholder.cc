#include "dataholder.h"
#include <algorithm>
#include <array>
#include <chrono>
#include <exception>
#include <future>
#include <tuple>
#include <type_traits>
#include "core/ft/ft_fast/frisosplitter.h"
#include "core/ft/limits.h"
#include "core/ft/numtotext.h"
#include "sort/pdqsort.hpp"
#include "tools/clock.h"
#include "tools/logger.h"
#include "tools/scope_guard.h"
#include "tools/serilize/wrserializer.h"
#include "tools/stringstools.h"
#include "tools/thread_exception_wrapper.h"

namespace reindexer {

namespace {

inline bool IsFtWordIndexable(std::string_view word) noexcept { return !word.empty() && word.size() <= kMaxFtWordLen; }
constexpr size_t kMinTypoWordLen = 3;

template <typename T>
void waitForTasksOnException(std::vector<std::future<T>>& tasks) noexcept {
	for (auto& task : tasks) {
		if (task.valid()) {
			try {
				if constexpr (std::is_void_v<T>) {
					task.get();
				} else {
					std::ignore = task.get();
				}
				// NOLINTBEGIN(bugprone-empty-catch) // Exceptions are rethrown by the original call path.
			} catch (...) {
			}
			// NOLINTEND(bugprone-empty-catch)
		}
	}
}

void awaitVoidTasks(std::vector<std::future<void>>& tasks, ExceptionPtrWrapper& exwr) {
	for (auto& task : tasks) {
		try {
			task.get();
		} catch (...) {
			exwr.SetException(std::current_exception());
		}
	}
}

void InsertSuffixKeys(SuffixTree& tree, std::vector<SuffixKey>& suffixes, const WordsStorage& words) {
	if (suffixes.empty()) {
		return;
	}
	if (tree.empty()) {
		boost::sort::pdqsort(suffixes.begin(), suffixes.end(), SuffixKeyCompare(words));
		auto hint = tree.end();
		for (const SuffixKey suffix : suffixes) {
			hint = tree.insert(hint, suffix);
		}
	} else {
		for (const SuffixKey suffix : suffixes) {
			tree.insert(suffix);
		}
	}
}

}  // namespace

template <typename IdCont>
DataHolder<IdCont>::DataHolder(FTConfig* c) {
	cfg_ = c;
	if (cfg_->splitterType == FTConfig::Splitter::Fast) {
		splitter_ = make_intrusive<FastTextSplitter>(cfg_->splitOptions);
	} else if (cfg_->splitterType == FTConfig::Splitter::MMSegCN) {
		splitter_ = make_intrusive<FrisoTextSplitter>();
	} else {
		assertrx_throw(false);
	}
}

size_t IDataHolder::GetMemStat() {
	size_t res =
		words_.heap_size() + suffixes_.capacity() * sizeof(decltype(suffixes_)::value_type) + wordIds_.capacity() * sizeof(WordIdType);

	res += wordsMap_.allocated_mem_size() + wordsMapStringsHeapSize_;

	for (const auto& suffixTree : suffixes_) {
		if (suffixTree) {
			res += sizeof(*suffixTree) + suffixTree->bytes_used();
		}
	}

	res += typos_.capacity() * sizeof(decltype(typos_)::value_type);
	for (const auto& typoSet : typos_) {
		if (typoSet) {
			res += sizeof(*typoSet) + typoSet->allocated_mem_size();
			for (const auto& typoGroup : *typoSet) {
				res += typoGroup.heap_size();
			}
		}
	}

	res += stemmedTermsBoost.allocated_mem_size();
	for (const auto& [term, boost] : stemmedTermsBoost) {
		(void)boost;
		res += term.capacity() * sizeof(typename std::string::value_type);
	}

	for (const auto& [lang, stemmerObj] : stemmers_) {
		(void)stemmerObj;
		res += lang.capacity() * sizeof(typename std::string::value_type);
	}
	if (translit_) {
		res += sizeof(*translit_);
	}
	if (kbLayout_) {
		res += sizeof(*kbLayout_);
	}
	if (synonyms_) {
		res += sizeof(*synonyms_) + synonyms_->heap_size();
	}

	return res;
}

template <typename IdCont>
size_t DataHolder<IdCont>::GetMemStat() {
	size_t res = IDataHolder::GetMemStat();
	res += wordOccurences_.capacity() * sizeof(IdCont) + wordOccurencesHeapSize_;
	return res;
}

template <typename IdCont>
void DataHolder<IdCont>::Clear() {
	words_.Clear();
	wordsMap_.clear();
	wordsMapStringsHeapSize_ = 0;
	for (auto& suffixTree : suffixes_) {
		suffixTree.reset();
	}
	for (auto& typoSet : typos_) {
		typoSet.reset();
	}
	wordIds_.clear();
	wordsProcessed_ = 0;
	wordOccurences_.clear();
	wordOccurencesHeapSize_ = 0;
	needRebuild_ = true;
}

// See TypoSet contract in typosholder.h: append-only; hash/equal use first key only.
static void AddTypoKey(TypoSet& typoSet, TypoKey key) {
	TypoKeyGroup group;
	AppendTypoKey(group, key);
	auto [it, inserted] = typoSet.insert(std::move(group));
	if (!inserted) {
		AppendTypoKey(*it, key);
	}
}

template <typename IdCont>
void DataHolder<IdCont>::collectSuffixes(size_t start, size_t end, std::vector<std::vector<SuffixKey>>& suffixesByFirstCh) const {
	for (size_t wordIdx = start; wordIdx < end; ++wordIdx) {
		const auto wordId = wordIds_[wordIdx];
		const auto word = words_.GetWord(wordId);
		for (uint32_t offset = 0; offset < word.size(); ++offset) {
			suffixesByFirstCh[SuffixTreeIndex(word[offset])].emplace_back(PackSuffixKey(wordId, offset));
		}
	}
}

template <typename IdCont>
TypoSet& DataHolder<IdCont>::getOrCreateTypoSet(char16_t firstCh) {
	auto& typoSet = typos_[TypoSetIndex(firstCh)];
	if (!typoSet) {
		typoSet = std::make_unique<TypoSet>(0, TypoKeyHash(words_), TypoKeyEqual(words_));
	}
	return *typoSet;
}

template <typename IdCont>
bool DataHolder<IdCont>::addExactAndSingleMissingTypos(WordIdType wordId, std::u16string_view word, size_t maxTyposInWord,
													   std::vector<std::vector<TypoKey>>& packedKeysByFirstCh) {
	assertrx(!word.empty());
	if (word.length() > cfg_->maxTypoLen || word.length() > kMaxTypoWordLen) {
		return false;
	}

	packedKeysByFirstCh[TypoSetIndex(word.front())].emplace_back(PackTypoKey(wordId, TyposVec()));
	if (maxTyposInWord == 0 || word.length() < kMinTypoWordLen) {
		return false;
	}

	for (size_t posMissing = 0; posMissing < word.length(); ++posMissing) {
		const auto firstCh = (posMissing == 0) ? word[1] : word[0];
		packedKeysByFirstCh[TypoSetIndex(firstCh)].emplace_back(PackTypoKey(wordId, TyposVec(uint8_t(posMissing))));
	}
	return maxTyposInWord > 1 && word.length() > kMinTypoWordLen;
}

template <typename IdCont>
void DataHolder<IdCont>::addTwoMissingTyposWithFirstMissing(WordIdType wordId, std::u16string_view word,
															std::vector<std::vector<TypoKey>>& packedKeysByFirstCh) {
	for (size_t secondMissingPos = 1; secondMissingPos < word.length(); ++secondMissingPos) {
		const auto firstCh = (secondMissingPos == 1) ? word[2] : word[1];
		packedKeysByFirstCh[TypoSetIndex(firstCh)].emplace_back(PackTypoKey(wordId, TyposVec(0, uint8_t(secondMissingPos))));
	}
}

template <typename IdCont>
void DataHolder<IdCont>::addTwoMissingTyposWithoutFirstMissing(WordIdType wordId, std::u16string_view word, TypoSet& typoSet) {
	for (size_t firstMissingPos = 1; firstMissingPos + 1 < word.length(); ++firstMissingPos) {
		for (size_t secondMissingPos = firstMissingPos + 1; secondMissingPos < word.length(); ++secondMissingPos) {
			AddTypoKey(typoSet, PackTypoKey(wordId, TyposVec(uint8_t(firstMissingPos), uint8_t(secondMissingPos))));
		}
	}
}

template <typename IdCont>
void DataHolder<IdCont>::fillTypoSetShard(TypoSet& typoSet, const std::vector<TypoKey>& packedKeys,
										  const std::vector<size_t>& wordIndexes) {
	for (const TypoKey key : packedKeys) {
		AddTypoKey(typoSet, key);
	}
	for (const size_t wordIdx : wordIndexes) {
		const auto wordId = wordIds_[wordIdx];
		const auto word = words_.GetWord(wordId);
		addTwoMissingTyposWithoutFirstMissing(wordId, word, typoSet);
	}
}

template <typename IdCont>
template <bool Multithreaded>
void DataHolder<IdCont>::processNewSuffixes(size_t start, size_t end) {
	if (start == end) {
		return;
	}

	std::vector<std::vector<SuffixKey>> suffixesByFirstCh(kSuffixTreesCount);
	collectSuffixes(start, end, suffixesByFirstCh);

	ExceptionPtrWrapper exwr;
	std::vector<std::future<void>> tasks;
	if constexpr (Multithreaded) {
		tasks.reserve(kSuffixTreesCount);
	}
	auto tasksWaiter = MakeScopeGuard([&tasks] {
		if constexpr (Multithreaded) {
			waitForTasksOnException(tasks);
		} else {
			(void)tasks;
		}
	});
	if constexpr (!Multithreaded) {
		tasksWaiter.Disable();
	}

	for (size_t firstCh = 0; firstCh < suffixesByFirstCh.size(); ++firstCh) {
		auto& suffixes = suffixesByFirstCh[firstCh];
		if (suffixes.empty()) {
			continue;
		}
		auto& suffixTree = suffixes_[firstCh];
		if (!suffixTree) {
			suffixTree = std::make_unique<SuffixTree>(SuffixKeyCompare(words_));
		}
		if constexpr (Multithreaded) {
			tasks.emplace_back(threadPool_.submit_task(
				[suffixTree = suffixTree.get(), &suffixes, &words = words_] { InsertSuffixKeys(*suffixTree, suffixes, words); }));
		} else {
			InsertSuffixKeys(*suffixTree, suffixes, words_);
		}
	}

	if constexpr (Multithreaded) {
		awaitVoidTasks(tasks, exwr);
		tasksWaiter.Disable();
		exwr.RethrowException();
	}
}

template <typename IdCont>
template <bool Multithreaded>
void DataHolder<IdCont>::processNewTypos(size_t start, size_t end) {
	if (start == end) {
		return;
	}

	const size_t maxTyposInWord = cfg_->MaxTyposInWord();
	if (maxTyposInWord > 2) [[unlikely]] {
		throw Error(errLogic, "Unexpected maxTyposInWord value for processNewTypos(): {}", maxTyposInWord);
	}

	std::vector<std::vector<TypoKey>> packedKeysByFirstCh(kTypoSetsCount);
	std::vector<std::vector<size_t>> twoMissingLettersWordsByFirstCh(kTypoSetsCount);
	for (size_t wordIdx = start; wordIdx < end; ++wordIdx) {
		const auto wordId = wordIds_[wordIdx];
		const auto word = words_.GetWord(wordId);
		if (addExactAndSingleMissingTypos(wordId, word, maxTyposInWord, packedKeysByFirstCh)) {
			addTwoMissingTyposWithFirstMissing(wordId, word, packedKeysByFirstCh);
			twoMissingLettersWordsByFirstCh[TypoSetIndex(word[0])].emplace_back(wordIdx);
		}
	}

	ExceptionPtrWrapper exwr;
	std::vector<std::future<void>> tasks;
	if constexpr (Multithreaded) {
		tasks.reserve(kTypoSetsCount);
	}
	auto tasksWaiter = MakeScopeGuard([&tasks] {
		if constexpr (Multithreaded) {
			waitForTasksOnException(tasks);
		} else {
			(void)tasks;
		}
	});
	if constexpr (!Multithreaded) {
		tasksWaiter.Disable();
	}

	for (size_t firstCh = 0; firstCh < kTypoSetsCount; ++firstCh) {
		const auto& packedKeys = packedKeysByFirstCh[firstCh];
		const auto& wordIndexes = twoMissingLettersWordsByFirstCh[firstCh];
		if (packedKeys.empty() && wordIndexes.empty()) {
			continue;
		}
		auto& typoSet = getOrCreateTypoSet(char16_t(firstCh));
		if constexpr (Multithreaded) {
			tasks.emplace_back(threadPool_.submit_task(
				[this, typoSet = &typoSet, &packedKeys, &wordIndexes] { fillTypoSetShard(*typoSet, packedKeys, wordIndexes); }));
		} else {
			fillTypoSetShard(typoSet, packedKeys, wordIndexes);
		}
	}

	if constexpr (Multithreaded) {
		awaitVoidTasks(tasks, exwr);
		tasksWaiter.Disable();
		exwr.RethrowException();
	}
}

static constexpr size_t kMinBuildTaskBytes = 1024;

static size_t getVdocTextSize(const VDocsTexts::value_type& vdoc) noexcept {
	size_t size = 0;
	for (const auto& [text, field] : vdoc) {
		(void)field;
		size += text.size();
	}
	return size;
}

static std::vector<std::pair<size_t, size_t>> makeBuildTasksByTextSize(const VDocsTexts& vdocsTexts, size_t threadCount) {
	std::vector<std::pair<size_t, size_t>> tasks;
	const size_t docsCount = vdocsTexts.size();
	if (docsCount == 0) {
		tasks.emplace_back(0, 0);
		return tasks;
	}

	size_t totalSize = 0;
	for (const auto& vdoc : vdocsTexts) {
		totalSize += getVdocTextSize(vdoc);
	}

	const size_t targetChunkSize = (totalSize + threadCount - 1) / threadCount;
	tasks.reserve(threadCount);
	size_t chunkFrom = 0;
	size_t chunkSize = 0;
	for (size_t docIdx = 0; docIdx < docsCount; ++docIdx) {
		chunkSize += getVdocTextSize(vdocsTexts[docIdx]);
		if (chunkSize >= targetChunkSize && chunkSize >= kMinBuildTaskBytes) {
			tasks.emplace_back(chunkFrom, docIdx + 1);
			chunkFrom = docIdx + 1;
			chunkSize = 0;
		}
	}

	if (chunkSize > 0) {
		tasks.emplace_back(chunkFrom, docsCount);
	}

	return tasks;
}

void NewWordsOccurences::appendOccurence(VDocIdType docId, unsigned pos, unsigned field, unsigned arrayIdx, words_map_t::iterator wordIt) {
	const uint32_t newIdx = occurrences_.size();
	if (wordIt->second.first == kInvalidLink) {
		wordIt->second = {newIdx, newIdx};
	} else {
		occurrences_[wordIt->second.second].link = newIdx;
		wordIt->second.second = newIdx;
	}
	occurrences_.emplace_back(Occurence{docId, kInvalidLink, PosType(pos, field, arrayIdx)});
}

void NewWordsOccurences::AddPrehashed(std::string_view word, size_t whash, VDocIdType docId, unsigned pos, unsigned field,
									  unsigned arrayIdx) {
	auto [wordIt, emplaced] = words_.try_emplace_prehashed(whash, word, WordIndices{kInvalidLink, kInvalidLink});
	appendOccurence(docId, pos, field, arrayIdx, wordIt);
	(void)emplaced;
}

void NewWordsOccurences::Add(std::string word, VDocIdType docId, unsigned pos, unsigned field, unsigned arrayIdx) {
	auto [wordIt, emplaced] = words_.try_emplace(std::move(word), WordIndices{kInvalidLink, kInvalidLink});
	appendOccurence(docId, pos, field, arrayIdx, wordIt);
	(void)emplaced;
}

template <typename IdCont>
size_t DataHolder<IdCont>::appendOccurenceChain(const std::vector<Occurence>& occurrences, uint32_t firstIdx, IdRelSet& chain,
												IdCont& dst) {
	chain.resize(0);
	chain.min_id_ = INT_MAX;
	chain.max_id_ = 0;

	uint32_t idx = firstIdx;
	while (idx != NewWordsOccurences::kInvalidLink) {
		const auto& occ = occurrences[idx];
		chain.Add(occ.docId, occ.pos.pos(), occ.pos.field(), occ.pos.arrayIdx());
		idx = occ.link;
	}
	const size_t heapBefore = dst.heap_size();
	if constexpr (std::is_same_v<IdCont, PackedIdRelVec>) {
		dst.insert_back(chain.begin(), chain.end());
	} else {
		dst.insert(dst.end(), chain.begin(), chain.end());
	}
	return dst.heap_size() - heapBefore;
}

template <typename IdCont>
NewWordsOccurencesPtr DataHolder<IdCont>::buildWordsMap(VDocsTexts::iterator textsBegin, std::vector<uint32_t>::const_iterator idsBegin,
														std::vector<h_vector<float, 3>>::iterator wordsCountsBegin, size_t numDocs,
														size_t numFields, std::atomic<size_t>* tooLongWordsSkipped) {
	auto nwo = std::make_shared<NewWordsOccurences>();
	std::vector<std::string_view> virtualWords;
	const word_hash h;
	std::string wordWithoutDelims;
	auto task = splitter_->CreateTask();
	size_t tooLongWordsSkippedLocal = 0;

	auto textsIt = textsBegin;
	auto idsIt = idsBegin;
	auto wordsCountsIt = wordsCountsBegin;
	for (size_t i = 0; i < numDocs; ++i, ++textsIt, ++idsIt, ++wordsCountsIt) {
		const size_t vdocId = *idsIt;
		auto& vdocsText = *textsIt;
		wordsCountsIt->resize(0);
		wordsCountsIt->resize(numFields, 0.0);

		for (size_t idx = 0, arrayIdx = 0, sz = vdocsText.size(); idx < sz; ++idx, ++arrayIdx) {
			task->SetText(vdocsText[idx].first);
			const unsigned field = vdocsText[idx].second;
			if (idx > 0 && field != vdocsText[idx - 1].second) {
				arrayIdx = 0;
			}

			assertrx_throw(field < numFields);

			const std::vector<WordWithPos>& occurences = task->GetResults();

			for (const auto& occurence : occurences) {
				if (!IsFtWordIndexable(occurence.word)) {
					++tooLongWordsSkippedLocal;
					continue;
				}
				++(*wordsCountsIt)[field];

				const auto whash = h(occurence.word);
				if (cfg_->stopWords.find(occurence.word, whash) != cfg_->stopWords.end()) {
					continue;
				}

				if (cfg_->splitOptions.ContainsDelims(occurence.word)) {
					cfg_->splitOptions.RemoveDelims(occurence.word, wordWithoutDelims);
					if (cfg_->stopWords.find(wordWithoutDelims) != cfg_->stopWords.end()) {
						continue;
					}
				}

				nwo->AddPrehashed(occurence.word, whash, vdocId, occurence.pos, field, arrayIdx);

				if (cfg_->enableNumbersSearch && is_number(occurence.word)) {
					std::ignore = NumToText::convert(occurence.word, virtualWords);
					for (const auto numberWord : virtualWords) {
						if (!IsFtWordIndexable(numberWord)) {
							++tooLongWordsSkippedLocal;
							continue;
						}
						nwo->Add(std::string(numberWord), vdocId, occurence.pos, field, arrayIdx);
						assertrx_dbg(wordsCountsIt->size() > field);
						++(*wordsCountsIt)[field];
					}
				}
			}
		}
	}

	if (tooLongWordsSkipped) {
		tooLongWordsSkipped->fetch_add(tooLongWordsSkippedLocal, std::memory_order_relaxed);
	}
	return nwo;
}

template <typename IdCont>
NewWordsOccurencesPtr DataHolder<IdCont>::buildWordsMap(VDocsTexts& vdocsTexts, const std::vector<uint32_t>& vdocsIds, size_t numFields,
														std::vector<h_vector<float, 3>>& vdocsWordsCountsByFields,
														std::atomic<size_t>* tooLongWordsSkipped) {
	vdocsWordsCountsByFields.resize(vdocsTexts.size());
	return buildWordsMap(vdocsTexts.begin(), vdocsIds.begin(), vdocsWordsCountsByFields.begin(), vdocsTexts.size(), numFields,
						 tooLongWordsSkipped);
}

template <typename IdCont>
std::vector<NewWordsOccurencesPtr> DataHolder<IdCont>::buildWordsMapParallel(VDocsTexts& vdocsTexts, const std::vector<uint32_t>& vdocsIds,
																			 size_t numFields,
																			 std::vector<h_vector<float, 3>>& vdocsWordsCountsByFields,
																			 std::atomic<size_t>* tooLongWordsSkipped) {
	vdocsWordsCountsByFields.resize(vdocsTexts.size());

	ExceptionPtrWrapper exwr;
	const std::vector<std::pair<size_t, size_t>> taskBorders = makeBuildTasksByTextSize(vdocsTexts, threadPool_.get_thread_count());
	std::vector<std::future<NewWordsOccurencesPtr>> tasks;
	tasks.reserve(taskBorders.size());
	auto tasksWaiter = MakeScopeGuard([&tasks] { waitForTasksOnException(tasks); });
	for (const auto& [from, to] : taskBorders) {
		tasks.emplace_back(
			threadPool_.submit_task([this, &vdocsTexts, &vdocsIds, from, to, numFields, &vdocsWordsCountsByFields, tooLongWordsSkipped] {
				return buildWordsMap(vdocsTexts.begin() + from, vdocsIds.begin() + from, vdocsWordsCountsByFields.begin() + from, to - from,
									 numFields, tooLongWordsSkipped);
			}));
	}

	std::vector<NewWordsOccurencesPtr> nwos;
	nwos.reserve(tasks.size());
	for (auto& task : tasks) {
		try {
			nwos.emplace_back(task.get());
		} catch (...) {
			exwr.SetException(std::current_exception());
		}
	}
	tasksWaiter.Disable();
	exwr.RethrowException();
	return nwos;
}

template <typename IdCont>
void DataHolder<IdCont>::updateOccurences(const NewWordsOccurencesPtr& nwo, std::vector<size_t>* updatedWordOrdinals) {
	if (wordsMap_.empty()) {
		const size_t n = nwo->Words().size();
		wordsMap_.reserve(n);
		wordIds_.reserve(n);
		wordOccurences_.reserve(n);
	}

	IdRelSet chain;
	for (auto&& [word, indices] : nwo->Words()) {
		auto [it, inserted] = wordsMap_.try_emplace(std::move(word), wordOccurences_.size());
		const size_t wordOrdinal = it->second;
		if (updatedWordOrdinals) {
			updatedWordOrdinals->emplace_back(wordOrdinal);
		}
		if (inserted) {
			wordsMapStringsHeapSize_ += it->first.capacity() * sizeof(std::string::value_type);
			const auto wordId = words_.Add(utf8_to_utf16(it->first), wordOrdinal);
			wordIds_.emplace_back(wordId);
			wordOccurences_.emplace_back();
		}
		wordOccurencesHeapSize_ += appendOccurenceChain(nwo->Occurences(), indices.first, chain, wordOccurences_[wordOrdinal]);
	}
}

template <typename IdCont>
void DataHolder<IdCont>::updateOccurencesParallel(const std::vector<NewWordsOccurencesPtr>& nwos,
												  std::vector<size_t>* updatedWordOrdinals) {
	struct [[nodiscard]] OccurenceJob {
		size_t wordOrdinal;
		uint32_t firstIdx;
		const NewWordsOccurences* nwo;
	};

	size_t totalJobs = 0;
	for (const auto& nwo : nwos) {
		totalJobs += nwo->Words().size();
	}
	if (updatedWordOrdinals) {
		updatedWordOrdinals->reserve(totalJobs);
	}

	if (wordsMap_.empty()) {
		wordsMap_.reserve(totalJobs);
		wordIds_.reserve(totalJobs);
		wordOccurences_.reserve(totalJobs);
	}

	std::vector<OccurenceJob> jobs;
	jobs.reserve(totalJobs);
	for (const auto& nwo : nwos) {
		for (auto&& [word, indices] : nwo->Words()) {
			auto [it, inserted] = wordsMap_.try_emplace(std::move(word), wordOccurences_.size());
			const size_t wordOrdinal = it->second;
			if (updatedWordOrdinals) {
				updatedWordOrdinals->emplace_back(wordOrdinal);
			}
			if (inserted) {
				wordsMapStringsHeapSize_ += it->first.capacity() * sizeof(std::string::value_type);
				const auto wordId = words_.Add(utf8_to_utf16(it->first), wordOrdinal);
				wordIds_.emplace_back(wordId);
				wordOccurences_.emplace_back();
			}
			jobs.push_back(OccurenceJob{wordOrdinal, indices.first, nwo.get()});
		}
	}

	std::array<std::vector<OccurenceJob>, kOccurenceUpdateShards> shards;
	const size_t shardReserve = totalJobs / kOccurenceUpdateShards + 1;
	for (auto& shard : shards) {
		shard.reserve(shardReserve);
	}
	for (const auto& job : jobs) {
		shards[job.wordOrdinal & kOccurenceUpdateShardMask].push_back(job);
	}

	ExceptionPtrWrapper exwr;
	std::vector<std::future<size_t>> appendTasks;
	appendTasks.reserve(kOccurenceUpdateShards);
	auto tasksWaiter = MakeScopeGuard([&appendTasks] { waitForTasksOnException(appendTasks); });
	for (size_t shard = 0; shard < kOccurenceUpdateShards; ++shard) {
		if (shards[shard].empty()) {
			continue;
		}
		appendTasks.emplace_back(threadPool_.submit_task([this, &shards, shard] {
			IdRelSet chain;
			size_t heapAdded = 0;
			for (const auto& job : shards[shard]) {
				heapAdded += appendOccurenceChain(job.nwo->Occurences(), job.firstIdx, chain, wordOccurences_[job.wordOrdinal]);
			}
			return heapAdded;
		}));
	}

	for (auto& task : appendTasks) {
		try {
			wordOccurencesHeapSize_ += task.get();
		} catch (...) {
			exwr.SetException(std::current_exception());
		}
	}
	tasksWaiter.Disable();
	exwr.RethrowException();
}

template <typename IdCont>
template <bool Multithreaded>
void DataHolder<IdCont>::shrinkWordOccurences() {
	if (wordOccurences_.empty()) {
		wordOccurencesHeapSize_ = 0;
		return;
	}

	if constexpr (!Multithreaded) {
		size_t heap = 0;
		for (auto& occ : wordOccurences_) {
			occ.shrink_to_fit();
			heap += occ.heap_size();
		}
		wordOccurencesHeapSize_ = heap;
		wordOccurences_.shrink_to_fit();
		return;
	}

	ExceptionPtrWrapper exwr;
	const size_t n = wordOccurences_.size();
	const size_t threadCount = std::max<size_t>(1, threadPool_.get_thread_count());
	const size_t chunkSize = (n + threadCount - 1) / threadCount;

	std::vector<std::future<size_t>> tasks;
	tasks.reserve((n + chunkSize - 1) / chunkSize);
	auto tasksWaiter = MakeScopeGuard([&tasks] { waitForTasksOnException(tasks); });
	for (size_t from = 0; from < n; from += chunkSize) {
		const size_t to = std::min(from + chunkSize, n);
		tasks.emplace_back(threadPool_.submit_task([this, from, to] {
			size_t heap = 0;
			for (size_t i = from; i < to; ++i) {
				wordOccurences_[i].shrink_to_fit();
				heap += wordOccurences_[i].heap_size();
			}
			return heap;
		}));
	}

	size_t heap = 0;
	for (auto& task : tasks) {
		try {
			heap += task.get();
		} catch (...) {
			exwr.SetException(std::current_exception());
		}
	}
	tasksWaiter.Disable();
	exwr.RethrowException();

	wordOccurencesHeapSize_ = heap;
}

template <typename IdCont>
void DataHolder<IdCont>::Process(VDocsTexts& vdocsTexts, const std::vector<uint32_t>& vdocsIds, size_t numDocsTotal, size_t numFields,
								 std::vector<h_vector<float, 3>>& vdocsWordsCountsByFields, bool multithreaded) {
	using namespace std::chrono;

	const auto tm0 = system_clock_w::now();
	system_clock_w::time_point tm1, tm2, tm3, tm4, tm5;
	std::atomic<size_t> tooLongWordsSkipped{0};
	std::vector<size_t> updatedWordOrdinals;
	auto* updatedWordOrdinalsPtr = (cfg_->logLevel >= LogInfo) ? &updatedWordOrdinals : nullptr;
	const bool firstBuild = (wordsProcessed_ == 0);

	if (!multithreaded) {
		auto nwo = buildWordsMap(vdocsTexts, vdocsIds, numFields, vdocsWordsCountsByFields, &tooLongWordsSkipped);
		tm1 = system_clock_w::now();
		if (updatedWordOrdinalsPtr) {
			updatedWordOrdinals.reserve(nwo->Words().size());
		}
		updateOccurences(std::move(nwo), updatedWordOrdinalsPtr);
		tm2 = system_clock_w::now();
		const size_t newWordsStart = wordsProcessed_;
		const size_t newWordsEnd = wordIds_.size();
		processNewSuffixes<false>(newWordsStart, newWordsEnd);
		tm3 = system_clock_w::now();
		processNewTypos<false>(newWordsStart, newWordsEnd);
		wordsProcessed_ = newWordsEnd;
		tm4 = system_clock_w::now();
		if (firstBuild) {
			shrinkWordOccurences<false>();
		}
		tm5 = system_clock_w::now();
	} else {
		auto nwos = buildWordsMapParallel(vdocsTexts, vdocsIds, numFields, vdocsWordsCountsByFields, &tooLongWordsSkipped);
		tm1 = system_clock_w::now();
		updateOccurencesParallel(nwos, updatedWordOrdinalsPtr);
		tm2 = system_clock_w::now();
		const size_t newWordsStart = wordsProcessed_;
		const size_t newWordsEnd = wordIds_.size();
		processNewSuffixes<true>(newWordsStart, newWordsEnd);
		tm3 = system_clock_w::now();
		processNewTypos<true>(newWordsStart, newWordsEnd);
		wordsProcessed_ = newWordsEnd;
		tm4 = system_clock_w::now();
		if (firstBuild) {
			shrinkWordOccurences<true>();
		}
		tm5 = system_clock_w::now();
	}
	if (updatedWordOrdinalsPtr) {
		logPotentialStopWords(updatedWordOrdinals, numDocsTotal);
	}

	if (cfg_->logLevel >= LogWarning) {
		const auto skipped = tooLongWordsSkipped.load(std::memory_order_relaxed);
		if (skipped > 0) {
			logFmt(LogWarning, "FT index build skipped {} too-long words (max length {})", skipped, kMaxFtWordLen);
		}
	}
	logFmt(LogInfo,
		   "DataHolder::Process elapsed {} ms total [ build words {} ms | update words {} ms | build suffixes {} ms | build typos {} ms | "
		   "shrink {} ms ]",
		   duration_cast<milliseconds>(tm5 - tm0).count(), duration_cast<milliseconds>(tm1 - tm0).count(),
		   duration_cast<milliseconds>(tm2 - tm1).count(), duration_cast<milliseconds>(tm3 - tm2).count(),
		   duration_cast<milliseconds>(tm4 - tm3).count(), duration_cast<milliseconds>(tm5 - tm4).count());
}

template <typename IdCont>
void DataHolder<IdCont>::logPotentialStopWords(std::vector<size_t>& updatedWordOrdinals, size_t numDocsTotal) const {
	if (cfg_->logLevel < LogInfo) {
		return;
	}

	WrSerializer out;
	std::sort(updatedWordOrdinals.begin(), updatedWordOrdinals.end());
	const auto end = std::unique(updatedWordOrdinals.begin(), updatedWordOrdinals.end());
	for (auto it = updatedWordOrdinals.begin(); it != end; ++it) {
		const size_t wordOrdinal = *it;
		const size_t docsCount = wordOccurences_[wordOrdinal].size();
		if (docsCount > 1000 && docsCount * 5 >= numDocsTotal) {
			out << utf16_to_utf8(words_.GetWord(wordIds_[wordOrdinal])) << "(" << docsCount << ") ";
		}
	}
	logFmt(LogInfo, "Total documents: {}. Potential stop words (with corresponding docs count): {}", numDocsTotal, out.Slice());
}

template class DataHolder<PackedIdRelVec>;
template class DataHolder<IdRelVec>;

}  // namespace reindexer
