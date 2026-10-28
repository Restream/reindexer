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
#include "estl/fast_hash_map.h"
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

template <typename Cont>
void waitForTasksOnException(Cont& tasks) noexcept {
	for (auto& task : tasks) {
		if (task.valid()) {
			try {
				using Fut = std::remove_reference_t<decltype(task)>;
				if constexpr (std::is_void_v<decltype(std::declval<Fut>().get())>) {
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
	fieldBits_ = PackedIdRelVec::NumBitsForFields(c->fieldsCfg.size());
	if (cfg_->splitterType == FTConfig::Splitter::Fast) {
		splitter_ = make_intrusive<FastTextSplitter>(cfg_->splitOptions);
	} else if (cfg_->splitterType == FTConfig::Splitter::MMSegCN) {
		splitter_ = make_intrusive<FrisoTextSplitter>();
	} else {
		assertrx_throw(false);
	}
}

IDataHolder::MemStat IDataHolder::GetMemStat() {
	MemStat stats;
	stats.text.wordsCount = wordsMap_.size();
	for (const auto& set : typos_) {
		if (set) {
			stats.text.typosGroupsCount += set->size();
		}
	}
	auto& mem = stats.memory;
	mem.wordsSize = words_.heap_size();
	mem.wordIdsSize = wordIds_.capacity() * sizeof(WordIdType);
	mem.wordsMapSize = wordsMap_.allocated_mem_size() + wordsMapStringsHeapSize_;
	const auto cached = memoryStats_.Get();
	stats.text.typosKeysCount = cached.typosKeys;
	mem.typosSize = typos_.capacity() * sizeof(decltype(typos_)::value_type) + cached.typos;
	mem.suffixesSize = suffixes_.capacity() * sizeof(decltype(suffixes_)::value_type);
	for (const auto& tree : suffixes_) {
		if (tree) {
			mem.suffixesSize += sizeof(*tree) + tree->bytes_used();
		}
	}
	mem.postingsCapacitySize = cached.postings.capacity;
	mem.packedDataSize = cached.postings.packed;

	mem.auxiliarySize = sizeof(memoryStats_) + stemmedTermsBoost.allocated_mem_size();
	for (const auto& [term, _] : stemmedTermsBoost) {
		mem.auxiliarySize += term.capacity() * sizeof(typename std::string::value_type);
	}
	for (const auto& [lang, _] : stemmers_) {
		mem.auxiliarySize += lang.capacity() * sizeof(typename std::string::value_type);
	}
	if (translit_) {
		mem.auxiliarySize += sizeof(*translit_);
	}
	if (kbLayout_) {
		mem.auxiliarySize += sizeof(*kbLayout_);
	}
	if (synonyms_) {
		mem.auxiliarySize += sizeof(*synonyms_) + synonyms_->heap_size();
	}

	return stats;
}

template <typename IdCont>
IDataHolder::MemStat DataHolder<IdCont>::GetMemStat() {
	auto stats = IDataHolder::GetMemStat();
	auto& mem = stats.memory;
	if constexpr (!std::is_same_v<IdCont, PackedIdRelVec>) {
		mem.packedDataSize.reset();
	}
	mem.postingsSize = wordOccurences_.capacity() * sizeof(typename decltype(wordOccurences_)::value_type) +
					   wordOccurences_.size() * sizeof(IdCont) + mem.postingsCapacitySize + sizeof(occurencePtrLocks_);
	return stats;
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
	memoryStats_.Clear();
	optimizeNextWordOrdinal_ = 0;
	optimizeResumePending_ = false;
	needRebuild_ = true;
}

// See TypoSet contract in typosholder.h: append-only; hash/equal use first key only.
static void AddTypoKey(TypoSet& typoSet, TypoKey key, TypoStatDelta& delta) {
	TypoKeyGroup group;
	AppendTypoKey(group, key);
	auto [it, inserted] = typoSet.insert(std::move(group));
	if (inserted) {
		delta.groupsHeapAdded += it->heap_size();
	} else {
		const size_t before = it->heap_size();
		try {
			AppendTypoKey(*it, key);
		} catch (...) {
			delta.groupsHeapAdded += it->heap_size() - before;
			throw;
		}
		delta.groupsHeapAdded += it->heap_size() - before;
	}
	++delta.keysAdded;
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
void DataHolder<IdCont>::addTwoMissingTyposWithoutFirstMissing(WordIdType wordId, std::u16string_view word, TypoSet& typoSet,
															   TypoStatDelta& delta) {
	for (size_t firstMissingPos = 1; firstMissingPos + 1 < word.length(); ++firstMissingPos) {
		for (size_t secondMissingPos = firstMissingPos + 1; secondMissingPos < word.length(); ++secondMissingPos) {
			AddTypoKey(typoSet, PackTypoKey(wordId, TyposVec(uint8_t(firstMissingPos), uint8_t(secondMissingPos))), delta);
		}
	}
}

template <typename IdCont>
void DataHolder<IdCont>::fillTypoSetShard(size_t shard, const std::vector<TypoKey>& packedKeys, const std::vector<size_t>& wordIndexes) {
	auto& set = typos_[shard];
	const size_t tableBefore = set ? sizeof(*set) + set->allocated_mem_size() : 0;
	if (!set) {
		set = std::make_unique<TypoSet>(0, TypoKeyHash(words_), TypoKeyEqual(words_));
	}
	auto& typoSet = *set;
	struct [[nodiscard]] TypoUpdate {
		FtMemoryStatCache& stats;
		TypoSet& set;
		size_t tableBefore;
		TypoStatDelta delta;
	} update{.stats = memoryStats_, .set = typoSet, .tableBefore = tableBefore, .delta = {}};
	auto statsGuard = MakeScopeGuard([&update]() noexcept {
		update.stats.UpdateTypos(update.tableBefore, sizeof(update.set) + update.set.allocated_mem_size(), update.delta);
	});
	for (const TypoKey key : packedKeys) {
		AddTypoKey(typoSet, key, update.delta);
	}
	for (const size_t wordIdx : wordIndexes) {
		const auto wordId = wordIds_[wordIdx];
		const auto word = words_.GetWord(wordId);
		addTwoMissingTyposWithoutFirstMissing(wordId, word, typoSet, update.delta);
	}
}

template <typename IdCont>
void DataHolder<IdCont>::updateSuffixShard(size_t shard, std::vector<SuffixKey>& suffixes) {
	auto& tree = suffixes_[shard];
	if (!tree) {
		tree = std::make_unique<SuffixTree>(SuffixKeyCompare(words_));
	}
	InsertSuffixKeys(*tree, suffixes, words_);
}

template <typename IdCont>
template <bool Multithreaded>
void DataHolder<IdCont>::processNewSuffixes(size_t start, size_t end) {
	if (start == end) {
		return;
	}

	std::vector<std::vector<SuffixKey>> suffixesByFirstCh(kSuffixTreesCount);
	collectSuffixes(start, end, suffixesByFirstCh);

	struct [[nodiscard]] SuffixContext {
		DataHolder& holder;
		std::vector<std::vector<SuffixKey>>& keys;
	};
	const SuffixContext context{.holder = *this, .keys = suffixesByFirstCh};
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
		if constexpr (Multithreaded) {
			tasks.emplace_back(
				threadPool_.submit_task([&context, firstCh] { context.holder.updateSuffixShard(firstCh, context.keys[firstCh]); }));
		} else {
			updateSuffixShard(firstCh, suffixes);
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

	struct [[nodiscard]] TypoContext {
		DataHolder& holder;
		const std::vector<std::vector<TypoKey>>& keys;
		const std::vector<std::vector<size_t>>& words;
	};
	const TypoContext context{.holder = *this, .keys = packedKeysByFirstCh, .words = twoMissingLettersWordsByFirstCh};
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
		if constexpr (Multithreaded) {
			tasks.emplace_back(threadPool_.submit_task(
				[&context, firstCh] { context.holder.fillTypoSetShard(firstCh, context.keys[firstCh], context.words[firstCh]); }));
		} else {
			fillTypoSetShard(firstCh, packedKeys, wordIndexes);
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

void NewWordsOccurences::appendOccurence(uint32_t vdocId, VDocVersion version, unsigned pos, unsigned field, unsigned arrayIdx,
										 words_map_t::iterator wordIt) {
	const uint32_t newIdx = occurrences_.size();
	if (wordIt->second.first == kInvalidLink) {
		wordIt->second = {newIdx, newIdx};
	} else {
		occurrences_[wordIt->second.second].link = newIdx;
		wordIt->second.second = newIdx;
	}
	occurrences_.emplace_back(Occurence{vdocId, version, kInvalidLink, PosType(pos, field, arrayIdx)});
}

void NewWordsOccurences::AddPrehashed(std::string_view word, size_t whash, uint32_t vdocId, VDocVersion version, unsigned pos,
									  unsigned field, unsigned arrayIdx) {
	auto [wordIt, emplaced] = words_.try_emplace_prehashed(whash, word, WordIndices{kInvalidLink, kInvalidLink});
	appendOccurence(vdocId, version, pos, field, arrayIdx, wordIt);
	(void)emplaced;
}

void NewWordsOccurences::Add(std::string word, uint32_t vdocId, VDocVersion version, unsigned pos, unsigned field, unsigned arrayIdx) {
	auto [wordIt, emplaced] = words_.try_emplace(std::move(word), WordIndices{kInvalidLink, kInvalidLink});
	appendOccurence(vdocId, version, pos, field, arrayIdx, wordIt);
	(void)emplaced;
}

template <typename IdCont>
void DataHolder<IdCont>::buildOccurenceChain(const std::vector<Occurence>& occurrences, uint32_t firstIdx, IdRelSet& chain) {
	chain.resize(0);

	uint32_t idx = firstIdx;
	while (idx != NewWordsOccurences::kInvalidLink) {
		const auto& occ = occurrences[idx];
		chain.Add(occ.vdocId, occ.version, occ.pos.pos(), occ.pos.field(), occ.pos.arrayIdx());
		idx = occ.link;
	}
}

template <typename IdCont>
PostingsMemSize DataHolder<IdCont>::appendOccurenceChain(const std::vector<Occurence>& occurrences, uint32_t firstIdx, IdRelSet& chain,
														 IdCont& dst) {
	buildOccurenceChain(occurrences, firstIdx, chain);
	const auto before = PostingsMemSize::Measure(dst);
	if constexpr (std::is_same_v<IdCont, PackedIdRelVec>) {
		PackedIdRelVec::AppendBuffer tail(dst);
		tail.Append(chain.begin(), chain.end());
		tail.Commit();
	} else {
		dst.insert(dst.end(), chain.begin(), chain.end());
	}
	const auto after = PostingsMemSize::Measure(dst);
	return {.capacity = after.capacity - before.capacity, .packed = after.packed - before.packed};
}

template <typename IdCont>
NewWordsOccurencesPtr DataHolder<IdCont>::buildWordsMap(VDocsTexts::iterator textsBegin,
														std::vector<VDocPosting>::const_iterator vdocsBegin,
														std::vector<VDocWordCounts>::iterator wordsCountsBegin, size_t numDocs,
														size_t numFields, std::atomic<size_t>* tooLongWordsSkipped) {
	auto nwo = std::make_shared<NewWordsOccurences>();
	std::vector<std::string_view> virtualWords;
	const word_hash h;
	std::string wordWithoutDelims;
	auto task = splitter_->CreateTask();
	size_t tooLongWordsSkippedLocal = 0;

	auto textsIt = textsBegin;
	auto vdocsIt = vdocsBegin;
	auto wordsCountsIt = wordsCountsBegin;
	for (size_t i = 0; i < numDocs; ++i, ++textsIt, ++vdocsIt, ++wordsCountsIt) {
		const uint32_t vdocId = vdocsIt->vdocId;
		const VDocVersion version = vdocsIt->version;
		auto& vdocsText = *textsIt;
		wordsCountsIt->resize(0);
		wordsCountsIt->resize(numFields, 0);

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

				nwo->AddPrehashed(occurence.word, whash, vdocId, version, occurence.pos, field, arrayIdx);

				if (cfg_->enableNumbersSearch && is_number(occurence.word)) {
					std::ignore = NumToText::convert(occurence.word, virtualWords);
					for (const auto numberWord : virtualWords) {
						if (!IsFtWordIndexable(numberWord)) {
							++tooLongWordsSkippedLocal;
							continue;
						}
						nwo->Add(std::string(numberWord), vdocId, version, occurence.pos, field, arrayIdx);
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
NewWordsOccurencesPtr DataHolder<IdCont>::buildWordsMap(VDocsTexts& vdocsTexts, const std::vector<VDocPosting>& vdocs, size_t numFields,
														std::vector<VDocWordCounts>& vdocsWordsCountsByFields,
														std::atomic<size_t>* tooLongWordsSkipped) {
	vdocsWordsCountsByFields.resize(vdocsTexts.size());
	return buildWordsMap(vdocsTexts.begin(), vdocs.begin(), vdocsWordsCountsByFields.begin(), vdocsTexts.size(), numFields,
						 tooLongWordsSkipped);
}

template <typename IdCont>
std::vector<NewWordsOccurencesPtr> DataHolder<IdCont>::buildWordsMapParallel(VDocsTexts& vdocsTexts, const std::vector<VDocPosting>& vdocs,
																			 size_t numFields,
																			 std::vector<VDocWordCounts>& vdocsWordsCountsByFields,
																			 std::atomic<size_t>* tooLongWordsSkipped) {
	vdocsWordsCountsByFields.resize(vdocsTexts.size());

	ExceptionPtrWrapper exwr;
	const std::vector<std::pair<size_t, size_t>> taskBorders = makeBuildTasksByTextSize(vdocsTexts, threadPool_.get_thread_count());
	std::vector<std::future<NewWordsOccurencesPtr>> tasks;
	tasks.reserve(taskBorders.size());
	auto tasksWaiter = MakeScopeGuard([&tasks] { waitForTasksOnException(tasks); });
	for (const auto& [from, to] : taskBorders) {
		tasks.emplace_back(
			threadPool_.submit_task([this, &vdocsTexts, &vdocs, from, to, numFields, &vdocsWordsCountsByFields, tooLongWordsSkipped] {
				return buildWordsMap(vdocsTexts.begin() + from, vdocs.begin() + from, vdocsWordsCountsByFields.begin() + from, to - from,
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
	PostingsMemSize added;
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
			wordOccurences_.emplace_back(makeEmptyOccurences());
		}
		added += appendOccurenceChain(nwo->Occurences(), indices.first, chain, *wordOccurences_[wordOrdinal]);
	}
	memoryStats_.AddPostings(added);
}

template <typename IdCont>
void DataHolder<IdCont>::updateOccurencesParallel(const std::vector<NewWordsOccurencesPtr>& nwos,
												  std::vector<size_t>* updatedWordOrdinals) {
	constexpr uint32_t kInvalidJob = std::numeric_limits<uint32_t>::max();
	struct [[nodiscard]] OccurenceJob {
		size_t wordOrdinal;
		uint32_t firstIdx;
		uint32_t nextJob = kInvalidJob;
		const NewWordsOccurences* nwo;
	};

	size_t totalJobs = 0;
	size_t maxWordsPerTask = 0;
	for (const auto& nwo : nwos) {
		const size_t wordCount = nwo->Words().size();
		assertrx_throw(wordCount <= kInvalidJob - totalJobs);
		totalJobs += wordCount;
		maxWordsPerTask = std::max(maxWordsPerTask, wordCount);
	}
	if (updatedWordOrdinals) {
		updatedWordOrdinals->reserve(totalJobs);
	}

	if (wordsMap_.empty()) {
		wordsMap_.reserve(2 * maxWordsPerTask);
		wordIds_.reserve(2 * maxWordsPerTask);
		wordOccurences_.reserve(2 * maxWordsPerTask);
	}

	std::vector<OccurenceJob> jobs;
	jobs.reserve(totalJobs);
	std::array<std::vector<uint32_t>, kOccurenceUpdateShards> shards;
	const size_t shardReserve = maxWordsPerTask / kOccurenceUpdateShards + 1;
	for (auto& shard : shards) {
		shard.reserve(shardReserve);
	}
	{
		// Track only words touched by this update; release the lookup before packing starts.
		fast_hash_map<size_t, uint32_t> lastJobs;
		const bool multipleTasks = nwos.size() > 1;
		if (multipleTasks) {
			lastJobs.reserve(maxWordsPerTask);
		}
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
					wordOccurences_.emplace_back(makeEmptyOccurences());
				}
				const uint32_t jobIdx = uint32_t(jobs.size());
				jobs.push_back(OccurenceJob{.wordOrdinal = wordOrdinal, .firstIdx = indices.first, .nwo = nwo.get()});
				if (!multipleTasks) {
					shards[wordOrdinal & kOccurenceUpdateShardMask].push_back(jobIdx);
					continue;
				}
				auto [last, firstJob] = lastJobs.try_emplace(wordOrdinal, jobIdx);
				if (firstJob) {
					shards[wordOrdinal & kOccurenceUpdateShardMask].push_back(jobIdx);
				} else {
					// Link jobs in task order to preserve packed IDs and array metadata.
					jobs[last->second].nextJob = jobIdx;
					last->second = jobIdx;
				}
			}
		}
	}

	struct [[nodiscard]] AppendContext {
		DataHolder& holder;
		const std::vector<OccurenceJob>& jobs;
		const std::array<std::vector<uint32_t>, kOccurenceUpdateShards>& shards;
	};
	const AppendContext context{.holder = *this, .jobs = jobs, .shards = shards};
	ExceptionPtrWrapper exwr;
	std::vector<std::future<void>> appendTasks;
	appendTasks.reserve(kOccurenceUpdateShards);
	auto tasksWaiter = MakeScopeGuard([&appendTasks] { waitForTasksOnException(appendTasks); });
	for (size_t shard = 0; shard < kOccurenceUpdateShards; ++shard) {
		if (shards[shard].empty()) {
			continue;
		}
		appendTasks.emplace_back(threadPool_.submit_task([&context, shard] {
			IdRelSet chain;
			PackedIdRelVec::AppendBuffer tail;
			PostingsMemSize added;
			auto statsGuard = MakeScopeGuard([&context, &added]() noexcept { context.holder.memoryStats_.AddPostings(added); });
			for (const uint32_t firstJob : context.shards[shard]) {
				auto& dst = *context.holder.wordOccurences_[context.jobs[firstJob].wordOrdinal];
				if constexpr (std::is_same_v<IdCont, PackedIdRelVec>) {
					const auto before = PostingsMemSize::Measure(dst);
					tail.Reset(dst);
					for (uint32_t idx = firstJob; idx != kInvalidJob; idx = context.jobs[idx].nextJob) {
						const auto& job = context.jobs[idx];
						buildOccurenceChain(job.nwo->Occurences(), job.firstIdx, chain);
						tail.Append(chain.begin(), chain.end());
					}
					tail.Commit();
					const auto after = PostingsMemSize::Measure(dst);
					added += PostingsMemSize{.capacity = after.capacity - before.capacity, .packed = after.packed - before.packed};
				} else {
					for (uint32_t idx = firstJob; idx != kInvalidJob; idx = context.jobs[idx].nextJob) {
						const auto& job = context.jobs[idx];
						added += context.holder.appendOccurenceChain(job.nwo->Occurences(), job.firstIdx, chain, dst);
					}
				}
			}
		}));
	}

	for (auto& task : appendTasks) {
		try {
			task.get();
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
		memoryStats_.SetPostings({});
		return;
	}

	if constexpr (!Multithreaded) {
		PostingsMemSize size;
		for (auto& occ : wordOccurences_) {
			occ->shrink_to_fit();
			size += PostingsMemSize::Measure(*occ);
		}
		memoryStats_.SetPostings(size);
		wordOccurences_.shrink_to_fit();
		return;
	}

	ExceptionPtrWrapper exwr;
	const size_t n = wordOccurences_.size();
	const size_t threadCount = std::max<size_t>(1, threadPool_.get_thread_count());
	const size_t chunkSize = (n + threadCount - 1) / threadCount;

	std::vector<std::future<PostingsMemSize>> tasks;
	tasks.reserve((n + chunkSize - 1) / chunkSize);
	auto tasksWaiter = MakeScopeGuard([&tasks] { waitForTasksOnException(tasks); });
	for (size_t from = 0; from < n; from += chunkSize) {
		const size_t to = std::min(from + chunkSize, n);
		tasks.emplace_back(threadPool_.submit_task([this, from, to] {
			PostingsMemSize size;
			for (size_t i = from; i < to; ++i) {
				wordOccurences_[i]->shrink_to_fit();
				size += PostingsMemSize::Measure(*wordOccurences_[i]);
			}
			return size;
		}));
	}

	PostingsMemSize size;
	for (auto& task : tasks) {
		try {
			size += task.get();
		} catch (...) {
			exwr.SetException(std::current_exception());
		}
	}
	tasksWaiter.Disable();
	exwr.RethrowException();

	memoryStats_.SetPostings(size);
}

template <typename IdCont>
DeletedScanStat DataHolder<IdCont>::OptimizeDeleted(const std::function<bool(uint32_t, VDocVersion)>& isDeleted,
													const index::ICancelable& cancelable) {
	// Cancel roughly every few hundred microseconds of scrub work on hot namespaces.
	constexpr size_t kCancelCheckPostings = 512;
	constexpr size_t kTargetChunkBytes = 100 * 1024;
	constexpr size_t kParallelTasks = 4;

	struct [[nodiscard]] RangeResult {
		DeletedScanStat stat;
		size_t resumeOrdinal = 0;  // valid only when stat.canceled
	};

	DeletedScanStat stat;
	optimizeResumePending_ = true;

	const auto listBytes = [](const std::shared_ptr<IdCont>& sp) noexcept -> size_t {
		if (!sp) {
			return 0;
		}
		if constexpr (std::is_same_v<IdCont, PackedIdRelVec>) {
			return sp->data_size();
		} else {
			return sp->heap_size();
		}
	};

	const auto scrubWord = [this, &isDeleted, &cancelable](size_t wordOrdinal, size_t& checked, DeletedScanStat& localStat,
														   PostingsMemDelta& delta) -> bool {
		const auto oldSp = loadOccurences(wordOrdinal);
		if (!oldSp) {
			return false;
		}

		++localStat.listsScanned;

		const auto checkPostingsCancel = [&checked, &cancelable, &localStat]() -> bool {
			if ((++checked % kCancelCheckPostings) == 0 && cancelable.IsCanceled()) {
				localStat.canceled = WasCanceled_True;
				return true;
			}
			return false;
		};

		if (cancelable.IsCanceled()) {
			localStat.canceled = WasCanceled_True;
			return true;
		}

		IdCont rebuilt;
		bool needPublish = false;

		if constexpr (std::is_same_v<IdCont, PackedIdRelVec>) {
			const uint8_t* const data = oldSp->data();
			const size_t dataSize = oldSp->data_size();
			const unsigned fieldBits = oldSp->FieldBits();
			const size_t arrayFoundPos = oldSp->array_found_pos();

			size_t firstDeletedByte = dataSize;
			PackedIdRelVec::state prefixSt;
			bool hasDeleted = false;
			{
				PackedIdRelVec::state st;
				IdRelTypePacked cur;
				size_t pos = 0;
				while (pos < dataSize) {
					if (checkPostingsCancel()) {
						return true;
					}
					const PackedIdRelVec::state stateBefore = st;
					const bool storeArrayIdx = (pos >= arrayFoundPos);
					const size_t itemSize =
						cur.unpackIdentity(data + pos, uint32_t(dataSize - pos), st.lastVdocId, fieldBits, storeArrayIdx);
					if (isDeleted(cur.VdocId(), cur.VdocVersion())) {
						hasDeleted = true;
						firstDeletedByte = pos;
						prefixSt = stateBefore;
						break;
					}
					st.lastVdocId = cur.VdocId();
					st.lastVersion = cur.VdocVersion();
					++st.size;
					pos += itemSize;
				}
			}

			if (hasDeleted) {
				rebuilt.SetFieldBits(fieldBits);
				rebuilt.InitFromUnchangedPrefix(*oldSp, firstDeletedByte, prefixSt);

				PackedIdRelVec::state st = prefixSt;
				IdRelTypePacked cur;
				size_t pos = firstDeletedByte;
				while (pos < dataSize) {
					if (checkPostingsCancel()) {
						return true;
					}
					const bool storeArrayIdx = (pos >= arrayFoundPos);
					const size_t itemSize =
						cur.unpackIdentity(data + pos, uint32_t(dataSize - pos), st.lastVdocId, fieldBits, storeArrayIdx);
					if (isDeleted(cur.VdocId(), cur.VdocVersion())) {
						++localStat.deletedPostings;
					} else {
						IdRelType live(cur.VdocId(), cur.VdocVersion());
						for (const auto& p : cur.Pos()) {
							live.Add(p);
						}
						rebuilt.insert_back(&live, &live + 1);
					}
					st.lastVdocId = cur.VdocId();
					st.lastVersion = cur.VdocVersion();
					pos += itemSize;
				}
				needPublish = true;
			}
		} else {
			const size_t listSize = oldSp->size();
			size_t firstDeleted = listSize;
			for (size_t i = 0; i < listSize; ++i) {
				if (checkPostingsCancel()) {
					return true;
				}
				const auto& occ = (*oldSp)[i];
				if (isDeleted(occ.VdocId(), occ.VdocVersion())) {
					firstDeleted = i;
					break;
				}
			}

			if (firstDeleted != listSize) {
				rebuilt.reserve(listSize - 1);
				rebuilt.insert(rebuilt.end(), oldSp->begin(), oldSp->begin() + ptrdiff_t(firstDeleted));
				for (size_t i = firstDeleted; i < listSize; ++i) {
					if (checkPostingsCancel()) {
						return true;
					}
					const auto& occ = (*oldSp)[i];
					if (isDeleted(occ.VdocId(), occ.VdocVersion())) {
						++localStat.deletedPostings;
					} else {
						rebuilt.push_back(occ);
					}
				}
				needPublish = true;
			}
		}

		if (needPublish) {
			rebuilt.shrink_to_fit();
			const auto before = PostingsMemSize::Measure(*oldSp);
			const auto after = PostingsMemSize::Measure(rebuilt);
			storeOccurences(wordOrdinal, std::make_shared<IdCont>(std::move(rebuilt)));
			delta.Replace(before, after);
		}
		return false;
	};

	const auto scrubRange = [this, &scrubWord](size_t from, size_t to) -> RangeResult {
		PostingsMemDelta delta;
		auto statsGuard = MakeScopeGuard([this, &delta]() noexcept { memoryStats_.ApplyPostings(delta); });
		RangeResult result;
		size_t checked = 0;
		for (size_t wordOrdinal = from; wordOrdinal < to; ++wordOrdinal) {
			if (scrubWord(wordOrdinal, checked, result.stat, delta)) {
				result.stat.canceled = WasCanceled_True;
				result.resumeOrdinal = wordOrdinal;
				return result;
			}
		}
		return result;
	};

	size_t cursor = optimizeNextWordOrdinal_;
	const size_t wordsCount = wordOccurences_.size();
	while (cursor < wordsCount) {
		if (cancelable.IsCanceled()) {
			stat.canceled = WasCanceled_True;
			optimizeNextWordOrdinal_ = cursor;
			return stat;
		}

		h_vector<std::pair<size_t, size_t>, kParallelTasks> ranges;
		size_t rangePos = cursor;
		for (size_t task = 0; task < kParallelTasks && rangePos < wordsCount; ++task) {
			const size_t rangeFrom = rangePos;
			size_t bytes = 0;
			do {
				bytes += listBytes(loadOccurences(rangePos));
				++rangePos;
			} while (rangePos < wordsCount && bytes < kTargetChunkBytes);
			ranges.emplace_back(rangeFrom, rangePos);
		}
		if (ranges.empty()) {
			break;
		}

		ExceptionPtrWrapper exwr;
		h_vector<std::future<RangeResult>, kParallelTasks> tasks;
		auto tasksWaiter = MakeScopeGuard([&tasks] { waitForTasksOnException(tasks); });
		for (const auto& [from, to] : ranges) {
			tasks.emplace_back(threadPool_.submit_task([&scrubRange, from, to] { return scrubRange(from, to); }));
		}

		WasCanceled canceled = WasCanceled_False;
		size_t resumeOrdinal = ranges.back().second;
		for (size_t i = 0; i < tasks.size(); ++i) {
			try {
				const RangeResult local = tasks[i].get();
				stat.listsScanned += local.stat.listsScanned;
				stat.deletedPostings += local.stat.deletedPostings;
				if (local.stat.canceled) {
					canceled = WasCanceled_True;
					resumeOrdinal = std::min(resumeOrdinal, local.resumeOrdinal);
				}
			} catch (...) {
				exwr.SetException(std::current_exception());
			}
		}
		tasksWaiter.Disable();
		exwr.RethrowException();

		if (canceled) {
			stat.canceled = WasCanceled_True;
			optimizeNextWordOrdinal_ = resumeOrdinal;
			return stat;
		}

		cursor = ranges.back().second;
		optimizeNextWordOrdinal_ = cursor;
	}

	optimizeNextWordOrdinal_ = 0;
	optimizeResumePending_ = false;
	return stat;
}

template <typename IdCont>
void DataHolder<IdCont>::Process(VDocsTexts& vdocsTexts, const std::vector<VDocPosting>& vdocs, size_t numDocsTotal, size_t numFields,
								 std::vector<VDocWordCounts>& vdocsWordsCountsByFields, bool multithreaded) {
	using namespace std::chrono;

	// Failed builds may have changed a buffer before throwing. Recover counters only
	// on this exceptional path, after all nested task waiters have finished.
	const StackUnwinding unwinding;
	auto statsGuard = MakeScopeGuard([this, &unwinding]() noexcept {
		if (unwinding()) {
			PostingsMemSize size;
			for (const auto& occ : wordOccurences_) {
				size += PostingsMemSize::Measure(*occ);
			}
			memoryStats_.SetPostings(size);
		}
	});

	const auto tm0 = system_clock_w::now();
	system_clock_w::time_point tm1, tm2, tm3, tm4, tm5;
	std::atomic<size_t> tooLongWordsSkipped{0};
	std::vector<size_t> updatedWordOrdinals;
	auto* updatedWordOrdinalsPtr = (cfg_->logLevel >= LogInfo) ? &updatedWordOrdinals : nullptr;
	const bool firstBuild = (wordsProcessed_ == 0);

	if (!multithreaded) {
		auto nwo = buildWordsMap(vdocsTexts, vdocs, numFields, vdocsWordsCountsByFields, &tooLongWordsSkipped);
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
		auto nwos = buildWordsMapParallel(vdocsTexts, vdocs, numFields, vdocsWordsCountsByFields, &tooLongWordsSkipped);
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
		const auto sp = loadOccurences(wordOrdinal);
		const size_t docsCount = sp ? sp->size() : 0;
		if (docsCount > 1000 && docsCount * 5 >= numDocsTotal) {
			out << utf16_to_utf8(words_.GetWord(wordIds_[wordOrdinal])) << "(" << docsCount << ") ";
		}
	}
	logFmt(LogInfo, "Total documents: {}. Potential stop words (with corresponding docs count): {}", numDocsTotal, out.Slice());
}

template class DataHolder<PackedIdRelVec>;
template class DataHolder<IdRelVec>;

}  // namespace reindexer
