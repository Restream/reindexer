#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include "core/ft/config/ftconfig.h"
#include "core/ft/ft_fast/splitter.h"
#include "core/ft/ft_fast/suffixesholder.h"
#include "core/ft/ft_fast/typosholder.h"
#include "core/ft/idrelset.h"
#include "core/ft/stemmer.h"
#include "core/ft/variants/kblayout.h"
#include "core/ft/variants/synonyms.h"
#include "core/ft/variants/translit.h"
#include "core/index/auxiliary_interfaces.h"
#include "estl/h_vector.h"
#include "estl/intrusive_ptr.h"
#include "estl/lock.h"
#include "estl/shared_mutex.h"
#include "estl/spin_lock.h"
#include "indextexttypes.h"
#include "tools/assertrx.h"
#include "tools/background_thread_pool.h"
#include "vendor/hopscotch/hopscotch_map.h"

namespace reindexer {

using VDocsTexts = std::vector<h_vector<std::pair<std::string_view, uint32_t>, 8>>;
using VDocWordCounts = h_vector<uint32_t, 3>;

struct [[nodiscard]] DeletedScanStat {
	size_t listsScanned = 0;
	size_t deletedPostings = 0;
	WasCanceled canceled = WasCanceled_False;
};

struct [[nodiscard]] Occurence {
	uint32_t vdocId = kEmptyVDocId;
	VDocVersion version = kEmptyVDocVersion;
	uint32_t link = std::numeric_limits<uint32_t>::max();
	PosType pos;
};

class [[nodiscard]] NewWordsOccurences {
public:
	static constexpr uint32_t kInvalidLink = std::numeric_limits<uint32_t>::max();
	using WordIndices = std::pair<uint32_t, uint32_t>;
	using words_map_t =
		tsl::hopscotch_map<std::string, WordIndices, word_hash, word_equal, std::allocator<std::pair<std::string, WordIndices>>, 30, true>;

	void AddPrehashed(std::string_view word, size_t whash, uint32_t vdocId, VDocVersion version, unsigned pos, unsigned field,
					  unsigned arrayIdx);
	void Add(std::string word, uint32_t vdocId, VDocVersion version, unsigned pos, unsigned field, unsigned arrayIdx);

	const std::vector<Occurence>& Occurences() const noexcept { return occurrences_; }
	const words_map_t& Words() const noexcept { return words_; }

private:
	void appendOccurence(uint32_t vdocId, VDocVersion version, unsigned pos, unsigned field, unsigned arrayIdx,
						 words_map_t::iterator wordIt);

	std::vector<Occurence> occurrences_;
	words_map_t words_;
};

using NewWordsOccurencesPtr = std::shared_ptr<NewWordsOccurences>;

class [[nodiscard]] IDataHolder {
public:
	static constexpr size_t kSuffixTreesCount = 64;
	static constexpr size_t kSuffixTreeMask = kSuffixTreesCount - 1;
	static constexpr size_t kTypoSetsCount = 64;
	static constexpr size_t kTypoSetMask = kTypoSetsCount - 1;

	IDataHolder() : words_(), wordsMap_(), suffixes_(kSuffixTreesCount), typos_(kTypoSetsCount) {}
	virtual ~IDataHolder() = default;
	virtual void Process(VDocsTexts& vdocsTexts, const std::vector<VDocPosting>& vdocs, size_t numDocsTotal, size_t numFields,
						 std::vector<VDocWordCounts>& vdocsWordsCountsByFields, bool multithreaded) = 0;
	virtual DeletedScanStat OptimizeDeleted(const std::function<bool(uint32_t /*vdocId*/, VDocVersion)>& isDeleted,
											const index::ICancelable& cancelable) = 0;
	virtual bool HasPendingOptimization() const noexcept = 0;
	virtual size_t GetMemStat() = 0;
	virtual void Clear() = 0;
	intrusive_ptr<const ISplitter> GetSplitter() const noexcept { return splitter_; }

	static constexpr size_t kIncorrectWordOrdinal = std::numeric_limits<size_t>::max();
	size_t FindWordOrdinal(std::string_view word) const {
		if (auto it = wordsMap_.find(word); it != wordsMap_.end()) {
			return it->second;
		}
		return kIncorrectWordOrdinal;
	}
	bool ContainsWord(std::string_view word) const { return FindWordOrdinal(word) != kIncorrectWordOrdinal; }
	WordIdType GetWordIdByOrdinal(size_t ordinal) const noexcept {
		assertrx_dbg(ordinal < wordIds_.size());
		return wordIds_[ordinal];
	}
	std::u16string_view GetWord(WordIdType id) const noexcept { return words_.GetWord(id); }
	const char16_t* GetWordData(WordIdType id) const noexcept { return words_.GetWordData(id); }
	const char16_t* GetSuffixData(SuffixKey key) const noexcept { return words_.GetWordData(key); }
	SuffixWordInfo ResolveSuffix(SuffixKey key) const noexcept { return words_.ResolveSuffixId(key); }
	const SuffixTree* Suffixes(char16_t firstCh) const noexcept { return suffixes_[SuffixTreeIndex(firstCh)].get(); }
	const TypoSet* Typos(std::u16string_view typo) const noexcept {
		if (typo.empty()) {
			return nullptr;
		}
		return typos_[TypoSetIndex(typo.front())].get();
	}

	static size_t SuffixTreeIndex(char16_t firstCh) noexcept { return size_t(firstCh) & kSuffixTreeMask; }
	static size_t TypoSetIndex(char16_t firstCh) noexcept { return size_t(firstCh) & kTypoSetMask; }

	// TODO: #1688 Fix private class data isolation here
	// language and corresponding stemmer object
	std::unordered_map<std::string, stemmer> stemmers_;

	// translit generator for russian and english (returns word + weight)
	std::unique_ptr<Translit> translit_;
	std::unique_ptr<KbLayout> kbLayout_;
	std::unique_ptr<Synonyms> synonyms_;

	TermsBoostMapT stemmedTermsBoost;

	WordsStorage words_;
	tsl::hopscotch_map<std::string, size_t, word_hash, word_equal> wordsMap_;
	size_t wordsMapStringsHeapSize_ = 0;
	std::vector<std::unique_ptr<SuffixTree>> suffixes_;
	std::vector<std::unique_ptr<TypoSet>> typos_;
	std::vector<WordIdType> wordIds_;
	size_t wordsProcessed_ = 0;
	bool needRebuild_ = false;

	FTConfig* cfg_{nullptr};
	intrusive_ptr<const ISplitter> splitter_;
};

template <typename IdCont>
class [[nodiscard]] DataHolder : public IDataHolder {
public:
	explicit DataHolder(FTConfig* c);
	void Process(VDocsTexts& vdocsTexts, const std::vector<VDocPosting>& vdocs, size_t numDocsTotal, size_t numFields,
				 std::vector<VDocWordCounts>& vdocsWordsCountsByFields, bool multithreaded) final;
	DeletedScanStat OptimizeDeleted(const std::function<bool(uint32_t /*vdocId*/, VDocVersion)>& isDeleted,
									const index::ICancelable& cancelable) final;
	bool HasPendingOptimization() const noexcept final { return optimizeResumePending_; }
	size_t GetMemStat() override final;
	void Clear() override final;
	std::shared_ptr<const IdCont> GetWordOccurences(WordIdType id) const noexcept {
		const size_t ordinal = words_.GetWordOrdinal(id);
		assertrx(ordinal < wordOccurences_.size());
		return loadOccurences(ordinal);
	}
	std::shared_ptr<const IdCont> GetWordOccurencesByOrdinal(size_t ordinal) const noexcept {
		assertrx(ordinal < wordOccurences_.size());
		return loadOccurences(ordinal);
	}

private:
	// Per-holder sharded locks for CoW publish of wordOccurences_ entries.
	// Avoids libstdc++ atomic_shared_ptr's process-wide pool of 16 mutexes.
	// Padding (not alignas) keeps adjacent locks on different cache lines without making
	// DataHolder over-aligned — tcmalloc delete hooks break on alignas(>16) objects.
	static constexpr size_t kOccurencePtrShards = 64;
	static constexpr size_t kOccurencePtrShardMask = kOccurencePtrShards - 1;
	static constexpr size_t kOccurencePtrLockStride = 64;

	struct [[nodiscard]] PaddedOccurencePtrLock {
		read_write_spinlock mtx;
		char padding_[kOccurencePtrLockStride - sizeof(read_write_spinlock)];
	};
	static_assert(sizeof(PaddedOccurencePtrLock) == kOccurencePtrLockStride);
	static_assert(alignof(PaddedOccurencePtrLock) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);

	std::shared_ptr<IdCont> makeEmptyOccurences() const {
		if constexpr (std::is_same_v<IdCont, PackedIdRelVec>) {
			return std::make_shared<IdCont>(fieldBits_);
		} else {
			return std::make_shared<IdCont>();
		}
	}
	read_write_spinlock& occurencePtrLock(size_t ordinal) const noexcept {
		return occurencePtrLocks_[ordinal & kOccurencePtrShardMask].mtx;
	}
	std::shared_ptr<IdCont> loadOccurences(size_t ordinal) const noexcept {
		shared_lock lck(occurencePtrLock(ordinal));
		return wordOccurences_[ordinal];
	}
	void storeOccurences(size_t ordinal, std::shared_ptr<IdCont> ptr) noexcept {
		lock_guard lck(occurencePtrLock(ordinal));
		wordOccurences_[ordinal].swap(ptr);
	}
	void adjustOccurencesHeapSize(size_t oldHeapSize, size_t newHeapSize) noexcept {
		if (newHeapSize >= oldHeapSize) {
			wordOccurencesHeapSize_.fetch_add(newHeapSize - oldHeapSize, std::memory_order_relaxed);
		} else {
			wordOccurencesHeapSize_.fetch_sub(oldHeapSize - newHeapSize, std::memory_order_relaxed);
		}
	}
	template <bool Multithreaded>
	void processNewSuffixes(size_t start, size_t end);
	template <bool Multithreaded>
	void processNewTypos(size_t start, size_t end);
	template <bool Multithreaded>
	void shrinkWordOccurences();
	void collectSuffixes(size_t start, size_t end, std::vector<std::vector<SuffixKey>>& suffixesByFirstCh) const;
	TypoSet& getOrCreateTypoSet(char16_t firstCh);
	bool addExactAndSingleMissingTypos(WordIdType wordId, std::u16string_view word, size_t maxTyposInWord,
									   std::vector<std::vector<TypoKey>>& packedKeysByFirstCh);
	void addTwoMissingTyposWithFirstMissing(WordIdType wordId, std::u16string_view word,
											std::vector<std::vector<TypoKey>>& packedKeysByFirstCh);
	void addTwoMissingTyposWithoutFirstMissing(WordIdType wordId, std::u16string_view word, TypoSet& typoSet);
	void fillTypoSetShard(TypoSet& typoSet, const std::vector<TypoKey>& packedKeys, const std::vector<size_t>& wordIndexes);

	static constexpr size_t kOccurenceUpdateShards = 64;
	static constexpr size_t kOccurenceUpdateShardMask = kOccurenceUpdateShards - 1;

	NewWordsOccurencesPtr buildWordsMap(VDocsTexts::iterator textsBegin, std::vector<VDocPosting>::const_iterator vdocsBegin,
										std::vector<VDocWordCounts>::iterator wordsCountsBegin, size_t numDocs, size_t numFields,
										std::atomic<size_t>* tooLongWordsSkipped);
	NewWordsOccurencesPtr buildWordsMap(VDocsTexts& vdocsTexts, const std::vector<VDocPosting>& vdocs, size_t numFields,
										std::vector<VDocWordCounts>& vdocsWordsCountsByFields, std::atomic<size_t>* tooLongWordsSkipped);
	std::vector<NewWordsOccurencesPtr> buildWordsMapParallel(VDocsTexts& vdocsTexts, const std::vector<VDocPosting>& vdocs,
															 size_t numFields, std::vector<VDocWordCounts>& vdocsWordsCountsByFields,
															 std::atomic<size_t>* tooLongWordsSkipped);
	void updateOccurences(const NewWordsOccurencesPtr& nwo, std::vector<size_t>* updatedWordOrdinals);
	void updateOccurencesParallel(const std::vector<NewWordsOccurencesPtr>& nwos, std::vector<size_t>* updatedWordOrdinals);
	size_t appendOccurenceChain(const std::vector<Occurence>& occurrences, uint32_t firstIdx, IdRelSet& chain, IdCont& dst);
	void logPotentialStopWords(std::vector<size_t>& updatedWordOrdinals, size_t numDocsTotal) const;

	// Documents for each word, addressable by word ordinal.
	std::vector<std::shared_ptr<IdCont>> wordOccurences_;
	mutable std::array<PaddedOccurencePtrLock, kOccurencePtrShards> occurencePtrLocks_{};
	// Updated from OptimizeDeleted under ns shared lock; read by GetMemStat concurrently.
	std::atomic<size_t> wordOccurencesHeapSize_{0};
	// Bits reserved for field id inside PackedIdRelVec simple records.
	unsigned fieldBits_ = 0;
	// Progress cursor for incremental postings cleanup.
	// Allows resuming OptimizeDeleted() from the last processed word ordinal on cancellation.
	// Parallel scrub tasks use disjoint [from,to) ordinal ranges; on cancel the earliest
	// interrupted ordinal is stored here for the next sweep.
	size_t optimizeNextWordOrdinal_ = 0;
	bool optimizeResumePending_ = false;
	BS::thread_pool<>& threadPool_ = GetBackgroundThreadPool();
};

}  // namespace reindexer
