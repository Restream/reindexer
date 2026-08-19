#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
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
#include "estl/h_vector.h"
#include "estl/intrusive_ptr.h"
#include "indextexttypes.h"
#include "tools/assertrx.h"
#include "tools/background_thread_pool.h"
#include "vendor/hopscotch/hopscotch_map.h"

namespace reindexer {

using VDocsTexts = std::vector<h_vector<std::pair<std::string_view, uint32_t>, 8>>;

struct [[nodiscard]] Occurence {
	VDocIdType docId = 0;
	uint32_t link = std::numeric_limits<uint32_t>::max();
	PosType pos;
};

class [[nodiscard]] NewWordsOccurences {
public:
	static constexpr uint32_t kInvalidLink = std::numeric_limits<uint32_t>::max();
	using WordIndices = std::pair<uint32_t, uint32_t>;
	using words_map_t =
		tsl::hopscotch_map<std::string, WordIndices, word_hash, word_equal, std::allocator<std::pair<std::string, WordIndices>>, 30, true>;

	void AddPrehashed(std::string_view word, size_t whash, VDocIdType docId, unsigned pos, unsigned field, unsigned arrayIdx);
	void Add(std::string word, VDocIdType docId, unsigned pos, unsigned field, unsigned arrayIdx);

	const std::vector<Occurence>& Occurences() const noexcept { return occurrences_; }
	const words_map_t& Words() const noexcept { return words_; }

private:
	void appendOccurence(VDocIdType docId, unsigned pos, unsigned field, unsigned arrayIdx, words_map_t::iterator wordIt);

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
	virtual void Process(VDocsTexts& vdocsTexts, const std::vector<uint32_t>& vdocsIds, size_t numDocsTotal, size_t numFields,
						 std::vector<h_vector<float, 3>>& vdocsWordsCountsByFields, bool multithreaded) = 0;
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
	// index - rowId, value vdocId (index in array vdocs_)
	intrusive_ptr<const ISplitter> splitter_;
};

template <typename IdCont>
class [[nodiscard]] DataHolder : public IDataHolder {
public:
	explicit DataHolder(FTConfig* c);
	void Process(VDocsTexts& vdocsTexts, const std::vector<uint32_t>& vdocsIds, size_t numDocsTotal, size_t numFields,
				 std::vector<h_vector<float, 3>>& vdocsWordsCountsByFields, bool multithreaded) final;
	size_t GetMemStat() override final;
	void Clear() override final;
	IdCont& GetWordOccurences(WordIdType id) noexcept {
		const size_t ordinal = words_.GetWordOrdinal(id);
		assertrx(ordinal < wordOccurences_.size());
		return wordOccurences_[ordinal];
	}
	const IdCont& GetWordOccurences(WordIdType id) const noexcept {
		const size_t ordinal = words_.GetWordOrdinal(id);
		assertrx(ordinal < wordOccurences_.size());
		return wordOccurences_[ordinal];
	}
	const IdCont& GetWordOccurencesByOrdinal(size_t ordinal) const noexcept {
		assertrx(ordinal < wordOccurences_.size());
		return wordOccurences_[ordinal];
	}

private:
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

	NewWordsOccurencesPtr buildWordsMap(VDocsTexts::iterator textsBegin, std::vector<uint32_t>::const_iterator idsBegin,
										std::vector<h_vector<float, 3>>::iterator wordsCountsBegin, size_t numDocs, size_t numFields,
										std::atomic<size_t>* tooLongWordsSkipped);
	NewWordsOccurencesPtr buildWordsMap(VDocsTexts& vdocsTexts, const std::vector<uint32_t>& vdocsIds, size_t numFields,
										std::vector<h_vector<float, 3>>& vdocsWordsCountsByFields,
										std::atomic<size_t>* tooLongWordsSkipped);
	std::vector<NewWordsOccurencesPtr> buildWordsMapParallel(VDocsTexts& vdocsTexts, const std::vector<uint32_t>& vdocsIds,
															 size_t numFields, std::vector<h_vector<float, 3>>& vdocsWordsCountsByFields,
															 std::atomic<size_t>* tooLongWordsSkipped);
	void updateOccurences(const NewWordsOccurencesPtr& nwo, std::vector<size_t>* updatedWordOrdinals);
	void updateOccurencesParallel(const std::vector<NewWordsOccurencesPtr>& nwos, std::vector<size_t>* updatedWordOrdinals);
	size_t appendOccurenceChain(const std::vector<Occurence>& occurrences, uint32_t firstIdx, IdRelSet& chain, IdCont& dst);
	void logPotentialStopWords(std::vector<size_t>& updatedWordOrdinals, size_t numDocsTotal) const;

	// Documents for each word, addressable by word ordinal.
	std::vector<IdCont> wordOccurences_;
	size_t wordOccurencesHeapSize_ = 0;
	BS::thread_pool<>& threadPool_ = GetBackgroundThreadPool();
};

}  // namespace reindexer
