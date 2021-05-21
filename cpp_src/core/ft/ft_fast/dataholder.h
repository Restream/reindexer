#pragma once
#include <memory>
#include <unordered_map>
#include "core/ft/config/ftfastconfig.h"
#include "core/ft/filters/itokenfilter.h"
#include "core/ft/idrelset.h"
#include "core/ft/stemmer.h"
#include "core/index/indextext/ftkeyentry.h"
#include "estl/fast_hash_map.h"
#include "estl/flat_str_map.h"
#include "estl/suffix_map.h"
#include "indextexttypes.h"

namespace reindexer {

// #define REINDEX_FT_EXTRA_DEBUG 1

struct VDocEntry {
#ifdef REINDEX_FT_EXTRA_DEBUG
	std::string keyDoc;
#endif
	const FtKeyEntryData* keyEntry;
	h_vector<float, 3> wordsCount;
	h_vector<float, 3> mostFreqWordCount;
};

class PackedWordEntry {
public:
	PackedIdRelSet vids_;
	size_t cur_step_pos_ = 0;
};
class WordEntry {
public:
	IdRelSet vids_;
	bool virtualWord = false;
};
enum ProcessStatus { FullRebuild, RecommitLast, CreateNew };

class DataHolder {
public:
	typedef fast_hash_map<WordIdType, pair<size_t, size_t>, WordIdTypeHash, WordIdTypequal> FondWordsType;
	struct CommitStep {
		CommitStep() : wordOffset_(0) {}

		CommitStep(const CommitStep&) = delete;
		CommitStep& operator=(const CommitStep&) = delete;
		CommitStep(CommitStep&& /*rhs*/) noexcept = default;
		CommitStep& operator=(CommitStep&& /*rhs*/) = default;

		// Suffix map. suffix <-> original word id
		suffix_map<char, WordIdType> suffixes_;
		// Typos maps. typo string <-> original word id
		// typosHalf_ contains words with <=maxTypos/2 typos
		flat_str_multimap<char, WordIdType> typosHalf_;
		// typosMax_ contains words with MaxTyposInWord() typos if MaxTyposInWord() != maxTypos/2
		flat_str_multimap<char, WordIdType> typosMax_;
		uint32_t wordOffset_;

		void clear() {
			suffixes_.clear();
			typosHalf_.clear();
			typosMax_.clear();
		}
	};
	vector<PackedWordEntry>& GetWords() noexcept { return words_; }
	suffix_map<char, WordIdType>& GetSuffix() noexcept { return steps.back().suffixes_; }
	void SetConfig(FtFastConfig* cfg);

	flat_str_multimap<char, WordIdType>& GetTyposHalf() noexcept { return steps.back().typosHalf_; }
	flat_str_multimap<char, WordIdType>& GetTyposMax() noexcept { return steps.back().typosMax_; }
	// returns id and found or not found
	WordIdType findWord(std::string_view word);
	WordIdType BuildWordId(uint32_t id);
	PackedWordEntry& getWordById(WordIdType id);
	string Dump();

	size_t GetMemStat();
	void SetWordsOffset(uint32_t word_offset);
	uint32_t GetWordsOffset();

	CommitStep& GetStep(WordIdType id);

	uint32_t GetSuffixWordId(WordIdType id);
	uint32_t GetSuffixWordId(WordIdType id, const CommitStep& step);
	void StartCommit(bool complte_updated);
	bool NeedRebuild(bool complte_updated);
	bool NeedRecomitLast();

	bool NeedClear(bool complte_updated);
	void Clear();

	vector<CommitStep> steps;
	vector<double> avgWordsCount_;
	vector<PackedWordEntry> words_;

	// Virtual documents, merged. Addresable by VDocIdType
	// Temp data for build
	vector<h_vector<pair<std::string_view, uint32_t>, 8>> vdocsTexts;
	size_t vodcsOffset_;
	size_t szCnt;
	std::unordered_map<string, stemmer> stemmers_;
	ProcessStatus status_;

	ITokenFilter::Ptr translit_;
	ITokenFilter::Ptr kbLayout_;
	ITokenFilter::Ptr synonyms_;

	vector<VDocEntry> vdocs_;
	vector<std::unique_ptr<string>> bufStrs_;
	size_t cur_vdoc_pos_ = 0;

	FtFastConfig* cfg_;
};
}  // namespace reindexer
