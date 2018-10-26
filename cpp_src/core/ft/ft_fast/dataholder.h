#pragma once
#include <memory>
#include <unordered_map>
#include "core/ft/config/ftfastconfig.h"
#include "core/ft/ft_fuzzy/searchers/isearcher.h"
#include "core/ft/idrelset.h"
#include "core/ft/stemmer.h"
#include "estl/fast_hash_map.h"
#include "estl/flat_str_map.h"
#include "estl/suffix_map.h"
#include "ftfastkeyentry.h"
#include "indextexttypes.h"

using std::unique_ptr;
using std::vector;
using std::pair;
using std::unordered_map;
using std::vector;

namespace reindexer {

struct VDocEntry {
#ifdef REINDEX_FT_EXTRA_DEBUG
	const void* keyDoc;
#endif
	const void* keyEntry;
	h_vector<float, 3> wordsCount;
	h_vector<float, 3> mostFreqWordCount;
};

class PackedWordEntry {
public:
	PackedIdRelSet vids_;
};
class WordEntry {
public:
	IdRelSet vids_;
	bool virtualWord = false;
};
enum ProcessStatus { FullRebuild, RecommitLast, CreateNewWithWords, CreateNewNoWords };

class DataHolder {
public:
	typedef fast_hash_map<WordIdType, pair<size_t, size_t>, WordIdTypeHash, WordIdTypequal> FondWordsType;
	struct CommitStep {
		//
		vector<PackedWordEntry> words_;
		// Suffix map. suffix <-> original word id
		suffix_map<string, WordIdType> suffixes_;

		uint32_t wordOffset_;
		bool need_find = false;

		void clear(bool nF = false) {
			suffixes_.clear();
			words_.clear();
			wordOffset_ = 0;
			need_find = nF;
		}
	};

	vector<PackedWordEntry>& GetWords();
	suffix_map<std::string, WordIdType>& GetSuffix();
	// returns id and found or not found
	WordIdType findWord(const string& word);
	WordIdType BuildWordId(uint32_t id);
	PackedWordEntry& getWordById(WordIdType id, const CommitStep& step);

	size_t GetMemStat();
	void SetWordsOffset(uint32_t word_offset);
	uint32_t GetWordsOffset();

	CommitStep& GetStep(WordIdType id);

	uint32_t GetSuffixWordId(WordIdType id);
	uint32_t GetSuffixWordId(WordIdType id, const CommitStep& step);
	bool IsGlobalWords();
	void StartCommit(bool complte_updated);
	bool NeedRebuild(bool complte_updated);
	bool NeedFind();
	bool NeedClear(bool complte_updated);
	void Clear();

	vector<CommitStep> steps;
	vector<double> avgWordsCount_;
	vector<PackedWordEntry> words_;
	// Typos map. typo string <-> original word id
	flat_str_multimap<string, WordIdType> typos_;
	// Virtual documents, merged. Addresable by VDocIdType
	// Temp data for build
	vector<h_vector<pair<string_view, uint32_t>, 8>> vdocsTexts;
	size_t vodcsOffset_;
	size_t szCnt;
	unordered_map<string, stemmer> stemmers_;
	ProcessStatus status_;

	vector<search_engine::ISeacher::Ptr> searchers_;

	vector<VDocEntry> vdocs_;
	vector<unique_ptr<string>> bufStrs_;
};
}  // namespace reindexer
