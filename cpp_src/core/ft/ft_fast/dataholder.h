#pragma once
#include <memory>
#include <unordered_map>
#include "core/ft/areaholder.h"
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

class RdxContext;

struct VDocEntry {
#ifdef REINDEX_FT_EXTRA_DEBUG
	std::string keyDoc;
#endif
	const FtKeyEntryData* keyEntry;
	h_vector<float, 3> wordsCount;
	h_vector<float, 3> mostFreqWordCount;
};

template <typename IdCont>
class PackedWordEntry {
public:
	IdCont vids_;
	size_t cur_step_pos_ = 0;
};
class WordEntry {
public:
	IdRelSet vids_;
	bool virtualWord = false;
};
enum ProcessStatus { FullRebuild, RecommitLast, CreateNew };

class IDataHolder {
public:
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
	// Final information about found document
	struct MergeInfo {
		IdType id;		 // Virual of merged document (index in vdocs)
		int32_t proc;	 // Rank of document
		int8_t matched;	 // Count of matched terms in document
		int8_t field;	 // Field index, where was match
		AreaHolder::UniquePtr holder;
	};

	struct MergeData : public vector<MergeInfo> {
		int maxRank = 0;
	};

	virtual ~IDataHolder() = default;
	virtual MergeData Select(FtDSLQuery& dsl, size_t fieldSize, bool needArea, int maxAreasInDoc, bool inTransaction,
							 const RdxContext&) = 0;
	virtual void Process(size_t fieldSize, bool multithread) = 0;
	virtual size_t GetMemStat() = 0;
	virtual void Clear() = 0;
	virtual void StartCommit(bool complte_updated) = 0;
	void SetConfig(FtFastConfig* cfg);
	CommitStep& GetStep(WordIdType id);
	bool NeedRebuild(bool complte_updated);
	bool NeedRecomitLast();
	void SetWordsOffset(uint32_t word_offset);
	bool NeedClear(bool complte_updated);
	suffix_map<char, WordIdType>& GetSuffix() noexcept { return steps.back().suffixes_; }
	flat_str_multimap<char, WordIdType>& GetTyposHalf() noexcept { return steps.back().typosHalf_; }
	flat_str_multimap<char, WordIdType>& GetTyposMax() noexcept { return steps.back().typosMax_; }
	WordIdType findWord(std::string_view word);
	uint32_t GetSuffixWordId(WordIdType id);
	uint32_t GetSuffixWordId(WordIdType id, const CommitStep& step);
	uint32_t GetWordsOffset();
	// returns id and found or not found
	WordIdType BuildWordId(uint32_t id);
	string Dump();

	std::unordered_map<string, stemmer> stemmers_;
	ITokenFilter::Ptr translit_;
	ITokenFilter::Ptr kbLayout_;
	ITokenFilter::Ptr synonyms_;
	std::vector<CommitStep> steps;
	std::vector<VDocEntry> vdocs_;
	size_t cur_vdoc_pos_ = 0;
	ProcessStatus status_{CreateNew};
	vector<double> avgWordsCount_;
	// Virtual documents, merged. Addresable by VDocIdType
	// Temp data for build
	vector<h_vector<pair<std::string_view, uint32_t>, 8>> vdocsTexts;
	vector<std::unique_ptr<string>> bufStrs_;
	size_t vodcsOffset_{0};
	size_t szCnt{0};
	FtFastConfig* cfg_{nullptr};
};

template <typename IdCont>
class DataHolder : public IDataHolder {
public:
	MergeData Select(FtDSLQuery& dsl, size_t fieldSize, bool needArea, int maxAreasInDoc, bool inTransaction,
					 const RdxContext&) override final;
	void Process(size_t fieldSize, bool multithread) override final;
	size_t GetMemStat() override final;
	void StartCommit(bool complte_updated) override final;
	void Clear() override final;
	vector<PackedWordEntry<IdCont>>& GetWords() noexcept { return words_; }
	PackedWordEntry<IdCont>& getWordById(WordIdType id);

	vector<PackedWordEntry<IdCont>> words_;
};

extern template class DataHolder<PackedIdRelVec>;
extern template class DataHolder<IdRelVec>;

}  // namespace reindexer
