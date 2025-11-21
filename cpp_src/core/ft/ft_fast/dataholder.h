#pragma once
#include <memory>
#include <unordered_map>
#include "core/ft/config/ftfastconfig.h"
#include "core/ft/filters/itokenfilter.h"
#include "core/ft/filters/synonyms.h"
#include "core/ft/ft_fast/splitter.h"
#include "core/ft/idrelset.h"
#include "core/ft/limits.h"
#include "core/ft/stemmer.h"
#include "core/ft/typos.h"
#include "core/index/indextext/ftkeyentry.h"
#include "estl/flat_str_map.h"
#include "estl/suffix_map.h"
#include "indextexttypes.h"

namespace reindexer {

class RdxContext;

// unique document in the namespace (if different rows contain the same text document, then it will correspond to one vdoc)
struct [[nodiscard]] VDocEntry {
#ifdef REINDEX_FT_EXTRA_DEBUG
	std::string keyDoc;
#endif

	const FtKeyEntryData* keyEntry{nullptr};
	h_vector<float, 3> wordsCount;
	h_vector<float, 3> mostFreqWordCount;

	bool IsRemoved() const noexcept { return keyEntry == nullptr; }
};

// documents for the word

template <typename IdCont>
class [[nodiscard]] PackedWordEntry;

template <>
class [[nodiscard]] PackedWordEntry<PackedIdRelVec> {
public:
	PackedWordEntry() noexcept = default;
	PackedWordEntry(const PackedWordEntry&) = delete;
	PackedWordEntry(PackedWordEntry&&) noexcept = default;
	PackedWordEntry& operator=(const PackedWordEntry&) = delete;
	PackedWordEntry& operator=(PackedWordEntry&&) noexcept = default;

	PackedIdRelVec vids;
	// Necessary for correct rebuilding of the last step
	PackedIdRelVec::state cur_step_state;
	size_t cur_step_data_size = 0;

	void SaveState() { vids.get_state(cur_step_state, cur_step_data_size); }

	void RestoreState() { vids.erase_back(cur_step_state, cur_step_data_size); }
};

template <>
class [[nodiscard]] PackedWordEntry<IdRelVec> {
public:
	PackedWordEntry() noexcept = default;
	PackedWordEntry(const PackedWordEntry&) = delete;
	PackedWordEntry(PackedWordEntry&&) noexcept = default;
	PackedWordEntry& operator=(const PackedWordEntry&) = delete;
	PackedWordEntry& operator=(PackedWordEntry&&) noexcept = default;

	IdRelVec vids;
	// Necessary for correct rebuilding of the last step
	size_t cur_step_data_size = 0;

	void SaveState() { cur_step_data_size = vids.pos(vids.end()); }

	void RestoreState() { vids.erase_back(cur_step_data_size); }
};

class [[nodiscard]] WordEntry {
public:
	WordEntry() noexcept = default;
	WordEntry(const IdRelSet& _vids) : vids_(_vids) {}
	WordEntry(const WordEntry&) = delete;
	WordEntry(WordEntry&&) noexcept = default;
	WordEntry& operator=(const WordEntry&) = delete;
	WordEntry& operator=(WordEntry&&) noexcept = default;

	// Explicit copy
	WordEntry MakeCopy() const { return WordEntry(vids_); }

	IdRelSet vids_;
};
enum [[nodiscard]] ProcessStatus { FullRebuild, RecommitLast, CreateNew };

struct [[nodiscard]] WordTypo {
	WordTypo() = default;
	explicit WordTypo(WordIdType w) noexcept : word(w) {}
	explicit WordTypo(WordIdType w, const typos_context::TyposVec& p) noexcept : word(w), positions(p) {}
	bool IsMultiValue() const noexcept { return multiMapFlag; }
	void SetMultiValueFlag(bool val) noexcept { multiMapFlag = val; }
	int32_t GetWordID() const noexcept { return word.GetID(); }
	void SetWordID(int32_t id) noexcept { word.SetID(id); }

	WordIdType word;
	typos_context::TyposVec positions;
	uint8_t multiMapFlag = 0;
};

static_assert(sizeof(WordTypo) <= 8, "This size is matter for overall size of the typos map");

class [[nodiscard]] IDataHolder {
public:
	struct [[nodiscard]] CommitStep {
		CommitStep() : wordOffset_(0) {}

		CommitStep(const CommitStep&) = delete;
		CommitStep& operator=(const CommitStep&) = delete;
		CommitStep(CommitStep&& /*rhs*/) noexcept = default;
		CommitStep& operator=(CommitStep&& /*rhs*/) = default;

		// Suffix map. suffix <-> original word id
		suffix_map<char, WordIdType> suffixes_;
		// Typos maps. typo string <-> original word id
		// typosHalf_ contains words with <=maxTypos/2 typos
		flat_str_multimap<char, WordTypo> typosHalf_;
		// typosMax_ contains words with MaxTyposInWord() typos if MaxTyposInWord() != maxTypos/2
		flat_str_multimap<char, WordTypo> typosMax_;
		// word offset for given step in DataHolder::words_
		uint32_t wordOffset_;

		void clear() {
			suffixes_.clear();
			typosHalf_.clear();
			typosMax_.clear();
		}
	};

	virtual ~IDataHolder() = default;
	virtual void Process(size_t fieldSize, bool multithread) = 0;
	virtual size_t GetMemStat() = 0;
	virtual void Clear() = 0;
	virtual void StartCommit(bool complete_updated) = 0;
	intrusive_ptr<const ISplitter> GetSplitter() const noexcept { return splitter_; }
	CommitStep& GetStep(WordIdType id) noexcept {
		assertrx(id.b.step_num < steps.size());
		return steps[id.b.step_num];
	}
	const CommitStep& GetStep(WordIdType id) const noexcept {
		assertrx(id.b.step_num < steps.size());
		return steps[id.b.step_num];
	}
	bool NeedRebuild(bool complete_updated) const noexcept {
		return steps.empty() || complete_updated || steps.size() >= size_t(cfg_->maxRebuildSteps) ||
			   (steps.size() == 1 && steps.front().suffixes_.word_size() < size_t(cfg_->maxStepSize));
	}
	bool NeedRecommitLast() const noexcept { return steps.back().suffixes_.word_size() < size_t(cfg_->maxStepSize); }
	void SetWordsOffset(uint32_t word_offset) noexcept {
		assertrx(!steps.empty());
		if (status_ == CreateNew) {
			steps.back().wordOffset_ = word_offset;
		}
	}
	bool NeedClear(bool complte_updated) const noexcept { return NeedRebuild(complte_updated) || !NeedRecommitLast(); }
	suffix_map<char, WordIdType>& GetSuffix() noexcept { return steps.back().suffixes_; }
	flat_str_multimap<char, WordTypo>& GetTyposHalf() noexcept { return steps.back().typosHalf_; }
	flat_str_multimap<char, WordTypo>& GetTyposMax() noexcept { return steps.back().typosMax_; }
	WordIdType findWord(std::string_view word) const;
	uint32_t GetSuffixWordId(WordIdType id) const noexcept { return GetSuffixWordId(id, steps.back()); }
	uint32_t GetSuffixWordId(WordIdType id, const CommitStep& step) const noexcept {
		assertrx(!id.IsEmpty());
		assertrx(id.b.step_num < steps.size());

		assertrx(id.b.id >= step.wordOffset_);
		assertrx(id.b.id - step.wordOffset_ < step.suffixes_.word_size());
		return id.b.id - step.wordOffset_;
	}
	uint32_t GetWordsOffset() const noexcept {
		assertrx(!steps.empty());
		return steps.back().wordOffset_;
	}
	// returns id and found or not found
	WordIdType BuildWordId(uint32_t id) const {
		WordIdType wId;
		if (id > kWordIdMaxIdVal) [[unlikely]] {
			throwWordIdOverflow(id);
		}
		if (steps.size() > kMaxStepsCount) [[unlikely]] {
			throwStepsOverflow();
		}

		wId.b.id = id;
		wId.b.step_num = steps.size() - 1;
		return wId;
	}
	std::string Dump() const;

private:
	[[noreturn]] static void throwWordIdOverflow(uint32_t id);
	[[noreturn]] void throwStepsOverflow() const;

public:	 // TODO: #1688 Fix private class data isolation here
	// language and corresponding stemmer object
	std::unordered_map<std::string, stemmer> stemmers_;

	// translit generator for russian and english (returns word + weight)
	ITokenFilter::Ptr translit_;
	ITokenFilter::Ptr kbLayout_;
	ITokenFilter::Ptr compositeWordsSplitter_;

	Synonyms::Ptr synonyms_;

	std::vector<CommitStep> steps;
	// array of unique documents
	std::vector<VDocEntry> vdocs_;
	size_t cur_vdoc_pos_ = 0;
	ProcessStatus status_{CreateNew};
	std::vector<double> avgWordsCount_;
	// Virtual documents, merged. Addressable by VDocIdType
	// Temp data for build
	std::vector<h_vector<std::pair<std::string_view, uint32_t>, 8>> vdocsTexts;
	std::vector<std::unique_ptr<std::string>> bufStrs_;
	size_t vdocsOffset_{0};
	size_t szCnt{0};
	FtFastConfig* cfg_{nullptr};
	// index - rowId, value vdocId (index in array vdocs_)
	std::vector<size_t> rowId2Vdoc_;
	intrusive_ptr<const ISplitter> splitter_;
};

template <typename IdCont>
class [[nodiscard]] DataHolder : public IDataHolder {
public:
	explicit DataHolder(FtFastConfig* c);
	void Process(size_t fieldSize, bool multithread) final;
	size_t GetMemStat() override final;
	void StartCommit(bool complte_updated) override final;
	void Clear() override final;
	std::vector<PackedWordEntry<IdCont>>& GetWords() noexcept { return words_; }
	const std::vector<PackedWordEntry<IdCont>>& GetWords() const noexcept { return words_; }
	PackedWordEntry<IdCont>& GetWordById(WordIdType id) noexcept {
		assertrx(!id.IsEmpty());
		assertrx(id.b.id < words_.size());
		return words_[id.b.id];
	}
	const PackedWordEntry<IdCont>& GetWordById(WordIdType id) const noexcept {
		assertrx(!id.IsEmpty());
		assertrx(id.b.id < words_.size());
		return words_[id.b.id];
	}
	std::vector<PackedWordEntry<IdCont>> words_;
};

}  // namespace reindexer
