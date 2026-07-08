#pragma once
#include <memory>
#include <unordered_map>
#include "core/ft/config/ftconfig.h"
#include "core/ft/ft_fast/splitter.h"
#include "core/ft/idrelset.h"
#include "core/ft/limits.h"
#include "core/ft/stemmer.h"
#include "core/ft/variants/kblayout.h"
#include "core/ft/variants/synonyms.h"
#include "core/ft/variants/translit.h"
#include "estl/suffix_map.h"
#include "indextexttypes.h"
#include "typosmap.h"

namespace reindexer {

using VDocsTexts = std::vector<h_vector<std::pair<std::string_view, uint32_t>, 8>>;

static_assert(kMaxStepsCount <= TyposMap::kMaxStepNum, "TyposMap max steps overflow");
static_assert(kTypoStepNumBits <= TyposMap::kStepBits, "TyposMap max steps overflow");

class RdxContext;

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

enum [[nodiscard]] ProcessStatus { FullRebuild, RecommitLast, CreateNew };

class [[nodiscard]] IDataHolder {
public:
	using WordsMapType = tsl::hopscotch_map<size_t, h_vector<WordIdType, 1>>;

	struct [[nodiscard]] CommitStep {
		CommitStep() : wordOffset_(0) {}

		CommitStep(const CommitStep&) = delete;
		CommitStep& operator=(const CommitStep&) = delete;
		CommitStep(CommitStep&& /*rhs*/) noexcept = default;
		CommitStep& operator=(CommitStep&& /*rhs*/) = default;

		// Suffix map. suffix <-> original word id
		suffix_map<char, WordIdType> suffixes_;
		// Typos maps. typo string <-> original word id
		TyposMap typos_;
		// word offset for given step in DataHolder::words_
		uint32_t wordOffset_;

		void clear() {
			suffixes_.clear();
			typos_.clear();
		}
	};

	virtual ~IDataHolder() = default;
	virtual void Process(VDocsTexts& vdocsTexts, const std::vector<uint32_t>& vdocsIds, size_t numDocsTotal, size_t fieldSize,
						 bool multithread, std::vector<h_vector<float, 3>>& wordsCounts) = 0;
	virtual size_t GetMemStat() = 0;
	virtual void Clear() = 0;
	virtual void StartCommit(bool complete_updated) = 0;
	intrusive_ptr<const ISplitter> GetSplitter() const noexcept { return splitter_; }
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

	WordIdType FindWord(std::string_view word, bool searchLastStep) const;
	suffix_map<char, WordIdType>& GetLastStepSuffix() noexcept { return steps.back().suffixes_; }
	TyposMap& GetLastStepTypos() noexcept { return steps.back().typos_; }

	CommitStep& GetStep(WordIdType id) noexcept {
		assertrx(id.b.step_num < steps.size());
		return steps[id.b.step_num];
	}
	const CommitStep& GetStep(WordIdType id) const noexcept {
		assertrx(id.b.step_num < steps.size());
		return steps[id.b.step_num];
	}
	std::string_view GetWord(WordIdType id) const noexcept {
		assertrx(!id.IsEmpty());
		const CommitStep& step = GetStep(id);
		assertrx(id.b.id >= step.wordOffset_);
		assertrx(id.b.id - step.wordOffset_ < step.suffixes_.word_size());
		uint32_t wordShiftInStep = id.b.id - step.wordOffset_;
		const char* word = step.suffixes_.word_at(wordShiftInStep);
		const size_t wordLength = step.suffixes_.word_len_at(wordShiftInStep);
		return std::string_view(word, wordLength);
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

	// TODO: #1688 Fix private class data isolation here
	// language and corresponding stemmer object
	std::unordered_map<std::string, stemmer> stemmers_;

	// translit generator for russian and english (returns word + weight)
	std::unique_ptr<Translit> translit_;
	std::unique_ptr<KbLayout> kbLayout_;
	std::unique_ptr<Synonyms> synonyms_;

	TermsBoostMapT stemmedTermsBoost;

	std::vector<CommitStep> steps;
	WordsMapType stepsWords_;
	WordsMapType lastStepWords_;

	ProcessStatus status_{CreateNew};
	// Virtual documents, merged. Addressable by VDocIdType
	// Temp data for build

	FTConfig* cfg_{nullptr};
	// index - rowId, value vdocId (index in array vdocs_)
	intrusive_ptr<const ISplitter> splitter_;

private:
	[[noreturn]] static void throwWordIdOverflow(uint32_t id);
	[[noreturn]] void throwStepsOverflow() const;
};

template <typename IdCont>
class [[nodiscard]] DataHolder : public IDataHolder {
public:
	explicit DataHolder(FTConfig* c);
	void Process(VDocsTexts& vdocsTexts, const std::vector<uint32_t>& vdocsIds, size_t numDocsTotal, size_t fieldSize, bool multithread,
				 std::vector<h_vector<float, 3>>& wordsCounts) final;
	size_t GetMemStat() override final;
	void StartCommit(bool complte_updated) override final;
	void Clear() override final;
	std::vector<PackedWordEntry<IdCont>>& GetWords() noexcept { return words_; }
	const std::vector<PackedWordEntry<IdCont>>& GetWords() const noexcept { return words_; }
	PackedWordEntry<IdCont>& GetWordEntry(WordIdType id) noexcept {
		assertrx(!id.IsEmpty());
		assertrx(id.b.id < words_.size());
		return words_[id.b.id];
	}
	const PackedWordEntry<IdCont>& GetWordEntry(WordIdType id) const noexcept {
		assertrx(!id.IsEmpty());
		assertrx(id.b.id < words_.size());
		return words_[id.b.id];
	}
	std::vector<PackedWordEntry<IdCont>> words_;
};

}  // namespace reindexer
