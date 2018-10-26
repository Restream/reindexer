#include "dataholder.h"

namespace reindexer {
const int kMaxCommitSteps = 10;

const int kMaxCommitsWithFind = 5;

const int KMaxCommitSize = 5000;

vector<PackedWordEntry>& DataHolder::GetWords() {
	if (IsGlobalWords()) {
		return words_;
	}
	assert(!steps.empty());
	return steps.back().words_;
}
suffix_map<string, WordIdType>& DataHolder::GetSuffix() { return steps.back().suffixes_; }

WordIdType DataHolder::findWord(const string& word) {
	for (auto& step : steps) {
		auto it = step.suffixes_.lower_bound(word);
		if (it != step.suffixes_.end()) {
			return it->second;
		}
	}
	WordIdType id;
	id.setEmpty();
	return id;
}
size_t DataHolder::GetMemStat() {
	size_t res = 0;
	for (auto& step : steps) {
		res += typos_.heap_size() + step.suffixes_.heap_size();

		for (auto& w : words_) {
			res += sizeof(w) + w.vids_.heap_size();
		}
		res += vdocs_.capacity() * sizeof(VDocEntry);
	}
	return res;
}

void DataHolder::SetWordsOffset(uint32_t word_offset) {
	assert(!steps.empty());
	steps.back().wordOffset_ = word_offset;
}
uint32_t DataHolder::GetWordsOffset() {
	assert(!steps.empty());
	return steps.back().wordOffset_;
}
WordIdType DataHolder::BuildWordId(uint32_t id) {
	WordIdType wId;
	assert(id < kWordIdMaxIdVal);
	assert(steps.size() - 1 < kWordIdMaxStepVal);

	wId.b.id = id;
	wId.b.step_num = steps.size() - 1;

	return wId;
}
bool DataHolder::NeedClear(bool complte_updated) {
	if (NeedRebuild(complte_updated) || steps.back().suffixes_.word_size() > KMaxCommitSize) return true;
	return false;
}

bool DataHolder::IsGlobalWords() {
	if (status_ == CreateNewNoWords || status_ == FullRebuild || (status_ == RecommitLast && steps.back().words_.empty())) {
		return true;
	}
	return false;
}

uint32_t DataHolder::GetSuffixWordId(WordIdType id, const CommitStep& step) {
	assert(!id.isEmpty());
	assert(id.b.step_num < steps.size());

	if (step.words_.empty()) {
		assert(id.b.id >= step.wordOffset_);
		assert(id.b.id - step.wordOffset_ < step.suffixes_.word_size());
		return id.b.id - step.wordOffset_;
	}
	assert(id.b.id < steps[id.b.step_num].suffixes_.word_size());
	return id.b.id;
}

uint32_t DataHolder::GetSuffixWordId(WordIdType id) { return GetSuffixWordId(id, steps.back()); }

DataHolder::CommitStep& DataHolder::GetStep(WordIdType id) {
	assert(id.b.step_num < steps.size());
	return steps[id.b.step_num];
}

PackedWordEntry& DataHolder::getWordById(WordIdType id, const CommitStep& step) {
	assert(!id.isEmpty());
	if (step.words_.empty()) {
		assert(id.b.id < words_.size());
		return words_[id.b.id];
	}
	assert(id.b.step_num < steps.size());
	assert(id.b.id < steps[id.b.step_num].words_.size());
	return steps[id.b.step_num].words_[id.b.id];
}

bool DataHolder::NeedFind() {
	if (status_ == FullRebuild || status_ == CreateNewWithWords || !(status_ == RecommitLast && steps.back().need_find)) return false;
	return true;
}

void DataHolder::Clear() {
	steps.resize(1);
	steps.front().clear();
	avgWordsCount_.clear();
	words_.clear();
	typos_.clear();
	vdocs_.clear();
	vdocsTexts.clear();
	vodcsOffset_ = 0;
	szCnt = 0;
}
void DataHolder::StartCommit(bool complte_updated) {
	if (NeedRebuild(complte_updated)) {
		status_ = FullRebuild;

		Clear();
	} else if (steps.back().suffixes_.word_size() < KMaxCommitSize) {
		status_ = RecommitLast;
		if (!steps.back().words_.empty()) steps.back().clear(true);
		steps.back().clear();
	} else if (steps.size() < kMaxCommitsWithFind) {
		status_ = CreateNewNoWords;
		steps.push_back({});
	} else {
		// TODO we don't test this with words for now
		// status_ = CreateNewWithWords;
		status_ = CreateNewNoWords;
		steps.push_back({});
	}
	return;
}  // namespace reindexer
bool DataHolder::NeedRebuild(bool complte_updated) {
	return ((steps.size() == 1 && steps.front().suffixes_.word_size() < KMaxCommitSize) || steps.empty() ||
			steps.size() > kMaxCommitSteps || complte_updated);
}

}  // namespace reindexer
