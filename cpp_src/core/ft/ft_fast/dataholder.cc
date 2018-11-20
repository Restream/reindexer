#include "dataholder.h"

namespace reindexer {

vector<PackedWordEntry>& DataHolder::GetWords() { return words_; }
suffix_map<string, WordIdType>& DataHolder::GetSuffix() { return steps.back().suffixes_; }

flat_str_multimap<string, WordIdType>& DataHolder::GetTypos() { return steps.back().typos_; }

WordIdType DataHolder::findWord(const string& word) {
	WordIdType id;
	id.setEmpty();
	if (steps.size() <= 1) return id;

	for (auto step = steps.begin(); step != steps.end() - 1; ++step) {
		auto it = step->suffixes_.lower_bound(word);
		if (it != step->suffixes_.end() && size_t(step->suffixes_.word_len_at(GetSuffixWordId(it->second, *step))) == word.size()) {
			return it->second;
		}
	}

	return id;
}
size_t DataHolder::GetWordsSize() {
	size_t res = 0;
	for (auto step = steps.begin(); step != steps.end() - 1; ++step) {
		res += step->suffixes_.word_size();
	}
	return res;
}

size_t DataHolder::GetMemStat() {
	size_t res = 0;
	for (auto& step : steps) {
		res += step.typos_.heap_size() + step.suffixes_.heap_size();

		for (auto& w : words_) {
			res += sizeof(w) + w.vids_.heap_size();
		}
		res += vdocs_.capacity() * sizeof(VDocEntry);
	}
	return res;
}

void DataHolder::SetWordsOffset(uint32_t word_offset) {
	assert(!steps.empty());
	if (status_ == CreateNew) steps.back().wordOffset_ = word_offset;
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
	if (NeedRebuild(complte_updated) || steps.back().suffixes_.word_size() > size_t(cfg_->maxStepSize)) return true;
	return false;
}

uint32_t DataHolder::GetSuffixWordId(WordIdType id, const CommitStep& step) {
	assert(!id.isEmpty());
	assert(id.b.step_num < steps.size());

	assert(id.b.id >= step.wordOffset_);
	assert(id.b.id - step.wordOffset_ < step.suffixes_.word_size());
	return id.b.id - step.wordOffset_;
}

uint32_t DataHolder::GetSuffixWordId(WordIdType id) { return GetSuffixWordId(id, steps.back()); }

DataHolder::CommitStep& DataHolder::GetStep(WordIdType id) {
	assert(id.b.step_num < steps.size());
	return steps[id.b.step_num];
}

PackedWordEntry& DataHolder::getWordById(WordIdType id) {
	assert(!id.isEmpty());
	assert(id.b.id < words_.size());
	return words_[id.b.id];
}

void DataHolder::Clear() {
	steps.resize(1);
	steps.front().clear();
	avgWordsCount_.clear();
	words_.clear();
	vdocs_.clear();
	vdocsTexts.clear();
	vodcsOffset_ = 0;
	szCnt = 0;
}
void DataHolder::StartCommit(bool complte_updated) {
	if (NeedRebuild(complte_updated)) {
		status_ = FullRebuild;

		Clear();
	} else if (steps.back().suffixes_.word_size() < size_t(cfg_->maxStepSize)) {
		status_ = RecommitLast;
		words_.erase(words_.begin() + steps.back().wordOffset_, words_.end());

		for (auto& word : words_) {
			word.vids_.erase_back(word.cur_step_pos_);
		}

		steps.back().clear();
	} else {
		status_ = CreateNew;
		steps.emplace_back(CommitStep{});
	}
	return;
}  // namespace reindexer
bool DataHolder::NeedRebuild(bool complte_updated) {
	return ((steps.size() == 1 && steps.front().suffixes_.word_size() < size_t(cfg_->maxStepSize)) || steps.empty() ||
			steps.size() >= size_t(cfg_->maxRebuildSteps) || complte_updated);
}
void DataHolder::SetConfig(FtFastConfig* cfg) {
	cfg_ = cfg;
	steps.reserve(cfg_->maxRebuildSteps + 1);
}

}  // namespace reindexer
