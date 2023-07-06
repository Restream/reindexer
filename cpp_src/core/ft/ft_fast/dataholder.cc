#include "dataholder.h"
#include <sstream>
#include "dataprocessor.h"
#include "selecter.h"

namespace reindexer {

void IDataHolder::SetConfig(FtFastConfig* cfg) {
	cfg_ = cfg;
	steps.reserve(cfg_->maxRebuildSteps + 1);
}

IDataHolder::CommitStep& IDataHolder::GetStep(WordIdType id) {
	assertrx(id.b.step_num < steps.size());
	return steps[id.b.step_num];
}

const IDataHolder::CommitStep& IDataHolder::GetStep(WordIdType id) const {
	assertrx(id.b.step_num < steps.size());
	return steps[id.b.step_num];
}

bool IDataHolder::NeedRebuild(bool complte_updated) {
	return steps.empty() || complte_updated || steps.size() >= size_t(cfg_->maxRebuildSteps) ||
		   (steps.size() == 1 && steps.front().suffixes_.word_size() < size_t(cfg_->maxStepSize));
}

bool IDataHolder::NeedRecomitLast() { return steps.back().suffixes_.word_size() < size_t(cfg_->maxStepSize); }

size_t IDataHolder::GetMemStat() {
	size_t res = 0;
	for (auto& step : steps) {
		res += step.typosHalf_.heap_size() + step.typosMax_.heap_size() + step.suffixes_.heap_size();
	}
	res += vdocs_.capacity() * sizeof(VDocEntry);
	res += rowId2Vdoc_.capacity() * sizeof(rowId2Vdoc_[0]);
	return res;
}

void IDataHolder::SetWordsOffset(uint32_t word_offset) {
	assertrx(!steps.empty());
	if (status_ == CreateNew) steps.back().wordOffset_ = word_offset;
}

void IDataHolder::Clear() {
	steps.resize(1);
	steps.front().clear();
	avgWordsCount_.clear();
	vdocs_.clear();
	vdocsTexts.clear();
	vdocsOffset_ = 0;
	szCnt = 0;
	rowId2Vdoc_.clear();
}

bool IDataHolder::NeedClear(bool complte_updated) {
	if (NeedRebuild(complte_updated) || !NeedRecomitLast()) return true;
	return false;
}

std::string IDataHolder::Dump() {
	std::stringstream ss;
	ss << "Holder dump: step count: " << steps.size() << std::endl;
	ss << "Status: ";
	switch (status_) {
		case CreateNew:
			ss << "\"create new\"";
			break;
		case RecommitLast:
			ss << "\"recomit last\"";
			break;
		case FullRebuild:
			ss << "\"full rebuild\"";
	}
	ss << " step count: " << steps.size() << std::endl;

	size_t counter = 0;
	for (auto& step : steps) {
		ss << "Step : " << std::to_string(counter);
		if (!step.suffixes_.word_size()) ss << " - empty step";
		ss << std::endl;
		for (size_t i = 0; i < step.suffixes_.word_size(); i++) {
			ss << step.suffixes_.word_at(i) << std::endl;
		}
		counter++;
	}
	return ss.str();
}

uint32_t IDataHolder::GetSuffixWordId(WordIdType id, const CommitStep& step) const noexcept {
	assertrx(!id.isEmpty());
	assertrx(id.b.step_num < steps.size());

	assertrx(id.b.id >= step.wordOffset_);
	assertrx(id.b.id - step.wordOffset_ < step.suffixes_.word_size());
	return id.b.id - step.wordOffset_;
}

WordIdType IDataHolder::findWord(std::string_view word) {
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

uint32_t IDataHolder::GetWordsOffset() {
	assertrx(!steps.empty());
	return steps.back().wordOffset_;
}

WordIdType IDataHolder::BuildWordId(uint32_t id) {
	WordIdType wId;
	assertrx(id < kWordIdMaxIdVal);
	assertrx(steps.size() - 1 < kWordIdMaxStepVal);

	wId.b.id = id;
	wId.b.step_num = steps.size() - 1;

	return wId;
}

template <typename IdCont>
size_t DataHolder<IdCont>::GetMemStat() {
	size_t res = IDataHolder::GetMemStat();
	for (auto& w : words_) {
		res += sizeof(w) + w.vids_.heap_size();
	}
	return res;
}

template <typename IdCont>
PackedWordEntry<IdCont>& DataHolder<IdCont>::getWordById(WordIdType id) noexcept {
	assertrx(!id.isEmpty());
	assertrx(id.b.id < words_.size());
	return words_[id.b.id];
}

template <typename IdCont>
const PackedWordEntry<IdCont>& DataHolder<IdCont>::getWordById(WordIdType id) const noexcept {
	assertrx(!id.isEmpty());
	assertrx(id.b.id < words_.size());
	return words_[id.b.id];
}

template <typename IdCont>
void DataHolder<IdCont>::Clear() {
	IDataHolder::Clear();
	words_.clear();
}

template <typename IdCont>
void DataHolder<IdCont>::StartCommit(bool complte_updated) {
	if (NeedRebuild(complte_updated)) {
		status_ = FullRebuild;

		Clear();
	} else if (NeedRecomitLast()) {
		status_ = RecommitLast;
		words_.erase(words_.begin() + steps.back().wordOffset_, words_.end());

		for (auto& word : words_) {
			word.vids_.erase_back(word.cur_step_pos_);
		}

		steps.back().clear();
	} else {  // if the last step is full, then create a new
		for (auto& word : words_) {
			word.cur_step_pos_ = word.vids_.pos(word.vids_.end());
		}
		status_ = CreateNew;
		steps.emplace_back(CommitStep{});
	}
	return;
}

template <typename IdCont>
void DataHolder<IdCont>::Process(size_t fieldSize, bool multithread) {
	DataProcessor<IdCont>{*this, fieldSize}.Process(multithread);
}

template <typename IdCont>
IDataHolder::MergeData DataHolder<IdCont>::Select(FtDSLQuery&& dsl, size_t fieldSize, bool needArea, int maxAreasInDoc, bool inTransaction,
												  FtMergeStatuses::Statuses&& mergeStatuses, FtUseExternStatuses useExternSt,
												  const RdxContext& rdxCtx) {
	switch (useExternSt) {
		case FtUseExternStatuses::No:
			return Selecter<IdCont>{*this, fieldSize, needArea, maxAreasInDoc}.template Process<FtUseExternStatuses::No>(
				std::move(dsl), inTransaction, std::move(mergeStatuses), rdxCtx);
		case FtUseExternStatuses::Yes:
			return Selecter<IdCont>{*this, fieldSize, needArea, maxAreasInDoc}.template Process<FtUseExternStatuses::Yes>(
				std::move(dsl), inTransaction, std::move(mergeStatuses), rdxCtx);
	}
}
template class DataHolder<PackedIdRelVec>;
template class DataHolder<IdRelVec>;

}  // namespace reindexer
