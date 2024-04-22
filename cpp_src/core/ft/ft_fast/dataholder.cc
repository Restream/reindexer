#include "dataholder.h"
#include <sstream>
#include "dataprocessor.h"
#include "selecter.h"

namespace reindexer {

void IDataHolder::SetConfig(FtFastConfig* cfg) {
	cfg_ = cfg;
	steps.reserve(cfg_->maxRebuildSteps + 1);
}

size_t IDataHolder::GetMemStat() {
	size_t res = 0;
	for (auto& step : steps) {
		res += step.typosHalf_.heap_size() + step.typosMax_.heap_size() + step.suffixes_.heap_size();
	}
	res += vdocs_.capacity() * sizeof(VDocEntry);
	res += rowId2Vdoc_.capacity() * sizeof(rowId2Vdoc_[0]);
	return res;
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

void IDataHolder::throwWordIdOverflow(uint32_t id) {
	throw Error(errLogic, "Too large word ID value (%d). Fulltext index can not contain more than %d unique words", id, kWordIdMaxIdVal);
}

void IDataHolder::throwStepsOverflow() const {
	throw Error(errLogic, "Too large index build step value (%d). Fulltext incremental build can not use more than %d steps",
				steps.size() - 1, kWordIdMaxStepVal);
}

WordIdType IDataHolder::findWord(std::string_view word) {
	WordIdType id;
	id.SetEmpty();
	if (steps.size() <= 1) return id;

	for (auto step = steps.begin(); step != steps.end() - 1; ++step) {
		auto it = step->suffixes_.lower_bound(word);
		if (it != step->suffixes_.end() && size_t(step->suffixes_.word_len_at(GetSuffixWordId(it->second, *step))) == word.size()) {
			return it->second;
		}
	}

	return id;
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

template class DataHolder<PackedIdRelVec>;
template class DataHolder<IdRelVec>;

}  // namespace reindexer
