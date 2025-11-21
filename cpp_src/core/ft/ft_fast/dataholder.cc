#include "dataholder.h"
#include <sstream>
#include "core/ft/ft_fast/frisosplitter.h"
#include "dataprocessor.h"

namespace reindexer {

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
	avgWordsCount_.resize(0);
	vdocs_.resize(0);
	vdocsTexts.resize(0);
	vdocsOffset_ = 0;
	szCnt = 0;
	rowId2Vdoc_.resize(0);
}

std::string IDataHolder::Dump() const {
	std::stringstream ss;
	ss << "Holder dump: step count: " << steps.size() << std::endl;
	ss << "Status: ";
	switch (status_) {
		case CreateNew:
			ss << "\"create new\"";
			break;
		case RecommitLast:
			ss << "\"recommit last\"";
			break;
		case FullRebuild:
			ss << "\"full rebuild\"";
	}
	ss << " step count: " << steps.size() << std::endl;

	size_t counter = 0;
	for (auto& step : steps) {
		ss << "Step : " << std::to_string(counter);
		if (!step.suffixes_.word_size()) {
			ss << " - empty step";
		}
		ss << std::endl;
		for (size_t i = 0; i < step.suffixes_.word_size(); i++) {
			ss << step.suffixes_.word_at(i) << std::endl;
		}
		counter++;
	}
	return ss.str();
}

void IDataHolder::throwWordIdOverflow(uint32_t id) {
	throw Error(errLogic, "Too large word ID value ({}). Fulltext index can not contain more than {} unique words", id, kWordIdMaxIdVal);
}

void IDataHolder::throwStepsOverflow() const {
	throw Error(errLogic, "Too large index build step value ({}). Fulltext incremental build can not use more than {} steps",
				steps.size() - 1, kWordIdMaxStepVal);
}

WordIdType IDataHolder::findWord(std::string_view word) const {
	WordIdType id;
	id.SetEmpty();
	if (steps.size() <= 1) {
		return id;
	}

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
		res += sizeof(w) + w.vids.heap_size();
	}
	return res;
}

template <typename IdCont>
void DataHolder<IdCont>::Clear() {
	IDataHolder::Clear();
	words_.resize(0);
}

template <typename IdCont>
void DataHolder<IdCont>::StartCommit(bool complete_updated) {
	if (NeedRebuild(complete_updated)) {
		status_ = FullRebuild;

		Clear();
	} else if (NeedRecommitLast()) {
		status_ = RecommitLast;
		words_.erase(words_.begin() + steps.back().wordOffset_, words_.end());

		for (auto& word : words_) {
			word.RestoreState();
		}

		steps.back().clear();
	} else {  // if the last step is full, then create a new
		for (auto& word : words_) {
			word.SaveState();
		}
		status_ = CreateNew;
		steps.emplace_back(CommitStep{});
	}
}

template <typename IdCont>
void DataHolder<IdCont>::Process(size_t fieldSize, bool multithread) {
	DataProcessor<IdCont>{*this, fieldSize}.Process(multithread);
}

template <typename IdCont>
DataHolder<IdCont>::DataHolder(FtFastConfig* c) {
	cfg_ = c;
	if (cfg_->splitterType == FtFastConfig::Splitter::Fast) {
		splitter_ = make_intrusive<FastTextSplitter>(cfg_->splitOptions);
	} else if (cfg_->splitterType == FtFastConfig::Splitter::MMSegCN) {
		splitter_ = make_intrusive<FrisoTextSplitter>();
	} else {
		assertrx_throw(false);
	}
}

template class DataHolder<PackedIdRelVec>;
template class DataHolder<IdRelVec>;

}  // namespace reindexer
