#include "dataholder.h"
#include <sstream>
#include "core/ft/ft_fast/frisosplitter.h"
#include "dataprocessor.h"

namespace reindexer {

size_t IDataHolder::GetMemStat() {
	size_t res = 0;
	for (auto& step : steps) {
		res += step.typos_.heap_size() + step.suffixes_.heap_size();
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
	stepsWords_.clear();
	lastStepWords_.clear();
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

WordIdType IDataHolder::FindWord(const std::string_view& word, bool searchLastStep) const {
	word_hash wh;
	word_equal we;
	size_t hash = wh(word);

	if (auto it = stepsWords_.find(hash); it != stepsWords_.end()) {
		for (WordIdType wid : it->second) {
			if (we(GetWord(wid), word)) {
				return wid;
			}
		}
	}

	if (searchLastStep) {
		if (auto itLast = lastStepWords_.find(hash); itLast != lastStepWords_.end()) {
			for (WordIdType wid : itLast->second) {
				if (we(GetWord(wid), word)) {
					return wid;
				}
			}
		}
	}

	WordIdType emptyId;
	emptyId.SetEmpty();
	return emptyId;
}

template <typename IdCont>
size_t DataHolder<IdCont>::GetMemStat() {
	size_t res = IDataHolder::GetMemStat();
	for (auto& w : words_) {
		res += sizeof(w) + w.vids.heap_size();
	}
	res += wordsCharsLen_.capacity() * sizeof(wordsCharsLen_[0]);
	return res;
}

template <typename IdCont>
void DataHolder<IdCont>::Clear() {
	IDataHolder::Clear();
	words_.resize(0);
	wordsCharsLen_.resize(0);
}

template <typename IdCont>
void DataHolder<IdCont>::StartCommit(bool complete_updated) {
	if (NeedRebuild(complete_updated)) {
		status_ = FullRebuild;

		Clear();
		words_.clear();
		wordsCharsLen_.clear();
		lastStepWords_.clear();
	} else if (NeedRecommitLast()) {
		status_ = RecommitLast;
		words_.erase(words_.begin() + steps.back().wordOffset_, words_.end());
		wordsCharsLen_.erase(wordsCharsLen_.begin() + steps.back().wordOffset_, wordsCharsLen_.end());

		for (auto& word : words_) {
			word.RestoreState();
		}

		steps.back().clear();
		lastStepWords_.clear();
	} else {  // if the last step is full, then create a new
		for (auto& word : words_) {
			word.SaveState();
		}
		status_ = CreateNew;
		steps.emplace_back(CommitStep{});
		for (auto& [wHash, Ids] : lastStepWords_) {
			auto& w = stepsWords_[wHash];
			for (auto id : Ids) {
				w.emplace_back(id);
			}
		}
		lastStepWords_.clear();
	}
}

template <typename IdCont>
void DataHolder<IdCont>::Process(size_t fieldSize, bool multithread) {
	DataProcessor<IdCont>{*this, fieldSize}.Process(multithread);
}

template <typename IdCont>
DataHolder<IdCont>::DataHolder(FTConfig* c) {
	cfg_ = c;
	if (cfg_->splitterType == FTConfig::Splitter::Fast) {
		splitter_ = make_intrusive<FastTextSplitter>(cfg_->splitOptions);
	} else if (cfg_->splitterType == FTConfig::Splitter::MMSegCN) {
		splitter_ = make_intrusive<FrisoTextSplitter>();
	} else {
		assertrx_throw(false);
	}
}

template class DataHolder<PackedIdRelVec>;
template class DataHolder<IdRelVec>;

}  // namespace reindexer
