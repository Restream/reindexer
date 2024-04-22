#pragma once
#include <memory>
#include <string_view>
#include "dataholder.h"

namespace reindexer {

template <typename IdCont>
class DataProcessor {
public:
	using words_map = RHashMap<std::string, WordEntry>;

	DataProcessor(DataHolder<IdCont>& holder, size_t fieldSize) : holder_(holder), fieldSize_(fieldSize) {}

	void Process(bool multithread);

private:
	size_t buildWordsMap(words_map& m, bool multithread);

	void buildVirtualWord(std::string_view word, words_map& words_um, VDocIdType docType, int rfield, size_t insertPos,
						  std::vector<std::string>& container);

	void buildTyposMap(uint32_t startPos, const std::vector<WordIdType>& found);

	std::vector<WordIdType> BuildSuffix(words_map& words_um, DataHolder<IdCont>& holder);

	DataHolder<IdCont>& holder_;
	size_t fieldSize_;
};

extern template class DataProcessor<PackedIdRelVec>;
extern template class DataProcessor<IdRelVec>;

}  // namespace reindexer
