#pragma once
#include <memory>
#include "dataholder.h"
#include "estl/fast_hash_map.h"
#include "estl/string_view.h"

namespace reindexer {

using std::vector;

class DataProcessor {
public:
	using words_map = fast_hash_map<string, WordEntry>;
	DataProcessor(DataHolder& holder, size_t fieldSize) : holder_(holder), multithread_(false), fieldSize_(fieldSize) {}

	void Process(bool multithread);

private:
	size_t buildWordsMap(words_map& m);

	void buildVirtualWord(string_view word, words_map& words_um, VDocIdType docType, int rfield, size_t insertPos,
						  std::vector<string>& output);

	void buildTyposMap(uint32_t startPos, const vector<WordIdType>& found);

	vector<WordIdType> BuildSuffix(words_map& words_um, DataHolder& holder);

	DataHolder& holder_;
	bool multithread_;
	size_t fieldSize_;
};

}  // namespace reindexer
