#pragma once
#include <memory>
#include "core/index/keyentry.h"
#include "core/index/payload_map.h"
#include "core/index/string_map.h"
#include "dataholder.h"
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"
#include "estl/string_view.h"

using std::pair;
using std::vector;
using std::unique_ptr;

namespace reindexer {

class DataProcessor {
public:
	DataProcessor(DataHolder& holder, size_t fieldSize) : holder_(holder), fieldSize_(fieldSize) {}

	void Process(bool multithread);

private:
	typedef pair<key_string, reindexer::KeyEntry<IdSetPlain>> BasePair;
	typedef pair<PayloadValue, reindexer::KeyEntry<IdSetPlain>> CompositePair;

	size_t buildWordsMap(fast_hash_map<string, WordEntry>& m);

	void buildVirtualWord(const string& word, fast_hash_map<string, WordEntry>& words_um, VDocIdType docType, int rfield, size_t insertPos,
						  std::vector<string>& output);

	void buildTyposMap(uint32_t startPos, const vector<WordIdType>& found);

	vector<WordIdType> BuildSuffix(fast_hash_map<string, WordEntry>& words_um, DataHolder& holder);

	// typename Data::value_type::first_type& GetFirst(typename Data::value_type& val);
	const BasePair& GetPair(const BasePair& pair) { return pair; }
	const BasePair& GetPair(BasePair* pair) { return *pair; }
	const CompositePair& GetPair(const CompositePair& pair) { return pair; }
	const CompositePair& GetPair(CompositePair* pair) { return *pair; }

	DataHolder& holder_;
	bool multithread_;
	size_t fieldSize_;
};

}  // namespace reindexer
