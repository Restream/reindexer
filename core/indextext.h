#pragma once

#include "core/indexunordered.h"

namespace reindexer {

template <typename T>
class IndexText : public IndexUnordered<T> {
public:
	IndexText(IndexType _type, const string& _name);
	SelectKeyResults SelectKey(const KeyValues& keys, CondType condition, SortType stype, Index::ResultType res_type) override;
	void Commit(const CommitContext& ctx) override;
	Index* Clone() override;
	void UpdateSortedIds(const UpdateSortedContext&) override;

protected:
	unordered_map<string, const key_string*> typos;
	vector<string> tokens_;
	T words_map_;
	bool prepared_ = false;
};

}  // namespace reindexer
