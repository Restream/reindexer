#pragma once

#include "algoritm/full_text_search/idvirtualizer.h"
#include "algoritm/full_text_search/searchengine.h"
#include "core/indexunordered.h"
namespace reindexer {
using search_engine::SearchEngine;

template <typename T>
class NewIndexText : public IndexUnordered<T> {
public:
	NewIndexText(IndexType _type, const string& _name);
	KeyRef Upsert(const KeyRef& key, IdType id) override;
	void Delete(const KeyRef& key, IdType id) override;
	SelectKeyResults SelectKey(const KeyValues& keys, CondType condition, SortType stype, Index::ResultType res_type) override;
	void Commit(const CommitContext& ctx) override;
	Index* Clone() override;
	void UpdateSortedIds(const UpdateSortedContext&) override;

protected:
	SearchEngine engine_;
	search_engine::IdVirtualizer id_holder_;
	bool updated_ = false;
};
}  // namespace reindexer
