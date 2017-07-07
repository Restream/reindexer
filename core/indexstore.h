#pragma once

#include <map>
#include "core/index.h"

using std::unordered_map;

namespace reindexer {

template <typename T>
class IndexStore : public Index {
public:
	IndexStore(IndexType _type, const string &_name, const IndexOpts &opts, const FieldsSet &fields = FieldsSet())
		: Index(_type, _name, opts, fields), tmpKeyVal_(make_shared<string>()) {}
	~IndexStore();

	KeyRef Upsert(const KeyRef &key, IdType id) override;
	void Delete(const KeyRef &key, IdType id) override;
	void DumpKeys() override{};
	SelectKeyResults SelectKey(const KeyValues &keys, CondType condition, SortType stype, ResultType res_type) override;
	void Commit(const CommitContext &) override;
	void UpdateSortedIds(const UpdateSortedContext & /*ctx*/) override {}
	Index *Clone() override;
	KeyEntry &Find(const KeyRef & /*key*/) override {
		// can't
		static KeyEntry ke;
		return ke;
	}

protected:
	unordered_str_map<int>::iterator find(const KeyRef &key);
	unordered_str_map<int> str_map;
	vector<T> idx_data;

	shared_ptr<string> tmpKeyVal_;
};  // namespace reindexer

}  // namespace reindexer