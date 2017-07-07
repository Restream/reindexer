#pragma once
#include <limits.h>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "core/idset.h"
#include "core/type_consts.h"
#include "dataholder/basebuildedholder.h"

namespace search_engine {
using std::string;
using std::unordered_map;
using std::unordered_set;
using std::set;
using std::map;

using namespace reindexer;

class IdVirtualizer {
public:
	IdVirtualizer() : next_free_(0) {}
	IdVirtualizer(const IdVirtualizer &rhs);

	IdVirtualizer &operator=(const IdVirtualizer &);

	~IdVirtualizer();

	void AddData(const string &key, IdType id);
	void RemoveData(const string &key, IdType id);
	IdSet::Ptr GetByVirtualId(IdType id) const;
	IdSet::Ptr GetByVirtualId(BaseHolder::SearchTypePtr ids) const;

	// slow function only fo test
	string GetDataByVirtualId(IdType id) const;

	map<IdType, string> GetDataForBuild();
	void Commit(const CommitContext &ctx);

private:
	struct Context {
		IdSet::Ptr real_ids_;
		IdType vir_id_;
	};

	Context *CreateContext(const string &key, IdType id);
	void DeleteContext(const string &key, IdType id);

	struct ContextsComp {
		using is_transparent = void;
		bool operator()(const Context *a, const Context *b) const { return a->vir_id_ < b->vir_id_; }
		bool operator()(const IdType a, const Context *b) const { return a < b->vir_id_; }
		bool operator()(const Context *a, const IdType b) const { return a->vir_id_ < b; }
	};

	int min_val_ = INT_MAX;
	int max_val_ = INT_MIN;

	size_t next_free_;
	unordered_map<string, Context *> data_;
	set<Context *, ContextsComp> contexts_;
	map<IdType, string> changed_virtual_;
	unordered_map<IdType, Context *> changed_real_;
};
}  // namespace search_engine
