#pragma once

#include <unordered_map>
#include "cbinding/serializer.h"
#include "core/item.h"
#include "tools/h_vector.h"

namespace reindexer {

using std::unordered_map;
using std::string;
class Namespace;

struct QueryResults : public h_vector<ItemRef, 32> {
	friend class Namespace;
	QueryResults(const ItemRef &id) { push_back(ItemRef(id)); }
	QueryResults() {}
	int totalCount = 0;
	// joinded fields 0 - 1st joined ns, 1 - second joined
	unordered_map<IdType, vector<QueryResults>> joined_;  // joinded items

	void Dump() const;
	void GetJSON(int idx, WrSerializer &wrser) const;

protected:
	const PayloadType *type_ = nullptr;
	const ItemTagsMatcher *tagsMatcher_ = nullptr;
};

}  // namespace reindexer
