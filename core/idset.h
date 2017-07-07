#pragma once

#include <core/type_consts.h>
#include <algorithm>
#include <memory>
#include <string>
#include "tools/h_vector.h"

using std::string;
using std::vector;
using std::shared_ptr;
using std::pair;

namespace reindexer {

class PayloadData;
class PayloadType;
class CommitContext {
public:
	virtual bool exists(IdType id) const = 0;
	virtual bool updated(IdType id) const = 0;
	virtual int getUpdatedCount() const = 0;
	virtual int phases() const = 0;
	virtual ~CommitContext(){};

	// Commit phases
	enum Phase {
		// Indexes shoud normalize their idsets (sort and remove deleted id and duplicates )
		// Namespace will reset updated and deleted flags
		MakeIdsets = 1,
		// Make sort orders
		MakeSortOrders = 4,
		// Indexes should be ready for processing selects
		PrepareForSelect = 8,
		// Flush data to storate
		FlushToStorage = 16,
	};
};

typedef h_vector<IdType, 4> base_idset;

class IdSet : public base_idset {
public:
	typedef shared_ptr<IdSet> Ptr;
	// add id back off IdSet, reset state to unique.
	void push_back(IdType id, bool forceCommit = false) {
		if (forceCommit) {
			auto pos = std::lower_bound(begin(), end(), id);
			assert((pos == end() || *pos != id));
			base_idset::insert(pos, id);
		} else {
			++inserted_;
			base_idset::push_back(id);
		}
	}
	template <typename InputIt>
	void insert(iterator pos, InputIt first, InputIt last) {
		base_idset::insert(pos, first, last);
		inserted_ += last - first;
	}
	void erase(IdType id, bool forceCommit = false) {
		if (forceCommit) {
			auto d = std::equal_range(begin(), end(), id);
			assert(d.first != d.second);
			base_idset::erase(d.first, d.second);

		} else {
			// actual removed in commit
			removed_++;
		}
	}
	void commit(const CommitContext &ctx);
	string dump();

protected:
	int inserted_ = 0;
	int removed_ = 0;
};

class IdSetRef : public h_vector_view<IdType> {
public:
	IdSetRef(const IdSet *ids) : h_vector_view<IdType>(ids->data(), ids->size()){};
	IdSetRef(const IdType *data, size_t len) : h_vector_view<IdType>(data, len){};
	IdSetRef(){};
};

}  // namespace reindexer
