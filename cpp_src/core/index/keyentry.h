#pragma once

#include <vector>
#include "core/idset.h"
#include "sort/pdqsort.hpp"
#include "tools/errors.h"

namespace reindexer {

using std::vector;

class UpdateSortedContext {
public:
	virtual ~UpdateSortedContext(){};
	virtual int getSortedIdxCount() const = 0;
	virtual SortType getCurSortId() const = 0;
	virtual const vector<SortType>& ids2Sorts() const = 0;
	virtual vector<SortType>& ids2Sorts() = 0;
};

template <typename IdSetT>
class KeyEntry {
public:
	IdSetT& Unsorted() { return ids_; }
	const IdSetT& Unsorted() const { return ids_; }
	IdSetRef Sorted(unsigned sortId) const {
		assertf(ids_.capacity() >= (sortId + 1) * ids_.size(), "error ids_.capacity()=%d,sortId=%d,ids_.size()=%d", ids_.capacity(), sortId,
				ids_.size());
		return IdSetRef(ids_.data() + sortId * ids_.size(), ids_.size());
	}
	void UpdateSortedIds(const UpdateSortedContext& ctx) {
		ids_.reserve((ctx.getSortedIdxCount() + 1) * ids_.size());
		assert(ctx.getCurSortId());

		auto idsAsc = Sorted(ctx.getCurSortId());

		size_t idx = 0;
		// For all ids of current key
		for (auto rowid : ids_) {
			assertf(rowid < int(ctx.ids2Sorts().size()), "id=%d,ctx.ids2Sorts().size()=%d", rowid, ctx.ids2Sorts().size());
			idsAsc[idx++] = ctx.ids2Sorts()[rowid];
		}
		boost::sort::pdqsort(idsAsc.begin(), idsAsc.end());
	}

	IdSetT ids_;
};
}  // namespace reindexer
