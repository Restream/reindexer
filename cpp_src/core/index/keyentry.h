#pragma once

#include <vector>
#include "core/idset.h"
#include "sort/pdqsort.hpp"
#include "tools/errors.h"

namespace reindexer {

class UpdateSortedContext {
public:
	virtual ~UpdateSortedContext() = default;
	virtual int getSortedIdxCount() const noexcept = 0;
	virtual SortType getCurSortId() const noexcept = 0;
	virtual const std::vector<SortType>& ids2Sorts() const noexcept = 0;
	virtual std::vector<SortType>& ids2Sorts() noexcept = 0;
};

template <typename IdSetT>
class KeyEntry {
public:
	IdSetT& Unsorted() noexcept { return ids_; }
	const IdSetT& Unsorted() const noexcept { return ids_; }
	IdSetRef Sorted(unsigned sortId) noexcept {
		assertf(ids_.capacity() >= (sortId + 1) * ids_.size(), "error ids_.capacity()=%d,sortId=%d,ids_.size()=%d", ids_.capacity(), sortId,
				ids_.size());
		return IdSetRef(ids_.data() + sortId * ids_.size(), ids_.size());
	}
	IdSetCRef Sorted(unsigned sortId) const noexcept {
		assertf(ids_.capacity() >= (sortId + 1) * ids_.size(), "error ids_.capacity()=%d,sortId=%d,ids_.size()=%d", ids_.capacity(), sortId,
				ids_.size());
		return IdSetCRef(ids_.data() + sortId * ids_.size(), ids_.size());
	}
	void UpdateSortedIds(const UpdateSortedContext& ctx) {
		ids_.reserve((ctx.getSortedIdxCount() + 1) * ids_.size());
		assertrx(ctx.getCurSortId());

		auto idsAsc = Sorted(ctx.getCurSortId());

		size_t idx = 0;
		const auto& ids2Sorts = ctx.ids2Sorts();
		[[maybe_unused]] const IdType maxRowId = IdType(ids2Sorts.size());
		// For all ids of current key
		for (auto rowid : ids_) {
			assertf(rowid < maxRowId, "id=%d,ctx.ids2Sorts().size()=%d", rowid, maxRowId);
			idsAsc[idx++] = ids2Sorts[rowid];
		}
		boost::sort::pdqsort_branchless(idsAsc.begin(), idsAsc.end());
	}
	void Dump(std::ostream& os, std::string_view step, std::string_view offset) const {
		std::string newOffset;
		if (ids_.size() > 10) {
			newOffset.reserve(offset.size() + step.size() + 1);
			newOffset += '\n';
			newOffset += offset;
			newOffset += step;
		}
		os << '{' << newOffset << "unsorted: " << Unsorted() << ',';
		if (newOffset.empty()) {
			os << ' ';
		} else {
			os << newOffset;
		}
		os << "sorted: [";
		if (ids_.size() != 0) {
			unsigned sortId = 0;
			while (ids_.capacity() >= ids_.size() * (sortId + 1)) {
				if (sortId != 0) {
					os << ", ";
				}
				os << '[';
				const auto sorted = Sorted(sortId);
				for (auto b = sorted.begin(), it = b, e = sorted.end(); it != e; ++it) {
					if (it != b) {
						os << ", ";
					}
					os << *it;
				}
				os << ']';
				++sortId;
			}
		}
		os << ']';
		if (!newOffset.empty()) {
			os << '\n' << offset;
		}
		os << '}';
	}

	IdSetT ids_;
};

}  // namespace reindexer
