#pragma once

#include <vector>
#include "core/formatters/id_type_fmt.h"
#include "core/id_type.h"
#include "core/idset/idset.h"
#include "sort/pdqsort.hpp"
#include "tools/errors.h"

namespace reindexer {

class [[nodiscard]] IUpdateSortedContext {
public:
	virtual ~IUpdateSortedContext() = default;
	virtual int GetSortedIdxCount() const noexcept = 0;
	virtual SortType GetCurSortId() const noexcept = 0;
	virtual const std::vector<SortType>& Ids2Sorts() const& noexcept = 0;
	virtual std::vector<SortType>& Ids2Sorts() & noexcept = 0;
};

template <typename IdSetT>
class [[nodiscard]] KeyEntry {
public:
	using IdSetType = IdSetT;

	IdSetT& Unsorted() noexcept { return ids_; }
	const IdSetT& Unsorted() const noexcept { return ids_; }
	IdSetCRef Sorted(SortType sortId) const noexcept
		requires(concepts::IdSetWithSortedIDs<IdSetT>)
	{
		return sorted(sortId);
	}
	IdSetCRef Sorted(const SortedIDsCtx& sortCtx) const noexcept {
		const auto sortId = sortCtx.SortID();
		if constexpr (!concepts::IdSetWithSortedIDs<IdSetT>) {
			if (sortId > 0 && !ids_.empty()) {
				return sortCtx.ExternalSortedID(ids_.begin()->ToNumber());
			}
			return IdSetCRef(ids_.data(), ids_.size());
		} else {
			return sorted(sortId);
		}
	}
	void UpdateSortedIds(const IUpdateSortedContext& ctx) {
		if constexpr (concepts::IdSetWithSortedIDs<IdSetT>) {
			const auto expectedCapacity = (ctx.GetSortedIdxCount() + 1) * ids_.size();
			// Checking expectedCapacity. We can not reallocate here to avoid incorrect idset iterators in the concurrent queries
			if (ids_.capacity() < expectedCapacity) [[unlikely]] {
				throw Error(errAssert,
							"Unexpected ids capacity: ids_.capacity()={},getSortedIdxCount={},ids_.size()={},expectedCapacity={}",
							ids_.capacity(), ctx.GetSortedIdxCount(), ids_.size(), expectedCapacity);
			}

			const auto curSortId = ctx.GetCurSortId();
			assertrx_throw(curSortId);
			auto idsAsc = sorted(curSortId);

			size_t idx = 0;
			const auto& ids2Sorts = ctx.Ids2Sorts();
			[[maybe_unused]] const IdType maxRowId = IdType::FromNumber(ids2Sorts.size());
			// For all ids of current key
			for (const auto& rowid : ids_) {
				assertf(rowid < maxRowId, "id={},ctx.Ids2Sorts().size()={}", rowid, maxRowId);
				idsAsc[idx++] = IdType::FromNumber(ids2Sorts[rowid.ToNumber()]);
			}
			boost::sort::pdqsort_branchless(idsAsc.begin(), idsAsc.end());
		}
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
		dumpSorted(os, *this);
		os << ']';
		if (!newOffset.empty()) {
			os << '\n' << offset;
		}
		os << '}';
	}

private:
	std::span<IdType> sorted(SortType sortId) noexcept
		requires(concepts::IdSetWithSortedIDs<IdSetT>)
	{
		assertf(ids_.capacity() >= (sortId + 1) * ids_.size(), "error ids_.capacity()={},sortId={},ids_.size()={}", ids_.capacity(), sortId,
				ids_.size());
		return std::span<IdType>(ids_.data() + sortId * ids_.size(), ids_.size());
	}
	IdSetCRef sorted(SortType sortId) const noexcept
		requires(concepts::IdSetWithSortedIDs<IdSetT>)
	{
		assertf(ids_.capacity() >= (sortId + 1) * ids_.size(), "error ids_.capacity()={},sortId={},ids_.size()={}", ids_.capacity(), sortId,
				ids_.size());
		return IdSetCRef(ids_.data() + sortId * ids_.size(), ids_.size());
	}
	template <typename KeyEntryT>
	static void dumpSorted(std::ostream& os, const KeyEntryT& keyEntry) {
		if constexpr (concepts::IdSetWithSortedIDs<typename KeyEntryT::IdSetType>) {
			if (keyEntry.ids_.size() != 0) {
				unsigned sortId = 0;
				while (keyEntry.ids_.capacity() >= keyEntry.ids_.size() * (sortId + 1)) {
					if (sortId != 0) {
						os << ", ";
					}
					os << '[';
					const auto sorted = keyEntry.Sorted(sortId);
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
		}
	}

	IdSetT ids_;
};

namespace concepts {

template <class KeyEntryT>
concept KeyEntryWithSortedIDs = IdSetWithSortedIDs<typename KeyEntryT::IdSetType>;

}  // namespace concepts

}  // namespace reindexer
