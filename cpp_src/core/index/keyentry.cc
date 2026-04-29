#include "keyentry.h"
#include "sort/pdqsort.hpp"

namespace reindexer {

template <typename IdSetT>
void KeyEntry<IdSetT>::UpdateSortedIds(const IUpdateSortedContext& ctx)
	requires(concepts::IdSetWithSortedIDs<IdSetT>)
{
	if (!IdSetT::IsCommitted()) [[unlikely]] {
		throw Error(errAssert, "UpdateSortedIds: IdSet is not commited}");
	}

	const auto sortIdxCount = ctx.GetSortedIdxCount();
	const auto expectedCapacity = (sortIdxCount + 1) * IdSetT::plainSize();
	// Checking expectedCapacity. We can not reallocate here to avoid incorrect idset iterators in the concurrent queries
	if (IdSetT::plainCapacity() < expectedCapacity) [[unlikely]] {
		throw Error(errAssert, "Unexpected ids capacity: ids_.capacity()={},getSortedIdxCount={},ids_.size()={},expectedCapacity={}",
					IdSetT::plainCapacity(), sortIdxCount, IdSetT::plainSize(), expectedCapacity);
	}

	const auto sortId = ctx.GetCurSortId();
	assertrx_throw(sortId);
	const auto& ids2Sorts = ctx.Ids2Sorts();

	size_t idx = 0;
	auto idsAsc = sortedIDsView(sortId);
	[[maybe_unused]] const IdType::UnderlyingType maxRowId = ids2Sorts.size();
	// For all ids of current key
	for (auto it = IdSetT::plainData(), end = IdSetT::plainData() + IdSetT::plainSize(); it != end; ++it) {
		auto rowid = it->ToNumber();
		assertf(rowid < maxRowId, "id={},ctx.Ids2Sorts().size()={}", rowid, maxRowId);
		idsAsc[idx++] = IdType::FromNumber(ids2Sorts[rowid]);
	}
	boost::sort::pdqsort_branchless(idsAsc.begin(), idsAsc.end());
}

template <typename IdSetT>
void reindexer::KeyEntry<IdSetT>::Dump(std::ostream& os, std::string_view step, std::string_view offset) const {
	std::string newOffset;
	if (IdSetT::Size() > 10) {
		newOffset.reserve(offset.size() + step.size() + 1);
		newOffset += '\n';
		newOffset += offset;
		newOffset += step;
	}
	os << '{' << newOffset << "unsorted: ";
	Unsorted().Dump(os);
	os << ',';
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

template <typename IdSetT>
KeyEntry<IdSetT>::IdSetRef KeyEntry<IdSetT>::sortedIDsView(SortType sortId) & noexcept
	requires(concepts::IdSetWithSortedIDs<IdSetT>)
{
	const size_t size = IdSetT::plainSize(), capacity = IdSetT::plainCapacity();
	assertrx_dbg(IdSetT::IsCommitted());
	assertf(capacity >= (sortId + 1) * size, "error ids_.capacity()={},sortId={},ids_.size()={}", capacity, sortId, size);
	return IdSetRef(IdSetT::plainData() + sortId * size, size);
}

template <typename IdSetT>
template <typename KeyEntryT>
void KeyEntry<IdSetT>::dumpSorted(std::ostream& os, const KeyEntryT& keyEntry) {
	if constexpr (concepts::IdSetWithSortedIDs<typename KeyEntryT::IdSetType>) {
		if (keyEntry.plainSize() != 0) {
			unsigned sortId = 0;
			while (keyEntry.plainCapacity() >= keyEntry.plainSize() * (sortId + 1)) {
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

template class KeyEntry<IdSetUnique>;
template class KeyEntry<IdSetPlain>;
template class KeyEntry<IdSet>;

}  // namespace reindexer
