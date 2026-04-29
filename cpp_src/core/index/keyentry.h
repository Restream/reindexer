#pragma once

#include <vector>
#include "core/idset/idset.h"

namespace reindexer {

class [[nodiscard]] IUpdateSortedContext {
public:
	virtual ~IUpdateSortedContext() = default;
	virtual int GetSortedIdxCount() const noexcept = 0;
	virtual SortType GetCurSortId() const noexcept = 0;
	virtual const std::vector<SortType>& Ids2Sorts() const& noexcept = 0;
	virtual std::vector<SortType>& Ids2Sorts() & noexcept = 0;
};

class [[nodiscard]] SortedIDsCtx {
public:
	SortedIDsCtx(SortType sortId, const std::vector<std::vector<IdType>>& externalSortedIds) noexcept
		: sortId_{sortId}, externalSortedIds_{externalSortedIds} {}

	SortType SortID() const noexcept { return sortId_; }
	std::span<const IdType> ExternalSortedID(size_t idx) const noexcept {
		assertrx_dbg(sortId_);
		assertrx(sortId_ <= externalSortedIds_.size());
		return std::span<const IdType>(&externalSortedIds_[sortId_ - 1][idx], 1);
	}

private:
	SortType sortId_;
	const std::vector<std::vector<IdType>>& externalSortedIds_;
};

template <typename IdSetT>
class [[nodiscard]] KeyEntry : private IdSetT {
public:
	using IdSetType = IdSetT;

	IdSetT& Unsorted() & noexcept { return *this; }
	const IdSetT& Unsorted() const& noexcept { return *this; }
	auto Unsorted() const&& = delete;
	IdSetCRef Sorted(SortType sortId) const&& = delete;
	IdSetCRef Sorted(SortType sortId) const& noexcept
		requires(concepts::IdSetWithSortedIDs<IdSetT>)
	{
		return sortedIDsView(sortId);
	}
	auto Sorted(const SortedIDsCtx& sortCtx) const&& = delete;
	IdSetCRef Sorted(const SortedIDsCtx& sortCtx) const& noexcept {
		const auto sortId = sortCtx.SortID();
		if constexpr (!concepts::IdSetWithSortedIDs<IdSetT>) {
			if (sortId > 0 && !IdSetT::IsEmpty()) {
				return sortCtx.ExternalSortedID(IdSetCRef(*this).begin()->ToNumber());
			}
			return IdSetCRef(*this);
		} else {
			return sortedIDsView(sortId);
		}
	}
	void UpdateSortedIds(const IUpdateSortedContext& ctx)
		requires(concepts::IdSetWithSortedIDs<IdSetT>);
	void Dump(std::ostream& os, std::string_view step, std::string_view offset) const;

private:
	using IdSetRef = std::span<IdType>;

	IdSetCRef sortedIDsView(SortType sortId) const& noexcept
		requires(concepts::IdSetWithSortedIDs<IdSetT>)
	{
		const size_t size = IdSetT::plainSize(), capacity = IdSetT::plainCapacity();
		assertrx_dbg(IdSetT::IsCommitted());
		assertf(capacity >= (sortId + 1) * size, "error ids_.capacity()={},sortId={},ids_.size()={}", capacity, sortId, size);
		return IdSetCRef(IdSetT::plainData() + sortId * size, size);
	}
	IdSetRef sortedIDsView(SortType sortId) & noexcept
		requires(concepts::IdSetWithSortedIDs<IdSetT>);
	auto sortedIDsView(SortType sortId) const&& = delete;
	template <typename KeyEntryT>
	static void dumpSorted(std::ostream& os, const KeyEntryT& keyEntry);
};

namespace concepts {

template <class KeyEntryT>
concept KeyEntryWithSortedIDs = IdSetWithSortedIDs<typename KeyEntryT::IdSetType>;

}  // namespace concepts

extern template class KeyEntry<IdSetUnique>;
extern template class KeyEntry<IdSetPlain>;
extern template class KeyEntry<IdSet>;

}  // namespace reindexer
