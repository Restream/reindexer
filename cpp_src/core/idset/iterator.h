#pragma once

#include <span>
#include <variant>
#include "core/id_type.h"
#include "cpp-btree/btree_set.h"

#if REINDEX_DEBUG_CONTAINERS
#include <vector>
#endif	// REINDEX_DEBUG_CONTAINERS

namespace reindexer {
namespace idset {

template <class T>
concept IdSetLike = requires(T a) {
	{ a.IsCommitted() } -> std::same_as<bool>;
	{ a.IsEmpty() } -> std::same_as<bool>;
	{ a.Size() } -> std::same_as<size_t>;
	{ a.BTreeSize() } -> std::same_as<size_t>;
	{ a.BTree() } -> std::same_as<const btree::btree_set<IdType>*>;
	{ a.Commit() } -> std::same_as<void>;
	a.idset_range();
	a.idset_reverse_range();
};

namespace iterators {

template <typename SequentialIterator, typename BtreeIterator>
class [[nodiscard]] Iterator {
public:
	using value_type = IdType;
	using difference_type = std::ptrdiff_t;
	using pointer = IdType*;
	using const_pointer = const IdType*;
	using reference = IdType&;
	using const_reference = const IdType&;

	Iterator() noexcept = default;
#if REINDEX_DEBUG_CONTAINERS
	explicit Iterator(std::span<const IdType>::iterator it) noexcept
		requires(!std::same_as<SequentialIterator, std::span<const IdType>::iterator>)
		: impl_{it} {}
	explicit Iterator(std::span<const IdType>::reverse_iterator it) noexcept
		requires(!std::same_as<SequentialIterator, std::span<const IdType>::reverse_iterator>)
		: impl_{it} {}
#endif	// REINDEX_DEBUG_CONTAINERS
	explicit Iterator(SequentialIterator it) noexcept : impl_{std::in_place_index_t<0>{}, it} {}
	explicit Iterator(BtreeIterator it) noexcept : impl_{it} {}

	Iterator(const Iterator& other) noexcept = default;
	Iterator(Iterator&& other) noexcept = default;
	Iterator& operator=(const Iterator& other) noexcept = default;
	Iterator& operator=(Iterator&& other) noexcept = default;

	// NOLINTNEXTLINE(bugprone-exception-escape)
	bool operator==(const Iterator& other) const noexcept = default;

	// NOLINTNEXTLINE(bugprone-exception-escape)
	const_reference operator*() const noexcept {
		return std::visit([](auto& impl) -> const_reference { return *impl; }, impl_);
	}

	// NOLINTNEXTLINE(bugprone-exception-escape)
	const_pointer operator->() const noexcept {
		return std::visit([](auto& impl) -> const_pointer { return std::addressof(impl); }, impl_);
	}

	// NOLINTNEXTLINE(bugprone-exception-escape)
	Iterator& operator++() noexcept {
		std::visit([](auto& impl) { ++impl; }, impl_);
		return *this;
	}

	// NOLINTNEXTLINE(bugprone-exception-escape)
	Iterator& operator--() noexcept {
		std::visit([](auto& impl) { --impl; }, impl_);
		return *this;
	}

	Iterator operator--(int) noexcept {
		Iterator tmp{*this};
		this->operator--();
		return tmp;
	}

	Iterator operator++(int) noexcept {
		Iterator tmp{*this};
		this->operator++();
		return tmp;
	}

private:
#if REINDEX_DEBUG_CONTAINERS
	using Impl =
		std::variant<SequentialIterator, BtreeIterator, std::span<const IdType>::iterator, std::span<const IdType>::reverse_iterator>;
#else	// !REINDEX_DEBUG_CONTAINERS
	using Impl = std::variant<SequentialIterator, BtreeIterator>;
#endif	// REINDEX_DEBUG_CONTAINERS

	Impl impl_;
};

template <typename Iterator>
class [[nodiscard]] IteratorRange {
public:
	IteratorRange() = default;
	IteratorRange(Iterator begin, Iterator end) noexcept : begin_{std::move(begin)}, end_{std::move(end)} {}

	const Iterator& begin() const& noexcept { return begin_; }
	const Iterator& end() const& noexcept { return end_; }

	Iterator&& begin() && noexcept { return std::move(begin_); }
	Iterator&& end() && noexcept { return std::move(end_); }

private:
	Iterator begin_;
	Iterator end_;
};

#if REINDEX_DEBUG_CONTAINERS
// We could use span-iterator instead of vector-iterator, but vector iterator provides debug checks for data invalidation after vector
// modification
using SequentialContainer = std::vector<IdType>;
using SequentialIterator = SequentialContainer::const_iterator;
using SequentialReverseIterator = SequentialContainer::const_reverse_iterator;
#else	// !REINDEX_DEBUG_CONTAINERS
using SequentialContainer = std::span<const IdType>;
using SequentialIterator = SequentialContainer::iterator;
using SequentialReverseIterator = SequentialContainer::reverse_iterator;
#endif	//  REINDEX_DEBUG_CONTAINERS

using BtreeContainer = btree::btree_set<IdType>;
using BtreeIterator = BtreeContainer::const_iterator;
using BtreeReverseIterator = BtreeContainer::const_reverse_iterator;

using ForwardIterator = Iterator<SequentialIterator, BtreeIterator>;
using ReverseIterator = Iterator<SequentialReverseIterator, BtreeReverseIterator>;

using ForwardIteratorRange = IteratorRange<ForwardIterator>;
using ReverseIteratorRange = IteratorRange<ReverseIterator>;

template <IdSetLike IdSet>
ForwardIteratorRange range(const IdSet& idSet) noexcept {
	auto set = idSet.BTree();
	assertrx_dbg(idSet.IsCommitted() == !set);
	return set ? ForwardIteratorRange{ForwardIterator{set->begin()}, ForwardIterator{set->end()}}
			   : ForwardIteratorRange{ForwardIterator{idSet.begin()}, ForwardIterator{idSet.end()}};
}

template <IdSetLike IdSet>
ReverseIteratorRange reverse_range(const IdSet& idSet) noexcept {
	auto set = idSet.BTree();
	assertrx_dbg(idSet.IsCommitted() == !set);
	return set ? ReverseIteratorRange{ReverseIterator{set->rbegin()}, ReverseIterator{set->rend()}}
			   : ReverseIteratorRange{ReverseIterator{idSet.rbegin()}, ReverseIterator{idSet.rend()}};
}

}  // namespace iterators
}  // namespace idset
}  // namespace reindexer

namespace std {
template <typename SequentialIterator, typename BtreeIterator>
struct iterator_traits<reindexer::idset::iterators::Iterator<SequentialIterator, BtreeIterator>> {
	using iterator_category = bidirectional_iterator_tag;
	using value_type = reindexer::IdType;
	using difference_type = ptrdiff_t;
	using pointer = const reindexer::IdType*;
	using reference = const reindexer::IdType&;
};
}  // namespace std
