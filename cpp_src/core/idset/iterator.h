#pragma once

#include <variant>
#include "core/type_consts.h"
#include "cpp-btree/btree_set.h"
#include "estl/h_vector.h"

namespace reindexer {
namespace idset {

template <class T>
concept IdSetLike = requires(T a) {
	{ a.IsCommitted() } -> std::same_as<bool>;
	{ a.IsEmpty() } -> std::same_as<bool>;
	{ a.Size() } -> std::same_as<size_t>;
	{ a.BTreeSize() } -> std::same_as<size_t>;
	{ a.BTree() } -> std::same_as<const btree::btree_set<int>*>;
	{ a.Commit() } -> std::same_as<void>;
	a.idset_range();
	a.idset_reverse_range();
};

namespace iterators {

template <typename SequentialIterator, typename BtreeIterator>
class Iterator {
public:
	using value_type = IdType;
	using difference_type = std::ptrdiff_t;
	using pointer = IdType*;
	using const_pointer = const IdType*;
	using reference = IdType&;
	using const_reference = const IdType&;

	Iterator() noexcept = default;
	explicit Iterator(SequentialIterator it) noexcept : impl_{it} {}
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
	using Impl = std::variant<SequentialIterator, BtreeIterator>;
	Impl impl_;
};

template <typename Iterator>
class IteratorRange {
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

using SequentialContainer = h_vector<IdType>;
using BtreeContainer = btree::btree_set<IdType>;
using SequentialIterator = SequentialContainer::const_iterator;
using SequentialReverseIterator = SequentialContainer::const_reverse_iterator;
using BtreeIterator = BtreeContainer::const_iterator;
using BtreeReverseIterator = BtreeContainer::const_reverse_iterator;

using ForwardIterator = Iterator<SequentialIterator, BtreeIterator>;
using ReverseIterator = Iterator<SequentialReverseIterator, BtreeReverseIterator>;

using ForwardIteratorRange = IteratorRange<ForwardIterator>;
using ReverseIteratorRange = IteratorRange<ReverseIterator>;

template <IdSetLike IdSet>
ForwardIteratorRange range(const IdSet& idSet) noexcept {
	if (idSet.IsCommitted()) {
		return ForwardIteratorRange{ForwardIterator{idSet.begin()}, ForwardIterator{idSet.end()}};
	}
	assertrx_dbg(idSet.BTree());
	return ForwardIteratorRange{ForwardIterator{idSet.BTree()->begin()}, ForwardIterator{idSet.BTree()->end()}};
}

template <IdSetLike IdSet>
ReverseIteratorRange reverse_range(const IdSet& idSet) noexcept {
	if (idSet.IsCommitted()) {
		return ReverseIteratorRange{ReverseIterator{idSet.rbegin()}, ReverseIterator{idSet.rend()}};
	}
	assertrx_dbg(idSet.BTree());
	return ReverseIteratorRange{ReverseIterator{idSet.BTree()->rbegin()}, ReverseIterator{idSet.BTree()->rend()}};
}

}  // namespace iterators
}  // namespace idset
}  // namespace reindexer

namespace std {
template <typename SequentialIterator, typename BtreeIterator>
struct iterator_traits<reindexer::idset::iterators::Iterator<SequentialIterator, BtreeIterator>> {
	using iterator_category = bidirectional_iterator_tag;
	using value_type = IdType;
	using difference_type = ptrdiff_t;
	using pointer = const IdType*;
	using reference = const IdType&;
};
}  // namespace std
