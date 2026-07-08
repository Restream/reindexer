#pragma once

#include <algorithm>
#include <atomic>
#include <span>
#include "cpp-btree/btree_set.h"
#include "estl/defines.h"
#include "estl/h_vector.h"
#include "estl/intrusive_ptr.h"
#include "iterator.h"

namespace reindexer {

constexpr int kIdSetBtreeNodeSize = 256;
#if !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
// Maximum size of idset without building btree
constexpr unsigned kMaxPlainIdsetSize = 256;
#else	// defined(REINDEX_WITH_ASAN) || defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
// Use smaller value in sanitizers build to get more IdSets states in testing environment
constexpr unsigned kMaxPlainIdsetSize = 16;
#endif	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)

using base_idset = h_vector<IdType, 3>;	 // const_iterator must be trivial (used in union)
using base_idsetset = btree::btree_set<IdType, std::less<IdType>, std::allocator<IdType>, kIdSetBtreeNodeSize>;

#ifndef REINDEX_DEBUG_CONTAINERS
static_assert(sizeof(base_idset) == 16, "base_idset must be 16 bytes to keep indexing structures compact");
#endif	// REINDEX_DEBUG_CONTAINERS

using IdSetCRef = std::span<const IdType>;

/// @brief Container that stores a single ID.
/// Used for primary keys.
class [[nodiscard]] IdSetUnique {
public:
	using const_iterator = std::span<const IdType>::iterator;
	using const_reverse_iterator = std::span<const IdType>::reverse_iterator;

	using idset_iterator_range = idset::iterators::ForwardIteratorRange;
	using idset_reverse_iterator_range = idset::iterators::ReverseIteratorRange;

	IdSetUnique() = default;
	IdSetUnique(const IdSetUnique&) = default;
	IdSetUnique& operator=(const IdSetUnique& other) = default;

	/// @brief Adds a new ID into the set while keeping it ordered.
	/// Used for primary keys.
	bool Add(IdType id, base_idset::size_type) {
		if (!IsEmpty()) [[unlikely]] {
			throwDuplicatedIDError();
		}
		id_ = id;
		return true;
	}

	/// @brief Removes an ID from the set.
	/// @return the number of deleted items.
	int Erase(IdType id) {
		if (id_ == id) {
			id_ = IdType::NotSet();
			return 1;
		}
		return 0;
	}

	/// @brief Does nothing for IdSetUnique, single ID is always sorted.
	void Commit([[maybe_unused]] base_idset::size_type sortedIdxCount) const noexcept {}
	constexpr static bool IsCommitted() noexcept { return true; }
	bool IsEmpty() const noexcept { return !id_.IsValid(); }
	size_t Size() const noexcept { return id_.IsValid() ? 1 : 0; }
	size_t BTreeHeapSize() const noexcept { return 0; }
	const base_idsetset* BTree() const noexcept { return nullptr; }
	void OnSortedIndexCountChanged(unsigned) {}
	void Dump(std::ostream&) const;
	size_t PlainHeapSize() const noexcept { return 0; }

	idset_iterator_range idset_range() const& noexcept {
		IdSetCRef sp(*this);
		return idset::iterators::range(sp.begin(), sp.end());
	}
	idset_iterator_range idset_range() const&& = delete;
	idset_reverse_iterator_range idset_reverse_range() const& noexcept {
		IdSetCRef sp(*this);
		return idset::iterators::reverse_range(sp.rbegin(), sp.rend());
	}
	idset_reverse_iterator_range idset_reverse_range() const&& = delete;
	operator IdSetCRef() const& noexcept { return IdSetCRef(&id_, Size()); }
	operator IdSetCRef() const&& = delete;

private:
	[[noreturn]] void throwDuplicatedIDError() const;

	IdType id_ = IdType::NotSet();
};

/// @brief Container that stores ordered IDs in a vector.
/// Besides the IDs themselves, it also stores a mapping from IDs to ordered index positions (sort orders), allowing
/// traversal in the order of the corresponding index.
class [[nodiscard]] IdSetPlain : protected base_idset {
public:
#if REINDEX_DEBUG_CONTAINERS
	using base_idset::const_iterator;
	using base_idset::const_reverse_iterator;
#else	// !REINDEX_DEBUG_CONTAINERS
	using const_iterator = std::span<const IdType>::iterator;
	using const_reverse_iterator = std::span<const IdType>::reverse_iterator;
#endif	// REINDEX_DEBUG_CONTAINERS

	using base_idset::reserve;
	using base_idset::shrink_to_fit;
	using base_idset::operator[];
	using idset_iterator = idset::iterators::ForwardIterator;
	using idset_reverse_iterator = idset::iterators::ReverseIterator;
	using idset_iterator_range = idset::iterators::ForwardIteratorRange;
	using idset_reverse_iterator_range = idset::iterators::ReverseIteratorRange;
	using Ptr = intrusive_ptr<intrusive_atomic_rc_wrapper<IdSetPlain>>;

	IdSetPlain() = default;
	IdSetPlain(base_idset&& idset) noexcept : base_idset(std::move(idset)) {}

	// Explicit construtors implementations to preserve IdSet's capacity (it's required for background indexes optimization)
	IdSetPlain(IdSetPlain&& other) noexcept : base_idset() {
		[[maybe_unused]] auto otherCapacity = other.capacity();
		static_cast<base_idset&>(*this).swap(static_cast<base_idset&>(other));
		assertrx_dbg(capacity() == otherCapacity);
	}
	// NOLINTNEXTLINE(bugprone-copy-constructor-init)
	IdSetPlain(const IdSetPlain& other) : base_idset() {
		reserve(other.capacity());
		insert(cbegin(), other.cbegin(), other.cend());
		assertrx_dbg(capacity() == other.capacity());
	}
	IdSetPlain& operator=(const IdSetPlain& other) {
		if (this != &other) {
			clear();
			shrink_to_fit();
			reserve(other.capacity());
			insert(cbegin(), other.cbegin(), other.cend());
		}
		assertrx_dbg(capacity() == other.capacity());
		return *this;
	}
	IdSetPlain& operator=(IdSetPlain&& other) noexcept {
		[[maybe_unused]] auto otherCapacity = other.capacity();
		if (this != &other) {
			this->swap(other);
		}
		assertrx_dbg(capacity() == otherCapacity);
		return *this;
	}

	static Ptr BuildFromUnsorted(base_idset&& ids);

	/// @brief Adds a new ID into the set while keeping it ordered.
	/// Preserves the set in the committed (sorted) state.
	bool Add(IdType id, base_idset::size_type sortedIdxCount) {
		reserveForIdAndSortOrders(sortedIdxCount);

		auto pos = std::lower_bound(base_idset::begin(), base_idset::end(), id);
		if ((pos == base_idset::end() || *pos != id)) {
			base_idset::insert(pos, id);
			return true;
		}
		return false;
	}

	/// @brief Adds a new ID into the end of the set without any ordering (O(1)).
	/// Commit() and Erase() should not be used after this type of insertion.
	void AddUnordered(IdType id) { push_back(id); }

	/// @brief Removes an ID from the set.
	/// Assumes that the container is sorted (not filled through AddUnordered()).
	/// @return the number of deleted items.
	int Erase(IdType id) {
		auto d = std::equal_range(base_idset::begin(), base_idset::end(), id);
		int count = std::distance(d.second, d.first);
		base_idset::erase(d.first, d.second);
		return count;
	}

	/// @brief Does nothing for IdSetPlain, assumes that the container is already sorted.
	void Commit([[maybe_unused]] base_idset::size_type sortedIdxCount) const noexcept {}
	constexpr static bool IsCommitted() noexcept { return true; }
	bool IsEmpty() const noexcept { return empty(); }
	size_t Size() const noexcept { return size(); }
	size_t BTreeHeapSize() const noexcept { return 0; }
	const base_idsetset* BTree() const noexcept { return nullptr; }
	void OnSortedIndexCountChanged(base_idset::size_type sortedIdxCount) {
		if (sortedIdxCount) {
			reserve(calcPlainReserveSize(size(), sortedIdxCount));
		} else {
			// Deallocate reserved buffer. Special case for disabled sort orderes.
			shrink_to_fit();
		}
	}
	void Dump(std::ostream&) const;
	size_t HeapSize() const noexcept { return heap_size(); }
	RX_ALWAYS_INLINE size_t PlainHeapSize() const noexcept { return heap_size(); }

	operator IdSetCRef() const& noexcept { return IdSetCRef(begin(), end()); }
	operator IdSetCRef() const&& = delete;

#if REINDEX_DEBUG_CONTAINERS
	const_iterator begin() const noexcept { return base_idset::cbegin(); }
	const_iterator end() const noexcept { return base_idset::cend(); }
	const_reverse_iterator rbegin() const noexcept { return base_idset::crbegin(); }
	const_reverse_iterator rend() const noexcept { return base_idset::crend(); }
#else	// !REINDEX_DEBUG_CONTAINERS
	const_iterator begin() const noexcept { return std::span(data(), size()).begin(); }
	const_iterator end() const noexcept { return std::span(data(), size()).end(); }
	const_reverse_iterator rbegin() const noexcept { return std::span(data(), size()).rbegin(); }
	const_reverse_iterator rend() const noexcept { return std::span(data(), size()).rend(); }
#endif	// !REINDEX_DEBUG_CONTAINERS

	idset_iterator_range idset_range() const& noexcept { return idset::iterators::range(begin(), end()); }
	idset_iterator_range idset_range() const&& = delete;
	idset_reverse_iterator_range idset_reverse_range() const& noexcept { return idset::iterators::reverse_range(rbegin(), rend()); }
	idset_reverse_iterator_range idset_reverse_range() const&& = delete;

protected:
	size_t plainCapacity() const noexcept { return base_idset::capacity(); }
	size_t plainSize() const noexcept { return base_idset::size(); }
	const IdType* plainData() const& noexcept { return base_idset::data(); }
	IdType* plainData() & noexcept { return base_idset::data(); }
	auto plainData() const&& = delete;
	static base_idset::size_type calcPlainReserveSize(base_idset::size_type ids, base_idset::size_type sortedIdxCount) noexcept {
		return ids * (sortedIdxCount + 1);
	}
	void reserveForIdAndSortOrders(base_idset::size_type sortedIdxCount) {
		// reserve extra space for sort orders data
		const auto requiredCap = calcPlainReserveSize(size() + 1, sortedIdxCount);
		if (requiredCap > capacity()) {
			reserve(std::max(requiredCap, capacity() * 2));
		}
	}
};

/// @brief Extended version of IdSetPlain that optimizes insertion time.
/// @details It achieves this by switching the underlying storage between a vector and a btree depending on the data size.
/// During background index optimization, Commit() is called (under the namespace read lock) and switches the storage to
/// a vector regardless of the current IdSet size.
/// Background index optimization runs in parallel with read queries, so if a btree is created at some point after an
/// insertion, it can no longer be deallocated without taking the namespace write lock.
class [[nodiscard]] IdSet : private IdSetPlain {
	friend class SingleSelectKeyResult;
	template <typename>
	friend class BtreeIndexForwardIteratorImpl;
	template <typename>
	friend class BtreeIndexReverseIteratorImpl;

public:
	using idset_iterator = idset::iterators::ForwardIterator;
	using idset_reverse_iterator = idset::iterators::ReverseIterator;
	using idset_iterator_range = idset::iterators::ForwardIteratorRange;
	using idset_reverse_iterator_range = idset::iterators::ReverseIteratorRange;

	IdSet() noexcept = default;
	IdSet(const IdSet& other) = default;
	IdSet(IdSet&& other) noexcept = default;
	IdSet& operator=(IdSet&& other) noexcept = default;
	IdSet& operator=(const IdSet& other) = default;

	/// @brief Adds a new ID into the set while keeping it ordered.
	/// @details It will be represented either as a small vector (insertion is O(logN)+O(N)) or as a btree (insertion is O(logN)).
	/// Commit() will transfer data from the btree into the vector.
	bool Add(IdType id, base_idset::size_type sortedIdxCount) {
		auto [set, isUsingBtree] = set_.Get(std::memory_order_relaxed);

		if (size() >= kMaxPlainIdsetSize && !set) [[unlikely]] {
			auto tmpSet = std::make_unique<base_idsetset>(begin(), end());
			set = tmpSet.get();
			set_.Reset(tmpSet.release(), std::memory_order_relaxed);
		}

		if (!set) {
			assertrx_dbg(size() <= kMaxPlainIdsetSize);
			IdSetPlain::reserveForIdAndSortOrders(sortedIdxCount);

			auto pos = std::lower_bound(base_idset::begin(), base_idset::end(), id);
			if ((pos == base_idset::end() || *pos != id)) {
				base_idset::insert(pos, id);
				return true;
			}
			return false;
		}

		if (!isUsingBtree) {
			clear();
			setUsingBtree(true);
		}

		assertrx_dbg(size() == 0);
		assertrx_dbg(!IsCommitted());
		return set->insert(id).second;
	}

	/// @brief Adds a new ID into the end of the set without any ordering (O(1)).
	/// @note Commit() and Erase() should not be used after this type of insertion.
	void AddUnordered(IdType id) {
		assertrx(!set_.Get(std::memory_order_relaxed).first);
		push_back(id);
	}

	/// @brief Выполняет поиск ID в set'е.
	/// @note Assumes that the container is sorted (not filled through AddUnordered()).
	/// @return true if the ID is found.
	bool Find(IdType id) const noexcept {
		auto set = set_.Get(std::memory_order_relaxed).first;
		if (!set) {
			return (std::find(begin(), end(), id) != end());
		}

		return (set->find(id) != set->end());
	}

	/// @brief Removes an ID from the set.
	/// @note Assumes that the container is sorted (not filled through AddUnordered()).
	/// @return the number of deleted items.
	int Erase(IdType id) noexcept {
		auto [set, isUsingBtree] = set_.Get(std::memory_order_relaxed);
		if (!set) {
			auto d = std::equal_range(base_idset::begin(), base_idset::end(), id);
			int count = std::distance(d.second, d.first);
			base_idset::erase(d.first, d.second);
			return count;
		}

		clear<false>();
		if (!isUsingBtree) {
			setUsingBtree(true);
		}
		return set->erase(id);
	}
	/// @note Has to be called either in background thread under namespace read lock or in foreground thread under namespace write lock
	void Commit(base_idset::size_type sortedIdxCount);

	RX_ALWAYS_INLINE bool IsCommitted() const noexcept { return !set_.Get(std::memory_order_acquire).second; }
	bool IsEmpty() const noexcept { return !Size(); }
	size_t Size() const noexcept {
		auto [set, isUsingBtree] = set_.Get(std::memory_order_acquire);
		return isUsingBtree ? set->size() : size();
	}
	const base_idsetset* BTree() const noexcept {
		auto [set, isUsingBtree] = set_.Get(std::memory_order_acquire);
		return isUsingBtree ? set : nullptr;
	}
	void Dump(std::ostream&) const;

	RX_ALWAYS_INLINE size_t PlainHeapSize() const noexcept { return IdSetPlain::PlainHeapSize(); }
	/// @note UNSAFE: May race with concurrent Commit() calls and has to be called under namespace write lock
	RX_ALWAYS_INLINE size_t BTreeHeapSize() const noexcept {
		auto set = set_.Get(std::memory_order_relaxed).first;
		return set ? sizeof(*set) + set->size() * sizeof(IdType) : 0;
	}
	/// @note UNSAFE: May race with concurrent Commit() calls and has to be called under namespace write lock
	void OnSortedIndexCountChanged(base_idset::size_type sortedIdxCount) {
		auto [set, usingBtree] = set_.Get(std::memory_order_relaxed);
		if (!usingBtree) {
			IdSetPlain::OnSortedIndexCountChanged(sortedIdxCount);
		}
	}

	idset_iterator_range idset_range() const& noexcept {
		auto [set, isUsingBtree] = set_.Get(std::memory_order_acquire);
		assertrx(!isUsingBtree || set);
		return isUsingBtree ? idset::iterators::range(set->begin(), set->end())
							: idset::iterators::range(IdSetPlain::begin(), IdSetPlain::end());
	}
	idset_iterator_range idset_range() const&& = delete;
	idset_reverse_iterator_range idset_reverse_range() const& noexcept {
		auto [set, isUsingBtree] = set_.Get(std::memory_order_acquire);
		assertrx(!isUsingBtree || set);
		return isUsingBtree ? idset::iterators::reverse_range(set->rbegin(), set->rend())
							: idset::iterators::reverse_range(IdSetPlain::rbegin(), IdSetPlain::rend());
	}
	idset_reverse_iterator_range idset_reverse_range() const&& = delete;

protected:
	size_t plainCapacity() const noexcept { return IdSetPlain::plainCapacity(); }
	size_t plainSize() const noexcept { return IdSetPlain::plainSize(); }
	const IdType* plainData() const& noexcept { return IdSetPlain::plainData(); }
	IdType* plainData() & noexcept { return IdSetPlain::plainData(); }
	auto plainData() const&& = delete;

private:
	void setUsingBtree(bool val) noexcept { set_.SetMark(val); }

	class [[nodiscard]] AtomicBtreePtr {
	public:
		AtomicBtreePtr() = default;
		~AtomicBtreePtr() { deleteMarkedPtr(set_.load()); }
		AtomicBtreePtr(const AtomicBtreePtr& other) {
			if (auto [oset, isMarked] = other.Get(std::memory_order_acquire); oset) {
				auto tmp = std::make_unique<base_idsetset>(*oset);
				set_.store(uint64_t(tmp.release()));
				SetMark(isMarked);
			}
		}
		AtomicBtreePtr(AtomicBtreePtr&& other) noexcept : set_{other.set_.load()} { other.set_ = 0; }
		AtomicBtreePtr& operator=(const AtomicBtreePtr& other) {
			if (&other != this) {
				AtomicBtreePtr tmp(other);
				std::swap(*this, tmp);
			}
			return *this;
		}
		AtomicBtreePtr& operator=(AtomicBtreePtr&& other) noexcept {
			if (&other != this) {
				auto oldSet = set_.exchange(other.set_.load());
				deleteMarkedPtr(oldSet);
				other.set_ = 0;
			}
			return *this;
		}

		std::pair<base_idsetset*, bool> Get(std::memory_order order) noexcept { return unmark(set_.load(order)); }
		std::pair<const base_idsetset*, bool> Get(std::memory_order order) const noexcept { return unmark(set_.load(order)); }
		void Reset(base_idsetset* ptr, std::memory_order order) noexcept {
			auto cur = set_.exchange(uint64_t(ptr), order);
			deleteMarkedPtr(cur);
		}
		void SetMark(bool val) noexcept {
			// Method is not really atomic, but it's ok for the current IdSet logic
			auto ptr = set_.load(std::memory_order_acquire);
			if (val) {
				[[maybe_unused]] auto oldValue = set_.exchange(mark(ptr), std::memory_order_release);
			} else {
				[[maybe_unused]] auto oldValue = set_.exchange(uint64_t(unmark(ptr).first), std::memory_order_release);
			}
		}

	private:
		constexpr static uint64_t kMark = uint64_t(0x1) << 63;
		static std::pair<base_idsetset*, bool> unmark(uint64_t ptr) noexcept {
			return std::make_pair(reinterpret_cast<base_idsetset*>(ptr & (~kMark)), ptr & kMark);
		}
		static uint64_t mark(uint64_t ptr) noexcept { return ptr | kMark; }
		void deleteMarkedPtr(uint64_t ptr) noexcept { delete unmark(ptr).first; }

		// Contains pointer and synchronization mark
		std::atomic<uint64_t> set_;
	};

	AtomicBtreePtr set_;
};

namespace concepts {

template <typename IdSetT>
concept IdSetWithSortedIDs = !std::same_as<IdSetT, IdSetUnique>;

}

}  // namespace reindexer
