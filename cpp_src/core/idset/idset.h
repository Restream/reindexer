#pragma once

#include <algorithm>
#include <atomic>
#include <span>
#include <string>
#include "core/type_consts.h"
#include "cpp-btree/btree_set.h"
#include "estl/h_vector.h"
#include "estl/intrusive_ptr.h"
#include "iterator.h"
#include "sort/pdqsort.hpp"
#include "tools/errors.h"

namespace reindexer {

constexpr int kIdSetBtreeNodeSize = 256;
#if !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)
// Maximum size of idset without building btree
constexpr int kMaxPlainIdsetSize = 256;
#else	// defined(REINDEX_WITH_ASAN) || defined(REINDEX_WITH_TSAN)
// Use smaller value in sanitizers build to get more IdSets states in testing environment
constexpr int kMaxPlainIdsetSize = 16;
#endif	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)

using base_idset = h_vector<IdType, 3>;	 // const_iterator must be trivial (used in union)
using base_idset_ptr = intrusive_ptr<intrusive_atomic_rc_wrapper<const base_idset>>;
using base_idsetset = btree::btree_set<IdType, std::less<IdType>, std::allocator<IdType>, kIdSetBtreeNodeSize>;

enum class [[nodiscard]] IdSetEditMode {
	Ordered,   // Keep idset ordered, and ready to select (insert is slow O(logN)+O(N))
	Auto,	   // Prepare idset for fast ordering by commit (insert is fast O(logN))
	Unordered  // Just add id, commit and erase is impossible
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

class [[nodiscard]] IdSetUnique {
public:
	using const_iterator = std::span<const IdType>::iterator;
	using const_reverse_iterator = std::span<const IdType>::reverse_iterator;

	using idset_iterator_range = idset::iterators::ForwardIteratorRange;
	using idset_reverse_iterator_range = idset::iterators::ReverseIteratorRange;

	IdSetUnique() = default;
	IdSetUnique(const IdSetUnique&) = default;
	IdSetUnique& operator=(const IdSetUnique& other) = default;

	bool Add(IdType id, IdSetEditMode, int) {
		if (!empty()) [[unlikely]] {
			throw DuplicatedItemIDError(
				id_.ToNumber(),
				Error(errConflict, std::string("Duplicated item id, that has to be unique: ").append(std::to_string(id_.ToNumber()))));
		}
		id_ = id;
		return true;
	}

	int Erase(IdType id) {
		if (id_.ToNumber() == id.ToNumber()) {
			id_ = IdType::NotSet();
			return 1;
		}
		return 0;
	}

	void Commit() const noexcept {}
	constexpr static bool IsCommitted() noexcept { return true; }
	bool IsEmpty() const noexcept { return empty(); }
	size_t Size() const noexcept { return size(); }
	size_t BTreeSize() const noexcept { return 0; }
	const base_idsetset* BTree() const noexcept { return nullptr; }
	void ReserveForSorted(int) {}
	std::string Dump() const;

	size_t size() const noexcept { return id_.IsValid() ? 1 : 0; }
	size_t capacity() const noexcept { return 1; }
	size_t heap_size() const noexcept { return 0; }
	bool empty() const noexcept { return !id_.IsValid(); }

	const_iterator begin() const noexcept { return std::span(data(), size()).begin(); }
	const_iterator end() const noexcept { return std::span(data(), size()).end(); }
	const_reverse_iterator rbegin() const noexcept { return std::span(data(), size()).rbegin(); }
	const_reverse_iterator rend() const noexcept { return std::span(data(), size()).rend(); }

	const IdType* data() const noexcept { return &id_; }

	idset_iterator_range idset_range() const noexcept { return idset::iterators::range(*this); }
	idset_reverse_iterator_range idset_reverse_range() const noexcept { return idset::iterators::reverse_range(*this); }

private:
	IdType id_ = IdType::NotSet();
};

std::ostream& operator<<(std::ostream&, const IdSetUnique&);

class [[nodiscard]] IdSetPlain : protected base_idset {
public:
#if REINDEX_DEBUG_CONTAINERS
	using base_idset::const_iterator;
	using base_idset::const_reverse_iterator;
#else	// !REINDEX_DEBUG_CONTAINERS
	using const_iterator = std::span<const IdType>::iterator;
	using const_reverse_iterator = std::span<const IdType>::reverse_iterator;
#endif	// REINDEX_DEBUG_CONTAINERS

	using base_idset::size;
	using base_idset::empty;
	using base_idset::data;
	using base_idset::erase;
	using base_idset::reserve;
	using base_idset::value_type;
	using base_idset::capacity;
	using base_idset::shrink_to_fit;
	using base_idset::back;
	using base_idset::heap_size;
	using base_idset::operator[];
	using idset_iterator = idset::iterators::ForwardIterator;
	using idset_reverse_iterator = idset::iterators::ReverseIterator;
	using idset_iterator_range = idset::iterators::ForwardIteratorRange;
	using idset_reverse_iterator_range = idset::iterators::ReverseIteratorRange;

	IdSetPlain() = default;
	bool Add(IdType id, IdSetEditMode editMode, int sortedIdxCount) {
		grow((size() + 1) * (sortedIdxCount + 1));
		if (editMode == IdSetEditMode::Unordered) {
			push_back(id);
			return true;
		}

		auto pos = std::lower_bound(base_idset::begin(), base_idset::end(), id);
		if ((pos == base_idset::end() || *pos != id)) {
			base_idset::insert(pos, id);
			return true;
		}
		return false;
	}

	int Erase(IdType id) {
		auto d = std::equal_range(base_idset::begin(), base_idset::end(), id);
		int count = std::distance(d.second, d.first);
		base_idset::erase(d.first, d.second);
		return count;
	}

	void Commit() const noexcept {}
	constexpr static bool IsCommitted() noexcept { return true; }
	bool IsEmpty() const noexcept { return empty(); }
	size_t Size() const noexcept { return size(); }
	size_t BTreeSize() const noexcept { return 0; }
	const base_idsetset* BTree() const noexcept { return nullptr; }
	void ReserveForSorted(int sortedIdxCount) { reserve(size() * (sortedIdxCount + 1)); }
	std::string Dump() const;

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

	idset_iterator_range idset_range() const noexcept { return idset::iterators::range(*this); }
	idset_reverse_iterator_range idset_reverse_range() const noexcept { return idset::iterators::reverse_range(*this); }
};

std::ostream& operator<<(std::ostream&, const IdSetPlain&);

class [[nodiscard]] IdSet : public IdSetPlain {
	friend class SingleSelectKeyResult;
	template <typename>
	friend class BtreeIndexForwardIteratorImpl;
	template <typename>
	friend class BtreeIndexReverseIteratorImpl;

public:
	using Ptr = intrusive_ptr<intrusive_atomic_rc_wrapper<IdSet>>;
	IdSet() noexcept = default;
	IdSet(const IdSet& other) = default;
	IdSet(IdSet&& other) noexcept = default;
	IdSet& operator=(IdSet&& other) noexcept = default;
	IdSet& operator=(const IdSet& other) = default;
	static Ptr BuildFromUnsorted(base_idset&& ids) {
		boost::sort::pdqsort_branchless(ids.begin(), ids.end());
		ids.erase(std::unique(ids.begin(), ids.end()), ids.cend());	 // TODO: It would be better to integrate unique into sort
		return make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>(std::move(ids));
	}
	bool Add(IdType id, IdSetEditMode editMode, int sortedIdxCount) {
		auto [set, isUsingBtree] = set_.Get(std::memory_order_relaxed);
		// reserve extra space for sort orders data
		grow(((set ? set->size() : size()) + 1) * (sortedIdxCount + 1));

		if (editMode == IdSetEditMode::Unordered) {
			assertrx(!set);
			push_back(id);
			return true;
		}

		if (int(size()) >= kMaxPlainIdsetSize && !set && editMode == IdSetEditMode::Auto) {
			auto tmpSet = std::make_unique<base_idsetset>(begin(), end());
			set = tmpSet.get();
			set_.Reset(tmpSet.release(), std::memory_order_relaxed);
		}

		if (!set) {
			auto pos = std::lower_bound(base_idset::begin(), base_idset::end(), id);
			if ((pos == base_idset::end() || *pos != id)) {
				base_idset::insert(pos, id);
				return true;
			}
			return false;
		}

		resize(0);
		if (!isUsingBtree) {
			setUsingBtree(true);
		}
		return set->insert(id).second;
	}

	void AddUnordered(IdType id) {
		assertrx(!set_.Get(std::memory_order_relaxed).first);
		push_back(id);
	}

	void SetUnordered(IdSetPlain&& other) {
		assertrx(!set_.Get(std::memory_order_relaxed).first);
		IdSetPlain::operator=(std::move(other));
	}

	template <typename InputIt>
	void Append(InputIt first, InputIt last, IdSetEditMode editMode = IdSetEditMode::Auto) {
		auto [set, isUsingBtree] = set_.Get(std::memory_order_relaxed);
		if (editMode == IdSetEditMode::Unordered) {
			assertrx(!set);
			insert(base_idset::end(), first, last);
		} else if (editMode == IdSetEditMode::Auto) {
			if (!set) {
				set = new base_idsetset;
				set_.Reset(set, std::memory_order_relaxed);
				set->insert(begin(), end());
				resize(0);
			}
			assertrx(!size());
			set->insert(first, last);
			if (!isUsingBtree) {
				setUsingBtree(true);
			}
		} else {
			assertrx(0);
		}
	}

	template <typename InputIt>
	void Append(InputIt first, InputIt last, const std::vector<bool>& mask, IdSetEditMode editMode = IdSetEditMode::Auto) {
		auto [set, isUsingBtree] = set_.Get(std::memory_order_relaxed);
		if (editMode == IdSetEditMode::Unordered) {
			assertrx(!set);
			for (; first != last; ++first) {
				if (mask[first->ToNumber()]) {
					push_back(*first);
				}
			}
		} else if (editMode == IdSetEditMode::Auto) {
			if (!set) {
				set = new base_idsetset;
				set_.Reset(set, std::memory_order_relaxed);
				set->insert(begin(), end());
				resize(0);
			}
			assertrx(!size());
			for (; first != last; ++first) {
				if (mask[first->ToNumber()]) {
					set->insert(*first);
				}
			}
			if (!isUsingBtree) {
				setUsingBtree(true);
			}
		} else {
			assertrx(0);
		}
	}

	bool Find(IdType id) const noexcept {
		auto set = set_.Get(std::memory_order_relaxed).first;
		if (!set) {
			return (std::find(begin(), end(), id) != end());
		}

		return (set->find(id) != set->end());
	}

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
	void Commit() {
		if (!size()) {
			auto set = set_.Get(std::memory_order_relaxed).first;
			if (set) {
				reserve(set->size());
				for (auto id : *set) {
					push_back(id);
				}
			}
		}

		setUsingBtree(false);
	}

	bool IsCommitted() const noexcept { return !set_.Get(std::memory_order_acquire).second; }
	bool IsEmpty() const noexcept { return !Size(); }
	size_t Size() const noexcept {
		auto [set, isUsingBtree] = set_.Get(std::memory_order_acquire);
		return isUsingBtree ? set->size() : size();
	}
	const base_idsetset* BTree() const noexcept {
		auto [set, isUsingBtree] = set_.Get(std::memory_order_acquire);
		return isUsingBtree ? set : nullptr;
	}
	void Dump(auto&) const;

	// UNSAFE (may race with concurrent Commit() calls)
	size_t BTreeSize() const noexcept {
		auto set = set_.Get(std::memory_order_relaxed).first;
		return set ? sizeof(*set) + set->size() * sizeof(IdType) : 0;
	}
	// UNSAFE (may race with concurrent Commit() calls)
	void ReserveForSorted(int sortedIdxCount) {
		auto set = set_.Get(std::memory_order_relaxed).first;
		reserve(((set ? set->size() : size())) * (sortedIdxCount + 1));
	}

	idset_iterator_range idset_range() const noexcept { return idset::iterators::range(*this); }
	idset_reverse_iterator_range idset_reverse_range() const noexcept { return idset::iterators::reverse_range(*this); }

protected:
	explicit IdSet(base_idset&& idset) noexcept : IdSetPlain(std::move(idset)) {}

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

using IdSetCRef = std::span<const IdType>;

namespace concepts {

template <typename IdSetT>
concept IdSetWithSortedIDs = !std::same_as<IdSetT, IdSetUnique>;

}

}  // namespace reindexer
