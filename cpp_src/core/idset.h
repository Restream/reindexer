#pragma once

#include <core/type_consts.h>
#include <algorithm>
#include <atomic>
#include <string>
#include "cpp-btree/btree_set.h"
#include "estl/h_vector.h"
#include "estl/intrusive_ptr.h"
#include "estl/span.h"
#include "sort/pdqsort.hpp"

namespace reindexer {

using base_idset = h_vector<IdType, 3>;
using base_idsetset = btree::btree_set<int>;

class IdSetPlain : protected base_idset {
public:
	using iterator = base_idset::const_iterator;
	using base_idset::clear;
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
	using base_idset::begin;
	using base_idset::end;
	using base_idset::rbegin;
	using base_idset::rend;
	using base_idset::const_reverse_iterator;
	using base_idset::const_iterator;

	enum EditMode {
		Ordered,   // Keep idset ordered, and ready to select (insert is slow O(logN)+O(N))
		Auto,	   // Prepare idset for fast ordering by commit (insert is fast O(logN))
		Unordered  // Just add id, commit and erase is impossible
	};

	IdSetPlain() = default;
	bool Add(IdType id, EditMode editMode, int sortedIdxCount) {
		grow((size() + 1) * (sortedIdxCount + 1));
		if (editMode == Unordered) {
			push_back(id);
			return true;
		}

		auto pos = std::lower_bound(begin(), end(), id);
		if ((pos == end() || *pos != id)) {
			base_idset::insert(pos, id);
			return true;
		}
		return false;
	}

	int Erase(IdType id) {
		auto d = std::equal_range(begin(), end(), id);
		base_idset::erase(d.first, d.second);
		return d.second - d.first;
	}

	void Commit() const noexcept {}
	bool IsCommited() const noexcept { return true; }
	bool IsEmpty() const noexcept { return empty(); }
	size_t Size() const noexcept { return size(); }
	size_t BTreeSize() const noexcept { return 0; }
	const base_idsetset* BTree() const noexcept { return nullptr; }
	void ReserveForSorted(int sortedIdxCount) { reserve(size() * (sortedIdxCount + 1)); }
	std::string Dump() const;

protected:
	IdSetPlain(base_idset&& idset) noexcept : base_idset(std::move(idset)) {}
};

std::ostream& operator<<(std::ostream&, const IdSetPlain&);

// maxmimum size of idset without building btree
const int kMaxPlainIdsetSize = 16;

class IdSet : public IdSetPlain {
	friend class SingleSelectKeyResult;

public:
	using Ptr = intrusive_ptr<intrusive_atomic_rc_wrapper<IdSet>>;
	IdSet() noexcept : usingBtree_(false) {}
	IdSet(const IdSet& other)
		: IdSetPlain(other), set_(!other.set_ ? nullptr : new base_idsetset(*other.set_)), usingBtree_(other.usingBtree_.load()) {}
	IdSet(IdSet&& other) noexcept : IdSetPlain(std::move(other)), set_(std::move(other.set_)), usingBtree_(other.usingBtree_.load()) {}
	IdSet& operator=(IdSet&& other) noexcept {
		if (&other != this) {
			IdSetPlain::operator=(std::move(other));
			set_ = std::move(other.set_);
			usingBtree_ = other.usingBtree_.load();
		}
		return *this;
	}
	IdSet& operator=(const IdSet& other) {
		if (&other != this) {
			IdSetPlain::operator=(other);
			set_.reset(!other.set_ ? nullptr : new base_idsetset(*other.set_));
			usingBtree_ = other.usingBtree_.load();
		}
		return *this;
	}
	static Ptr BuildFromUnsorted(base_idset&& ids) {
		boost::sort::pdqsort_branchless(ids.begin(), ids.end());
		ids.erase(std::unique(ids.begin(), ids.end()), ids.end());	// TODO: It would be better to integrate unique into sort
		return make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>(std::move(ids));
	}
	bool Add(IdType id, EditMode editMode, int sortedIdxCount) {
		// reserve extra space for sort orders data
		grow(((set_ ? set_->size() : size()) + 1) * (sortedIdxCount + 1));

		if (editMode == Unordered) {
			assertrx(!set_);
			push_back(id);
			return true;
		}

		if (int(size()) >= kMaxPlainIdsetSize && !set_ && editMode == Auto) {
			set_.reset(new base_idsetset);
			set_->insert(begin(), end());
		}

		if (!set_) {
			auto pos = std::lower_bound(begin(), end(), id);
			if ((pos == end() || *pos != id)) {
				base_idset::insert(pos, id);
				return true;
			}
			return false;
		} else {
			resize(0);
			usingBtree_.store(true, std::memory_order_release);
			return set_->insert(id).second;
		}
	}

	void AddUnordered(IdType id) {
		assertrx(!set_);
		push_back(id);
	}

	template <typename InputIt>
	void Append(InputIt first, InputIt last, EditMode editMode = Auto) {
		if (editMode == Unordered) {
			assertrx(!set_);
			insert(base_idset::end(), first, last);
		} else if (editMode == Auto) {
			if (!set_) {
				set_.reset(new base_idsetset);
				set_->insert(begin(), end());
				resize(0);
			}
			assertrx(!size());
			set_->insert(first, last);
			usingBtree_.store(true, std::memory_order_release);
		} else {
			assertrx(0);
		}
	}

	template <typename InputIt>
	void Append(InputIt first, InputIt last, const std::vector<bool>& mask, EditMode editMode = Auto) {
		if (editMode == Unordered) {
			assertrx(!set_);
			for (; first != last; ++first) {
				if (mask[*first]) {
					push_back(*first);
				}
			}
		} else if (editMode == Auto) {
			if (!set_) {
				set_.reset(new base_idsetset);
				set_->insert(begin(), end());
				resize(0);
			}
			assertrx(!size());
			for (; first != last; ++first) {
				if (mask[*first]) {
					set_->insert(*first);
				}
			}
			usingBtree_.store(true, std::memory_order_release);
		} else {
			assertrx(0);
		}
	}

	int Erase(IdType id) {
		if (!set_) {
			auto d = std::equal_range(begin(), end(), id);
			base_idset::erase(d.first, d.second);
			return d.second - d.first;
		} else {
			clear<false>();
			usingBtree_.store(true, std::memory_order_release);
			return set_->erase(id);
		}
	}
	void Commit() {
		if (!size() && set_) {
			reserve(set_->size());
			for (auto id : *set_) {
				push_back(id);
			}
		}

		usingBtree_.store(false, std::memory_order_release);
	}
	bool IsCommited() const noexcept { return !usingBtree_.load(std::memory_order_acquire); }
	bool IsEmpty() const noexcept { return empty() && (!set_ || set_->empty()); }
	size_t Size() const noexcept { return usingBtree_.load(std::memory_order_acquire) ? set_->size() : size(); }
	size_t BTreeSize() const noexcept { return set_ ? sizeof(*set_.get()) + set_->size() * sizeof(int) : 0; }
	const base_idsetset* BTree() const noexcept { return set_.get(); }
	void ReserveForSorted(int sortedIdxCount) { reserve(((set_ ? set_->size() : size())) * (sortedIdxCount + 1)); }

protected:
	template <typename>
	friend class BtreeIndexForwardIteratorImpl;
	template <typename>
	friend class BtreeIndexReverseIteratorImpl;

	IdSet(base_idset&& idset) noexcept : IdSetPlain(std::move(idset)), usingBtree_(false) {}

	std::unique_ptr<base_idsetset> set_;
	std::atomic<bool> usingBtree_;
};

using IdSetRef = span<IdType>;
using IdSetCRef = span<const IdType>;

}  // namespace reindexer
