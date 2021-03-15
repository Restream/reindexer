#pragma once

#include <core/type_consts.h>
#include <algorithm>
#include <atomic>
#include <string>
#include "cpp-btree/btree_set.h"
#include "estl/h_vector.h"
#include "estl/intrusive_ptr.h"
#include "estl/span.h"

namespace reindexer {
using std::string;
using std::shared_ptr;

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

	void Add(IdType id, EditMode editMode, int sortedIdxCount) {
		grow((size() + 1) * (sortedIdxCount + 1));
		if (editMode == Unordered) {
			push_back(id);
			return;
		}

		auto pos = std::lower_bound(begin(), end(), id);
		if ((pos == end() || *pos != id)) base_idset::insert(pos, id);
	}

	int Erase(IdType id) {
		auto d = std::equal_range(begin(), end(), id);
		base_idset::erase(d.first, d.second);
		return d.second - d.first;
	}

	void Commit();
	bool IsCommited() const { return true; }
	bool IsEmpty() const { return empty(); }
	size_t Size() const { return size(); }
	size_t BTreeSize() const { return 0; }
	const base_idsetset *BTree() const { return nullptr; }
	void ReserveForSorted(int sortedIdxCount) { reserve(size() * (sortedIdxCount + 1)); }
	string Dump();
};

// maxmimum size of idset without building btree
const int kMaxPlainIdsetSize = 16;

class IdSet : public IdSetPlain {
	friend class SingleSelectKeyResult;

public:
	using Ptr = intrusive_ptr<intrusive_atomic_rc_wrapper<IdSet>>;
	IdSet() : usingBtree_(false) {}
	IdSet(const IdSet &other)
		: IdSetPlain(other), set_(!other.set_ ? nullptr : new base_idsetset(*other.set_)), usingBtree_(other.usingBtree_.load()) {}
	IdSet(IdSet &&other) noexcept : IdSetPlain(std::move(other)), set_(std::move(other.set_)), usingBtree_(other.usingBtree_.load()) {}
	IdSet &operator=(IdSet &&other) noexcept {
		if (&other != this) {
			IdSetPlain::operator=(std::move(other));
			set_ = std::move(other.set_);
			usingBtree_ = other.usingBtree_.load();
		}
		return *this;
	}
	IdSet &operator=(const IdSet &other) {
		if (&other != this) {
			IdSetPlain::operator=(other);
			set_.reset(!other.set_ ? nullptr : new base_idsetset(*other.set_));
			usingBtree_ = other.usingBtree_.load();
		}
		return *this;
	}
	void Add(IdType id, EditMode editMode, int sortedIdxCount) {
		// Reserve extra space for sort orders data
		grow(((set_ ? set_->size() : size()) + 1) * (sortedIdxCount + 1));

		if (editMode == Unordered) {
			assert(!set_);
			push_back(id);
			return;
		}

		if (int(size()) >= kMaxPlainIdsetSize && !set_ && editMode == Auto) {
			set_.reset(new base_idsetset);
			set_->insert(begin(), end());
			usingBtree_ = true;
			resize(0);
		}

		if (!set_) {
			auto pos = std::lower_bound(begin(), end(), id);
			if ((pos == end() || *pos != id)) base_idset::insert(pos, id);
		} else {
			resize(0);
			set_->insert(id);
			usingBtree_ = true;
		}
	}

	template <typename InputIt>
	void Append(InputIt first, InputIt last, EditMode editMode = Auto) {
		if (editMode == Unordered) {
			assert(!set_);
			insert(base_idset::end(), first, last);
		} else if (editMode == Auto) {
			if (!set_) {
				set_.reset(new base_idsetset);
				set_->insert(begin(), end());
				resize(0);
			}
			assert(!size());
			set_->insert(first, last);
			usingBtree_ = true;
		} else {
			assert(0);
		}
	}

	int Erase(IdType id) {
		if (!set_) {
			auto d = std::equal_range(begin(), end(), id);
			base_idset::erase(d.first, d.second);
			return d.second - d.first;
		} else {
			resize(0);
			usingBtree_ = true;
			return set_->erase(id);
		}
		return 0;
	}
	void Commit();
	bool IsCommited() const { return !usingBtree_; }
	bool IsEmpty() const { return empty() && (!set_ || set_->empty()); }
	size_t Size() const { return usingBtree_.load(std::memory_order_relaxed) ? set_->size() : size(); }
	size_t BTreeSize() const { return set_ ? sizeof(*set_.get()) + set_->size() * sizeof(int) : 0; }
	const base_idsetset *BTree() const { return set_.get(); }
	void ReserveForSorted(int sortedIdxCount) { reserve(((set_ ? set_->size() : size())) * (sortedIdxCount + 1)); }

protected:
	template <typename>
	friend class BtreeIndexForwardIteratorImpl;
	template <typename>
	friend class BtreeIndexReverseIteratorImpl;

	std::unique_ptr<base_idsetset> set_;
	std::atomic<bool> usingBtree_;
};

using IdSetRef = span<IdType>;

}  // namespace reindexer
