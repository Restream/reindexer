#pragma once

#include <core/type_consts.h>
#include <algorithm>
#include <string>
#include "cpp-btree/btree_set.h"
#include "estl/h_vector.h"

namespace reindexer {
using std::string;
using std::shared_ptr;

class CommitContext {
public:
	virtual int getSortedIdxCount() const = 0;
	virtual int phases() const = 0;
	virtual ~CommitContext(){};

	// Commit phases
	enum Phase {
		// Indexes shoud normalize their idsets (sort and remove deleted id and duplicates )
		// Namespace will reset updated and deleted flags
		MakeIdsets = 1,
		// Make sort orders
		MakeSortOrders = 4,
		// Indexes should be ready for processing selects
		PrepareForSelect = 8,
	};
};

using base_idset = h_vector<IdType, 3>;

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
	iterator begin() { return base_idset::begin(); }
	iterator end() { return base_idset::end(); }

	enum EditMode {
		Ordered,   // Keep idset ordered, and ready to select (insert is slow O(logN)+O(N))
		Auto,	  // Prepare idset for fast ordering by commit (insert is fast O(logN))
		Unordered  // Just add id, commit and erase is impossible
	};

	void Add(IdType id, EditMode editMode) {
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
	void Commit(const CommitContext &ctx);
	bool IsCommited() { return true; }
	string Dump();
};

using base_idsetset = btree::btree_set<int>;

// maxmimum size of idset without building btree
const int kMaxPlainIdsetSize = 16;

class IdSet : public IdSetPlain {
public:
	typedef shared_ptr<IdSet> Ptr;
	IdSet() {}
	IdSet(const IdSet &other) : IdSetPlain(other), set_(!other.set_ ? nullptr : new base_idsetset(*other.set_)) {}
	IdSet &operator=(IdSet &&other) noexcept {
		if (&other != this) {
			IdSetPlain::operator=(other);
			set_ = std::move(other.set_);
		}
		return *this;
	}
	void Add(IdType id, EditMode editMode) {
		if (editMode == Unordered) {
			assert(!set_);
			push_back(id);
			return;
		}

		if (int(size()) >= kMaxPlainIdsetSize && !set_ && editMode == Auto) {
			set_.reset(new base_idsetset);
			set_->insert(begin(), end());
			resize(0);
		}

		if (!set_) {
			auto pos = std::lower_bound(begin(), end(), id);
			if ((pos == end() || *pos != id)) base_idset::insert(pos, id);
		} else {
			resize(0);
			set_->insert(id);
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
			return set_->erase(id);
		}
		return 0;
	}
	void Commit(const CommitContext &ctx);
	bool IsCommited() { return (!set_ || !set_->size() || size()) && std::is_sorted(begin(), end()); }

protected:
	std::unique_ptr<base_idsetset> set_;
};

class IdSetRef : public h_vector_view<IdType> {
public:
	template <typename IdSetT>
	IdSetRef(const IdSetT *ids) : h_vector_view<IdType>(ids->data(), ids->size()) {}
	IdSetRef(const IdType *data, size_t len) : h_vector_view<IdType>(data, len) {}
	IdSetRef() {}
};

}  // namespace reindexer
