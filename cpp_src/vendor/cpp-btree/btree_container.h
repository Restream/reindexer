// Copyright 2013 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef UTIL_BTREE_BTREE_CONTAINER_H__
#define UTIL_BTREE_BTREE_CONTAINER_H__

#include <iosfwd>
#include <utility>

#include "btree.h"

namespace btree {

// A common base class for btree_set, btree_map, btree_multiset and
// btree_multimap.
template <typename Tree>
class btree_container {
	typedef btree_container<Tree> self_type;

public:
	typedef typename Tree::params_type params_type;
	typedef typename Tree::key_type key_type;
	typedef typename Tree::value_type value_type;
	typedef typename Tree::key_compare key_compare;
	typedef typename Tree::allocator_type allocator_type;
	typedef typename Tree::pointer pointer;
	typedef typename Tree::const_pointer const_pointer;
	typedef typename Tree::reference reference;
	typedef typename Tree::const_reference const_reference;
	typedef typename Tree::size_type size_type;
	typedef typename Tree::difference_type difference_type;
	typedef typename Tree::iterator iterator;
	typedef typename Tree::const_iterator const_iterator;
	typedef typename Tree::reverse_iterator reverse_iterator;
	typedef typename Tree::const_reverse_iterator const_reverse_iterator;

public:
	// Default constructor.
	btree_container(const key_compare &comp, const allocator_type &alloc) : tree_(comp, alloc) {}

	// Copy constructor.
	btree_container(const self_type &x) : tree_(x.tree_) {}

	// Move constructor.
	btree_container(self_type &&x) noexcept : tree_(std::move(x.tree_)) {}

	// Iterator routines.
	iterator begin() noexcept(noexcept(std::declval<Tree>().begin())) { return tree_.begin(); }
	const_iterator begin() const noexcept(noexcept(std::declval<Tree>().begin())) { return tree_.begin(); }
	iterator end() noexcept(noexcept(std::declval<Tree>().end())) { return tree_.end(); }
	const_iterator end() const noexcept(noexcept(std::declval<Tree>().end())) { return tree_.end(); }
	reverse_iterator rbegin() noexcept(noexcept(std::declval<Tree>().rbegin())) { return tree_.rbegin(); }
	const_reverse_iterator rbegin() const noexcept(noexcept(std::declval<Tree>().rbegin())) { return tree_.rbegin(); }
	reverse_iterator rend() noexcept(noexcept(std::declval<Tree>().rend())) { return tree_.rend(); }
	const_reverse_iterator rend() const noexcept(noexcept(std::declval<Tree>().rend())) { return tree_.rend(); }

	// Lookup routines.
	iterator lower_bound(const key_type &key) { return tree_.lower_bound(key); }
	const_iterator lower_bound(const key_type &key) const { return tree_.lower_bound(key); }
	iterator upper_bound(const key_type &key) { return tree_.upper_bound(key); }
	const_iterator upper_bound(const key_type &key) const { return tree_.upper_bound(key); }
	std::pair<iterator, iterator> equal_range(const key_type &key) { return tree_.equal_range(key); }
	std::pair<const_iterator, const_iterator> equal_range(const key_type &key) const { return tree_.equal_range(key); }

	template <typename K>
	iterator lower_bound(const K &key) {
		return tree_.lower_bound(key);
	}
	template <typename K>
	const_iterator lower_bound(const K &key) const {
		return tree_.lower_bound(key);
	}
	template <typename K>
	iterator upper_bound(const K &key) {
		return tree_.upper_bound(key);
	}
	template <typename K>
	const_iterator upper_bound(const K &key) const {
		return tree_.upper_bound(key);
	}
	template <typename K>
	std::pair<iterator, iterator> equal_range(const K &key) {
		return tree_.equal_range(key);
	}
	template <typename K>
	std::pair<const_iterator, const_iterator> equal_range(const K &key) const {
		return tree_.equal_range(key);
	}

	// Utility routines.
	void clear() { tree_.clear(); }
	void swap(self_type &x) { tree_.swap(x.tree_); }
	void dump(std::ostream &os) const { tree_.dump(os); }
	void verify() const { tree_.verify(); }

	// Size routines.
	size_type size() const noexcept(noexcept(std::declval<Tree>().size())) { return tree_.size(); }
	size_type max_size() const noexcept(noexcept(std::declval<Tree>().max_size())) { return tree_.max_size(); }
	bool empty() const noexcept(noexcept(std::declval<Tree>().empty())) { return tree_.empty(); }
	size_type height() const noexcept(noexcept(std::declval<Tree>().height())) { return tree_.height(); }
	size_type internal_nodes() const noexcept(noexcept(std::declval<Tree>().internal_nodes())) { return tree_.internal_nodes(); }
	size_type leaf_nodes() const noexcept(noexcept(std::declval<Tree>().leaf_nodes())) { return tree_.leaf_nodes(); }
	size_type nodes() const noexcept(noexcept(std::declval<Tree>().nodes())) { return tree_.nodes(); }
	size_type bytes_used() const noexcept(noexcept(std::declval<Tree>().bytes_used())) { return tree_.bytes_used(); }
	static double average_bytes_per_value() noexcept(noexcept(Tree::average_bytes_per_value())) { return Tree::average_bytes_per_value(); }
	double fullness() const noexcept(noexcept(std::declval<Tree>().fullness())) { return tree_.fullness(); }
	double overhead() const noexcept(noexcept(std::declval<Tree>().overhead())) { return tree_.overhead(); }
	const key_compare &key_comp() const noexcept(noexcept(std::declval<Tree>().key_comp())) { return tree_.key_comp(); }

	bool operator==(const self_type &x) const {
		if (size() != x.size()) {
			return false;
		}
		for (const_iterator i = begin(), xi = x.begin(); i != end(); ++i, ++xi) {
			if (*i != *xi) {
				return false;
			}
		}
		return true;
	}

	bool operator!=(const self_type &other) const { return !operator==(other); }

protected:
	Tree tree_;
};

template <typename T>
inline std::ostream &operator<<(std::ostream &os, const btree_container<T> &b) {
	b.dump(os);
	return os;
}

// A common base class for btree_set and safe_btree_set.
template <typename Tree>
class btree_unique_container : public btree_container<Tree> {
	typedef btree_unique_container<Tree> self_type;
	typedef btree_container<Tree> super_type;

public:
	typedef typename Tree::key_type key_type;
	typedef typename Tree::value_type value_type;
	typedef typename Tree::size_type size_type;
	typedef typename Tree::key_compare key_compare;
	typedef typename Tree::allocator_type allocator_type;
	typedef typename Tree::iterator iterator;
	typedef typename Tree::const_iterator const_iterator;

public:
	// Default constructor.
	btree_unique_container(const key_compare &comp = key_compare(), const allocator_type &alloc = allocator_type())
		: super_type(comp, alloc) {}

	// Copy constructor.
	btree_unique_container(const self_type &x) : super_type(x) {}

	// Move constructor.
	btree_unique_container(self_type &&x) noexcept : super_type(std::move(x)) {}

	// Range constructor.
	template <class InputIterator>
	btree_unique_container(InputIterator b, InputIterator e, const key_compare &comp = key_compare(),
						   const allocator_type &alloc = allocator_type())
		: super_type(comp, alloc) {
		insert(b, e);
	}

	// Lookup routines.
	iterator find(const key_type &key) { return this->tree_.find_unique(key); }
	const_iterator find(const key_type &key) const { return this->tree_.find_unique(key); }
	size_type count(const key_type &key) const { return this->tree_.count_unique(key); }

	template <typename K>
	iterator find(const K &key) {
		return this->tree_.find_unique(key);
	}
	template <typename K>
	const_iterator find(const K &key) const {
		return this->tree_.find_unique(key);
	}
	template <typename K>
	size_type count(const K &key) const {
		return this->tree_.count_unique(key);
	}

	// Insertion routines.
	std::pair<iterator, bool> insert(const value_type &x) { return this->tree_.insert_unique(x); }
	iterator insert(iterator position, const value_type &x) { return this->tree_.insert_unique(position, x); }
	template <typename InputIterator>
	void insert(InputIterator b, InputIterator e) {
		this->tree_.insert_unique(b, e);
	}

	// Deletion routines.
	int erase(const key_type &key) { return this->tree_.erase_unique(key); }
	// Erase the specified iterator from the btree. The iterator must be valid
	// (i.e. not equal to end()).  Return an iterator pointing to the node after
	// the one that was erased (or end() if none exists).
	iterator erase(const iterator &iter) { return this->tree_.erase(iter); }
	void erase(const iterator &first, const iterator &last) { this->tree_.erase(first, last); }
};

// A common base class for btree_map and safe_btree_map.
template <typename Tree>
class btree_map_container : public btree_unique_container<Tree> {
	typedef btree_map_container<Tree> self_type;
	typedef btree_unique_container<Tree> super_type;

public:
	typedef typename Tree::key_type key_type;
	typedef typename Tree::data_type data_type;
	typedef typename Tree::value_type value_type;
	typedef typename Tree::mapped_type mapped_type;
	typedef typename Tree::key_compare key_compare;
	typedef typename Tree::allocator_type allocator_type;

private:
	// A pointer-like object which only generates its value when
	// dereferenced. Used by operator[] to avoid constructing an empty data_type
	// if the key already exists in the map.
	struct generate_value {
		generate_value(const key_type &k) : key(k) {}
		value_type operator*() const { return std::make_pair(key, data_type()); }
		const key_type &key;
	};

public:
	// Default constructor.
	btree_map_container(const key_compare &comp = key_compare(), const allocator_type &alloc = allocator_type())
		: super_type(comp, alloc) {}

	// Copy constructor.
	btree_map_container(const self_type &x) : super_type(x) {}

	// Move constructor.
	btree_map_container(self_type &&x) noexcept : super_type(std::move(x)) {}

	// Range constructor.
	template <class InputIterator>
	btree_map_container(InputIterator b, InputIterator e, const key_compare &comp = key_compare(),
						const allocator_type &alloc = allocator_type())
		: super_type(b, e, comp, alloc) {}

	// Insertion routines.
	data_type &operator[](const key_type &key) { return this->tree_.insert_unique(key, generate_value(key)).first->second; }
};

// A common base class for btree_multiset and btree_multimap.
template <typename Tree>
class btree_multi_container : public btree_container<Tree> {
	typedef btree_multi_container<Tree> self_type;
	typedef btree_container<Tree> super_type;

public:
	typedef typename Tree::key_type key_type;
	typedef typename Tree::value_type value_type;
	typedef typename Tree::size_type size_type;
	typedef typename Tree::key_compare key_compare;
	typedef typename Tree::allocator_type allocator_type;
	typedef typename Tree::iterator iterator;
	typedef typename Tree::const_iterator const_iterator;

public:
	// Default constructor.
	btree_multi_container(const key_compare &comp = key_compare(), const allocator_type &alloc = allocator_type())
		: super_type(comp, alloc) {}

	// Copy constructor.
	btree_multi_container(const self_type &x) : super_type(x) {}

	// Move constructor.
	btree_multi_container(self_type &&x) noexcept : super_type(std::move(x)) {}

	// Range constructor.
	template <class InputIterator>
	btree_multi_container(InputIterator b, InputIterator e, const key_compare &comp = key_compare(),
						  const allocator_type &alloc = allocator_type())
		: super_type(comp, alloc) {
		insert(b, e);
	}

	// Lookup routines.
	iterator find(const key_type &key) { return this->tree_.find_multi(key); }
	const_iterator find(const key_type &key) const { return this->tree_.find_multi(key); }
	size_type count(const key_type &key) const { return this->tree_.count_multi(key); }

	template <typename K>
	iterator find(const K &key) {
		return this->tree_.find_multi(key);
	}
	template <typename K>
	const_iterator find(const K &key) const {
		return this->tree_.find_multi(key);
	}
	template <typename K>
	size_type count(const K &key) const {
		return this->tree_.count_multi(key);
	}

	// Insertion routines.
	iterator insert(const value_type &x) { return this->tree_.insert_multi(x); }
	iterator insert(iterator position, const value_type &x) { return this->tree_.insert_multi(position, x); }
	template <typename InputIterator>
	void insert(InputIterator b, InputIterator e) {
		this->tree_.insert_multi(b, e);
	}

	// Deletion routines.
	int erase(const key_type &key) { return this->tree_.erase_multi(key); }
	// Erase the specified iterator from the btree. The iterator must be valid
	// (i.e. not equal to end()).  Return an iterator pointing to the node after
	// the one that was erased (or end() if none exists).
	iterator erase(const iterator &iter) { return this->tree_.erase(iter); }
	void erase(const iterator &first, const iterator &last) { this->tree_.erase(first, last); }
};

}  // namespace btree

#endif	// UTIL_BTREE_BTREE_CONTAINER_H__
