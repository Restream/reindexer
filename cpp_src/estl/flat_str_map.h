#pragma once
#include <memory>
#include "hopscotch/hopscotch_map.h"
#include "tools/customhash.h"

namespace reindexer {

using std::move;
using std::unique_ptr;

const unsigned kMultiValueLinkFlag = 0x80000000;

template <typename K, typename V, bool Multi = false>
class flat_str_map {
protected:
	struct equal_flat_str_map {
		using is_transparent = void;
		equal_flat_str_map(const K *buf) : buf_(buf) {}
		bool operator()(int lhs, int rhs) const { return !strcmp(buf_->data() + lhs, buf_->data() + rhs); }
		bool operator()(const typename K::value_type *lhs, int rhs) const { return !strcmp(lhs, buf_->data() + rhs); }
		bool operator()(int lhs, const typename K::value_type *rhs) const { return !strcmp(buf_->data() + lhs, rhs); }
		bool operator()(const K &lhs, int rhs) const { return lhs == buf_->data() + rhs; }
		bool operator()(int lhs, const K &rhs) const { return rhs == buf_->data() + lhs; }

	protected:
		const K *buf_ = nullptr;
	};

	struct hash_flat_str_map {
		using is_transparent = void;
		hash_flat_str_map(const K *buf) : buf_(buf) {}
		size_t operator()(const typename K::value_type *hs) const { return _Hash_bytes(hs, strlen(hs)); }
		size_t operator()(const K &hs) const { return _Hash_bytes(hs.data(), hs.length()); }
		size_t operator()(int hs) const { return operator()(buf_->data() + hs); }

	protected:
		const K *buf_ = nullptr;
	};
	using hash_map = tsl::hopscotch_map<int, V, hash_flat_str_map, equal_flat_str_map, std::allocator<std::pair<int, V>>, 30, false,
										tsl::mod_growth_policy<std::ratio<3, 2>>>;

public:
	flat_str_map() : buf_(new K), map_(new hash_map(16, hash_flat_str_map(buf_.get()), equal_flat_str_map(buf_.get()))) {}
	flat_str_map(const flat_str_map &other) = delete;
	flat_str_map &operator=(const flat_str_map &other) = delete;

	flat_str_map(flat_str_map &&rhs) noexcept : buf_(move(rhs.buf_)), map_(move(rhs.map_)), multi_(move(rhs.multi_)) {}
	flat_str_map &operator=(flat_str_map &&rhs) noexcept {
		if (&rhs != this) {
			buf_ = move(rhs.buf_);
			map_ = move(rhs.map_);
			multi_ = move(rhs.multi_);
		}
		return *this;
	}

	template <typename VV>
	class value_type : public std::pair<const typename K::value_type *, VV> {
	public:
		value_type(const typename K::value_type *k, VV v) : std::pair<const typename K::value_type *, VV>(k, v) {}
		const value_type *operator->() const { return this; }
		value_type *operator->() { return this; }
	};

	template <typename map_type, typename map_iterator, typename value_type>
	class base_iterator {
		friend class flat_str_map;

	public:
		base_iterator(map_iterator it, map_type *m, int multi_idx) : it_(it), m_(m), multi_idx_(multi_idx) {}
		base_iterator(map_iterator it, map_type *m)
			: it_(it),
			  m_(m),
			  multi_idx_((Multi && it_ != m_->map_->end() && it_->second & kMultiValueLinkFlag) ? it_->second & ~kMultiValueLinkFlag : -1) {
		}
		base_iterator(const base_iterator &other) : it_(other.it_), m_(other.m_), multi_idx_(other.multi_idx_) {}
		base_iterator &operator=(const base_iterator &other) {
			it_ = other.it_;
			m_ = other.m_;
			multi_idx_ = other.multi_idx_;
			return *this;
		}

		value_type operator->() {
			if (Multi && it_->second & kMultiValueLinkFlag) {
				assert(multi_idx_ != -1);
				return value_type(&m_->buf()[it_->first], m_->multi_[multi_idx_].val);
			} else
				return value_type(&m_->buf()[it_->first], it_->second);
		}

		const value_type operator->() const {
			if (Multi && it_->second & kMultiValueLinkFlag) {
				assert(multi_idx_ != -1);
				return value_type(&m_->buf()[it_->first], m_->multi_[multi_idx_].val);
			} else
				return value_type(&m_->buf()[it_->first], it_->second);
		}

		base_iterator &operator++() {
			if (Multi && multi_idx_ != -1) {
				multi_idx_ = m_->multi_[multi_idx_].next;
				if (multi_idx_ != -1) return *this;
			}
			++it_;
			if (Multi && it_ != m_->map_->end() && it_->second & kMultiValueLinkFlag) {
				multi_idx_ = it_->second & ~kMultiValueLinkFlag;
			}
			return *this;
		}
		base_iterator &operator--() {
			static_assert(Multi, "Sorry, flat_std_multimap::iterator::operator-- () is not implemented");
			--it_;
			return *this;
		}
		base_iterator operator++(int) {
			iterator ret = *this;
			++(*this);
			return ret;
		}
		base_iterator operator--(int) {
			iterator ret = *this;
			--(*this);
			return ret;
		}
		template <typename it2>
		bool operator!=(const it2 &rhs) const {
			return it_ != rhs.it_ || multi_idx_ != rhs.multi_idx_;
		}
		template <typename it2>
		bool operator==(const it2 &rhs) const {
			return it_ == rhs.it_ && multi_idx_ == rhs.multi_idx_;
		}

	protected:
		map_iterator it_;
		map_type *m_;
		int multi_idx_;
	};

	using iterator = base_iterator<flat_str_map, typename hash_map::iterator, value_type<V &>>;
	using const_iterator = base_iterator<const flat_str_map, typename hash_map::const_iterator, value_type<const V &>>;

	iterator begin() { return iterator(map_->begin(), this); }
	iterator end() { return iterator(map_->end(), this); }
	const_iterator begin() const { return const_iterator(map_->begin(), this); }
	const_iterator end() const { return const_iterator(map_->end(), this); }

	template <typename KK>
	const_iterator find(const KK &str) const {
		return const_iterator(map_->find(str), this);
	}
	template <typename KK>
	iterator find(const KK &str) {
		return iterator(map_->find(str), this);
	}

	template <typename KK>
	std::pair<iterator, iterator> equal_range(const KK &str) {
		auto it = map_->find(str);
		return {iterator(it, this), iterator(it == map_->end() ? it : ++it, this)};
	}

	template <typename KK>
	std::pair<const_iterator, const_iterator> equal_range(const KK &str) const {
		auto it = map_->find(str);
		return {iterator(it, this), iterator(it == map_->end() ? it : ++it, this)};
	}

	template <typename KK>
	std::pair<iterator, bool> insert(const KK &str, const V &v) {
		int pos = buf().size();
		buf() += str;
		buf() += '\0';
		auto res = map_->emplace(pos, v);
		int multi_pos = -1;
		if (!res.second) {
			buf().resize(pos);
			if (Multi) {
				multi_pos = multi_.size();
				if (!(res.first->second & kMultiValueLinkFlag)) {
					multi_.push_back({res.first->second, multi_pos + 1});
					multi_.push_back({v, -1});
				} else {
					int old_multi_pos = res.first->second & ~kMultiValueLinkFlag;
					multi_.push_back({v, old_multi_pos});
				}
				res.first->second = multi_pos | kMultiValueLinkFlag;
			}
		}
		return {iterator(res.first, this, multi_pos), res.second};
	}
	void clear() {
		buf().clear();
		map_->clear();
		multi_.clear();
	}

	template <typename KK>
	std::pair<iterator, bool> emplace(const KK &str, const V &v) {
		return insert(str.c_str(), v);
	}

	void reserve(size_t map_sz, size_t str_sz) {
		map_->reserve(map_sz);
		buf().reserve(str_sz);
		if (Multi) multi_.reserve(map_sz / 10);
	}

	size_t size() const { return map_->size(); }
	size_t heap_size() const { return buf().capacity() + map_->size() * sizeof(V) + multi_.capacity() * sizeof(multi_node); }

	void shrink_to_fit() {
		buf().shrink_to_fit();
		multi_.shrink_to_fit();
	}

	string &buf() { return *buf_; }
	const string &buf() const { return *buf_; }

protected:
	// Single buffer for storing all strings in null terminated format
	unique_ptr<K> buf_;
	// Underlying map container
	unique_ptr<hash_map> map_;
	struct multi_node {
		V val;
		int next;
	};
	// flat linked list for values of non uniq keys
	std::vector<multi_node> multi_;
};

template <typename K, typename V>
using flat_str_multimap = flat_str_map<K, V, true>;

}  // namespace reindexer
