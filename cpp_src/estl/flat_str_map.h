#pragma once
#include <memory>
#include <vector>
#include "estl/string_view.h"
#include "hopscotch/hopscotch_map.h"
#include "tools/customhash.h"
#include "tools/varint.h"

namespace reindexer {

const unsigned kMultiValueLinkFlag = 0x80000000;

template <typename CharT, typename V, bool Multi = false>
class flat_str_map {
protected:
	using base_holder_t = std::vector<CharT>;
	using string_view_t = string_view;

	class holder_t : public base_holder_t {
	public:
		string_view_t get(int pos) const {
			auto ptr = reinterpret_cast<const uint8_t *>(base_holder_t::data() + pos);
			auto l = scan_varint(10, ptr);
			size_t len = parse_uint32(l, ptr);
			return string_view(reinterpret_cast<const char *>(ptr + l), len);
		}
		int put(string_view_t str) {
			int pos = base_holder_t::size();
			base_holder_t::resize(str.size() + 8 + pos);
			int l = string_pack(str.data(), str.size(), reinterpret_cast<uint8_t *>(base_holder_t::data()) + pos);
			base_holder_t::resize(pos + l);
			return pos;
		}
	};

	struct equal_flat_str_map {
		using is_transparent = void;
		equal_flat_str_map(const holder_t *buf) : buf_(buf) {}
		bool operator()(int lhs, int rhs) const { return buf_->get(lhs) == buf_->get(rhs); }
		bool operator()(string_view_t lhs, int rhs) const { return lhs == buf_->get(rhs); }
		bool operator()(int lhs, string_view_t rhs) const { return rhs == buf_->get(lhs); }

	protected:
		const holder_t *buf_ = nullptr;
	};

	struct hash_flat_str_map {
		using is_transparent = void;
		hash_flat_str_map(const holder_t *buf) : buf_(buf) {}
		size_t operator()(string_view_t hs) const { return _Hash_bytes(hs.data(), hs.length()); }
		size_t operator()(int hs) const { return operator()(buf_->get(hs)); }

	protected:
		const holder_t *buf_ = nullptr;
	};
	using hash_map = tsl::hopscotch_map<int, V, hash_flat_str_map, equal_flat_str_map, std::allocator<std::pair<int, V>>, 30, false,
										tsl::mod_growth_policy<std::ratio<3, 2>>>;

public:
	flat_str_map() : holder_(new holder_t), map_(new hash_map(16, hash_flat_str_map(holder_.get()), equal_flat_str_map(holder_.get()))) {}
	flat_str_map(const flat_str_map &other) = delete;
	flat_str_map &operator=(const flat_str_map &other) = delete;

	flat_str_map(flat_str_map &&rhs) noexcept : holder_(move(rhs.holder_)), map_(move(rhs.map_)), multi_(move(rhs.multi_)) {}
	flat_str_map &operator=(flat_str_map &&rhs) noexcept {
		if (&rhs != this) {
			holder_ = std::move(rhs.buf_);
			map_ = std::move(rhs.map_);
			multi_ = std::move(rhs.multi_);
		}
		return *this;
	}

	template <typename VV>
	class value_type : public std::pair<string_view_t, VV> {
	public:
		value_type(string_view_t k, VV v) : std::pair<string_view_t, VV>(k, v) {}
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
				return value_type(m_->holder_->get(it_->first), m_->multi_[multi_idx_].val);
			} else
				return value_type(m_->holder_->get(it_->first), it_->second);
		}

		const value_type operator->() const {
			if (Multi && it_->second & kMultiValueLinkFlag) {
				assert(multi_idx_ != -1);
				return value_type(m_->holder_->get(it_->first), m_->multi_[multi_idx_].val);
			} else
				return value_type(m_->holder_->get(it_->first), it_->second);
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

	const_iterator find(string_view_t str) const { return const_iterator(map_->find(str), this); }
	iterator find(string_view_t str) { return iterator(map_->find(str), this); }

	std::pair<iterator, iterator> equal_range(string_view_t str) {
		auto it = map_->find(str);
		return {iterator(it, this), iterator(it == map_->end() ? it : ++it, this)};
	}

	std::pair<const_iterator, const_iterator> equal_range(string_view_t str) const {
		auto it = map_->find(str);
		return {iterator(it, this), iterator(it == map_->end() ? it : ++it, this)};
	}

	std::pair<iterator, bool> insert(string_view_t str, const V &v) {
		int pos = holder_->put(str);
		auto res = map_->emplace(pos, v);
		int multi_pos = -1;
		if (!res.second) {
			holder_->resize(pos);
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
		holder_->clear();
		map_->clear();
		multi_.clear();
	}

	std::pair<iterator, bool> emplace(string_view_t str, const V &v) { return insert(str, v); }

	void reserve(size_t map_sz, size_t str_sz) {
		map_->reserve(map_sz);
		holder_->reserve(str_sz);
		if (Multi) multi_.reserve(map_sz / 10);
	}

	size_t size() const { return map_->size(); }
	size_t heap_size() const {
		return holder_->capacity() + map_->size() * sizeof(typename hash_map::value_type) + multi_.capacity() * sizeof(multi_node);
	}

	void shrink_to_fit() {
		holder_->shrink_to_fit();
		multi_.shrink_to_fit();
	}

protected:
	// Single buffer for storing all strings in null terminated format
	std::unique_ptr<holder_t> holder_;
	// Underlying map container
	std::unique_ptr<hash_map> map_;
	struct multi_node {
		V val;
		int next;
	};
	// flat linked list for values of non uniq keys
	std::vector<multi_node> multi_;
};

template <typename CharT, typename V>
using flat_str_multimap = flat_str_map<CharT, V, true>;

}  // namespace reindexer
