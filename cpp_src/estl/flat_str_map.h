#pragma once
#include <memory>
#include <string_view>
#include <vector>
#include "hopscotch/hopscotch_map.h"
#include "tools/customhash.h"
#include "tools/varint.h"

namespace reindexer {

template <typename CharT, typename V, bool Multi = false>
class [[nodiscard]] flat_str_map {
protected:
	using base_holder_t = std::vector<CharT>;
	using string_view_t = std::string_view;

	class [[nodiscard]] holder_t : public base_holder_t {
	public:
		string_view_t get(size_t pos) const {
			auto ptr = reinterpret_cast<const uint8_t*>(base_holder_t::data() + pos);
			auto l = scan_varint(10, ptr);
			size_t len = parse_uint32(l, ptr);
			return std::string_view(reinterpret_cast<const char*>(ptr + l), len);
		}
		size_t put(string_view_t str) {
			size_t pos = base_holder_t::size();
			base_holder_t::resize(str.size() + 8 + pos);
			size_t l = string_pack(str.data(), str.size(), reinterpret_cast<uint8_t*>(base_holder_t::data()) + pos);
			base_holder_t::resize(pos + l);
			return pos;
		}
	};

	struct [[nodiscard]] equal_flat_str_map {
		using is_transparent = void;
		equal_flat_str_map(const holder_t* buf) noexcept : buf_(buf) {}
		bool operator()(size_t lhs, size_t rhs) const { return buf_->get(lhs) == buf_->get(rhs); }
		bool operator()(string_view_t lhs, size_t rhs) const { return lhs == buf_->get(rhs); }
		bool operator()(size_t lhs, string_view_t rhs) const { return rhs == buf_->get(lhs); }

	private:
		const holder_t* buf_;
	};

	struct [[nodiscard]] hash_flat_str_map {
		using is_transparent = void;
		hash_flat_str_map(const holder_t* buf) noexcept : buf_(buf) {}
		size_t operator()(string_view_t hs) const noexcept { return _Hash_bytes(hs.data(), hs.length()); }
		size_t operator()(size_t hs) const { return operator()(buf_->get(hs)); }

	private:
		const holder_t* buf_;
	};

	using hash_map = tsl::hopscotch_map<size_t, V, hash_flat_str_map, equal_flat_str_map, std::allocator<std::pair<size_t, V>>, 30, false,
										tsl::mod_growth_policy<std::ratio<3, 2>>>;

public:
	flat_str_map()
		: holder_(std::make_unique<holder_t>()),
		  map_(std::make_unique<hash_map>(16, hash_flat_str_map(holder_.get()), equal_flat_str_map(holder_.get()))) {}
	flat_str_map(const flat_str_map& other) = delete;
	flat_str_map& operator=(const flat_str_map& other) = delete;
	flat_str_map(flat_str_map&& rhs) noexcept = default;
	flat_str_map& operator=(flat_str_map&& rhs) noexcept = default;

	template <typename VV>
	class [[nodiscard]] value_type : public std::pair<string_view_t, VV> {
	public:
		value_type(string_view_t k, VV v) : std::pair<string_view_t, VV>(k, v) {}
		const value_type* operator->() const { return this; }
		value_type* operator->() { return this; }
	};

	template <typename map_type, typename map_iterator, typename value_type>
	class [[nodiscard]] base_iterator {
		friend class flat_str_map;

	public:
		base_iterator(map_iterator it, map_type* m, int multi_idx) noexcept : it_(it), m_(m), multi_idx_(multi_idx) {}
		base_iterator(map_iterator it, map_type* m) noexcept
			: it_(it), m_(m), multi_idx_((Multi && it_ != m_->map_->end() && it_->second.IsMultiValue()) ? it_->second.GetWordID() : -1) {}
		base_iterator(const base_iterator& other) : it_(other.it_), m_(other.m_), multi_idx_(other.multi_idx_) {}
		// NOLINTNEXTLINE(bugprone-unhandled-self-assignment)
		base_iterator& operator=(const base_iterator& other) noexcept {
			it_ = other.it_;
			m_ = other.m_;
			multi_idx_ = other.multi_idx_;
			return *this;
		}

		value_type operator->() {
			if constexpr (Multi) {
				if (it_->second.IsMultiValue()) {
					assertrx(multi_idx_ != -1);
					return value_type(m_->holder_->get(it_->first), m_->multi_[multi_idx_].val);
				}
			}
			return value_type(m_->holder_->get(it_->first), it_->second);
		}

		const value_type operator->() const {
			if constexpr (Multi) {
				if (it_->second.IsMultiValue()) {
					assertrx(multi_idx_ != -1);
					return value_type(m_->holder_->get(it_->first), m_->multi_[multi_idx_].val);
				}
			}
			return value_type(m_->holder_->get(it_->first), it_->second);
		}

		base_iterator& operator++() noexcept {
			if constexpr (Multi) {
				if (multi_idx_ != -1) {
					multi_idx_ = m_->multi_[multi_idx_].next;
					if (multi_idx_ != -1) {
						return *this;
					}
				}
			}
			++it_;
			if constexpr (Multi) {
				if (it_ != m_->map_->end() && it_->second.IsMultiValue()) {
					multi_idx_ = it_->second.GetWordID();
				}
			}
			return *this;
		}
		base_iterator& operator--() noexcept {
			static_assert(Multi, "Sorry, flat_std_multimap::iterator::operator-- () is not implemented");
			--it_;
			return *this;
		}
		base_iterator operator++(int) noexcept {
			base_iterator ret = *this;
			++(*this);
			return ret;
		}
		base_iterator operator--(int) noexcept {
			base_iterator ret = *this;
			--(*this);
			return ret;
		}
		template <typename it2>
		bool operator!=(const it2& rhs) const noexcept {
			return it_ != rhs.it_ || multi_idx_ != rhs.multi_idx_;
		}
		template <typename it2>
		bool operator==(const it2& rhs) const noexcept {
			return it_ == rhs.it_ && multi_idx_ == rhs.multi_idx_;
		}

	protected:
		map_iterator it_;
		map_type* m_;
		int multi_idx_;
	};

	using iterator = base_iterator<flat_str_map, typename hash_map::iterator, value_type<V&>>;
	using const_iterator = base_iterator<const flat_str_map, typename hash_map::const_iterator, value_type<const V&>>;

	iterator begin() noexcept { return iterator(map_->begin(), this); }
	iterator end() noexcept { return iterator(map_->end(), this); }
	const_iterator begin() const noexcept { return const_iterator(map_->begin(), this); }
	const_iterator end() const noexcept { return const_iterator(map_->end(), this); }

	const_iterator find(string_view_t str) const noexcept { return const_iterator(map_->find(str), this); }
	iterator find(string_view_t str) noexcept { return iterator(map_->find(str), this); }

	std::pair<iterator, iterator> equal_range(string_view_t str) noexcept {
		auto it = map_->find(str);
		auto it2 = it;
		if (it2 != map_->end()) {
			++it2;
		}
		return {iterator(it, this), iterator(it2, this)};
	}

	std::pair<const_iterator, const_iterator> equal_range(string_view_t str) const noexcept {
		auto it = map_->find(str);
		auto it2 = it;
		if (it2 != map_->end()) {
			++it2;
		}
		return {const_iterator(it, this), const_iterator(it2, this)};
	}

	std::pair<iterator, bool> insert(string_view_t str, const V& v) {
		size_t pos = holder_->put(str);
		const auto h = hash_flat_str_map(holder_.get())(str);
		auto res = map_->try_emplace_prehashed(h, pos, v);
		int multi_pos = -1;
		if (!res.second) {
			holder_->resize(pos);
			if constexpr (Multi) {
				multi_pos = multi_.size();
				if (!(res.first->second.IsMultiValue())) {
					multi_.emplace_back(res.first->second, multi_pos + 1);
					multi_.emplace_back(v, -1);
				} else {
					int old_multi_pos = res.first->second.GetWordID();
					multi_.emplace_back(v, old_multi_pos);
				}
				res.first->second.SetWordID(multi_pos);
				res.first->second.SetMultiValueFlag(true);
			}
		}
		return {iterator(res.first, this, multi_pos), res.second};
	}
	void clear() noexcept {
		holder_->clear();
		map_->clear();
		if constexpr (Multi) {
			multi_.clear();
		}
	}

	std::pair<iterator, bool> emplace(string_view_t str, const V& v) { return insert(str, v); }

	void reserve(size_t map_sz, size_t str_sz) {
		map_->reserve(map_sz);
		holder_->reserve(str_sz);
		if constexpr (Multi) {
			multi_.reserve(map_sz / 10);
		}
	}

	size_t size() const noexcept { return map_->size(); }
	size_t bucket_count() const noexcept { return map_->bucket_count(); }
	size_t str_size() const noexcept { return holder_->size(); }
	size_t heap_size() const noexcept { return holder_->capacity() + map_->allocated_mem_size() + multi_.capacity() * sizeof(multi_node); }

	void shrink_to_fit() {
		holder_->shrink_to_fit();
		if constexpr (Multi) {
			multi_.shrink_to_fit();
		}
	}

protected:
	// Single buffer for storing all strings in null terminated format
	std::unique_ptr<holder_t> holder_;
	// Underlying map container
	std::unique_ptr<hash_map> map_;
	struct [[nodiscard]] multi_node {
		multi_node(const V& v, int n) : val(v), next(n) {}
		multi_node(V&& v, int n) noexcept : val(std::move(v)), next(n) {}

		V val;
		int next;
	};
	// flat linked list for values of non uniq keys
	std::vector<multi_node> multi_;
};

template <typename CharT, typename V>
using flat_str_multimap = flat_str_map<CharT, V, true>;

}  // namespace reindexer
