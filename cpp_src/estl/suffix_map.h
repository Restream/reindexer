#pragma once

#include <string_view>
#include <vector>
#include "libdivsufsort/divsufsort.h"

namespace reindexer {

template <typename CharT, typename V>
class [[nodiscard]] suffix_map {
	typedef size_t size_type;
	typedef unsigned char char_type;

	class [[nodiscard]] value_type : public std::pair<const CharT*, V> {
	public:
		value_type(std::pair<const CharT*, V>&& v) noexcept : std::pair<const CharT*, V>(std::move(v)) {}
		value_type(const std::pair<const CharT*, V>& v) : std::pair<const CharT*, V>(v) {}
		const value_type* operator->() const noexcept { return this; }
	};

	class [[nodiscard]] iterator {
		friend class suffix_map;

	public:
		iterator(size_type idx, const suffix_map* m) noexcept : idx_(idx), m_(m) {}
		iterator(const iterator& other) noexcept : idx_(other.idx_), m_(other.m_) {}
		value_type operator->() {
			auto* p = &m_->text_[m_->sa_[idx_]];
			return value_type(std::make_pair(p, m_->mapped_[m_->sa_[idx_]]));
		}

		value_type operator->() const {
			auto* p = &m_->text_[m_->sa_[idx_]];
			return value_type(std::make_pair(p, m_->mapped_[m_->sa_[idx_]]));
		}

		iterator& operator++() noexcept {
			++idx_;
			return *this;
		}
		iterator& operator--() noexcept {
			--idx_;
			return *this;
		}
		iterator operator++(int) noexcept {
			iterator ret = *this;
			idx_++;
			return ret;
		}
		iterator operator--(int) noexcept {
			iterator ret = *this;
			idx_--;
			return ret;
		}
		int lcp() noexcept { return m_->lcp_[idx_]; }
		bool operator!=(const iterator& rhs) const noexcept { return idx_ != rhs.idx_; }
		bool operator==(const iterator& rhs) const noexcept { return idx_ == rhs.idx_; }

	protected:
		size_type idx_;
		const suffix_map* m_;
	};

public:
	suffix_map() = default;
	suffix_map(const suffix_map& /*other*/) = delete;
	suffix_map& operator=(const suffix_map& /*other*/) = default;
	suffix_map(suffix_map&& /*rhs*/) noexcept = default;

	iterator begin() const noexcept { return iterator(0, this); }
	iterator end() const noexcept { return iterator(sa_.size(), this); }

	std::pair<iterator, iterator> match_range(std::string_view str) const {
		iterator start = lower_bound(str);
		if (start == end()) {
			return {end(), end()};
		}
		int idx_ = start.idx_ + 1;
		while (idx_ < int(sa_.size()) && lcp_[idx_ - 1] >= int(str.length())) {
			idx_++;
		}
		return {start, iterator(idx_, this)};
	}

	iterator lower_bound(std::string_view str) const {
		if (!built_) {
			throw std::logic_error("Should call suffix_map::build before search");
		}

		size_type lo = 0, hi = sa_.size(), mid;
		int lcp_lo = 0, lcp_hi = 0;
		auto P = reinterpret_cast<const char_type*>(str.data());
		auto T = reinterpret_cast<const char_type*>(text_.data());
		while (lo <= hi) {
			mid = (lo + hi) / 2;
			int i = std::min(lcp_hi, lcp_lo);
			bool plt = true;
			if (mid >= sa_.size()) {
				return end();
			}
			while (i < int(str.length()) && sa_[mid] + i < int(text_.size())) {
				if (P[i] < T[sa_[mid] + i]) {
					break;
				} else if (P[i] > T[sa_[mid] + i]) {
					plt = false;
					break;
				}
				i++;
			}
			if (plt) {
				if (mid == lo + 1) {
					if (strncmp(str.data(), &text_[sa_[mid]], std::min(str.length(), strlen(&text_[sa_[mid]]))) != 0) {
						return end();
					}
					return iterator(mid, this);
				}
				lcp_hi = i;
				hi = mid;
			} else {
				if (mid == hi - 1) {
					if (hi >= sa_.size() || strncmp(str.data(), &text_[sa_[hi]], std::min(str.length(), strlen(&text_[sa_[hi]]))) != 0) {
						return end();
					}
					return iterator(hi, this);
				}
				lcp_lo = i;
				lo = mid;
			}
		}
		return end();
	}

	int insert(std::string_view word, const V& val) {
		int wpos = text_.size();
		size_t real_len = word.length();
		text_.insert(text_.end(), word.begin(), word.end());
		text_.emplace_back('\0');
		mapped_.insert(mapped_.end(), real_len + 1, val);
		words_.emplace_back(wpos);
		words_len_.emplace_back(real_len);
		built_ = false;
		return wpos;
	}

	const CharT* word_at(int idx) const noexcept { return &text_[words_[idx]]; }

	int16_t word_len_at(int idx) const noexcept { return words_len_[idx]; }

	void build() {
		if (built_) {
			return;
		}
		text_.shrink_to_fit();
		sa_.resize(text_.size());
		if (!sa_.empty()) {
			::divsufsort(reinterpret_cast<const char_type*>(text_.data()), &sa_[0], text_.size());
		}
		build_lcp();
		built_ = true;
	}

	void reserve(size_type sz_text, size_type sz_words) {
		text_.reserve(sz_text + 1);
		mapped_.reserve(sz_text + 1);
		words_.reserve(sz_words);
		words_len_.reserve(sz_words);
	}
	void clear() noexcept {
		sa_.clear();
		lcp_.clear();
		mapped_.clear();
		words_.clear();
		words_len_.clear();
		text_.clear();
		built_ = false;
	}
	size_type size() const noexcept { return sa_.size(); }
	size_type word_size() const noexcept { return words_.size(); }

	const std::vector<CharT>& text() const noexcept { return text_; }
	size_t heap_size() noexcept {
		return (sa_.capacity() + words_.capacity()) * sizeof(int) +			  //
			   (lcp_.capacity() + words_len_.capacity()) * sizeof(int16_t) +  //
			   mapped_.capacity() * sizeof(V) + text_.capacity();
	}

protected:
	void build_lcp() {
		std::vector<int> rank_;
		rank_.resize(sa_.size());
		lcp_.resize(sa_.size());
		int k = 0, n = size();

		for (int i = 0; i < n; i++) {
			rank_[sa_[i]] = i;
		}
		for (int i = 0; i < n; i++, k ? k-- : 0) {
			auto r = rank_[i];
			if (r == n - 1) {
				k = 0;
				continue;
			}
			int j = sa_[r + 1];
			while (i + k < n && j + k < n && text_[i + k] == text_[j + k]) {
				k++;
			}
			lcp_[r] = k;
		}
	}

	std::vector<int> sa_, words_;
	std::vector<int16_t> lcp_;
	std::vector<uint8_t> words_len_;
	std::vector<V> mapped_;
	std::vector<CharT> text_;
	bool built_ = false;
};

}  // namespace reindexer
