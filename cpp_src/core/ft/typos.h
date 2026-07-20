#pragma once

#include <functional>
#include <string>
#include <string_view>
#include "core/ft/limits.h"
#include "tools/errors.h"

namespace reindexer {

class [[nodiscard]] TyposVec {
public:
	using value_type = uint8_t;
	using size_type = uint8_t;
	using const_iterator = const value_type*;
	using iterator = value_type*;

	static_assert(std::numeric_limits<value_type>::max() >= kMaxTypoLenLimit,
				  "'Positions' array must be able to store any available typos postions");

	TyposVec(const TyposVec& o) noexcept = default;
	TyposVec() noexcept = default;
	TyposVec(value_type v) noexcept {
		arr_[0] = v;
		size_ = 1;
	}

	TyposVec(value_type v1, value_type v2) noexcept {
		arr_[0] = v1;
		arr_[1] = v2;
		size_ = 2;
	}

	size_type size() const noexcept { return size_; }
	const_iterator begin() const noexcept { return arr_; }
	const_iterator end() const noexcept { return arr_ + size_; }
	iterator begin() noexcept { return arr_; }
	iterator end() noexcept { return arr_ + size_; }
	value_type operator[](size_type pos) const { return arr_[pos]; }
	TyposVec& operator=(const TyposVec& o) noexcept = default;

private:
	value_type arr_[kMaxTyposInWord];
	size_type size_ = 0;
};

using TyposCallBack = std::function<void(std::wstring_view typo, const TyposVec& positions, std::wstring_view originalWord)>;

void mktypos(const std::wstring& word, size_t maxTyposInWord, uint8_t maxTyposLen, const TyposCallBack& callback, std::wstring& buf);

}  // namespace reindexer
