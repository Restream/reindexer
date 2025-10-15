#pragma once

#include <functional>
#include <string>
#include <string_view>
#include "core/ft/limits.h"
#include "tools/errors.h"

namespace reindexer {

struct [[nodiscard]] typos_context {
	class [[nodiscard]] TyposVec {
	public:
		using value_type = int8_t;
		using size_type = uint8_t;
		using const_iterator = const value_type*;

		static_assert(std::numeric_limits<value_type>::max() >= kMaxTypoLenLimit,
					  "'Positions' array must be able to store any available typos postions");

		TyposVec() noexcept = default;
		TyposVec(const TyposVec& o) noexcept = default;

		void emplace_back(value_type v) {
			if (size_ >= kMaxTyposInWord) {
				throw Error(errLogic, "TyposVec's overwhelming (max size is {})", kMaxTyposInWord);
			}
			arr_[size_++] = v;
		}
		void pop_back() {
			if (!size_) {
				throw Error(errLogic, "TyposVec's underwhelming");
			}
			--size_;
		}
		static constexpr size_type capacity() noexcept { return kMaxTyposInWord; }
		size_type size() const noexcept { return size_; }
		const_iterator begin() const noexcept { return arr_; }
		const_iterator end() const noexcept { return arr_ + size_; }
		value_type operator[](size_type pos) const { return arr_[pos]; }
		TyposVec& operator=(const TyposVec& o) noexcept = default;

	private:
		value_type arr_[kMaxTyposInWord];
		size_type size_ = 0;
	};

	using CallBack = std::function<void(std::string_view, int, const TyposVec&, const std::wstring_view pattern)>;
	std::wstring utf16Word, utf16Typo;
	std::string typo;
};

void mktypos(typos_context* ctx, const std::wstring& word, int level, int maxTyposLen, const typos_context::CallBack& callback);
void mktypos(typos_context* ctx, std::string_view word, int level, int maxTyposLen, const typos_context::CallBack& callback);

}  // namespace reindexer
