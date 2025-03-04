#pragma once

#include <string>
#include "core/ft/usingcontainer.h"
#include "core/keyvalue/variant.h"

namespace reindexer {

enum token_type { TokenEnd, TokenName, TokenNumber, TokenString, TokenOp, TokenSymbol, TokenSign };

class token {
public:
	explicit token(token_type t = TokenSymbol) noexcept : type(t) {}
	token(const token&) = delete;
	token& operator=(const token&) = delete;
	token(token&&) noexcept = default;
	token& operator=(token&&) noexcept = default;

	[[nodiscard]] RX_ALWAYS_INLINE std::string_view text() const noexcept { return std::string_view(text_.data(), text_.size()); }

	token_type type = TokenSymbol;
	RVector<char, 20> text_;
};

class tokenizer {
public:
	class flags {
	public:
		enum values : int { no_flags = 0, to_lower = 1, treat_sign_as_token = 1 << 1, in_order_by = 1 << 2, last = in_order_by };

		explicit flags(int f) noexcept : f_(f) {
			assertrx(f <= (values::no_flags | values::to_lower | values::treat_sign_as_token | values::in_order_by | values::last));
		}
		flags(values f) noexcept : f_(f) {}

		RX_ALWAYS_INLINE bool has_to_lower() const noexcept { return f_ & values::to_lower; }
		RX_ALWAYS_INLINE bool has_treat_sign_as_token() const noexcept { return f_ & values::treat_sign_as_token; }
		RX_ALWAYS_INLINE bool has_in_order_by() const noexcept { return f_ & values::in_order_by; }

	private:
		int f_ = values::no_flags;
	};

	explicit tokenizer(std::string_view query) noexcept : q_(query), cur_(query.begin()) {}
	token next_token(flags f = flags(flags::to_lower));
	[[nodiscard]] token peek_token(flags f = flags(flags::to_lower)) {
		auto save_cur = cur_;
		auto save_pos = pos_;
		auto res = next_token(f);
		cur_ = save_cur;
		pos_ = save_pos;
		return res;
	}
	[[nodiscard]] token peek_second_token(flags f = flags(flags::to_lower)) {
		auto save_cur = cur_;
		auto save_pos = pos_;
		auto res = next_token(f);
		if (res.type != TokenEnd) {
			res = next_token(f);
		}
		cur_ = save_cur;
		pos_ = save_pos;
		return res;
	}
	void skip_space() noexcept;
	[[nodiscard]] bool end() const noexcept { return cur_ == q_.end(); }
	[[nodiscard]] size_t getPos() const noexcept { return pos_; }
	[[nodiscard]] size_t getPrevPos() const noexcept;
	void setPos(size_t pos) noexcept {
		int delta = pos - pos_;
		pos_ += delta;
		cur_ += delta;
	}
	[[nodiscard]] std::string where() const;
	[[nodiscard]] size_t length() const noexcept { return q_.length(); }
	[[nodiscard]] const char* begin() const noexcept { return q_.data(); }

private:
	std::string_view q_;
	std::string_view::const_iterator cur_;
	size_t pos_ = 0;
};

enum class CompositeAllowed : bool { No = false, Yes = true };
enum class FieldAllowed : bool { No = false, Yes = true };
Variant token2kv(const token& currTok, tokenizer& parser, CompositeAllowed allowComposite, FieldAllowed allowField);
Variant getVariantFromToken(const token& tok);

}  // namespace reindexer
