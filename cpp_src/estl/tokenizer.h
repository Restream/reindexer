#pragma once

#include <string>
#include "core/keyvalue/variant.h"

namespace reindexer {

enum [[nodiscard]] TokenType { TokenEnd, TokenName, TokenNumber, TokenString, TokenOp, TokenSymbol, TokenSign };

class [[nodiscard]] Token {
public:
	explicit Token(TokenType type = TokenSymbol, size_t pos = 0) noexcept : type_(type), pos_(pos) {}
	template <std::input_iterator It>
	Token(TokenType type, It textBeg, It textEnd) : type_(type), text_(textBeg, textEnd) {}
	Token(const Token&) = delete;
	Token& operator=(const Token&) = delete;
	Token(Token&&) noexcept = default;
	Token& operator=(Token&&) noexcept = default;

	RX_ALWAYS_INLINE std::string_view Text() const noexcept { return std::string_view(text_.data(), text_.size()); }
	TokenType Type() const noexcept { return type_; }

private:
	friend class Tokenizer;

	TokenType type_ = TokenSymbol;
	h_vector<char, 20> text_;
	size_t pos_;
};

class [[nodiscard]] Tokenizer {
public:
	class [[nodiscard]] Flags {
	public:
		enum [[nodiscard]] Values : int { NoFlags = 0, ToLower = 1, TreatSignAsToken = 1 << 1, InOrderBy = 1 << 2, Last = InOrderBy };

		explicit Flags(int f) noexcept : f_(f) {
			assertrx(f <= (Values::NoFlags | Values::ToLower | Values::TreatSignAsToken | Values::InOrderBy | Values::Last));
		}
		Flags(Values f) noexcept : f_(f) {}

		RX_ALWAYS_INLINE bool HasToLower() const noexcept { return f_ & Values::ToLower; }
		RX_ALWAYS_INLINE bool HasTreatSignAsToken() const noexcept { return f_ & Values::TreatSignAsToken; }
		RX_ALWAYS_INLINE bool HasInOrderBy() const noexcept { return f_ & Values::InOrderBy; }

	private:
		int f_ = Values::NoFlags;
	};

	explicit Tokenizer(std::string_view query) noexcept : q_(query), cur_(query.begin()) {}
	Token NextToken(Flags f = Flags(Flags::ToLower));
	void SkipToken(Flags f = Flags(Flags::ToLower)) { std::ignore = NextToken(f); }
	Token PeekToken(Flags f = Flags(Flags::ToLower)) {
		auto saveCur = cur_;
		auto savePos = pos_;
		auto res = NextToken(f);
		cur_ = saveCur;
		pos_ = savePos;
		return res;
	}
	Token PeekSecondToken(Flags f = Flags(Flags::ToLower)) {
		auto saveCur = cur_;
		auto savePos = pos_;
		auto res = NextToken(f);
		if (res.Type() != TokenEnd) {
			res = NextToken(f);
		}
		cur_ = saveCur;
		pos_ = savePos;
		return res;
	}
	void SkipSpace() noexcept;
	bool End() const noexcept { return cur_ == q_.end(); }
	size_t GetPos() const noexcept { return pos_; }
	size_t GetPrevPos() const noexcept;
	void SetPos(size_t pos) noexcept {
		int delta = pos - pos_;
		pos_ += delta;
		cur_ += delta;
	}
	std::string Where();
	std::string Where(const Token& token);
	std::string Where(size_t start_pos, size_t last_pos);
	size_t Length() const noexcept { return q_.length(); }
	const char* Begin() const noexcept { return q_.data(); }

private:
	std::string_view q_;
	std::string_view::const_iterator cur_;
	size_t pos_ = 0;
};

Variant Token2kv(const Token&, Tokenizer&, CompositeAllowed, FieldAllowed, NullAllowed);
Variant GetVariantFromToken(const Token& tok);

}  // namespace reindexer
