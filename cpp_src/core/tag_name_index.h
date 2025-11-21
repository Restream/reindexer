#pragma once

#include <cstdint>
#include "estl/concepts.h"
#include "tools/errors.h"

namespace reindexer {

class [[nodiscard]] TagName {
public:
	using value_type = uint16_t;

	struct [[nodiscard]] Hash : public std::hash<value_type> {
		using Base = std::hash<value_type>;
		size_t operator()(TagName v) const noexcept { return Base::operator()(v.value_); }
	};

	constexpr explicit TagName(std::signed_integral auto v) : TagName(uint64_t(v)) {
		using namespace std::string_literals;
		if (v < 0) [[unlikely]] {
			throw Error{errLogic, "TagName onderflow - min value is 0, got "s.append(std::to_string(v))};
		}
	}
	constexpr explicit TagName(std::unsigned_integral auto v) : TagName(uint64_t(v)) {}
	constexpr explicit TagName(uint64_t v) : value_(v) {
		using namespace std::string_literals;
		if (v > std::numeric_limits<value_type>::max()) [[unlikely]] {
			throw Error{errLogic, "TagName overflow - max value is 65535, got "s.append(std::to_string(v))};
		}
	}

	static constexpr TagName Empty() noexcept { return {}; }

	constexpr bool IsEmpty() const noexcept { return value_ == 0; }
	constexpr auto operator<=>(const TagName&) const noexcept = default;
	constexpr value_type AsNumber() const noexcept { return value_; }

private:
	constexpr TagName() noexcept = default;

	value_type value_{0};
};
inline constexpr TagName operator""_Tag(unsigned long long v) noexcept { return TagName(v); }

class [[nodiscard]] TagIndex {
	using value_type = uint32_t;
	static constexpr value_type all_v = std::numeric_limits<value_type>::max();

public:
	struct [[nodiscard]] Hash : public std::hash<value_type> {
		using Base = std::hash<value_type>;
		size_t operator()(TagIndex v) const noexcept { return Base::operator()(v.value_); }
	};

	constexpr explicit TagIndex(std::signed_integral auto v) : TagIndex(uint64_t(v)) {
		using namespace std::string_literals;
		if (v < 0) [[unlikely]] {
			throw Error{errLogic, "TagIndex onderflow - min value is 0, got "s.append(std::to_string(v))};
		}
	}
	constexpr explicit TagIndex(std::unsigned_integral auto v) : TagIndex(uint64_t(v)) {}
	constexpr explicit TagIndex(uint64_t v) : value_(v) {
		if (v >= all_v) [[unlikely]] {
			throwOverflow(v);
		}
	}

	static constexpr TagIndex All() noexcept { return TagIndex{}; }
	bool IsAll() const noexcept { return value_ == all_v; }
	value_type AsNumber() const noexcept { return value_; }
	bool operator==(TagIndex other) const noexcept { return IsAll() || other.IsAll() || value_ == other.value_; }

private:
	constexpr explicit TagIndex() noexcept = default;
	[[noreturn]] void throwOverflow(auto);

	value_type value_{all_v};
};

namespace concepts {

template <typename T>
concept TagNameOrIndex = OneOf<T, TagName, TagIndex>;

}  // namespace concepts

}  // namespace reindexer
