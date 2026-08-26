#pragma once

#include <limits>
#include <memory>

namespace reindexer {

class [[nodiscard]] IdType {
public:
	using UnderlyingType = int;

	static constexpr IdType FromNumber(UnderlyingType v) noexcept { return IdType{v}; }
	constexpr UnderlyingType ToNumber() const noexcept { return value_; }

	IdType() noexcept : IdType(0) {}
	IdType(const IdType&) noexcept = default;
	IdType(IdType&&) noexcept = default;
	IdType& operator=(const IdType&) noexcept = default;
	IdType& operator=(IdType&&) noexcept = default;
	std::strong_ordering operator<=>(const IdType&) const noexcept = default;

	constexpr IdType Incr() const noexcept { return IdType::FromNumber(value_ + 1); }
	constexpr IdType Decr() const noexcept { return IdType::FromNumber(value_ - 1); }
	constexpr bool IsValid() const noexcept { return value_ >= 0; }

	static constexpr IdType NotSet() noexcept { return FromNumber(-1); }
	static consteval IdType Zero() noexcept { return FromNumber(0); }
	static constexpr IdType Min() noexcept { return FromNumber(std::numeric_limits<UnderlyingType>::min()); }
	static constexpr IdType Max() noexcept { return FromNumber(std::numeric_limits<UnderlyingType>::max()); }

private:
	explicit constexpr IdType(UnderlyingType v) noexcept : value_(v) {}

	UnderlyingType value_;
};

template <typename Os>
Os& operator<<(Os& os, const IdType& id) {
	return os << id.ToNumber();
}

}  // namespace reindexer

namespace std {

template <>
struct hash<reindexer::IdType> : private hash<reindexer::IdType::UnderlyingType> {
	size_t operator()(reindexer::IdType id) const noexcept { return hash<reindexer::IdType::UnderlyingType>::operator()(id.ToNumber()); }
};

}  // namespace std
