#pragma once

#include <limits>
#include "tools/errors.h"

namespace reindexer {

#define BOOL_ENUM(Name)                                                                                       \
	class [[nodiscard]] Name {                                                                                \
	public:                                                                                                   \
		constexpr explicit Name(bool v) noexcept : value_{v} {}                                               \
		constexpr Name& operator|=(bool other) & noexcept {                                                   \
			value_ |= other;                                                                                  \
			return *this;                                                                                     \
		}                                                                                                     \
		constexpr Name& operator&=(bool other) & noexcept {                                                   \
			value_ &= other;                                                                                  \
			return *this;                                                                                     \
		}                                                                                                     \
		constexpr Name operator!() const noexcept { return Name{!value_}; }                                   \
		constexpr Name operator||(Name other) const noexcept { return Name{value_ || other.value_}; }         \
		constexpr Name operator&&(Name other) const noexcept { return Name{value_ && other.value_}; }         \
		[[nodiscard]] constexpr bool operator==(Name other) const noexcept { return value_ == other.value_; } \
		[[nodiscard]] constexpr bool operator!=(Name other) const noexcept { return !operator==(other); }     \
		[[nodiscard]] explicit constexpr operator bool() const noexcept { return value_; }                    \
		[[nodiscard]] constexpr bool operator*() const noexcept { return value_; }                            \
                                                                                                              \
	private:                                                                                                  \
		bool value_;                                                                                          \
	};                                                                                                        \
	static constexpr Name Name##_True = Name(true);                                                           \
	static constexpr Name Name##_False = Name(false);

BOOL_ENUM(IsRanked)
BOOL_ENUM(ContainRanked)
BOOL_ENUM(ForcedFirst)
BOOL_ENUM(CheckUnsigned)
BOOL_ENUM(NeedSort)
BOOL_ENUM(IsMergeQuery)
BOOL_ENUM(ReplaceDeleted)
BOOL_ENUM(DumpWithMask)

#undef BOOL_ENUM

enum class [[nodiscard]] VectorMetric { L2, InnerProduct, Cosine };
enum class [[nodiscard]] RankedTypeQuery { NotSet, No, FullText, KnnL2, KnnIP, KnnCos };
enum class [[nodiscard]] RankSortType { RankOnly, RankAndID, ExternalExpression };
enum class [[nodiscard]] RankOrdering { Off, Asc, Desc };

class [[nodiscard]] FloatVectorDimension {
public:
	using value_type = uint16_t;

	FloatVectorDimension() noexcept = default;
	explicit FloatVectorDimension(uint64_t value) : value_(value) {
		if rx_unlikely (value > std::numeric_limits<value_type>::max()) {
			throw Error(errLogic,
						std::string("Float vector dimensions overflow - max vector size is 65535, got ").append(std::to_string(value)));
		}
	}

	explicit operator uint64_t() const noexcept { return value_; }
	explicit operator uint32_t() const noexcept { return value_; }
	explicit operator uint16_t() const noexcept { return value_; }

	[[nodiscard]] bool operator==(const FloatVectorDimension&) const noexcept = default;
	[[nodiscard]] bool operator!=(const FloatVectorDimension&) const noexcept = default;
	value_type Value() const noexcept { return value_; }
	[[nodiscard]] bool IsZero() const noexcept { return value_ == 0; }

private:
	value_type value_{0};
};

}  // namespace reindexer
