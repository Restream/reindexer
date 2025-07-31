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
		constexpr Name& operator|=(Name other) & noexcept { return operator|=(other.value_); }                \
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
BOOL_ENUM(CompositeAllowed)
BOOL_ENUM(FieldAllowed)
BOOL_ENUM(NullAllowed)
BOOL_ENUM(CanAddField)
BOOL_ENUM(WasUpdated)
BOOL_ENUM(Matched)
BOOL_ENUM(IsIndexed)
BOOL_ENUM(IsPk)
BOOL_ENUM(IsSparse)
BOOL_ENUM(IsDense)
BOOL_ENUM(IsArray)
BOOL_ENUM(InArray)
BOOL_ENUM(IsNoIndexColumn)
BOOL_ENUM(Invert)
BOOL_ENUM(ConvertToString)
BOOL_ENUM(ConvertNull)
BOOL_ENUM(Append)
BOOL_ENUM(IsDBInitCall)
BOOL_ENUM(LogCreation)
BOOL_ENUM(SkipSortingEntry)
BOOL_ENUM(IsDistinct)
BOOL_ENUM(Changed)
BOOL_ENUM(Desc)
BOOL_ENUM(ExtraIndexDescription)
BOOL_ENUM(NeedCreate)
BOOL_ENUM(IsRequired)
BOOL_ENUM(AllowAdditionalProps)
BOOL_ENUM(MustExist)
BOOL_ENUM(PrefAndStemmersForbidden)
BOOL_ENUM(SetLimit0ForChangeJoin)

#undef BOOL_ENUM

enum class [[nodiscard]] ObjType {
	TypeObject,
	TypeArray,
	TypeObjectArray,
	TypePlain,
};

enum class [[nodiscard]] VectorMetric { L2, InnerProduct, Cosine };
enum class [[nodiscard]] RankedTypeQuery { NotSet, No, FullText, KnnL2, KnnIP, KnnCos, Hybrid };
enum class [[nodiscard]] RankSortType : unsigned { RankOnly, RankAndID, ExternalExpression, IDOnly, IDAndPositions };
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
	[[nodiscard]] value_type Value() const noexcept { return value_; }
	[[nodiscard]] bool IsZero() const noexcept { return value_ == 0; }

private:
	value_type value_{0};
};

class [[nodiscard]] TagName {
public:
	using value_type = uint16_t;

	struct Hash : public std::hash<value_type> {
		using Base = std::hash<value_type>;
		[[nodiscard]] bool operator()(TagName v) const noexcept { return Base::operator()(v.value_); }
	};

	constexpr explicit TagName(uint64_t v) : value_(v) {
		using namespace std::string_literals;
		if rx_unlikely (v > std::numeric_limits<value_type>::max()) {
			throw Error{errLogic, "TagName overflow - max value is 65535, got "s.append(std::to_string(v))};
		}
	}

	static constexpr TagName Empty() noexcept { return {}; }

	[[nodiscard]] constexpr bool IsEmpty() const noexcept { return value_ == 0; }
	[[nodiscard]] constexpr auto operator<=>(const TagName&) const noexcept = default;
	[[nodiscard]] constexpr value_type AsNumber() const noexcept { return value_; }

private:
	constexpr TagName() noexcept = default;

	value_type value_{0};
};
inline constexpr TagName operator""_Tag(unsigned long long v) noexcept { return TagName(v); }

}  // namespace reindexer
