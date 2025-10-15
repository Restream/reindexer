#include "gtest/gtest.h"
#include "gtests/tests/gtest_cout.h"

#include "fmt/format.h"
#include "tools/enum_compare.h"

#define TEST_ENUM(name)                       \
	enum class [[nodiscard]] name : uint8_t { \
		none = 0,                             \
		v1 = 1,                               \
		v2 = 1 << 1,                          \
		v3 = 1 << 2,                          \
		v4 = 1 << 3,                          \
		v5 = 1 << 4,                          \
		v6 = 1 << 5,                          \
	};

#define ALL_ENUMS(E) E::v1, E::v2, E::v3, E::v4, E::v5, E::v6

template <auto... e>
static void Set(auto& diff, auto mask) {
	(diff.template Set<e>(!(uint8_t(mask) & uint8_t(e))), ...);
}

template <typename Enum>
static void DiffFromMask(auto& diff, Enum mask) {
	Set<ALL_ENUMS(Enum)>(diff, mask);
}

enum class [[nodiscard]] Bits { Unset, Set };

template <auto... e>
static auto tupleFromMask(auto mask, Bits bits) {
	auto cond = [&](bool set) { return bits == Bits::Set ? set : !set; };
	return std::make_tuple(cond(uint8_t(mask) & uint8_t(e)) ? e : decltype(e)::none...);
}

template <typename Enum>
static auto tupleOfEqualBitsFromMask(Enum mask) {
	return tupleFromMask<ALL_ENUMS(Enum)>(mask, Bits::Unset);
}

template <typename Enum>
static auto tupleOfNonEqualBitsFromMask(Enum mask) {
	return tupleFromMask<ALL_ENUMS(Enum)>(mask, Bits::Set);
}

template <typename... Enums>
static bool CheckAllOfIsEqual(const auto& diff, Enums... masks) {
	return std::apply([&diff](auto... args) { return diff.template AllOfIsEqual<decltype(args)...>(args...); },
					  std::tuple_cat(tupleOfEqualBitsFromMask(masks)...));
}

template <typename... Enums>
static auto& SkipByMask(auto& diff, Enums... masks) {
	return std::apply([&diff](auto... args) -> auto& { return diff.template Skip<decltype(args)...>(args...); },
					  std::tuple_cat(tupleOfNonEqualBitsFromMask(masks)...));
}

TEST_ENUM(E1)
TEST_ENUM(E2)
TEST_ENUM(E3)

TEST(EnumDiffClass, BaseTest) {
	const auto maskE1 = E1(1 + std::rand() % 63);
	const auto maskE2 = E2(1 + std::rand() % 63);
	const auto maskE3 = E3(1 + std::rand() % 63);

	TestCout() << fmt::format("Test for maskE1 = {}, maskE2 = {}, maskE3 = {}\n", uint8_t(maskE1), uint8_t(maskE2), uint8_t(maskE3));

	compare_enum::Diff<E1, E2, E3> diff;
	compare_enum::Diff<E2, E3> subDiff;

	DiffFromMask(diff, maskE1);

	DiffFromMask(subDiff, maskE2);
	DiffFromMask(subDiff, maskE3);

	diff.Set(subDiff);

	EXPECT_EQ(diff.Get<E1>(), uint8_t(maskE1));
	EXPECT_EQ(diff.Get<E2>(), uint8_t(maskE2));
	EXPECT_EQ(diff.Get<E3>(), uint8_t(maskE3));

	EXPECT_EQ(subDiff.Get<E2>(), uint8_t(maskE2));
	EXPECT_EQ(subDiff.Get<E3>(), uint8_t(maskE3));

	EXPECT_TRUE((CheckAllOfIsEqual(subDiff, maskE2, maskE3)));
	EXPECT_TRUE((CheckAllOfIsEqual(diff, maskE1, maskE2)));
	EXPECT_TRUE((CheckAllOfIsEqual(diff, maskE1, maskE2, maskE3)));

	auto diffCopy = diff;
	SkipByMask(diff, maskE3);
	EXPECT_FALSE(diff.Equal());
	SkipByMask(diff, maskE2);
	EXPECT_FALSE(diff.Equal());
	SkipByMask(diff, maskE1);
	EXPECT_TRUE(diff.Equal());

	SkipByMask(diffCopy, maskE1, maskE2, maskE3);
	EXPECT_TRUE(diffCopy.Equal());
}