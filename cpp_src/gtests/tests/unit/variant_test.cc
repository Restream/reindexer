#include <gtest/gtest.h>
#include <tuple>

#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/variant.h"
#include "gtests/tools.h"

namespace reindexer_tests {

using reindexer::Variant;
using reindexer::ConstFloatVectorView;
using reindexer::FloatVector;
using reindexer::FloatVectorView;

TEST(Variant, FloatVectorBasics) {
	constexpr unsigned kDim = 10;
	std::array<float, kDim> vect1, vect2;
	reindexer_tests_tools::rndFloatVector(vect1);
	reindexer_tests_tools::rndFloatVector(vect2);

	// Check copy
	Variant v1{ConstFloatVectorView{vect1}, Variant::hold};
	Variant v2 = v1;
	ASSERT_EQ(v1, v2);
	v1 = Variant();
	Variant v3{ConstFloatVectorView{vect2}, Variant::hold};
	v3 = v2;
	ASSERT_EQ(v2, v3);

	// Check move
	Variant v4{ConstFloatVectorView{vect1}, Variant::hold};
	Variant v5(std::move(v4));
	ASSERT_EQ(v2, v5);
	Variant v6{ConstFloatVectorView{vect2}, Variant::hold};
	v6 = std::move(v5);
	ASSERT_EQ(v2, v6);
}

TEST(Variant, EmptyFloatVector) {
	FloatVector empty;
	ASSERT_TRUE(empty.IsEmpty());

	// Empty has no heap buffer: conversions go through Span(), and ownsHeap stays 0.
	FloatVector fromView{ConstFloatVectorView{}};
	ASSERT_TRUE(fromView.IsEmpty());
	ConstFloatVectorView fromMut{FloatVectorView{}};
	ASSERT_TRUE(fromMut.IsEmpty());

	Variant v{std::move(empty)};
	ASSERT_FALSE(v.OwnsHeap());
	ASSERT_TRUE(ConstFloatVectorView{v}.IsEmpty());

	Variant held{ConstFloatVectorView{}, Variant::hold};
	ASSERT_FALSE(held.OwnsHeap());
	ASSERT_TRUE(ConstFloatVectorView{held}.IsEmpty());

	Variant noHold{ConstFloatVectorView{}};
	std::ignore = noHold.EnsureHold();
	ASSERT_FALSE(noHold.OwnsHeap());
	ASSERT_TRUE(ConstFloatVectorView{noHold}.IsEmpty());

	FloatVector copied{held};
	ASSERT_TRUE(copied.IsEmpty());
	FloatVector taken{std::move(held)};
	ASSERT_TRUE(taken.IsEmpty());

	Variant copy = v;
	ASSERT_EQ(v, copy);
	ASSERT_TRUE(ConstFloatVectorView{copy}.IsEmpty());

	Variant moved{std::move(v)};
	ASSERT_TRUE(ConstFloatVectorView{moved}.IsEmpty());
}

TEST(Variant, ArrayComparisonWithNulls) {
	using reindexer::ComparationResult;
	using reindexer::NotComparable;
	using reindexer::NullsHandling;
	using reindexer::VariantArray;

	struct [[nodiscard]] TestCase {
		VariantArray lhs;
		VariantArray rhs;
		ComparationResult expected;
	};
	TestCase cases[] = {
		{.lhs = {Variant{}}, .rhs = {Variant{}}, .expected = ComparationResult::Eq},
		{.lhs = {Variant{}, Variant(1.25), Variant(2.5)}, .rhs = {Variant{}}, .expected = ComparationResult::Gt},
		{.lhs = {Variant{}, Variant(1.25)}, .rhs = {Variant{}, Variant(2.5)}, .expected = ComparationResult::Lt},
		{.lhs = {Variant(1.25), Variant{}, Variant(2.5)},
		 .rhs = {Variant(1.25), Variant{}, Variant(3.75)},
		 .expected = ComparationResult::Lt},
		{.lhs = {Variant{}, Variant{}, Variant(1.25)}, .rhs = {Variant{}, Variant{}, Variant(2.5)}, .expected = ComparationResult::Lt},
		{.lhs = {Variant{}, Variant(1.25)}, .rhs = {Variant{}, Variant(1.25)}, .expected = ComparationResult::Eq},
		{.lhs = {Variant(1.25), Variant{}}, .rhs = {Variant(1.25), Variant{}}, .expected = ComparationResult::Eq},
		{.lhs = {Variant{}, Variant(2.5)}, .rhs = {Variant(1.25), Variant{}}, .expected = ComparationResult::Lt},
		{.lhs = {Variant(1.25), Variant{}}, .rhs = {Variant(2.5), Variant{}}, .expected = ComparationResult::Lt},
		{.lhs = {Variant{}}, .rhs = {}, .expected = ComparationResult::Gt},
		{.lhs = {}, .rhs = {}, .expected = ComparationResult::Eq},
	};
	for (auto& [lhs, rhs, expected] : cases) {
		std::ignore = lhs.MarkArray();
		std::ignore = rhs.MarkArray();
		SCOPED_TRACE(lhs.Dump() + " vs " + rhs.Dump());
		EXPECT_EQ((lhs.Compare<NotComparable::Return, NullsHandling::AlwaysLess>(rhs)), expected);
		EXPECT_EQ((rhs.Compare<NotComparable::Return, NullsHandling::AlwaysLess>(lhs)), -expected);
		EXPECT_EQ((lhs.Compare<NotComparable::Throw, NullsHandling::AlwaysLess>(rhs)), expected);
		EXPECT_EQ((rhs.Compare<NotComparable::Throw, NullsHandling::AlwaysLess>(lhs)), -expected);
		EXPECT_EQ(lhs == rhs, expected == ComparationResult::Eq);
		EXPECT_EQ(lhs != rhs, expected != ComparationResult::Eq);
	}
}

TEST(Variant, ArrayComparisonWithIncomparableNulls) {
	using reindexer::ComparationResult;
	using reindexer::NotComparable;
	using reindexer::NullsHandling;
	using reindexer::VariantArray;

	const auto lhs = VariantArray::Create(Variant{}, Variant(1.25)).MarkArray();
	for (auto rhs : {VariantArray::Create(Variant{}), VariantArray::Create(Variant{}, Variant(2.5)), VariantArray::Create(Variant(1.25))}) {
		std::ignore = rhs.MarkArray();
		SCOPED_TRACE(lhs.Dump() + " vs " + rhs.Dump());
		EXPECT_EQ((lhs.Compare<NotComparable::Return, NullsHandling::NotComparable>(rhs)), ComparationResult::NotComparable);
		EXPECT_EQ((rhs.Compare<NotComparable::Return, NullsHandling::NotComparable>(lhs)), ComparationResult::NotComparable);
		EXPECT_THROW((std::ignore = lhs.Compare<NotComparable::Throw, NullsHandling::NotComparable>(rhs)), reindexer::Error);
		EXPECT_THROW((std::ignore = rhs.Compare<NotComparable::Throw, NullsHandling::NotComparable>(lhs)), reindexer::Error);
	}
}

}  // namespace reindexer_tests
