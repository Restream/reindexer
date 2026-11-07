#include <gtest/gtest.h>

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

}  // namespace reindexer_tests
