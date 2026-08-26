#include <gtest/gtest.h>

#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/variant.h"
#include "gtests/tools.h"

namespace reindexer_tests {

using reindexer::Variant;
using reindexer::ConstFloatVectorView;

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

}  // namespace reindexer_tests