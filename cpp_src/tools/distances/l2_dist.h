#pragma once

#if RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "estl/defines.h"

namespace reindexer::vector_dists {

namespace impl {
template <typename T>
using L2SqrPtrT = float (*)(const T*, const T*, size_t);

extern L2SqrPtrT<float> L2SqrPtrF;
extern L2SqrPtrT<uint8_t> L2SqrPtrU;
}  // namespace impl

template <typename T>
RX_ALWAYS_INLINE float L2SqrDistance(const T* x, const T* y, size_t d) {
	if constexpr (std::is_same_v<T, float>) {
		return impl::L2SqrPtrF(x, y, d);
	} else if constexpr (std::is_same_v<T, uint8_t>) {
		return impl::L2SqrPtrU(x, y, d);
	} else {
		static_assert(std::is_same_v<T, float> || std::is_same_v<T, uint8_t>, "Incorrect type for L2 distance func");
	}
}

}  // namespace reindexer::vector_dists

#endif	// RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES
