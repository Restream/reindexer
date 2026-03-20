#pragma once

#if RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "estl/defines.h"
namespace reindexer::vector_dists {

namespace impl {
template <typename T>
using InnerProductPtrT = float (*)(const T*, const T*, size_t);

extern InnerProductPtrT<float> InnerProductPtrF;
extern InnerProductPtrT<uint8_t> InnerProductPtrU;
}  // namespace impl

template <typename T>
RX_ALWAYS_INLINE float InnerProductDistance(const T* x, const T* y, size_t d) {
	if constexpr (std::is_same_v<T, float>) {
		return impl::InnerProductPtrF(x, y, d);
	} else if constexpr (std::is_same_v<T, uint8_t>) {
		return impl::InnerProductPtrU(x, y, d);
	} else {
		static_assert(std::is_same_v<T, float> || std::is_same_v<T, uint8_t>, "Incorrect type for InnerProductPtrU distance func");
	}
}
}  // namespace reindexer::vector_dists

#endif	// RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES
