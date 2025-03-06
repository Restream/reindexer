/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#if defined(__aarch64__) || defined(REINDEXER_WITH_SSE)

#include <faiss/utils/distances_fused/distances_fused.h>

#include <faiss/impl/platform_macros.h>

#include <faiss/utils/distances_fused/avx512.h>
#include <faiss/utils/distances_fused/simdlib_based.h>

#include "tools/cpucheck.h"

namespace faiss {


bool exhaustive_L2sqr_fused_cmax(
        const float* x,
        const float* y,
        size_t d,
        size_t nx,
        size_t ny,
        Top1BlockResultHandler<CMax<float, int64_t>>& res,
        const float* y_norms) {
#if REINDEXER_WITH_SSE
    if (nx == 0 || ny == 0) {
        // nothing to do
        return true;
    }
    static const bool hasAVX512 = reindexer::IsAVX512Allowed();
    if (hasAVX512) {
        // avx512 kernel
        return exhaustive_L2sqr_fused_cmax_AVX512(x, y, d, nx, ny, res, y_norms);
    }
    static const bool hasAVX2 = reindexer::IsAVX2Allowed();
    if (hasAVX2) {
        return exhaustive_L2sqr_fused_cmax_simdlib(x, y, d, nx, ny, res, y_norms);
    }
#endif // REINDEXER_WITH_SSE
#if defined(__aarch64__) || defined(REINDEXER_WITH_SSE)
    // arm neon kernel
    return exhaustive_L2sqr_fused_cmax_simdlib(x, y, d, nx, ny, res, y_norms);
#else
    (void)x;
    (void)y;
    (void)d;
    (void)res;
    (void)y_norms;
    // not supported, please use a general-purpose kernel
    return false;
#endif
}

} // namespace faiss

#endif // defined(__aarch64__) || defined(REINDEXER_WITH_SSE)
