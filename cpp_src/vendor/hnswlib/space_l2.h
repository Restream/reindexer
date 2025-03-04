#pragma once
#include "hnswlib.h"

#include "tools/distances/l2_dist.h"

namespace hnswlib {

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
static float
L2Sqr(const void *pVect1v, const void *pVect2v, const void *qty_ptr) noexcept {
    return reindexer::vector_dists::L2Sqr((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

#if REINDEXER_WITH_SSE

RX_AVX512_TARGET_ATTR
static float L2SqrSIMD16ExtAVX512(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
    return reindexer::vector_dists::L2SqrSIMD16ExtAVX512((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

RX_AVX_TARGET_ATTR
static float L2SqrSIMD16ExtAVX(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
    return reindexer::vector_dists::L2SqrSIMD16ExtAVX((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

static float L2SqrSIMD16ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
    return reindexer::vector_dists::L2SqrSIMD16ExtSSE((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

static float L2SqrSIMD4Ext(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
    return reindexer::vector_dists::L2SqrSIMD4Ext((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

static DISTFUNC<float> initL2SqrSIMD16Ext() noexcept {
    if (reindexer::vector_dists::L2WithAVX512()) {
        return L2SqrSIMD16ExtAVX512;
    }
    if (reindexer::vector_dists::L2WithAVX()) {
        return L2SqrSIMD16ExtAVX;
    }
        return L2SqrSIMD16ExtSSE;
}

static const DISTFUNC<float> L2SqrSIMD16Ext = initL2SqrSIMD16Ext();

static float
L2SqrSIMD16ExtResiduals(const void *pVect1v, const void *pVect2v, const void *qty_ptr) noexcept {
    return reindexer::vector_dists::L2SqrResiduals16Ext((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

static float
L2SqrSIMD4ExtResiduals(const void *pVect1v, const void *pVect2v, const void *qty_ptr) noexcept {
    return reindexer::vector_dists::L2SqrResiduals4Ext((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}
#endif // REINDEXER_WITH_SSE

class L2Space final : public SpaceInterface<float> {
    DISTFUNC<float> fstdistfunc_;
    size_t data_size_;
    size_t dim_;

 public:
    L2Space(size_t dim) noexcept {
        fstdistfunc_ = L2Sqr;
#if REINDEXER_WITH_SSE
        if (dim % 16 == 0)
			fstdistfunc_ = L2SqrSIMD16Ext;
        else if (dim > 16)
            fstdistfunc_ = L2SqrSIMD16ExtResiduals;
        else if (dim % 4 == 0)
			fstdistfunc_ = L2SqrSIMD4Ext;
        else if (dim > 4)
            fstdistfunc_ = L2SqrSIMD4ExtResiduals;
#endif
        dim_ = dim;
        data_size_ = dim * sizeof(float);
    }

    size_t get_data_size() noexcept override {
        return data_size_;
    }

    DistCalculatorParam<float> get_dist_calculator_param() noexcept override {
        return DistCalculatorParam<float>{.f = fstdistfunc_, .metric = MetricType::L2, .dims = dim_};
    }

    void *get_dist_func_param() noexcept override {
        return &dim_;
    }

    ~L2Space() override {}
};

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
static int
L2SqrI4x(const void *__restrict pVect1, const void *__restrict pVect2, const void *__restrict qty_ptr) noexcept {
    size_t qty = *((size_t *) qty_ptr);
    int res = 0;
    unsigned char *a = (unsigned char *) pVect1;
    unsigned char *b = (unsigned char *) pVect2;

    qty = qty >> 2;
    for (size_t i = 0; i < qty; i++) {
        res += ((*a) - (*b)) * ((*a) - (*b));
        a++;
        b++;
        res += ((*a) - (*b)) * ((*a) - (*b));
        a++;
        b++;
        res += ((*a) - (*b)) * ((*a) - (*b));
        a++;
        b++;
        res += ((*a) - (*b)) * ((*a) - (*b));
        a++;
        b++;
    }
    return (res);
}

static int L2SqrI(const void* __restrict pVect1, const void* __restrict pVect2, const void* __restrict qty_ptr) noexcept {
    size_t qty = *((size_t*)qty_ptr);
    int res = 0;
    unsigned char* a = (unsigned char*)pVect1;
    unsigned char* b = (unsigned char*)pVect2;

    for (size_t i = 0; i < qty; i++) {
        res += ((*a) - (*b)) * ((*a) - (*b));
        a++;
        b++;
    }
    return (res);
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

class L2SpaceI final : public SpaceInterface<int> {
    DISTFUNC<int> fstdistfunc_;
    size_t data_size_;
    size_t dim_;

 public:
    L2SpaceI(size_t dim) noexcept {
        if (dim % 4 == 0) {
            fstdistfunc_ = L2SqrI4x;
        } else {
            fstdistfunc_ = L2SqrI;
        }
        dim_ = dim;
        data_size_ = dim * sizeof(unsigned char);
    }

    size_t get_data_size() noexcept override {
        return data_size_;
    }

    DistCalculatorParam<int> get_dist_calculator_param() noexcept override {
        return DistCalculatorParam<int>{.f = fstdistfunc_, .metric = MetricType::L2, .dims = dim_};
    }

    void *get_dist_func_param() noexcept override {
        return &dim_;
    }

    ~L2SpaceI() override {}
};
}  // namespace hnswlib
