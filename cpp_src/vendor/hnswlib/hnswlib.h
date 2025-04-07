#pragma once

#ifndef _MSC_VER
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wtype-limits"
#endif

// https://github.com/nmslib/hnswlib/pull/508
// This allows others to provide their own error stream (e.g. RcppHNSW)
#ifndef HNSWLIB_ERR_OVERRIDE
  #define HNSWERR std::cerr
#else
  #define HNSWERR HNSWLIB_ERR_OVERRIDE
#endif

#include <queue>
#include <vector>
#include <string.h>
#include <cstddef>

#include "tools/cpucheck.h"
#include "tools/normalize.h"
#include "estl/defines.h"
#include "core/enums.h"

namespace hnswlib {
typedef uint32_t labeltype;

// This can be extended to store state for filtering (e.g. from a std::set)
class BaseFilterFunctor {
 public:
    virtual bool operator()(hnswlib::labeltype id) { return true; }
    virtual ~BaseFilterFunctor() {}
};

template<typename dist_t>
class BaseSearchStopCondition {
 public:
    virtual void add_point_to_result(labeltype label, const void *datapoint, dist_t dist) = 0;

    virtual void remove_point_from_result(labeltype label, const void *datapoint, dist_t dist) = 0;

    virtual bool should_stop_search(dist_t candidate_dist, dist_t lowerBound) = 0;

    virtual bool should_consider_candidate(dist_t candidate_dist, dist_t lowerBound) = 0;

    virtual bool should_remove_extra() = 0;

    virtual void filter_results(std::vector<std::pair<dist_t, labeltype >> &candidates) = 0;

    virtual ~BaseSearchStopCondition() {}
};

template <typename T>
class pairGreater {
 public:
    bool operator()(const T& p1, const T& p2) {
        return p1.first > p2.first;
    }
};

template<typename T>
static void writeBinaryPOD(std::ostream &out, const T &podRef) {
    out.write((char *) &podRef, sizeof(T));
}

template<typename T>
static void readBinaryPOD(std::istream &in, T &podRef) {
    in.read((char *) &podRef, sizeof(T));
}

template<typename MTYPE>
using DISTFUNC = MTYPE(*)(const void *, const void *, const void *) noexcept;

enum class MetricType {
    NONE,
    L2,
    INNER_PRODUCT,
    COSINE,
};

template<typename MTYPE>
struct DistCalculatorParam {
    DISTFUNC<MTYPE> f{nullptr};
    MetricType metric{MetricType::NONE};
    size_t dims{0};
};

template<typename MTYPE>
class DistCalculator {
public:
    DistCalculator() = default;
    DistCalculator(DistCalculatorParam<MTYPE>&& param, size_t maxElements):
        maxElements_{maxElements},
        param_{std::move(param)} {
        assertrx(param_.f);
        assertrx(param_.metric != MetricType::NONE);
        assertrx(param_.dims > 0);
        if (maxElements_ && param_.metric == MetricType::COSINE) {
            normCoefs_ = std::make_unique<MTYPE[]>(maxElements);
        }
    }
#if RX_WITH_STDLIB_DEBUG
    DistCalculator(DistCalculator&& other) noexcept :
      maxElements_{other.maxElements_},
      param_{std::move(other.param_)},
      normCoefs_{std::move(other.normCoefs_)},
      initialized_{std::move(other.initialized_)}
    { }

    DistCalculator& operator=(DistCalculator&& other) noexcept {
        if (&other != this) {
            std::scoped_lock lck(mtx_, other.mtx_);
            maxElements_ = other.maxElements_;
            param_ = std::move(other.param_);
            normCoefs_ = std::move(other.normCoefs_);
            initialized_ = std::move(other.initialized_);
        }
        return *this;
    }
#endif // RX_WITH_STDLIB_DEBUG

    void Resize(size_t newSize) {
        if (maxElements_ != newSize) {
            if (param_.metric == MetricType::COSINE) {
                assertrx_dbg(normCoefs_);
                auto newData = std::make_unique<MTYPE[]>(newSize);
                const size_t copyCount = std::min(newSize, maxElements_) * sizeof(MTYPE);
                std::memcpy(newData.get(), normCoefs_.get(), copyCount);
                normCoefs_ = std::move(newData);
            } else {
                assertrx_dbg(!normCoefs_);
            }
            maxElements_ = newSize;
        }
    }
    void CopyValuesFrom(const DistCalculator<MTYPE>& other) {
        if (maxElements_ < other.maxElements_) {
            throw std::logic_error("Unable to copy norm values for dist calc");
        }
        if (param_.metric == MetricType::COSINE) {
            assertrx_dbg(normCoefs_);
            assertrx_dbg(other.normCoefs_);
            const size_t copyCount = other.maxElements_ * sizeof(MTYPE);
            std::memcpy(normCoefs_.get(), other.normCoefs_.get(), copyCount);
        }
    }
    RX_ALWAYS_INLINE void AddNorm(const void* v, unsigned id) noexcept {
        assertrx_dbg(id < maxElements_);
        if (param_.metric == MetricType::COSINE) {
            assertrx_dbg(normCoefs_);
            normCoefs_[id] = reindexer::ann::CalculateL2Module(static_cast<const MTYPE*>(v), int32_t(param_.dims));
#if RX_WITH_STDLIB_DEBUG
            std::lock_guard lck(mtx_);
            initialized_.emplace(id);
#endif // RX_WITH_STDLIB_DEBUG
        } else {
            assertrx_dbg(!normCoefs_);
        }
    }
    RX_ALWAYS_INLINE void MoveNorm(unsigned oldId, unsigned newId) noexcept {
        assertrx_dbg(oldId < maxElements_);
        assertrx_dbg(newId < maxElements_);
        if (param_.metric == MetricType::COSINE) {
            assertrx_dbg(normCoefs_);
            normCoefs_.get()[newId] = normCoefs_.get()[oldId];
#if RX_WITH_STDLIB_DEBUG
            std::lock_guard lck(mtx_);
            assertrx_dbg(initialized_.find(oldId) != initialized_.end());
            initialized_.erase(oldId);
            initialized_.emplace(newId);
#endif // RX_WITH_STDLIB_DEBUG
        } else {
            assertrx_dbg(!normCoefs_);
        }
    }
    RX_ALWAYS_INLINE void EraseNorm(unsigned id) noexcept {
        assertrx_dbg(id < maxElements_);
#if RX_WITH_STDLIB_DEBUG
        if (param_.metric == MetricType::COSINE) {
            std::lock_guard lck(mtx_);
            assertrx_dbg(initialized_.find(id) != initialized_.end());
            initialized_.erase(id);
        } else {
            assertrx_dbg(!normCoefs_);
        }
#endif // RX_WITH_STDLIB_DEBUG
    }
    RX_ALWAYS_INLINE size_t Dims() const noexcept {
        return param_.dims;
    }
    RX_ALWAYS_INLINE MTYPE operator()(const void *v1, unsigned id1, const void *v2, unsigned id2) const noexcept {
        auto dist = param_.f(v1, v2, &param_.dims);
        if (param_.metric == MetricType::COSINE) {
            assertrx(normCoefs_);
            assertrx(id1 < maxElements_);
            assertrx(id2 < maxElements_);
#if RX_WITH_STDLIB_DEBUG
            {
                std::lock_guard lck(mtx_);
                assertrx_dbg(initialized_.find(id1) != initialized_.end());
                assertrx_dbg(initialized_.find(id2) != initialized_.end());
            }
#endif // RX_WITH_STDLIB_DEBUG
            dist *= normCoefs_[id1];
            dist *= normCoefs_[id2];
        } else {
            assertrx_dbg(!normCoefs_);
        }
        return dist;
    }
    RX_ALWAYS_INLINE MTYPE operator()(const void *v1, const void *v2, unsigned id) const noexcept {
        auto dist = param_.f(v1, v2, &param_.dims);
        if (param_.metric == MetricType::COSINE) {
            assertrx_dbg(normCoefs_);
            assertrx_dbg(id < maxElements_);
#if RX_WITH_STDLIB_DEBUG
            {
                std::lock_guard lck(mtx_);
                assertrx_dbg(initialized_.find(id) != initialized_.end());
            }
#endif // RX_WITH_STDLIB_DEBUG
            dist *= normCoefs_[id];
        } else {
            assertrx_dbg(!normCoefs_);
        }
        return dist;
    }

private:
    size_t maxElements_{0};
    DistCalculatorParam<MTYPE> param_;
    std::unique_ptr<MTYPE[]> normCoefs_;
#if RX_WITH_STDLIB_DEBUG
    mutable std::mutex mtx_;
    std::set<unsigned> initialized_;
#endif // RX_WITH_STDLIB_DEBUG
};

template<typename MTYPE>
class SpaceInterface {
 public:
    // virtual void search(void *);
    virtual size_t get_data_size() noexcept = 0;

    virtual DistCalculatorParam<MTYPE> get_dist_calculator_param() noexcept = 0;

    virtual void *get_dist_func_param() noexcept = 0;

    virtual ~SpaceInterface() {}
};

template<typename dist_t>
class AlgorithmInterface {
 protected:
    struct ResizeResult {
       const void* oldPosition;
       const void* newPosition;
    };

 public:
    virtual ResizeResult resizeIndex(size_t newSize) = 0;
    virtual void addPoint(const void *datapoint, labeltype label) = 0;

    virtual std::priority_queue<std::pair<dist_t, labeltype>>
        searchKnn(const void*, size_t k, size_t ef = 0, BaseFilterFunctor* isIdAllowed = nullptr) const = 0;

    // Return k nearest neighbor in the order of closer fist
    virtual std::vector<std::pair<dist_t, labeltype>>
        searchKnnCloserFirst(const void* query_data, size_t k, size_t ef = 0, BaseFilterFunctor* isIdAllowed = nullptr) const;

    virtual ~AlgorithmInterface() = default;
    virtual size_t getMaxElements() const noexcept = 0;
    virtual size_t getCurrentElementCount() const noexcept = 0;
    virtual const char* ptrByExternalLabel(labeltype) const = 0;
};

template<typename dist_t>
std::vector<std::pair<dist_t, labeltype>>
AlgorithmInterface<dist_t>::searchKnnCloserFirst(const void* query_data, size_t k, size_t ef,
                                                 BaseFilterFunctor* isIdAllowed) const {
    std::vector<std::pair<dist_t, labeltype>> result;

    // here searchKnn returns the result in the order of further first
    auto ret = searchKnn(query_data, k, ef, isIdAllowed);
    {
        size_t sz = ret.size();
        result.resize(sz);
        while (!ret.empty()) {
            result[--sz] = ret.top();
            ret.pop();
        }
    }

    return result;
}

class IWriter {
public:
    virtual void PutVarUInt(uint64_t) = 0;
    virtual void PutVarUInt(uint32_t) = 0;
    virtual void PutVarInt(int64_t) = 0;
    virtual void PutVarInt(int32_t) = 0;
    virtual void PutVString(std::string_view) = 0;
    virtual void AppendPKByID(labeltype) = 0;
};
class IReader {
public:
    virtual uint64_t GetVarUInt() = 0;
    virtual int64_t GetVarInt() = 0;
    virtual std::string_view GetVString() = 0;
    virtual labeltype ReadPkEncodedData(char* destBuf) = 0;
};

}  // namespace hnswlib

#include "space_l2.h"
#include "space_ip.h"
#include "space_cosine.h"
#include "stop_condition.h"
#include "bruteforce.h"
#include "hnswalg.h"

#ifndef _MSC_VER
#pragma GCC diagnostic pop
#endif
