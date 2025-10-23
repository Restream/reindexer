// Based on https://github.com/nmslib/hnswlib/tree/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c
// Apache 2.0 license (copyright by yurymalkov) may be found here:
// https://github.com/nmslib/hnswlib/blob/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c/LICENSE

#pragma once

#include <cmath>
#include <vector>
#include "type_consts.h"
#ifndef _MSC_VER
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wtype-limits"
#endif

#include "estl/defines.h"
#include "tools/cpucheck.h"
#include "tools/normalize.h"

#if RX_WITH_STDLIB_DEBUG
#include <set>
#include "estl/lock.h"
#include "estl/mutex.h"
#endif	// RX_WITH_STDLIB_DEBUG

namespace hnswlib {
typedef uint32_t labeltype;

// This can be extended to store state for filtering (e.g. from a std::set)
class [[nodiscard]] BaseFilterFunctor {
public:
	virtual bool operator()(hnswlib::labeltype id) { return true; }
	virtual ~BaseFilterFunctor() {}
};

template <typename dist_t>
class [[nodiscard]] BaseSearchStopCondition {
public:
	virtual void add_point_to_result(labeltype label, const void* datapoint, dist_t dist) = 0;

	virtual void remove_point_from_result(labeltype label, const void* datapoint, dist_t dist) = 0;

	virtual bool should_stop_search(dist_t candidate_dist, dist_t lowerBound) = 0;

	virtual bool should_consider_candidate(dist_t candidate_dist, dist_t lowerBound) = 0;

	virtual bool should_remove_extra() = 0;

	virtual void filter_results(std::vector<std::pair<dist_t, labeltype>>& candidates) = 0;

	virtual ~BaseSearchStopCondition() {}
};

template <typename T>
class [[nodiscard]] pairGreater {
public:
	bool operator()(const T& p1, const T& p2) { return p1.first > p2.first; }
};

template <typename T>
static void writeBinaryPOD(std::ostream& out, const T& podRef) {
	out.write((char*)&podRef, sizeof(T));
}

template <typename T>
static void readBinaryPOD(std::istream& in, T& podRef) {
	in.read((char*)&podRef, sizeof(T));
}

using DISTFUNC = float (*)(const void*, const void*, const void*) noexcept;

struct [[nodiscard]] DistCalculatorParam {
	DISTFUNC f{nullptr};
	MetricType metric{MetricType::NONE};
	size_t dims{0};

	struct [[nodiscard]] QuantizeParams {
		QuantizeParams() noexcept = default;

		template <typename QuantizerPtrT>
		QuantizeParams(const QuantizerPtrT& quantizer = nullptr) noexcept {
			if (!quantizer) {
				return;
			}

			alpha = quantizer->Alpha();
			alpha_2 = std::pow(alpha, 2.f);
			nonLinearCorrection = quantizer->kNonLinearCorrection;
		}

		Sq8NonLinearCorrection nonLinearCorrection = Sq8NonLinearCorrection::Disabled;

		float alpha = 0.f;
		float alpha_2 = 1.f;
	} quantizeParams{};

	float CalcDist(const void* lhs, const void* rhs) const noexcept { return f(lhs, rhs, &dims); }

	float CalcDist(const void* lhs, const void* rhs, CorrectiveOffsets lhsCorrectiveOffsets,
				   CorrectiveOffsets rhsCorrectiveOffsets) const noexcept {
		switch (metric) {
			case MetricType::L2: {
				return euclideanDist(lhs, rhs, lhsCorrectiveOffsets, rhsCorrectiveOffsets);
			}
			case MetricType::COSINE:
			case MetricType::INNER_PRODUCT: {
				return scalarProduct(lhs, rhs, lhsCorrectiveOffsets, rhsCorrectiveOffsets);
			}
			case MetricType::NONE:
			default:
				std::abort();
		}
	}

	static float calcNonLinearAddition(const void* lhs, const void* rhs, CorrectiveOffsets lhsCorrectiveOffsets,
									   CorrectiveOffsets rhsCorrectiveOffsets, size_t dims, float alpha) {
		float res = 0;
		const auto i1 = static_cast<const uint8_t*>(lhs);
		const auto i2 = static_cast<const uint8_t*>(rhs);

		for (size_t i = 0; i < dims; ++i) {
			const float err1 =
				i1[i] == 0 ? lhsCorrectiveOffsets.negOutlierErr
						   : (i1[i] == uint8_t(KDefaultSq8Range) ? lhsCorrectiveOffsets.posOutlierErr : lhsCorrectiveOffsets.roundingErr);
			const float err2 =
				i2[i] == 0 ? rhsCorrectiveOffsets.negOutlierErr
						   : (i2[i] == uint8_t(KDefaultSq8Range) ? rhsCorrectiveOffsets.posOutlierErr : rhsCorrectiveOffsets.roundingErr);

			res += alpha * (i1[i] * err2 + i2[i] * err1) /*+ err1 * err2*/;
		}

		return res;
	}

	float euclideanDist(const void* lhs, const void* rhs, CorrectiveOffsets lhsCorrectiveOffsets,
						CorrectiveOffsets rhsCorrectiveOffsets) const {
		float res = quantizeParams.alpha_2 * f(lhs, rhs, &dims) + lhsCorrectiveOffsets.precalc + rhsCorrectiveOffsets.precalc;
		if (quantizeParams.nonLinearCorrection == Sq8NonLinearCorrection::Enabled) {
			res -= 2 * calcNonLinearAddition(lhs, rhs, lhsCorrectiveOffsets, rhsCorrectiveOffsets, dims, quantizeParams.alpha);
		}
		return res;
	}
	float scalarProduct(const void* lhs, const void* rhs, CorrectiveOffsets lhsCorrectiveOffsets,
						CorrectiveOffsets rhsCorrectiveOffsets) const {
		float res = quantizeParams.alpha_2 * -f(lhs, rhs, &dims) + lhsCorrectiveOffsets.precalc + rhsCorrectiveOffsets.precalc;
		if (quantizeParams.nonLinearCorrection == Sq8NonLinearCorrection::Enabled) {
			res += calcNonLinearAddition(lhs, rhs, lhsCorrectiveOffsets, rhsCorrectiveOffsets, dims, quantizeParams.alpha);
		}
		return -res;
	}
};

class [[nodiscard]] DistCalculator {
public:
	DistCalculator() = default;
	DistCalculator(DistCalculatorParam&& param, size_t maxElements, const std::vector<CorrectiveOffsets>* sq8CorrectiveOffsets)
		: maxElements_{maxElements}, param_{std::move(param)}, sq8CorrectiveOffsets_(sq8CorrectiveOffsets) {
		assertrx(param_.f);
		assertrx(param_.metric != MetricType::NONE);
		assertrx(param_.dims > 0);
		if (maxElements_ && param_.metric == MetricType::COSINE) {
			normCoefs_ = std::make_unique<float[]>(maxElements);
		}
	}
#if RX_WITH_STDLIB_DEBUG
	DistCalculator(DistCalculator&& other) noexcept
		: maxElements_{other.maxElements_},
		  param_{std::move(other.param_)},
		  normCoefs_{std::move(other.normCoefs_)},
		  sq8CorrectiveOffsets_(other.sq8CorrectiveOffsets_),
		  initialized_{std::move(other.initialized_)} {}

	DistCalculator& operator=(DistCalculator&& other) noexcept {
		if (&other != this) {
			reindexer::scoped_lock lck(mtx_, other.mtx_);
			maxElements_ = other.maxElements_;
			param_ = std::move(other.param_);
			normCoefs_ = std::move(other.normCoefs_);
			sq8CorrectiveOffsets_ = other.sq8CorrectiveOffsets_;
			initialized_ = std::move(other.initialized_);
		}
		return *this;
	}
#endif	// RX_WITH_STDLIB_DEBUG

	void Resize(size_t newSize) {
		if (maxElements_ != newSize) {
			if (param_.metric == MetricType::COSINE) {
				assertrx_dbg(normCoefs_);
				auto newData = std::make_unique<float[]>(newSize);
				const size_t copyCount = std::min(newSize, maxElements_) * sizeof(float);
				std::memcpy(newData.get(), normCoefs_.get(), copyCount);
				normCoefs_ = std::move(newData);
			} else {
				assertrx_dbg(!normCoefs_);
			}
			maxElements_ = newSize;
		}
	}
	void CopyValuesFrom(const DistCalculator& other) {
		if (maxElements_ < other.maxElements_) {
			throw std::logic_error("Unable to copy norm values for dist calc");
		}
		if (param_.metric == MetricType::COSINE) {
			assertrx_dbg(normCoefs_);
			assertrx_dbg(other.normCoefs_);
			const size_t copyCount = other.maxElements_ * sizeof(float);
			std::memcpy(normCoefs_.get(), other.normCoefs_.get(), copyCount);
#if RX_WITH_STDLIB_DEBUG
			reindexer::scoped_lock lck(mtx_, other.mtx_);
			const auto it = initialized_.equal_range(other.maxElements_).second;
			auto initCopy = other.initialized_;
			initCopy.insert(it, initialized_.end());
			initialized_ = std::move(initCopy);
#endif	// RX_WITH_STDLIB_DEBUG
		}
	}
	RX_ALWAYS_INLINE void AddNorm(const void* v, unsigned id) noexcept {
		assertrx_dbg(id < maxElements_);
		if (param_.metric == MetricType::COSINE) {
			assertrx_dbg(normCoefs_);
			// FIXME! here can be passed uint8_t* after loading from storage quantized graph (by method 'loadIndex')
			normCoefs_[id] = reindexer::ann::CalculateL2Module(static_cast<const float*>(v), int32_t(param_.dims));
#if RX_WITH_STDLIB_DEBUG
			reindexer::lock_guard lck(mtx_);
			initialized_.emplace(id);
#endif	// RX_WITH_STDLIB_DEBUG
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
			reindexer::lock_guard lck(mtx_);
			assertrx_dbg(initialized_.find(oldId) != initialized_.end());
			initialized_.erase(oldId);
			initialized_.emplace(newId);
#endif	// RX_WITH_STDLIB_DEBUG
		} else {
			assertrx_dbg(!normCoefs_);
		}
	}
	RX_ALWAYS_INLINE void EraseNorm(unsigned id) noexcept {
		assertrx_dbg(id < maxElements_);
#if RX_WITH_STDLIB_DEBUG
		if (param_.metric == MetricType::COSINE) {
			reindexer::lock_guard lck(mtx_);
			assertrx_dbg(initialized_.find(id) != initialized_.end());
			initialized_.erase(id);
		} else {
			assertrx_dbg(!normCoefs_);
		}
#endif	// RX_WITH_STDLIB_DEBUG
	}
	RX_ALWAYS_INLINE size_t Dims() const noexcept { return param_.dims; }
	RX_ALWAYS_INLINE MetricType Metric() const noexcept { return param_.metric; }
	RX_ALWAYS_INLINE float operator()(const void* v1, unsigned id1, const void* v2, unsigned id2) const noexcept {
		auto dist = sq8CorrectiveOffsets_ && !sq8CorrectiveOffsets_->empty()
						? param_.CalcDist(v1, v2, getCorrectiveOffsets(id1), getCorrectiveOffsets(id2))
						: param_.CalcDist(v1, v2);

		if (param_.metric == MetricType::COSINE) {
			assertrx(normCoefs_);
			assertrx(id1 < maxElements_);
			assertrx(id2 < maxElements_);
#if RX_WITH_STDLIB_DEBUG
			{
				reindexer::lock_guard lck(mtx_);
				assertrx_dbg(initialized_.find(id1) != initialized_.end());
				assertrx_dbg(initialized_.find(id2) != initialized_.end());
			}
#endif	// RX_WITH_STDLIB_DEBUG
			dist *= normCoefs_[id1];
			dist *= normCoefs_[id2];
		} else {
			assertrx_dbg(!normCoefs_);
		}
		return dist;
	}
	RX_ALWAYS_INLINE float operator()(const void* v1, const void* v2, unsigned id) const noexcept {
		auto dist = sq8CorrectiveOffsets_ && !sq8CorrectiveOffsets_->empty()
						? param_.CalcDist(v1, v2, getCorrectiveOffsets(v1), getCorrectiveOffsets(id))
						: param_.CalcDist(v1, v2);
		if (param_.metric == MetricType::COSINE) {
			assertrx_dbg(normCoefs_);
			assertrx_dbg(id < maxElements_);
#if RX_WITH_STDLIB_DEBUG
			{
				reindexer::lock_guard lck(mtx_);
				assertrx_dbg(initialized_.find(id) != initialized_.end());
			}
#endif	// RX_WITH_STDLIB_DEBUG
			dist *= normCoefs_[id];
		} else {
			assertrx_dbg(!normCoefs_);
		}
		return dist;
	}

	template <typename QuantizerT>
	void UpdateQuantizeParams(const QuantizerT& quantizer) noexcept {
		param_.quantizeParams = DistCalculatorParam::QuantizeParams(quantizer);
	}

private:
	size_t maxElements_{0};
	DistCalculatorParam param_;
	std::unique_ptr<float[]> normCoefs_;
	const std::vector<CorrectiveOffsets>* sq8CorrectiveOffsets_ = nullptr;
#if RX_WITH_STDLIB_DEBUG
	mutable reindexer::mutex mtx_;
	std::set<unsigned> initialized_;
#endif	// RX_WITH_STDLIB_DEBUG

	RX_ALWAYS_INLINE CorrectiveOffsets getCorrectiveOffsets(unsigned id) const noexcept { return (*sq8CorrectiveOffsets_)[id]; }

	RX_ALWAYS_INLINE CorrectiveOffsets getCorrectiveOffsets(const void* data) const noexcept {
		auto ptr = reinterpret_cast<const float*>(reinterpret_cast<const uint8_t*>(data) + param_.dims);
		CorrectiveOffsets res;
		std::memmove(&res, ptr, sizeof(CorrectiveOffsets));
		return res;
	}
};

class [[nodiscard]] SpaceInterface {
public:
	// virtual void search(void *);
	virtual size_t get_data_size() noexcept = 0;

	virtual DistCalculatorParam get_dist_calculator_param() noexcept = 0;

	virtual void* get_dist_func_param() noexcept = 0;

	virtual ~SpaceInterface() {}
};

struct [[nodiscard]] ResizeResult {
	const void* oldPosition;
	const void* newPosition;
};

class [[nodiscard]] IWriter {
public:
	virtual void PutVarUInt(uint64_t) = 0;
	virtual void PutVarUInt(uint32_t) = 0;
	virtual void PutVarInt(int64_t) = 0;
	virtual void PutVarInt(int32_t) = 0;
	virtual void PutVString(std::string_view) = 0;
	virtual void AppendPKByID(labeltype) = 0;
};
class [[nodiscard]] IReader {
public:
	virtual uint64_t GetVarUInt() = 0;
	virtual int64_t GetVarInt() = 0;
	virtual std::string_view GetVString() = 0;
	virtual labeltype ReadPkEncodedData(char* destBuf) = 0;
};

}  // namespace hnswlib

#include "bruteforce.h"
#include "hnswalg.h"
#include "space_cosine.h"
#include "space_ip.h"
#include "space_l2.h"
#include "stop_condition.h"

#ifndef _MSC_VER
#pragma GCC diagnostic pop
#endif
