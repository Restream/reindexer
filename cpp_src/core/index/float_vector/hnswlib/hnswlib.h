// Based on https://github.com/nmslib/hnswlib/tree/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c
// Apache 2.0 license (copyright by yurymalkov) may be found here:
// https://github.com/nmslib/hnswlib/blob/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c/LICENSE

#pragma once

#include <cmath>
#include <optional>
#include <vector>
#include "core/enums.h"
#include "core/index/float_vector/hnswlib/type_consts.h"
#include "estl/defines.h"
#include "tools/normalize.h"

#include "tools/distances/ip_dist.h"
#include "tools/distances/l2_dist.h"
#include "tools/unaligned.h"

#if RX_WITH_STDLIB_DEBUG
#include <set>
#include "estl/lock.h"
#include "estl/mutex.h"
#endif	// RX_WITH_STDLIB_DEBUG

namespace hnswlib {

template <typename T>
class [[nodiscard]] DistCalculator {
public:
	explicit DistCalculator(reindexer::VectorMetric metric, size_t dim, size_t maxElements, std::optional<float> alpha2 = std::nullopt)
		: metric_(metric), dim_(dim), alpha2_(initAlpha2(alpha2)), distFn_(initDistFn()), maxElements_{maxElements} {
		assertrx(dim_ > 0);
		if (maxElements_ && metric_ == reindexer::VectorMetric::Cosine) {
			normCoefs_ = std::make_unique<float[]>(maxElements_);
		}
		if constexpr (std::is_same_v<T, uint8_t>) {
			correctiveOffsets_.resize(maxElements_);
		}
	}

	template <typename OtherT>
	explicit DistCalculator(const DistCalculator<OtherT>& other, size_t newMaxElements, std::optional<float> alpha2 = std::nullopt)
		: metric_(other.metric_),
		  dim_(other.dim_),
		  alpha2_(initAlpha2(alpha2)),
		  distFn_(initDistFn()),
		  maxElements_(std::max(other.maxElements_, newMaxElements)),
		  normCoefs_(maxElements_ && metric_ == reindexer::VectorMetric::Cosine ? std::make_unique<float[]>(maxElements_) : nullptr),
		  correctiveOffsets_(other.correctiveOffsets_) {
		copyValuesFrom(other);
		if constexpr (std::is_same_v<T, uint8_t>) {
			correctiveOffsets_.resize(maxElements_);
		}
	}

	void Resize(size_t newSize) {
		if (maxElements_ != newSize) {
			if (metric_ == reindexer::VectorMetric::Cosine) {
				assertrx_dbg(normCoefs_);
				auto newData = std::make_unique<float[]>(newSize);
				const size_t copyCount = std::min(newSize, maxElements_) * sizeof(float);
				std::memcpy(newData.get(), normCoefs_.get(), copyCount);
				normCoefs_ = std::move(newData);
			} else {
				assertrx_dbg(!normCoefs_);
			}

			if constexpr (std::is_same_v<T, float>) {
				assertrx(correctiveOffsets_.empty());
			} else {
				static_assert(std::is_same_v<T, uint8_t>);
				assertrx(correctiveOffsets_.size() == maxElements_);
				correctiveOffsets_.resize(newSize);
			}

			maxElements_ = newSize;
		}
	}

	RX_ALWAYS_INLINE void AddNorm(const float* v, unsigned id) noexcept {
		assertrx_dbg(id < maxElements_);
		if (metric_ == reindexer::VectorMetric::Cosine) {
			assertrx_dbg(normCoefs_);
			normCoefs_[id] = reindexer::ann::CalculateL2Module(v, dim_);
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
		if (metric_ == reindexer::VectorMetric::Cosine) {
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
	RX_ALWAYS_INLINE void EraseNorm([[maybe_unused]] unsigned id) noexcept {
		assertrx_dbg(id < maxElements_);
#if RX_WITH_STDLIB_DEBUG
		if (metric_ == reindexer::VectorMetric::Cosine) {
			reindexer::lock_guard lck(mtx_);
			assertrx_dbg(initialized_.find(id) != initialized_.end());
			initialized_.erase(id);
		} else {
			assertrx_dbg(!normCoefs_);
		}
#endif	// RX_WITH_STDLIB_DEBUG
	}

	RX_ALWAYS_INLINE size_t Dim() const noexcept { return dim_; }
	RX_ALWAYS_INLINE reindexer::VectorMetric Metric() const noexcept { return metric_; }

	RX_ALWAYS_INLINE float operator()(const T* v1, unsigned id1, const T* v2, unsigned id2) const {
		assertrx_dbg(distFn_);
		float dist = (this->*distFn_)(v1, v2, getCorrectiveOffset(id1), getCorrectiveOffset(id2));

		if (metric_ == reindexer::VectorMetric::Cosine) {
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
	RX_ALWAYS_INLINE float operator()(const T* v1, const T* v2, unsigned id) const {
		assertrx_dbg(distFn_);
		float dist = (this->*distFn_)(v1, v2, getCorrectiveOffset(v1), getCorrectiveOffset(id));

		if (metric_ == reindexer::VectorMetric::Cosine) {
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

	std::vector<CorrectiveOffset>& Sq8CorrectiveOffsets() noexcept { return correctiveOffsets_; }
	const std::vector<CorrectiveOffset>& Sq8CorrectiveOffsets() const noexcept { return correctiveOffsets_; }

private:
	template <typename OtherT>
	friend class DistCalculator;

	using DistFn = float (DistCalculator::*)(const T*, const T*, CorrectiveOffset, CorrectiveOffset) const noexcept;

	const reindexer::VectorMetric metric_;
	const size_t dim_ = 0;
	const float alpha2_ = 1.f;
	const DistFn distFn_ = nullptr;

	float initAlpha2(std::optional<float> alpha2) {
		if constexpr (std::is_same_v<T, float>) {
			assertrx(!alpha2);
			return 1.f;
		} else {
			static_assert(std::is_same_v<T, uint8_t>);
			assertrx(alpha2);
			return *alpha2;
		}
	}

	RX_ALWAYS_INLINE float l2(const T* lhs, const T* rhs, CorrectiveOffset lhsCorr, CorrectiveOffset rhsCorr) const noexcept {
		return alpha2_ * reindexer::vector_dists::L2SqrDistance(lhs, rhs, dim_) + lhsCorr + rhsCorr;
	}
	RX_ALWAYS_INLINE float ip(const T* lhs, const T* rhs, CorrectiveOffset lhsCorr, CorrectiveOffset rhsCorr) const noexcept {
		return -(alpha2_ * reindexer::vector_dists::InnerProductDistance(lhs, rhs, dim_) + lhsCorr + rhsCorr);
	}

	DistFn initDistFn() const noexcept {
		switch (metric_) {
			case reindexer::VectorMetric::L2:
				return &DistCalculator::l2;
			case reindexer::VectorMetric::InnerProduct:
			case reindexer::VectorMetric::Cosine:
				return &DistCalculator::ip;
			default:
				std::abort();
		}
	}

	void copyValuesFrom(const auto& other) {
		if (maxElements_ < other.maxElements_) {
			throw std::logic_error("Unable to copy norm values for dist calc");
		}
		if (metric_ == reindexer::VectorMetric::Cosine) {
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

	size_t maxElements_{0};
	std::unique_ptr<float[]> normCoefs_;
	std::vector<CorrectiveOffset> correctiveOffsets_;
#if RX_WITH_STDLIB_DEBUG
	mutable reindexer::mutex mtx_;
	std::set<unsigned> initialized_;
#endif	// RX_WITH_STDLIB_DEBUG

	RX_ALWAYS_INLINE CorrectiveOffset getCorrectiveOffset(unsigned id) const {
		if constexpr (std::is_same_v<T, float>) {
			assertrx(correctiveOffsets_.empty());
			return CorrectiveOffset{};
		} else {
			static_assert(std::is_same_v<T, uint8_t>);
			assertrx(correctiveOffsets_.size() > id);
			return correctiveOffsets_[id];
		}
	}

	RX_ALWAYS_INLINE CorrectiveOffset getCorrectiveOffset(const T* data) const noexcept {
		if constexpr (std::is_same_v<T, float>) {
			assertrx(correctiveOffsets_.empty());
			return CorrectiveOffset{};
		} else {
			static_assert(std::is_same_v<T, uint8_t>);
			CorrectiveOffset res = reindexer::unaligned::read<CorrectiveOffset>(data + dim_);
			return res;
		}
	}
};

}  // namespace hnswlib
