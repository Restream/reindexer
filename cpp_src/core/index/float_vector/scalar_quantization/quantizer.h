#pragma once

#include <atomic>
#include <span>
#include "estl/concepts.h"
#include "estl/defines.h"
#include "quantization_params.h"

namespace hnswlib {
class [[nodiscard]] Quantizer {
public:
	template <typename MapT>
	explicit Quantizer(const MapT& hnsw, QuantizingParams params)
		: params_(std::move(params)),
		  // for the 1-quantile make a reserve for outliers in 1 pct
		  kConfidenceInterval_([quantile = params_.Quantile(hnsw.fstdistfunc_.Dims())]() {
			  assertrx_throw(quantile >= 0.95f && quantile <= 1.f);
			  return (1.f / quantile <= 1.01f) ? 1.01f : (1.f / quantile);
		  }()),
		  kDim(hnsw.fstdistfunc_.Dims()),
		  kMetric(hnsw.fstdistfunc_.Metric()),
		  statistic_(*this, [&hnsw]() noexcept { return hnsw.cur_element_count.load(std::memory_order_relaxed); }) {}

	template <typename MapT>
	explicit Quantizer(const MapT& hnsw, const Quantizer& other)
		: params_(other.params_),
		  kConfidenceInterval_(other.kConfidenceInterval_),
		  kDim(other.kDim),
		  kMetric(other.kMetric),
		  statistic_(other.statistic_, *this, [&hnsw]() noexcept { return hnsw.cur_element_count.load(std::memory_order_relaxed); }) {}

	CorrectiveOffset Quantize(auto from, auto to) const noexcept { return quantize(from, to); }

	CorrectiveOffset Requantize(auto point, const QuantizingParams& dequantizeParams) const noexcept {
		return quantize(point, point, dequantizeParams);
	}

	const QuantizingParams& Params() const noexcept { return params_; }

	void UpdateStatistic(const void* data) noexcept { statistic_.Update(data); }

	[[nodiscard]] bool NeedRequantize() const noexcept { return statistic_.OutliersPct() > (kConfidenceInterval_ - 1.f); }

	QuantizingParams PrepareToRequantize() noexcept {
		QuantizingParams oldQuantizingParams = params_;

		params_.minQ = statistic_.Min();
		params_.maxQ = statistic_.Max();
		params_.alpha = (params_.maxQ - params_.minQ) / kSq8Range;
		params_.alpha_2 = std::pow(params_.alpha, 2.f);
		params_.delta = 0.5 * std::pow(params_.minQ, 2.f) * kDim;

		statistic_.Reset();

		return oldQuantizingParams;
	}

	float Dequantize(uint8_t val) const noexcept { return dequantize(val, params_); }

private:
	static float dequantize(uint8_t val, QuantizingParams dequantizeParams) noexcept {
		return uint8t2floatImpl(val, dequantizeParams.alpha, dequantizeParams.minQ);
	}

	[[nodiscard]] RX_ALWAYS_INLINE uint8_t float2uint8t(float val) const noexcept {
		return std::clamp((val - params_.minQ) / params_.alpha, 0.f, kSq8Range);
	}
	[[nodiscard]] static RX_ALWAYS_INLINE float uint8t2floatImpl(uint8_t val, float alpha, float min) noexcept { return alpha * val + min; }

	[[nodiscard]] RX_ALWAYS_INLINE float uint8t2float(uint8_t val) const noexcept {
		return uint8t2floatImpl(val, params_.alpha, params_.minQ);
	}

	/**
	 * @brief Performs scalar quantization on vectors with error correction.
	 *
	 * This implementation is based on the principles outlined in the articles:
	 * 1. https://www.elastic.co/search-labs/blog/scalar-quantization-101
	 * 2. https://www.elastic.co/search-labs/blog/vector-db-optimized-scalar-quantization
	 *
	 * Unlike classical scalar quantization, which is primarily used for inference
	 * acceleration, this implementation is optimized specifically for the vector
	 * database use case. The key optimization applied in this code is a first-order
	 * additive correction to the quantized dot product.
	 *
	 * Instead of only computing the dot product using integer arithmetic, we
	 * compensate for the systematic quantization error that occurs when comparing
	 * a query with documents. The correction assumes that relevant queries for a
	 * given document lie in the vicinity of its embedding. This allows us to
	 * precompute and store one additional float per vector, improving ranking
	 * accuracy without sacrificing the performance of the integer dot product
	 * computation.
	 */
	template <reindexer::concepts::OneOf<QuantizingParams, std::nullopt_t> DequantizeParams = std::nullopt_t>
	CorrectiveOffset quantize(auto from, auto to, const DequantizeParams& dequantizeParams = std::nullopt) const noexcept {
		CorrectiveOffset res{};
		const bool isL2 = kMetric == hnswlib::MetricType::L2;
		float mathExpectShift = 0.f;
		std::transform(from.begin(), from.end(), to.begin(), [&](auto _val) noexcept {
			float val = _val;
			if constexpr (std::is_same_v<DequantizeParams, QuantizingParams>) {
				val = dequantize(val, dequantizeParams);
			}

			const auto uint8 = float2uint8t(val);
			const auto err = val - uint8t2float(uint8);
			if (isL2) {
				res += (2 * params_.alpha * uint8 + err) * err;
				mathExpectShift -= 2.f * params_.alpha * err * uint8;
			} else {
				res += params_.alpha * uint8 + err;
				mathExpectShift += params_.alpha * err * uint8;
			}

			return uint8;
		});

		if (!isL2) {
			res *= params_.minQ;
			res += params_.delta;
		}

		res += mathExpectShift;

		return res;
	}

	QuantizingParams params_;

	const float kConfidenceInterval_;
	const size_t kDim;
	const MetricType kMetric;

	struct [[nodiscard]] Statistic {
		Statistic(const Quantizer& q, auto curSizeFn) noexcept : quantizer_(q), curSizeFn_(std::move(curSizeFn)) {}
		Statistic(const Statistic& other, const Quantizer& q, auto curSizeFn) noexcept : quantizer_(q), curSizeFn_(std::move(curSizeFn)) {
			kMinQconfidence = other.kMinQconfidence;
			kMaxQconfidence = other.kMaxQconfidence;
			curMin_.store(other.curMin_, std::memory_order_relaxed);
			curMax_.store(other.curMax_, std::memory_order_relaxed);
			cnt_.store(other.cnt_, std::memory_order_relaxed);
			cntOutliers_.store(other.cntOutliers_, std::memory_order_relaxed);
		}

		void Reset() {
			kMinQconfidence = quantizer_.params_.minQ * quantizer_.kConfidenceInterval_;
			kMaxQconfidence = quantizer_.params_.maxQ * quantizer_.kConfidenceInterval_;
			curMin_ = kMinQconfidence;
			curMax_ = kMaxQconfidence;
			cnt_.store(0, std::memory_order_relaxed);
			cntOutliers_ = 0;
		}

		float OutliersPct() const noexcept {
			auto cnt = cnt_.load(std::memory_order_relaxed);
			// not requantize if processed less than 10% of all points
			if (cnt < 0.1 * curSizeFn_() * quantizer_.kDim) {
				return 0;
			}
			return float(cntOutliers_) / cnt;
		}

		void Update(const void* dataPtr) noexcept {
			auto data = std::span(static_cast<const float*>(dataPtr), quantizer_.kDim);

			float min{kMinQconfidence};
			float max{kMaxQconfidence};

			int outliers = 0;
			for (auto value : data) {
				if (value < kMinQconfidence || value > kMaxQconfidence) {
					++outliers;
				}

				if (value < min) {
					min = value;
				}

				if (value > max) {
					max = value;
				}
			}

			cntOutliers_.fetch_add(outliers, std::memory_order_relaxed);
			cnt_.fetch_add(quantizer_.kDim, std::memory_order_relaxed);

			auto change = [](std::atomic<float>& value, float desired, auto op) {
				auto prev = value.load(std::memory_order_relaxed);
				if (op(desired, prev)) {
					while (!value.compare_exchange_weak(prev, desired, std::memory_order_relaxed, std::memory_order_relaxed)) {
						if (!op(desired, prev)) {
							break;
						}
					}
				}
			};

			change(curMin_, min, std::less<float>{});
			change(curMax_, max, std::greater<float>{});
		}

		float Min() const noexcept { return curMin_; }
		float Max() const noexcept { return curMax_; }

	private:
		const Quantizer& quantizer_;
		std::function<size_t()> curSizeFn_;

		float kMinQconfidence = quantizer_.params_.minQ * quantizer_.kConfidenceInterval_;
		float kMaxQconfidence = quantizer_.params_.maxQ * quantizer_.kConfidenceInterval_;

		std::atomic<float> curMin_ = kMinQconfidence;
		std::atomic<float> curMax_ = kMaxQconfidence;

		std::atomic<size_t> cnt_ = 0;
		std::atomic<size_t> cntOutliers_ = 0;
	} statistic_;
};

};	// namespace hnswlib
