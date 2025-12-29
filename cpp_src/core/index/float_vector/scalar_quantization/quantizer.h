#pragma once

#include <atomic>
#include <span>
#include "estl/defines.h"
#include "quantization_params.h"

namespace hnswlib {
class [[nodiscard]] Quantizer {
public:
	template <typename MapT>
	explicit Quantizer(const MapT& hnsw, QuantizingParams params)
		: params_(std::move(params)),
		  // for the 1-quantile make a reserve for outliers in 1 pct
		  kConfidenceInterval_([quantile = params_.config.quantile]() {
			  assertrx_throw(quantile >= 0.95f && quantile <= 1.f);
			  return (1.f / quantile <= 1.01f) ? 1.01f : (1.f / quantile);
		  }()),
		  kDim(hnsw.fstdistfunc_.Dims()),
		  kMetric(hnsw.fstdistfunc_.Metric()),
		  statistic_(*this, [&hnsw]() noexcept { return hnsw.cur_element_count.load(std::memory_order_relaxed); }) {}

	CorrectiveOffsets Quantize(auto from, auto to) const noexcept {
		switch (params_.config.nonLinearCorrection) {
			case Sq8NonLinearCorrection::Disabled:
				return quantize<Sq8NonLinearCorrection::Disabled>(from, to);
			case Sq8NonLinearCorrection::Enabled:
				return quantize<Sq8NonLinearCorrection::Enabled>(from, to);
			default:
				std::abort();
		}
	}
	CorrectiveOffsets Requantize(auto point, const auto& dequantizer) const noexcept {
		switch (params_.config.nonLinearCorrection) {
			case Sq8NonLinearCorrection::Disabled:
				return quantize<Sq8NonLinearCorrection::Disabled>(point, point, dequantizer);
			case Sq8NonLinearCorrection::Enabled:
				return quantize<Sq8NonLinearCorrection::Enabled>(point, point, dequantizer);
			default:
				std::abort();
		}
	}

	const QuantizingParams& Params() const noexcept { return params_; }

	void UpdateStatistic(const void* data) noexcept { statistic_.Update(data); }

	[[nodiscard]] bool NeedRequantize() const noexcept { return statistic_.OutliersPct() > (kConfidenceInterval_ - 1.f); }

	[[nodiscard]] auto PrepareToRequantize() noexcept {
		auto dequantizer = [p = params_](uint8_t val) { return uint8t2floatImpl(val, p.minQ, p.maxQ, p.alpha, p.uint8offset); };

		params_.minQ = statistic_.Min();
		params_.maxQ = statistic_.Max();
		params_.alpha = (params_.maxQ - params_.minQ) / params_.sq8Range;
		params_.alpha_2 = std::pow(params_.alpha, 2.f);
		params_.delta = 0.5 * std::pow(params_.minQ, 2.f) * kDim;

		statistic_.Reset();

		return dequantizer;
	}

private:
	[[nodiscard]] RX_ALWAYS_INLINE uint8_t float2uint8t(float val) const noexcept {
		return val < params_.minQ ? 0
			   : val > params_.maxQ
				   ? KDefaultSq8Range
				   : std::clamp((val - params_.minQ) / params_.alpha + params_.uint8offset, params_.uint8offset, params_.sq8Range);
	}
	[[nodiscard]] static RX_ALWAYS_INLINE float uint8t2floatImpl(uint8_t val, float min, float max, float alpha, float uint8offset) {
		return val == 0 ? min : val == uint8_t(KDefaultSq8Range) ? max : alpha * (val - uint8offset) + min;
	}

	[[nodiscard]] RX_ALWAYS_INLINE float uint8t2float(uint8_t val) const noexcept {
		return uint8t2floatImpl(val, params_.minQ, params_.maxQ, params_.alpha, params_.uint8offset);
	}

	template <Sq8NonLinearCorrection nonLinearCorrection, typename DequantizerT = void*>
	CorrectiveOffsets quantize(auto from, auto to, const DequantizerT& dequantizer = {}) const noexcept {
		CorrectiveOffsets res{};
		const bool isL2 = kMetric == hnswlib::MetricType::L2;
		int cnt = 0;
		int cntPos = 0;
		int cntNeg = 0;

		std::transform(from.begin(), from.end(), to.begin(), [&](auto _val) noexcept {
			float val = _val;
			if constexpr (!std::is_same_v<void*, DequantizerT>) {
				val = dequantizer(val);
			}

			const auto uint8 = float2uint8t(val);
			const auto err = val - uint8t2float(uint8);
			if (isL2) {
				res.precalc += (2 * params_.alpha * uint8 + err) * err;
			} else {
				res.precalc += params_.alpha * uint8 + err;
			}

			if constexpr (nonLinearCorrection == Sq8NonLinearCorrection::Enabled) {
				res.roundingErr += (val >= params_.minQ && val <= params_.maxQ) ? ++cnt, err : 0;
				res.negOutlierErr += val < params_.minQ ? ++cntNeg, err : 0;
				res.posOutlierErr += val > params_.maxQ ? ++cntPos, err : 0;
			}

			return uint8;
		});

		if (!isL2) {
			res.precalc *= params_.minQ;
			res.precalc += params_.delta;
		}

		res.roundingErr /= cnt;

		if constexpr (nonLinearCorrection == Sq8NonLinearCorrection::Enabled) {
			if (cntNeg) {
				res.negOutlierErr /= cntNeg;
			}
			if (cntPos) {
				res.posOutlierErr /= cntPos;
			}
		}
		return res;
	}

	QuantizingParams params_;

	const float kConfidenceInterval_;
	const size_t kDim;
	const MetricType kMetric;

	struct [[nodiscard]] Statistic {
		Statistic(const Quantizer& q, auto curSizeFn) noexcept : quantizer_(q), curSizeFn_(std::move(curSizeFn)) {}

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
