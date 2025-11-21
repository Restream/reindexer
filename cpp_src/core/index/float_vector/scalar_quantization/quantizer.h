#pragma once

#include <atomic>
#include <span>
#include "core/index/float_vector/hnswlib/type_consts.h"
#include "estl/fast_hash_set.h"
#include "hnsw_view_iterator.h"
#include "tools/float_comparison.h"

namespace hnswlib {

// The template parameter DataHandlerT is needed to avoid cyclic references
// in header files for the case of header-only implementation.
template <typename DataHandlerT>
class [[nodiscard]] Quantizer {
public:
	static constexpr size_t kDefaultSampleSize = 20'000;

	std::pair<float, float> FindNthMinMax(auto&& s, size_t dataSize) const {
		using T = std::decay_t<decltype(*s.begin())>;
		T min, max;
		const size_t n = 0.5f * (1 - quantile_) * dataSize;
		size_t cnt = 0;
		reindexer::fast_hash_set<size_t> scannedIndexes;
		do {
			min = std::numeric_limits<T>::max();
			max = std::numeric_limits<T>::lowest();
			size_t minIdx = -1, maxIdx = -1;
			size_t i = 0;
			for (auto el : s) {
				if (scannedIndexes.count(i) > 0) {
					i++;
					continue;
				}

				if (el < min) {
					min = el;
					minIdx = i;
				}
				if (el > max) {
					max = el;
					maxIdx = i;
				}
				i++;
			}
			if (n > 0) {
				scannedIndexes.insert(minIdx);
				scannedIndexes.insert(maxIdx);
			}
		} while (++cnt < n);
		if constexpr (std::is_same_v<T, uint8_t>) {
			return {uint8t2float(min), uint8t2float(max)};
		} else if constexpr (std::is_same_v<T, float>) {
			return {min, max};
		} else {
			assertrx(false);
			return {};
		}
	}

	explicit Quantizer(DataHandlerT& dataHandler, float quantile = 1.f, size_t sampleSize = kDefaultSampleSize,
					   Sq8NonLinearCorrection nonLinearCorrection = Sq8NonLinearCorrection::Disabled)
		: kNonLinearCorrection(nonLinearCorrection),
		  dataHandler_(dataHandler),
		  quantile_([&quantile]() {
			  assertrx_throw(quantile >= 0.95f && quantile <= 1.f);
			  return quantile;
		  }()),
		  // for the 1-quantile make a reserve for outliers in 1 pct
		  kConfidenceInterval_(reindexer::fp::EqualWithinULPs(quantile_, 1.f) ? 1.01f : (1.f / quantile_)),
		  partialSampleSize_(sampleSize),
		  params_(calcParams()),
		  statistic_(*this) {}

	CorrectiveOffsets Quantize(auto from, auto to) const noexcept {
		switch (kNonLinearCorrection) {
			case Sq8NonLinearCorrection::Disabled:
				return quantize<Sq8NonLinearCorrection::Disabled>(from, to);
			case Sq8NonLinearCorrection::Enabled:
				return quantize<Sq8NonLinearCorrection::Enabled>(from, to);
			default:
				std::abort();
		}
	}
	CorrectiveOffsets Requantize(auto point, const auto& dequantizer) const noexcept {
		switch (kNonLinearCorrection) {
			case Sq8NonLinearCorrection::Disabled:
				return quantize<Sq8NonLinearCorrection::Disabled>(point, point, dequantizer);
			case Sq8NonLinearCorrection::Enabled:
				return quantize<Sq8NonLinearCorrection::Enabled>(point, point, dequantizer);
			default:
				std::abort();
		}
	}

	float Alpha() const noexcept { return params_.alpha_; }
	float MinQ() const noexcept { return params_.minQ_; }
	float MaxQ() const noexcept { return params_.maxQ_; }
	size_t Dims() const noexcept { return dataHandler_.fstdistfunc_.Dims(); }

	void UpdateStatistic(const void* data) noexcept { statistic_.Update(data); }

	[[nodiscard]] bool NeedRequantize() const noexcept { return statistic_.OutliersPct() > (kConfidenceInterval_ - 1.f); }

	[[nodiscard]] auto PrepareToRequantize() noexcept {
		auto res = dequantizer();

		params_.minQ_ = statistic_.Min();
		params_.maxQ_ = statistic_.Max();
		params_.alpha_ = (params_.maxQ_ - params_.minQ_) / KSq8Range;
		params_.delta_ = 0.5 * std::pow(params_.minQ_, 2.f) * Dims();

		statistic_.Reset();

		return res;
	}

	const Sq8NonLinearCorrection kNonLinearCorrection;

private:
	[[nodiscard]] RX_ALWAYS_INLINE uint8_t float2uint8t(float val) const noexcept {
		return val < params_.minQ_	 ? 0
			   : val > params_.maxQ_ ? KDefaultSq8Range
									 : std::clamp((val - params_.minQ_) / params_.alpha_ + kUint8offset, kUint8offset, KSq8Range);
	}
	[[nodiscard]] static RX_ALWAYS_INLINE float uint8t2floatImpl(uint8_t val, float min, float max, float alpha, float uint8offset) {
		return val == 0 ? min : val == uint8_t(KDefaultSq8Range) ? max : alpha * (val - uint8offset) + min;
	}

	[[nodiscard]] RX_ALWAYS_INLINE float uint8t2float(uint8_t val) const noexcept {
		return uint8t2floatImpl(val, params_.minQ_, params_.maxQ_, params_.alpha_, kUint8offset);
	}

	[[nodiscard]] auto dequantizer() const noexcept {
		return [p = params_, this](uint8_t val) { return uint8t2floatImpl(val, p.minQ_, p.maxQ_, p.alpha_, kUint8offset); };
	}

	template <Sq8NonLinearCorrection nonLinearCorrection, typename DequantizerT = void*>
	CorrectiveOffsets quantize(auto from, auto to, const DequantizerT& dequantizer = {}) const noexcept {
		CorrectiveOffsets res{};
		const bool isL2 = dataHandler_.fstdistfunc_.Metric() == hnswlib::MetricType::L2;
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
				res.precalc += (2 * params_.alpha_ * uint8 + err) * err;
			} else {
				res.precalc += params_.alpha_ * uint8 + err;
			}

			if constexpr (nonLinearCorrection == Sq8NonLinearCorrection::Enabled) {
				res.roundingErr += (val >= params_.minQ_ && val <= params_.maxQ_) ? ++cnt, err : 0;
				res.negOutlierErr += val < params_.minQ_ ? ++cntNeg, err : 0;
				res.posOutlierErr += val > params_.maxQ_ ? ++cntPos, err : 0;
			}

			return uint8;
		});

		if (!isL2) {
			res.precalc *= params_.minQ_;
			res.precalc += params_.delta_;
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

	auto calcParams() {
		params_.minQ_ = 0.f;
		params_.maxQ_ = 0.f;
		size_t size = 0;
		for (auto& sample : HNSWView(dataHandler_, partialSampleSize_)) {
			auto [min, max] = FindNthMinMax(sample, dataHandler_.fstdistfunc_.Dims() * kSampleBatchSize);
			params_.minQ_ += min;
			params_.maxQ_ += max;
			size++;
		}
		params_.minQ_ /= size;
		params_.maxQ_ /= size;

		params_.alpha_ = (params_.maxQ_ - params_.minQ_) / KSq8Range;
		params_.delta_ = 0.5 * std::pow(params_.minQ_, 2.f) * Dims();

		return params_;
	}

	const float KSq8Range = KDefaultSq8Range - (kNonLinearCorrection == Sq8NonLinearCorrection::Enabled ? 2 : 0);
	const float kUint8offset = kNonLinearCorrection == Sq8NonLinearCorrection::Enabled ? 1.f : 0.f;

	const DataHandlerT& dataHandler_;
	const float quantile_ = 1.f;
	const float kConfidenceInterval_;
	const size_t partialSampleSize_ = kDefaultSampleSize;

	struct [[nodiscard]] {
		float minQ_ = std::numeric_limits<float>::max();
		float maxQ_ = std::numeric_limits<float>::lowest();
		float alpha_ = 0.f;
		float delta_ = 0.f;
	} params_;

	struct [[nodiscard]] Statistic {
		Statistic(const Quantizer& q) noexcept : quantizer_(q) {}

		void Reset() {
			kMinQconfidence = quantizer_.MinQ() * quantizer_.kConfidenceInterval_;
			kMaxQconfidence = quantizer_.MaxQ() * quantizer_.kConfidenceInterval_;
			curMin_ = kMinQconfidence;
			curMax_ = kMaxQconfidence;
			cnt_.store(0, std::memory_order_relaxed);
			cntOutliers_ = 0;
		}

		float OutliersPct() const noexcept {
			auto cnt = cnt_.load(std::memory_order_relaxed);
			// not requantize if processed less than 10% of all points
			if (cnt < 0.1 * quantizer_.dataHandler_.cur_element_count * dim) {
				return 0;
			}
			return float(cntOutliers_) / cnt;
		}

		void Update(const void* dataPtr) noexcept {
			auto data = std::span(static_cast<const float*>(dataPtr), dim);

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
			cnt_.fetch_add(dim, std::memory_order_relaxed);

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
		const int dim = quantizer_.dataHandler_.fstdistfunc_.Dims();

		float kMinQconfidence = quantizer_.MinQ() * quantizer_.kConfidenceInterval_;
		float kMaxQconfidence = quantizer_.MaxQ() * quantizer_.kConfidenceInterval_;

		std::atomic<float> curMin_ = kMinQconfidence;
		std::atomic<float> curMax_ = kMaxQconfidence;

		std::atomic<size_t> cnt_ = 0;
		std::atomic<size_t> cntOutliers_ = 0;
	} statistic_;
};

};	// namespace hnswlib
