#if 0
#pragma once

#include <atomic>
#include <numeric>
#include <span>

namespace hnswlib {
struct [[nodiscard]] ComponentNthQuantiles : std::vector<std::pair<float, float>> {
	static constexpr auto kMin = std::numeric_limits<float>::lowest();
	static constexpr auto kMax = std::numeric_limits<float>::max();

	explicit ComponentNthQuantiles(size_t size) : std::vector<std::pair<float, float>>(size, {kMax, kMin}) {}

	auto GetUpdated(const void* dataPtr) const noexcept {
		auto data = static_cast<const float*>(dataPtr);
		std::pair<float, float> pair;
		std::optional<reindexer::fast_hash_map<int, std::pair<float, float>>> res;
		for (size_t i = 0; i < size(); ++i) {
			if (merge(operator[](i), data[i], pair)) {
				if (!res) {
					res = reindexer::fast_hash_map<int, std::pair<float, float>>{};
				}
				(*res)[i] = std::move(pair);
			}
		}

		return res;
	}

	void Update(const reindexer::fast_hash_map<int, std::pair<float, float>>& data) noexcept {
		const auto origMinmax = std::pair<float, float>{min, max};
		auto minmax = origMinmax;
		std::pair<float, float> pair;
		for (const auto& [idx, p] : data) {
			if (merge(minmax, p, pair)) {
				minmax = std::move(pair);
			}

			auto& comp = operator[](idx);
			if (merge(comp, p, pair)) {
				comp = std::move(pair);
			}
		}

		if (minmax != origMinmax) {
			min = minmax.first;
			max = minmax.second;
		}
	}

	float min = kMax;
	float max = kMin;

private:
	bool merge(const auto& orig, float value, auto& res) const noexcept {
		if (orig.first <= value && value <= orig.second) {
			return false;
		}

		res = std::make_pair(std::min(orig.first, value), std::max(orig.second, value));
		return true;
	}
	bool merge(const auto& orig, const auto& other, auto& res) const noexcept {
		if (orig.first <= other.first && other.second <= orig.second) {
			return false;
		}

		res = std::make_pair(std::min(orig.first, other.first), std::max(orig.second, other.second));
		return true;
	}
};
}  // namespace hnswlib
#endif
