#include "prometheus/registry.h"

namespace prometheus {

std::vector<MetricFamily> Registry::Collect() {
	std::vector<MetricFamily> results;

	std::lock_guard<std::mutex> lock{mutex_};
	for (auto&& collectable : collectables_) {
		auto metrics = collectable->Collect();
		results.insert(results.end(), metrics.begin(), metrics.end());
	}

	return results;
}

void Registry::RemoveOutdated(int64_t currentEpoch) {
	std::lock_guard<std::mutex> lock{mutex_};
	for (auto&& collectable : collectables_) {
		collectable->RemoveOutdated(currentEpoch);
	}
}

}  // namespace prometheus
