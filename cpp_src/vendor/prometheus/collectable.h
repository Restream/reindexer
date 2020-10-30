#pragma once

#include <map>
#include <vector>

namespace prometheus {
struct MetricFamily;
}

namespace prometheus {

constexpr int64_t kNoEpoch = -1;

/// @brief Interface implemented by anything that can be used by Prometheus to
/// collect metrics.
///
/// A Collectable has to be registered for collection. See Registry.
class Collectable {
public:
	virtual ~Collectable() = default;

	/// \brief Returns a list of metrics and their samples.
	virtual std::vector<MetricFamily> Collect() = 0;

	/// \brief Removes outdated metrics
	virtual void RemoveOutdated(int64_t currentEpoch) = 0;
};

}  // namespace prometheus
