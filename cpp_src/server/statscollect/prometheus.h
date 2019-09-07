#pragma once

#include "estl/string_view.h"
#include "net/http/router.h"
#include "prometheus/registry.h"
#include "prometheus/text_serializer.h"

namespace reindexer_server {

using reindexer::string_view;
using namespace reindexer::net;

class Prometheus {
public:
	template <typename T>
	using PFamily = prometheus::Family<T>;
	using PGauge = prometheus::Gauge;
	using PTextSerializer = prometheus::TextSerializer;
	using PRegistry = prometheus::Registry;
	using PCollectable = prometheus::Collectable;

	void Attach(http::Router &router);
	void RegisterQPS(const string &db, const string &ns, string_view queryType, size_t qps) {
		setMetricValue(qps_, qps, db, ns, queryType);
	}
	void RegisterLatency(const string &db, const string &ns, string_view queryType, size_t latencyUS) {
		setMetricValue(latency_, static_cast<double>(latencyUS) / 1e6, db, ns, queryType);
	}
	void RegisterCachesSize(const string &db, const string &ns, size_t size) { setMetricValue(caches_, size, db, ns); }
	void RegisterIndexesSize(const string &db, const string &ns, size_t size) { setMetricValue(indexes_, size, db, ns); }
	void RegisterDataSize(const string &db, const string &ns, size_t size) { setMetricValue(data_, size, db, ns); }
	void RegisterAllocatedMemory(size_t memoryConsumationBytes) { setMetricValue(memory_, memoryConsumationBytes); }
	void RegisterRPCClients(const string &db, size_t count) { setMetricValue(rpcClients_, count, db); }
	void RegisterInputTraffic(const string &db, string_view type, size_t bytes) { setMetricValue(inputTraffic_, bytes, db, type); }
	void RegisterOutputTraffic(const string &db, string_view type, size_t bytes) { setMetricValue(outputTraffic_, bytes, db, type); }

	static void setMetricValue(PFamily<PGauge> *metricFamily, double value, const string &db = "", const string &ns = "",
							   string_view queryType = "") noexcept;
	static void setMetricValue(PFamily<PGauge> *metricFamily, double value, const string &db, string_view type) noexcept;
	int collect(http::Context &ctx);

	PRegistry registry_;
	PTextSerializer serializer_;
	std::vector<PCollectable *> collectables_;
	PFamily<PGauge> *qps_{nullptr};
	PFamily<PGauge> *latency_{nullptr};
	PFamily<PGauge> *caches_{nullptr};
	PFamily<PGauge> *indexes_{nullptr};
	PFamily<PGauge> *data_{nullptr};
	PFamily<PGauge> *memory_{nullptr};
	PFamily<PGauge> *rpcClients_{nullptr};
	PFamily<PGauge> *inputTraffic_{nullptr};
	PFamily<PGauge> *outputTraffic_{nullptr};
};

}  // namespace reindexer_server
