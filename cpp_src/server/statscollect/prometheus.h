#pragma once

#include <string_view>
#include "net/http/router.h"
#include "prometheus/registry.h"
#include "prometheus/text_serializer.h"

namespace reindexer_server {

using namespace reindexer::net;

class [[nodiscard]] Prometheus {
public:
	template <typename T>
	using PFamily = prometheus::Family<T>;
	using PGauge = prometheus::Gauge;
	using PTextSerializer = prometheus::TextSerializer;
	using PRegistry = prometheus::Registry;
	using PCollectable = prometheus::Collectable;

	void Attach(http::Router& router);
	void RegisterQPS(const std::string& db, const std::string& ns, std::string_view queryType, size_t qps) {
		setMetricValue(qps_, qps, currentEpoch_, db, ns, queryType);
	}
	void RegisterLatency(const std::string& db, const std::string& ns, std::string_view queryType, size_t latencyUS) {
		setMetricValue(latency_, static_cast<double>(latencyUS) / 1e6, currentEpoch_, db, ns, queryType);
	}
	void RegisterCachesSize(const std::string& db, const std::string& ns, size_t size) {
		setMetricValue(caches_, size, currentEpoch_, db, ns);
	}
	void RegisterIndexesSize(const std::string& db, const std::string& ns, size_t size) {
		setMetricValue(indexes_, size, currentEpoch_, db, ns);
	}
	void RegisterDataSize(const std::string& db, const std::string& ns, size_t size) { setMetricValue(data_, size, currentEpoch_, db, ns); }
	void RegisterItemsCount(const std::string& db, const std::string& ns, size_t count) {
		setMetricValue(itemsCount_, count, currentEpoch_, db, ns);
	}
	void RegisterAllocatedMemory(size_t memoryConsumationBytes) { setMetricValue(memory_, memoryConsumationBytes, prometheus::kNoEpoch); }
	void RegisterRPCClients(const std::string& db, std::string_view protocol, size_t count) {
		setNetMetricValue(rpcClients_, count, currentEpoch_, db, std::string_view(), protocol);
	}
	void RegisterInputTraffic(const std::string& db, std::string_view type, std::string_view protocol, size_t bytes) {
		setNetMetricValue(inputTraffic_, bytes, prometheus::kNoEpoch, db, type, protocol);
	}
	void RegisterOutputTraffic(const std::string& db, std::string_view type, std::string_view protocol, size_t bytes) {
		setNetMetricValue(outputTraffic_, bytes, prometheus::kNoEpoch, db, type, protocol);
	}
	void RegisterStorageStatus(const std::string& db, const std::string& ns, bool isOK) {
		setMetricValue(storageStatus_, isOK ? 1.0 : 0.0, prometheus::kNoEpoch, db, ns);
	}
	void RegisterEmbedderLastSecQps(const std::string& db, const std::string& ns, const std::string& index, std::string_view type,
									size_t param) {
		setMetricEmbedder(embedderLastSecQps_, param, currentEpoch_, db, ns, index, type);
	}
	void RegisterEmbedderLastSecDps(const std::string& db, const std::string& ns, const std::string& index, std::string_view type,
									size_t param) {
		setMetricEmbedder(embedderLastSecDps_, param, currentEpoch_, db, ns, index, type);
	}

	void RegisterEmbedderLastSecErrorsCount(const std::string& db, const std::string& ns, const std::string& index, std::string_view type,
											size_t param) {
		setMetricEmbedder(embedderLastSecErrorsCount_, param, currentEpoch_, db, ns, index, type);
	}
	void RegisterEmbedderConnInUse(const std::string& db, const std::string& ns, const std::string& index, std::string_view type,
								   size_t param) {
		setMetricEmbedder(embedderConnInUse_, param, currentEpoch_, db, ns, index, type);
	}
	void RegisterEmbedderLastSecAvgLatencyUs(const std::string& db, const std::string& ns, const std::string& index, std::string_view type,
											 size_t param) {
		setMetricEmbedder(embedderLastSecAvgLatencyUs_, param, currentEpoch_, db, ns, index, type);
	}
	void RegisterEmbedderLastSecAvgEmbedLatencyUs(const std::string& db, const std::string& ns, const std::string& index,
												  std::string_view type, size_t param) {
		setMetricEmbedder(embedderLastSecAvgEmbedLatencyUs_, param, currentEpoch_, db, ns, index, type);
	}

	void NextEpoch();

private:
	static void setMetricValue(PFamily<PGauge>* metricFamily, double value, int64_t epoch);
	static void setMetricValue(PFamily<PGauge>* metricFamily, double value, int64_t epoch, const std::string& db, const std::string& ns,
							   std::string_view queryType = "");
	static void setNetMetricValue(PFamily<PGauge>* metricFamily, double value, int64_t epoch, const std::string& db, std::string_view type,
								  std::string_view protocol);
	static void setMetricEmbedder(PFamily<PGauge>* metricFamily, double value, int64_t epoch, const std::string& db, const std::string& ns,
								  const std::string& index, std::string_view type);
	void fillRxInfo();
	int collect(http::Context& ctx);

	PRegistry registry_;
	int64_t currentEpoch_ = 1;
	PFamily<PGauge>* qps_{nullptr};
	PFamily<PGauge>* latency_{nullptr};
	PFamily<PGauge>* caches_{nullptr};
	PFamily<PGauge>* indexes_{nullptr};
	PFamily<PGauge>* data_{nullptr};
	PFamily<PGauge>* memory_{nullptr};
	PFamily<PGauge>* rpcClients_{nullptr};
	PFamily<PGauge>* inputTraffic_{nullptr};
	PFamily<PGauge>* outputTraffic_{nullptr};
	PFamily<PGauge>* storageStatus_{nullptr};
	PFamily<PGauge>* itemsCount_{nullptr};
	PFamily<PGauge>* rxInfo_{nullptr};

	PFamily<PGauge>* embedderLastSecQps_{nullptr};
	PFamily<PGauge>* embedderLastSecDps_{nullptr};
	PFamily<PGauge>* embedderLastSecErrorsCount_{nullptr};
	PFamily<PGauge>* embedderConnInUse_{nullptr};
	PFamily<PGauge>* embedderLastSecAvgLatencyUs_{nullptr};
	PFamily<PGauge>* embedderLastSecAvgEmbedLatencyUs_{nullptr};
};

}  // namespace reindexer_server
