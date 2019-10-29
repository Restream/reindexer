#include "prometheus.h"
#include "prometheus/gauge.h"

namespace reindexer_server {

void Prometheus::Attach(http::Router& router) {
	using prometheus::BuildGauge;
	qps_ = &BuildGauge().Name("reindexer_qps_total").Help("Shows queries per second").Register(registry_);
	collectables_.emplace_back(qps_);
	latency_ = &BuildGauge().Name("reindexer_avg_latency").Help("Average requests latency (seconds)").Register(registry_);
	collectables_.emplace_back(latency_);
	caches_ = &BuildGauge().Name("reindexer_caches_size_bytes").Help("Namespace caches size in bytes").Register(registry_);
	collectables_.emplace_back(caches_);
	indexes_ = &BuildGauge().Name("reindexer_indexes_size_bytes").Help("Namespace indexes size in bytes").Register(registry_);
	collectables_.emplace_back(indexes_);
	data_ = &BuildGauge().Name("reindexer_data_size_bytes").Help("Namespace data size in bytes").Register(registry_);
	collectables_.emplace_back(data_);
	itemsCount_ = &BuildGauge().Name("reindexer_items_count").Help("Items count in namespace").Register(registry_);
	collectables_.emplace_back(itemsCount_);
	memory_ = &BuildGauge()
				   .Name("reindexer_memory_allocated_bytes")
				   .Help("Currently allocated bytes, according to allocator library")
				   .Register(registry_);
	collectables_.emplace_back(memory_);
	rpcClients_ = &BuildGauge().Name("reindexer_rpc_clients_count").Help("Current RPC server clients count").Register(registry_);
	collectables_.emplace_back(rpcClients_);
	inputTraffic_ = &BuildGauge().Name("reindexer_input_traffic_total_bytes").Help("Total RPC input traffic in bytes").Register(registry_);
	collectables_.emplace_back(inputTraffic_);
	outputTraffic_ =
		&BuildGauge().Name("reindexer_output_traffic_total_bytes").Help("Total RPC output traffic in bytes").Register(registry_);
	collectables_.emplace_back(outputTraffic_);

	router.GET<Prometheus, &Prometheus::collect>("/metrics", this);
}

void Prometheus::setMetricValue(PFamily<Prometheus::PGauge>* metricFamily, double value, const std::string& db, const std::string& ns,
								string_view queryType) noexcept {
	if (metricFamily) {
		std::map<std::string, std::string> labels;
		if (!db.empty()) {
			labels.emplace("db", db);
		}
		if (!ns.empty()) {
			labels.emplace("ns", ns);
		}
		if (!queryType.empty()) {
			labels.emplace("query", std::string(queryType));
		}
		auto& metric = metricFamily->Add(std::move(labels));
		metric.Set(value);
	}
}

void Prometheus::setMetricValue(PFamily<PGauge>* metricFamily, double value, const string& db, string_view type) noexcept {
	if (metricFamily) {
		std::map<std::string, std::string> labels;
		if (!db.empty()) {
			labels.emplace("db", std::string(db));
		}
		if (!type.empty()) {
			labels.emplace("type", std::string(type));
		}
		auto& metric = metricFamily->Add(std::move(labels));
		metric.Set(value);
	}
}

int Prometheus::collect(http::Context& ctx) {
	std::vector<prometheus::MetricFamily> metrics;
	for (auto el : collectables_) {
		auto&& metric = el->Collect();
		metrics.insert(metrics.end(), std::make_move_iterator(metric.begin()), std::make_move_iterator(metric.end()));
	}
	return ctx.String(http::StatusOK, serializer_.Serialize(metrics));
}

}  // namespace reindexer_server
