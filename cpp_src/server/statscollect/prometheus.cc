#include "prometheus.h"
#include "prometheus/gauge.h"
#include "reindexer_version.h"

namespace reindexer_server {

void Prometheus::Attach(http::Router& router) {
	using prometheus::BuildGauge;
	qps_ = &BuildGauge().Name("reindexer_qps_total").Help("Shows queries per second").Register(registry_);
	latency_ = &BuildGauge().Name("reindexer_avg_latency").Help("Average requests latency (seconds)").Register(registry_);
	caches_ = &BuildGauge().Name("reindexer_caches_size_bytes").Help("Namespace caches size in bytes").Register(registry_);
	indexes_ = &BuildGauge().Name("reindexer_indexes_size_bytes").Help("Namespace indexes size in bytes").Register(registry_);
	data_ = &BuildGauge().Name("reindexer_data_size_bytes").Help("Namespace data size in bytes").Register(registry_);
	itemsCount_ = &BuildGauge().Name("reindexer_items_count").Help("Items count in namespace").Register(registry_);
	memory_ = &BuildGauge()
				   .Name("reindexer_memory_allocated_bytes")
				   .Help("Currently allocated bytes, according to allocator library")
				   .Register(registry_);
	rpcClients_ = &BuildGauge().Name("reindexer_rpc_clients_count").Help("Current RPC server clients count").Register(registry_);
	inputTraffic_ = &BuildGauge().Name("reindexer_input_traffic_total_bytes").Help("Total RPC input traffic in bytes").Register(registry_);
	outputTraffic_ =
		&BuildGauge().Name("reindexer_output_traffic_total_bytes").Help("Total RPC output traffic in bytes").Register(registry_);
	rxInfo_ = &BuildGauge().Name("reindexer_info").Help("Generic reindexer info").Register(registry_);
	fillRxInfo();

	router.GET<Prometheus, &Prometheus::collect>("/metrics", this);
}

void Prometheus::NextEpoch() { registry_.RemoveOutdated(currentEpoch_++ - 1); }

void Prometheus::setMetricValue(PFamily<Prometheus::PGauge>* metricFamily, double value, int64_t epoch, const std::string& db,
								const std::string& ns, string_view queryType) {
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
		metricFamily->Add(std::move(labels), epoch).Set(value);
	}
}

void Prometheus::setMetricValue(PFamily<PGauge>* metricFamily, double value, int64_t epoch, const string& db, string_view type) {
	if (metricFamily) {
		std::map<std::string, std::string> labels;
		if (!db.empty()) {
			labels.emplace("db", std::string(db));
		}
		if (!type.empty()) {
			labels.emplace("type", std::string(type));
		}
		metricFamily->Add(std::move(labels), epoch).Set(value);
	}
}

void Prometheus::fillRxInfo() {
	assert(rxInfo_);
	rxInfo_->Add({{"vesion", REINDEX_VERSION}}, prometheus::kNoEpoch).Set(1.0);
}

int Prometheus::collect(http::Context& ctx) { return ctx.String(http::StatusOK, PTextSerializer().Serialize(registry_.Collect())); }

}  // namespace reindexer_server
