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
	storageStatus_ = &BuildGauge()
						  .Name("reindexer_storage_ok")
						  .Help("Shows if storage is enabled and writable (value 1 means, that everything is fine)")
						  .Register(registry_);
	rxInfo_ = &BuildGauge().Name("reindexer_info").Help("Generic reindexer info").Register(registry_);
	fillRxInfo();

	embedderLastSecQps_ =
		&BuildGauge().Name("reindexer_embed_last_sec_qps").Help("Number of calls to the embedder in the last second").Register(registry_);
	embedderLastSecDps_ =
		&BuildGauge().Name("reindexer_embed_last_sec_dps").Help("Number of embedded documents in the last second").Register(registry_);

	embedderLastSecErrorsCount_ =
		&BuildGauge().Name("reindexer_embed_last_sec_errors_count").Help("Number of errors in the last second").Register(registry_);
	embedderConnInUse_ = &BuildGauge().Name("reindexer_embed_conn_in_use").Help("Current number of connections in use").Register(registry_);
	embedderLastSecAvgLatencyUs_ =
		&BuildGauge().Name("reindexer_embed_last_sec_avg_latency_us").Help("Average autoembed time (last second)").Register(registry_);
	embedderLastSecAvgEmbedLatencyUs_ = &BuildGauge()
											 .Name("reindexer_embed_last_sec_avg_embed_latency_us")
											 .Help("Average auto-embedding latency for cache misses (last second)")
											 .Register(registry_);

	router.GET<Prometheus, &Prometheus::collect>("/metrics", this);
}

void Prometheus::NextEpoch() { registry_.RemoveOutdated(currentEpoch_++ - 1); }

void Prometheus::setMetricValue(PFamily<Prometheus::PGauge>* metricFamily, double value, int64_t epoch) {
	if (metricFamily) {
		metricFamily->Add(std::map<std::string, std::string>{}, epoch).Set(value);
	}
}

void Prometheus::setMetricValue(PFamily<Prometheus::PGauge>* metricFamily, double value, int64_t epoch, const std::string& db,
								const std::string& ns, std::string_view queryType) {
	if (metricFamily) {
		std::map<std::string, std::string> labels;
		labels.emplace("db", db);
		if (!ns.empty()) {
			labels.emplace("ns", ns);
		}
		if (!queryType.empty()) {
			labels.emplace("query", std::string(queryType));
		}
		metricFamily->Add(std::move(labels), epoch).Set(value);
	}
}

void Prometheus::setNetMetricValue(PFamily<PGauge>* metricFamily, double value, int64_t epoch, const std::string& db, std::string_view type,
								   std::string_view protocol) {
	if (metricFamily) {
		std::map<std::string, std::string> labels;
		labels.emplace("db", db);
		if (!type.empty()) {
			labels.emplace("type", std::string(type));
		}
		if (!protocol.empty()) {
			labels.emplace("protocol_domain", std::string(protocol));
		}
		metricFamily->Add(std::move(labels), epoch).Set(value);
	}
}

void Prometheus::setMetricEmbedder(PFamily<PGauge>* metricFamily, double value, int64_t epoch, const std::string& db, const std::string& ns,
								   const std::string& index, std::string_view type) {
	if (metricFamily) {
		std::map<std::string, std::string> labels;
		labels.emplace("db", db);
		labels.emplace("ns", ns);
		labels.emplace("index", index);
		labels.emplace("type", type);
		metricFamily->Add(std::move(labels), epoch).Set(value);
	}
}

void Prometheus::fillRxInfo() {
	assertrx(rxInfo_);
	rxInfo_->Add({{"version", REINDEX_VERSION}}, prometheus::kNoEpoch).Set(1.0);
}

int Prometheus::collect(http::Context& ctx) { return ctx.String(http::StatusOK, PTextSerializer().Serialize(registry_.Collect())); }

}  // namespace reindexer_server
