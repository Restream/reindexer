#include "core/embedding/connectorpool.h"
#include "core/embedding/httpconnector.h"
#include "core/rdxcontext.h"
#include "estl/lock.h"

namespace reindexer {

ConnectorPool::ConnectorProxy::ConnectorProxy() noexcept : pool_{nullptr}, connector_{nullptr} {}

ConnectorPool::ConnectorProxy::ConnectorProxy(ConnectorPool* pool, HttpConnector* connector) noexcept
	: pool_{pool}, connector_{connector} {}

ConnectorPool::ConnectorProxy::ConnectorProxy(ConnectorPool::ConnectorProxy&& other) noexcept
	: pool_{other.pool_}, connector_{other.connector_} {
	other.pool_ = nullptr;
	other.connector_ = nullptr;
}

ConnectorPool::ConnectorProxy::~ConnectorProxy() {
	if (pool_ != nullptr) {
		pool_->ReleaseConnection(*this);
	}
}

HttpConnector& ConnectorPool::ConnectorProxy::operator*() { return *connector_; }

namespace {

bool CheckConnect(HttpConnector& connector, const PoolConfig& config) noexcept {
	try {
		if (!connector.Connected()) {
			return connector.Connect(config.connect_timeout_ms, config.read_timeout_ms, config.write_timeout_ms);
		}

		return true;
	} catch (...) {
		return false;
	}
}

}  // namespace

ConnectorPool::ConnectorPool(PoolConfig&& config) : config_{std::move(config)} {
	for (auto k = config_.connections; k > 0; --k) {
		// std::make_unique can't be used because constructor is hidden
		auto connection = std::unique_ptr<HttpConnector>(new HttpConnector{config_.endpointUrl});
		auto key = connection.get();
		idle_.emplace(key, std::move(connection));
	}
}

ConnectorPool::~ConnectorPool() = default;

std::pair<Error, ConnectorPool::ConnectorProxy> ConnectorPool::GetConnector(const RdxContext& ctx) noexcept {
	try {
		unique_lock lock(mtx_);
		if (idle_.empty()) {
			cond_.wait(lock, [this]() noexcept { return !idle_.empty(); }, ctx);
		}

		// NOTE: check only one - first
		auto key = idle_.cbegin()->first;
		auto node = idle_.extract(key);
		busy_.insert(std::move(node));
		busySize_.fetch_add(1, std::memory_order_relaxed);
		lock.unlock();

		if (CheckConnect(*key, config_)) {
			return std::make_pair(Error(), ConnectorProxy{this, key});
		}
		lock.lock();
		idle_.insert(busy_.extract(key));
		busySize_.fetch_sub(1, std::memory_order_relaxed);
	} catch (Error& err) {
		if (err.code() == errTimeout || err.code() == errCanceled) {
			return std::make_pair(Error(err.code(), "Some of the connectors are not available (request was canceled/timed out)"),
								  ConnectorProxy{});
		}
		return std::make_pair(std::move(err), ConnectorProxy{});
	} catch (...) {
		return std::make_pair(Error(errNetwork, "Unknown error with connectors or connector pool"), ConnectorProxy{});
	}

	return std::make_pair(Error(errNetwork, "Some of the connectors are not available"), ConnectorProxy{});
}

void ConnectorPool::ReleaseConnection(const ConnectorProxy& proxy) {
	if (proxy.connector_ == nullptr) {
		return;
	}

	bool notify = false;
	{
		unique_lock lock{mtx_};
		notify = idle_.empty();

		if (auto it = busy_.find(proxy.connector_); it != busy_.end()) {
			std::ignore = CheckConnect(*it->second, config_);
			idle_.insert(busy_.extract(it));
			busySize_.fetch_sub(1, std::memory_order_relaxed);
		}
	}

	if (notify) {
		cond_.notify_all();
	}
}

size_t ConnectorPool::ConnectionInUse() const noexcept { return busySize_.load(std::memory_order_relaxed); }

}  // namespace reindexer
