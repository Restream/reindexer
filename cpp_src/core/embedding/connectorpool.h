#pragma once

#include <unordered_map>
#include "core/embedding/embeddingconfig.h"
#include "estl/contexted_cond_var.h"
#include "estl/mutex.h"
#include "tools/errors.h"

namespace reindexer {

class HttpConnector;
class RdxContext;

class [[nodiscard]] ConnectorPool final {
public:
	class [[nodiscard]] ConnectorProxy final {
	public:
		ConnectorProxy(const ConnectorProxy&) noexcept = delete;
		ConnectorProxy& operator=(const ConnectorProxy&) noexcept = delete;
		ConnectorProxy& operator=(ConnectorProxy&&) noexcept = delete;

		ConnectorProxy(ConnectorProxy&&) noexcept;

		~ConnectorProxy();

		HttpConnector& operator*();

	private:
		friend ConnectorPool;
		ConnectorProxy() noexcept;
		ConnectorProxy(ConnectorPool* pool, HttpConnector* connector) noexcept;

		ConnectorPool* pool_{nullptr};
		HttpConnector* connector_{nullptr};
	};

public:
	explicit ConnectorPool(PoolConfig&& config);
	~ConnectorPool();

	std::pair<Error, ConnectorProxy> GetConnector(const RdxContext& ctx) noexcept;
	void ReleaseConnection(const ConnectorProxy& proxy);
	size_t ConnectionInUse() const noexcept;

private:
	mutex mtx_;
	std::atomic<uint64_t> busySize_{0};
	contexted_cond_var cond_;

	const PoolConfig config_;
	std::unordered_map<HttpConnector*, std::unique_ptr<HttpConnector>> idle_;
	std::unordered_map<HttpConnector*, std::unique_ptr<HttpConnector>> busy_;
};

}  // namespace reindexer
