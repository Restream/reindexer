#pragma once

#include <string_view>

namespace reindexer_server {

struct IStatsWatcher {
	virtual void OnInputTraffic(const std::string& db, std::string_view source, size_t bytes) noexcept = 0;
	virtual void OnOutputTraffic(const std::string& db, std::string_view source, size_t bytes) noexcept = 0;
	virtual void OnClientConnected(const std::string& db, std::string_view source) noexcept = 0;
	virtual void OnClientDisconnected(const std::string& db, std::string_view source) noexcept = 0;
	virtual ~IStatsWatcher() noexcept = default;
};

}  // namespace reindexer_server
