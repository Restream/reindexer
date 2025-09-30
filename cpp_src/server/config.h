#pragma once

#include <chrono>
#include <string>
#include <vector>
#include "tools/errors.h"

using reindexer::Error;

namespace YAML {
class Node;
}

namespace reindexer_server {

enum class [[nodiscard]] ServerMode { Standalone, Builtin };

struct [[nodiscard]] ServerConfig {
	// This timeout is required to avoid locks, when raft leader does not exist
	constexpr static auto kDefaultHttpWriteTimeout = std::chrono::seconds(60);

	ServerConfig(bool tcMallocIsAvailable)
#ifdef REINDEX_WITH_GPERFTOOLS
		: tcMallocIsAvailable_(tcMallocIsAvailable)
#endif
	{
		(void)tcMallocIsAvailable;
		Reset();
	}

	const std::vector<std::string>& Args() const { return args_; }
	void Reset();

	Error ParseYaml(const std::string& yaml);
	Error ParseFile(const std::string& configPath);
	Error ParseCmd(int argc, char* argv[]);

	std::string WebRoot;
	std::string StorageEngine;
	std::string HTTPAddr;
	std::string RPCAddr;
	std::string HTTPsAddr;
	std::string RPCsAddr;
	std::string RPCUnixAddr;
	std::string RPCThreadingMode;
	std::string RPCUnixThreadingMode;
	std::string HttpThreadingMode;
	std::string LogLevel;
	std::string ServerLog;
	std::string CoreLog;
	std::string HttpLog;
	std::string RpcLog;
	std::string StoragePath;
	std::string SslCertPath;
	std::string SslKeyPath;
	bool StartWithErrors;
	bool AllowNamespaceLeak;
#ifndef _WIN32
	std::string UserName;
	std::string DaemonPidFile;
	bool Daemonize;
#else
	bool InstallSvc;
	bool RemoveSvc;
	bool SvcMode;
#endif
	bool EnableSecurity;
	bool DebugPprof;
	bool EnablePrometheus;
	bool EnableConnectionsStats;
	std::chrono::milliseconds PrometheusCollectPeriod;
	bool DebugAllocs;
	std::chrono::seconds TxIdleTimeout;
	std::chrono::seconds HttpReadTimeout;
	size_t MaxUpdatesSize;
	bool EnableGRPC;
	std::string GRPCAddr;
	size_t MaxHttpReqSize;
	std::chrono::seconds RPCQrIdleTimeout;
	int64_t AllocatorCacheLimit;
	float AllocatorCachePart;

	constexpr static std::string_view kDedicatedThreading = "dedicated";
	constexpr static std::string_view kSharedThreading = "shared";

	void SetHttpWriteTimeout(std::chrono::seconds val) noexcept;
	std::chrono::seconds HttpWriteTimeout() const noexcept {
		return defaultHttpWriteTimeout_ ? kDefaultHttpWriteTimeout : httpWriteTimeout_;
	}
	bool HasDefaultHttpWriteTimeout() const noexcept { return defaultHttpWriteTimeout_; }

protected:
	Error fromYaml(YAML::Node& root);

private:
	bool defaultHttpWriteTimeout_ = true;
	std::chrono::seconds httpWriteTimeout_;
	std::vector<std::string> args_;
#if REINDEX_WITH_GPERFTOOLS
	bool tcMallocIsAvailable_;
#endif	// REINDEX_WITH_GPERFTOOLS
};

}  // namespace reindexer_server
