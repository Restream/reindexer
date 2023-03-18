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

enum class ServerMode { Standalone, Builtin };

struct ServerConfig {
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
	std::string RPCThreadingMode;
	std::string HttpThreadingMode;
	std::string LogLevel;
	std::string ServerLog;
	std::string CoreLog;
	std::string HttpLog;
	std::string RpcLog;
	std::string StoragePath;
	bool StartWithErrors;
	bool Autorepair;
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
	size_t MaxUpdatesSize;
	bool EnableGRPC;
	std::string GRPCAddr;
	size_t MaxHttpReqSize;
	std::chrono::seconds RPCQrIdleTimeout;
	int64_t AllocatorCacheLimit;
	float AllocatorCachePart;

	static const std::string kDedicatedThreading;
	static const std::string kSharedThreading;
	static const std::string kPoolThreading;

protected:
	Error fromYaml(YAML::Node& root);

private:
	std::vector<std::string> args_;
#if REINDEX_WITH_GPERFTOOLS
	bool tcMallocIsAvailable_;
#endif	// REINDEX_WITH_GPERFTOOLS
};

}  // namespace reindexer_server
