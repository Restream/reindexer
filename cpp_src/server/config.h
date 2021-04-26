#pragma once

#include <chrono>
#include <string>
#include <vector>
#include "tools/errors.h"

using std::string;
using std::vector;
using reindexer::Error;

namespace Yaml {
class Node;
}

namespace reindexer_server {

struct ServerConfig {
	ServerConfig() { Reset(); }

	const vector<string>& Args() const { return args_; }
	void Reset();

	Error ParseYaml(const string& yaml);
	Error ParseFile(const string& configPath);
	Error ParseCmd(int argc, char* argv[]);

	string WebRoot;
	string StorageEngine;
	string HTTPAddr;
	string RPCAddr;
	string ThreadingMode;
	string LogLevel;
	string ServerLog;
	string CoreLog;
	string HttpLog;
	string RpcLog;
	string StoragePath;
	bool StartWithErrors;
	bool Autorepair;
#ifndef _WIN32
	string UserName;
	string DaemonPidFile;
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
	string GRPCAddr;

	static const string kDedicatedThreading;
	static const string kSharedThreading;
	static const string kPoolThreading;

protected:
	Error fromYaml(Yaml::Node& root);

private:
	vector<string> args_;
};

}  // namespace reindexer_server
