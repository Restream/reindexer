#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include "config.h"
#include "loggerwrapper.h"
#include "net/ev/ev.h"

#ifndef _WIN32
#include "pidfile.h"
#endif

#include "tools/errors.h"

namespace reindexer_server {
using namespace reindexer::net;

class HTTPServer;
class RPCServer;
class DBManager;
struct IDBManagerStatsCollector;
struct IRPCServerStatsCollector;

class ServerImpl {
	using SinkMap = std::unordered_map<std::string, std::shared_ptr<spdlog::sinks::fast_file_sink>>;

public:
	ServerImpl(ServerMode mode);
	~ServerImpl();

	Error InitFromCLI(int argc, char* argv[]);
	Error InitFromYAML(const std::string& yaml);
	Error InitFromFile(const char* filepath);
	int Start();
	void Stop();
	void EnableHandleSignals(bool enable = true) { enableHandleSignals_ = enable; }
	DBManager& GetDBManager() { return *dbMgr_; }
	bool IsReady() const { return storageLoaded_.load(); }
	bool IsRunning() const { return running_.load(); }
	void ReopenLogFiles();

protected:
	int run();
	Error init();

private:
	Error daemonize();
	Error loggerConfigure();
	void initCoreLogger();

private:
	std::vector<std::string> args_;
	ServerConfig config_;
	LoggerWrapper logger_;
	int coreLogLevel_;

#ifndef _WIN32
	PidFile pid_;
#endif
	std::unique_ptr<DBManager> dbMgr_;
	SinkMap sinks_;

private:
	std::atomic_bool storageLoaded_;
	std::atomic_bool running_;
	bool enableHandleSignals_ = false;
	ev::async async_;
	ev::dynamic_loop loop_;
#ifndef REINDEX_WITH_ASAN
	ServerMode mode_ = ServerMode::Builtin;
#endif	// REINDEX_WITH_ASAN
};
}  // namespace reindexer_server
