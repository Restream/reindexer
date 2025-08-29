#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include "config.h"
#include "loggerwrapper.h"
#include "net/ev/ev.h"
#include "spdlog/sinks/reopen_file_sink.h"

#ifndef _WIN32
#include "pidfile.h"
#endif

namespace reindexer_server {
using namespace reindexer::net;

class HTTPServer;
class RPCServer;
class ClusterManagementServer;
class DBManager;
struct IDBManagerStatsCollector;
struct IRPCServerStatsCollector;

class [[nodiscard]] ServerImpl {
	using SinkMap = std::unordered_map<std::string, std::shared_ptr<spdlog::sinks::reopen_file_sink_st>>;

public:
	ServerImpl(ServerMode mode);
	~ServerImpl();

	Error InitFromCLI(int argc, char* argv[]);
	Error InitFromYAML(const std::string& yaml);
	Error InitFromFile(const char* filepath);
	int Start();
	void Stop() noexcept;
	void EnableHandleSignals(bool enable = true) noexcept { enableHandleSignals_ = enable; }
	DBManager& GetDBManager() noexcept { return *dbMgr_; }
	bool IsReady() const noexcept { return storageLoaded_.load(); }
	bool IsRunning() const noexcept { return running_.load(); }
	void ReopenLogFiles();
	std::string GetCoreLogPath() const;

protected:
	int run();
	Error init();

private:
	Error daemonize();
	Error loggerConfigure();
	void initCoreLogger();

private:
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
	ServerMode mode_ = ServerMode::Builtin;
};
}  // namespace reindexer_server
