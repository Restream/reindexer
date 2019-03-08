#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include "config.h"
#include "dbmanager.h"
#include "loggerwrapper.h"
#include "net/ev/ev.h"

#ifndef _WIN32
#include "pidfile.h"
#endif

#include "tools/errors.h"

namespace reindexer_server {
using std::string;
using std::unique_ptr;
using std::atomic_bool;
using std::atomic_flag;
using namespace reindexer::net;

class ServerImpl {
	using SinkMap = std::unordered_map<string, std::shared_ptr<spdlog::sinks::simple_file_sink_mt>>;

public:
	ServerImpl();
	~ServerImpl();

	Error InitFromCLI(int argc, char* argv[]);
	Error InitFromYAML(const string& yaml);
	Error InitFromFile(const char* filepath);
	int Start();
	void Stop();
	void EnableHandleSignals(bool enable = true) { enableHandleSignals_ = enable; }
	DBManager& GetDBManager() { return *dbMgr_; }
	bool IsReady() { return storageLoaded_.load(); }

protected:
	int run();
	Error init();

private:
	Error daemonize();
	Error loggerConfigure();
	void initCoreLogger();

#ifndef _WIN32
	void loggerReopen() {
		for (auto& sync : sinks_) {
			sync.second->reopen();
		}
	}
#endif

private:
	vector<string> args_;
	ServerConfig config_;
	LoggerWrapper logger_;
	LoggerWrapper coreLogger_;
	int coreLogLevel_ = LogNone;

#ifndef _WIN32
	PidFile pid_;
#endif
	std::unique_ptr<DBManager> dbMgr_;
	SinkMap sinks_;

private:
	atomic_bool storageLoaded_;
	atomic_bool running_;
	bool enableHandleSignals_ = false;
	ev::async async_;
	ev::dynamic_loop loop_;

};  // class server
}  // namespace reindexer_server
