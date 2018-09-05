#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include "cbinding/server_c.h"
#include "config.h"
#include "httpserver.h"
#include "loggerwrapper.h"

#ifndef _WIN32
#include "pidfile.h"
#endif

#include "rpcserver.h"
#include "tools/errors.h"

using std::string;
using std::unique_ptr;
using std::atomic_bool;
using std::atomic_flag;
using reindexer::Error;

namespace reindexer_server {

class Server {
	using SinkMap = std::unordered_map<string, std::shared_ptr<spdlog::sinks::simple_file_sink_mt>>;
	friend struct ::server_access;

public:
	static int main(int argc, char* argv[]);
	Server();

	Error InitFromCLI(int argc, char* argv[]);
	Error InitFromYAML(const string& yaml);
	Error InitFromFile(const char* filepath);
	int Start();
	void Stop();
	void EnableHandleSignals(bool enable = true) { enableHandleSignals_ = enable; }

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
	LogLevel logLevel_;

#ifndef _WIN32
	PidFile pid_;
#endif
	unique_ptr<DBManager> dbMgr_;
	SinkMap sinks_;

private:
	atomic_bool storageLoaded_;
	atomic_bool running_;
	bool enableHandleSignals_ = false;
	ev::async async_;
	ev::dynamic_loop loop_;

};  // class server
}  // namespace reindexer_server
