#include <pwd.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <csignal>
#include <thread>
#include "args/args.hpp"
#include "core/reindexer.h"
#include "dbmanager.h"
#include "debug/allocdebug.h"
#include "debug/backtrace.h"
#include "estl/fast_hash_map.h"
#include "httpserver.h"
#include "loggerwrapper.h"
#include "rpcserver.h"
#include "spdlog/spdlog.h"
#include "time/fast_time.h"
#include "tools/fsops.h"
#include "yaml/yaml.h"

using namespace reindexer_server;
struct ServerConfig {
	string StoragePath = "/tmp/reindex";
	string WebRoot;
	string StorageEngine = "leveldb";
	string HTTPAddr = "0:9088";
	string RPCAddr = "0:6534";
	string UserName;
	bool EnableSecurity = false;
	string LogLevel = "info";
	string ServerLog = "stdout";
	string CoreLog = "stdout";
	string HttpLog = "stdout";
	string RpcLog = "stdout";
	bool DebugPprof = false;
	bool DebugAllocs = false;
};

ServerConfig config;
LoggerWrapper coreLogger;
LogLevel logLevel = LogNone;

#define STR_EXPAND(tok) #tok
#define STR(tok) STR_EXPAND(tok)

static void changeUser(const char *userName) {
	struct passwd pwd, *result;
	char buf[0x4000];

	int res = getpwnam_r(userName, &pwd, buf, sizeof(buf), &result);
	if (result == nullptr) {
		if (res == 0)
			fprintf(stderr, "User %s not found\n", userName);
		else {
			errno = res;
			perror("getpwnam_r");
		}
		exit(1);
	}

	if (setgid(pwd.pw_gid) != 0) {
		fprintf(stderr, "Can't change user to %s\n", userName);
		exit(1);
	}
	if (setuid(pwd.pw_uid) != 0) {
		fprintf(stderr, "Can't change user to %s\n", userName);
		exit(1);
	}
}

static void logWrite(int level, char *buf) {
	if (level <= logLevel) {
		switch (level) {
			case LogNone:
				break;
			case LogError:
				coreLogger.error(buf);
				break;
			case LogWarning:
				coreLogger.warn(buf);
				break;
			case LogTrace:
				coreLogger.trace(buf);
				break;
			case LogInfo:
				coreLogger.info(buf);
				break;
			default:
				coreLogger.debug(buf);
				break;
		}
	}
}

void parseConfigFile(const string &filePath) {
	Yaml::Node root;

	try {
		Yaml::Parse(root, filePath.c_str());
		config.StoragePath = root["storage"]["path"].As<std::string>(config.StoragePath);
		config.LogLevel = root["logger"]["loglevel"].As<std::string>(config.LogLevel);
		config.ServerLog = root["logger"]["serverlog"].As<std::string>(config.ServerLog);
		config.CoreLog = root["logger"]["corelog"].As<std::string>(config.CoreLog);
		config.HttpLog = root["logger"]["httplog"].As<std::string>(config.HttpLog);
		config.RpcLog = root["logger"]["rpclog"].As<std::string>(config.RpcLog);
		config.HTTPAddr = root["net"]["httpaddr"].As<std::string>(config.HTTPAddr);
		config.RPCAddr = root["net"]["rpcaddr"].As<std::string>(config.RPCAddr);
		config.WebRoot = root["net"]["webroot"].As<std::string>(config.WebRoot);
		config.EnableSecurity = root["net"]["security"].As<bool>(config.EnableSecurity);
		config.UserName = root["system"]["user"].As<std::string>(config.UserName);
		config.DebugAllocs = root["debug"]["allocs"].As<bool>(config.DebugAllocs);
		config.DebugPprof = root["debug"]["allocs"].As<bool>(config.DebugPprof);
	} catch (Yaml::Exception ex) {
		fprintf(stderr, "Error with config file '%s': %s\n", filePath.c_str(), ex.Message());
		exit(-1);
	}
}

void parseCmdLine(int argc, char **argv) {
	string execFile = string(argv[0]);
	size_t lastSlashPos = execFile.find_last_of('/');
	config.WebRoot = execFile.substr(0, lastSlashPos + 1);

	args::ArgumentParser parser("reindexer server");
	args::HelpFlag help(parser, "help", "Show this message", {'h', "help"});
	args::ValueFlag<string> userF(parser, "USER", "System user name", {'u', "user"}, config.UserName, args::Options::Single);
	args::Flag securityF(parser, "", "Enable per-user security", {"security"});
	args::ValueFlag<string> configF(parser, "CONFIG", "Path to reidexer config file", {'c', "config"}, args::Options::Single);

	args::Group dbGroup(parser, "Database options");
	args::ValueFlag<string> storageF(dbGroup, "PATH", "path to 'reindexer' storage", {'s', "db"}, config.StoragePath,
									 args::Options::Single);

	args::Group netGroup(parser, "Network options");
	args::ValueFlag<string> httpAddrF(netGroup, "PORT", "http listen host:port", {'p', "httpaddr"}, config.HTTPAddr, args::Options::Single);
	args::ValueFlag<string> rpcAddrF(netGroup, "RPORT", "RPC listen host:port", {'r', "rpcaddr"}, config.RPCAddr, args::Options::Single);
	args::ValueFlag<string> webRootF(netGroup, "PATH", "web root", {'w', "webroot"}, config.WebRoot, args::Options::Single);

	args::Group logGroup(parser, "Logging options");
	args::ValueFlag<string> logLevelF(logGroup, "", "log level (none, warning, error, info, trace)", {'l', "loglevel"}, config.LogLevel,
									  args::Options::Single);
	args::ValueFlag<string> serverLogF(logGroup, "", "Server log file", {"serverlog"}, config.ServerLog, args::Options::Single);
	args::ValueFlag<string> coreLogF(logGroup, "", "Core log file", {"corelog"}, config.CoreLog, args::Options::Single);
	args::ValueFlag<string> httpLogF(logGroup, "", "Http log file", {"httplog"}, config.HttpLog, args::Options::Single);
	args::ValueFlag<string> rpcLogF(logGroup, "", "Rpc log file", {"rpclog"}, config.RpcLog, args::Options::Single);

	try {
		parser.ParseCLI(argc, argv);
	} catch (args::Help) {
		std::cout << parser;
		exit(0);
	} catch (args::Error &e) {
		std::cerr << e.what() << std::endl << parser;
		exit(1);
	}

	if (configF) {
		parseConfigFile(args::get(configF));
	}

	if (storageF) config.StoragePath = args::get(storageF);
	if (logLevelF) config.LogLevel = args::get(logLevelF);
	if (httpAddrF) config.HTTPAddr = args::get(httpAddrF);
	if (rpcAddrF) config.RPCAddr = args::get(rpcAddrF);
	if (webRootF) config.WebRoot = args::get(webRootF);
	if (userF) config.UserName = args::get(userF);
	if (securityF) config.EnableSecurity = args::get(securityF);
	if (serverLogF) config.ServerLog = args::get(serverLogF);
	if (coreLogF) config.CoreLog = args::get(coreLogF);
	if (httpLogF) config.HttpLog = args::get(httpLogF);
	if (rpcLogF) config.RpcLog = args::get(rpcLogF);
}

static void loggerConfigure() {
	spdlog::drop_all();

	spdlog::set_async_mode(4096);

	vector<pair<string, string>> loggers = {
		{"server", config.ServerLog}, {"core", config.CoreLog}, {"http", config.HttpLog}, {"rpc", config.RpcLog}};

	for (auto &logger : loggers) {
		if (logger.second == "stdout" || logger.second == "-") {
			spdlog::stdout_color_mt(logger.first);
		} else if (!logger.second.empty()) {
			spdlog::basic_logger_mt(logger.first, logger.second);
		}
	}
}

int main(int argc, char **argv) {
	ev::dynamic_loop loop;

	backtrace_init();

	parseCmdLine(argc, argv);

	if (!config.UserName.empty()) {
		changeUser(config.UserName.c_str());
	}
	if (config.DebugAllocs) {
		allocdebug_init();
	}

	setvbuf(stdout, 0, _IONBF, 0);
	setvbuf(stderr, 0, _IONBF, 0);

	fast_hash_map<string, LogLevel> levels = {
		{"none", LogNone}, {"warning", LogWarning}, {"error", LogError}, {"info", LogInfo}, {"trace", LogTrace}};

	auto configLevelIt = levels.find(config.LogLevel);
	if (configLevelIt != levels.end()) {
		logLevel = configLevelIt->second;
	}

	loggerConfigure();

	coreLogger = LoggerWrapper("core");
	LoggerWrapper logger("server");

	reindexer::logInstallWriter(logWrite);

	try {
		DBManager dbMgr(config.StoragePath, !config.EnableSecurity);
		auto status = dbMgr.Init();
		if (!status.ok()) {
			logger.error("Error init database manager: {0}", status.what());
			exit(1);
		}

		logger.info("Starting reindexer_server ({0}) on {1} HTTP, {2} RPC, with db '{3}'", STR(REINDEX_VERSION), config.HTTPAddr,
					config.RPCAddr, config.StoragePath);

		LoggerWrapper httpLogger("http");
		HTTPServer httpServer(dbMgr, config.WebRoot, httpLogger, config.DebugAllocs);
		if (!httpServer.Start(config.HTTPAddr, loop)) {
			logger.error("Can't listen HTTP on '{0}'", config.HTTPAddr);
			exit(-1);
		}

		LoggerWrapper rpcLogger("rpc");
		RPCServer rpcServer(dbMgr, rpcLogger, config.DebugAllocs);
		if (!rpcServer.Start(config.RPCAddr, loop)) {
			logger.error("Can't listen RPC on '{0}'", config.RPCAddr);
			exit(-1);
		}

		bool terminate = false;
		auto sigCallback = [&](ev::sig &sig) {
			logger.info("Signal received. Terminating...");
			terminate = true;
			sig.loop.break_loop();
		};
		auto sigHupCallback = [&](ev::sig &sig) {
			(void)sig;
			loggerConfigure();
		};

		ev::sig sterm, sint, shup;
		sterm.set(loop);
		sterm.set(sigCallback);
		sterm.start(SIGTERM);
		sint.set(loop);
		sint.set(sigCallback);
		sint.start(SIGINT);
		shup.set(loop);
		shup.set(sigHupCallback);
		shup.start(SIGHUP);

		while (!terminate) {
			loop.run();
		}

		logger.info("Reindexer server terminating...");

		rpcServer.Stop();
		httpServer.Stop();
	} catch (const Error &err) {
		logger.error("Unhandled exception occuried: {0}", err.what());
	}
	logger.info("Reindexer server shutdown completed.");

	return 0;
}
