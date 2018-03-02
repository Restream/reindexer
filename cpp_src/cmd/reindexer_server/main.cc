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
#include "httpserver.h"
#include "rpcserver.h"
#include "time/fast_time.h"
#include "tools/fsops.h"
#include "tools/logger.h"
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
	int LogLevel = 3;
	bool EnableRPCLog = false;
	bool EnableHTTPLog = false;
	bool DebugPprof = false;
	bool DebugAllocs = false;
};

ServerConfig config;

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
	if (level <= config.LogLevel) {
		std::tm tm;
		std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		fast_gmtime_r(&t, &tm);
		char lc = 'I';
		switch (level) {
			case LogError:
				lc = 'E';
				break;
			case LogWarning:
				lc = 'W';
				break;
			case LogTrace:
			case LogInfo:
			default:
				lc = 'I';
				break;
		}

		printf("%c:[%02d.%02d.%04d %02d:%02d:%02d] %s\n", lc, tm.tm_mday, tm.tm_mon + 1, tm.tm_year + 1900, tm.tm_hour, tm.tm_min,
			   tm.tm_sec, buf);
	}
}

void parseConfigFile(const string &filePath) {
	Yaml::Node root;

	try {
		Yaml::Parse(root, filePath.c_str());
		config.StoragePath = root["storage"]["path"].As<std::string>(config.StoragePath);
		config.LogLevel = root["logger"]["loglevel"].As<int>(config.LogLevel);
		config.EnableRPCLog = root["logger"]["rpclog"].As<bool>(config.EnableRPCLog);
		config.EnableHTTPLog = root["logger"]["httplog"].As<bool>(config.EnableHTTPLog);
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
	args::ValueFlag<int> logLevelF(logGroup, "INT", "log level (MAX = 5)", {'l', "loglevel"}, config.LogLevel, args::Options::Single);
	args::Flag httpLogF(logGroup, "", "Enable log HTTP requests", {"httplog"});
	args::Flag rpcLogF(logGroup, "", "Enable log RPC requests", {"rpclog"});

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
	if (rpcLogF) config.EnableRPCLog = args::get(rpcLogF);
	if (httpLogF) config.EnableHTTPLog = args::get(httpLogF);
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

	reindexer::logInstallWriter(logWrite);

	try {
		DBManager dbMgr(config.StoragePath, !config.EnableSecurity);
		auto status = dbMgr.Init();
		if (!status.ok()) {
			fprintf(stderr, "Error init database manager: %s\n", status.what().c_str());
			exit(1);
		}

		printf("Starting reindexer_server (%s) on %s HTTP, %s RPC, with db '%s'\n", STR(REINDEX_VERSION), config.HTTPAddr.c_str(),
			   config.RPCAddr.c_str(), config.StoragePath.c_str());

		HTTPServer httpServer(dbMgr, config.WebRoot, config.EnableHTTPLog);
		if (!httpServer.Start(config.HTTPAddr, loop)) {
			fprintf(stderr, "Can't listen HTTP on '%s'\n", config.HTTPAddr.c_str());
			exit(-1);
		}

		RPCServer rpcServer(dbMgr, config.EnableRPCLog);
		if (!rpcServer.Start(config.RPCAddr, loop)) {
			fprintf(stderr, "Can't listen RPC on '%s'\n", config.RPCAddr.c_str());
			exit(-1);
		}

		bool terminate = false;
		auto sigCallback = [&terminate](ev::sig &sig) {
			printf("Signal received. Terminating...\n");
			terminate = true;
			sig.loop.break_loop();
		};

		ev::sig sterm, sint;
		sterm.set(loop);
		sterm.set(sigCallback);
		sterm.start(SIGTERM);
		sint.set(loop);
		sint.set(sigCallback);
		sint.start(SIGINT);

		while (!terminate) {
			loop.run();
		}

		printf("Reindexer server terminating...\n");
		rpcServer.Stop();
		httpServer.Stop();
	} catch (const Error &err) {
		fprintf(stderr, "Unhandled exception occuried: %s", err.what().c_str());
	}
	if (config.DebugAllocs) {
		allocdebug_show();
	}
	printf("Reindexer server shutdown completed.\n");
	return 0;
}
