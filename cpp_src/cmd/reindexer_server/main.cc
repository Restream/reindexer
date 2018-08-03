#include <stdio.h>
#include <csignal>
#include <cstdlib>
#include <unordered_map>
#include "args/args.hpp"
#include "core/reindexer.h"
#include "dbmanager.h"
#include "debug/allocdebug.h"
#include "debug/backtrace.h"
#include "httpserver.h"
#include "loggerwrapper.h"
#include "reindexer_version.h"
#include "rpcserver.h"
#include "tools/fsops.h"
#include "tools/stringstools.h"
#include "winservice.h"
#include "yaml/yaml.h"

#ifdef LINK_RESOURCES
#include <cmrc/cmrc.hpp>
#endif

using namespace reindexer_server;
struct ServerConfig {
#ifndef _WIN32
	string StoragePath = "/tmp/reindex";
	string UserName;
	bool Daemonize = false;
	string DaemonPidFile = "/tmp/reindexer.pid";
#else
	string StoragePath = "\\reindexer";
	bool InstallSvc = false, RemoveSvc = false, SvcMode = false;
#endif
	string WebRoot;
	string StorageEngine = "leveldb";
	string HTTPAddr = "0.0.0.0:9088";
	string RPCAddr = "0.0.0.0:6534";
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
LoggerWrapper logger;
LoggerWrapper coreLogger;
LogLevel logLevel = LogNone;

string GetDirPath(const string &path) {
	size_t lastSlashPos = path.find_last_of("/\\");
	return path.substr(0, lastSlashPos + 1);
}

#ifndef _WIN32

#include <pwd.h>
#include <unistd.h>

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
		exit(EXIT_FAILURE);
	}

	if (setgid(pwd.pw_gid) != 0) {
		fprintf(stderr, "Can't change user to %s\n", userName);
		exit(EXIT_FAILURE);
	}
	if (setuid(pwd.pw_uid) != 0) {
		fprintf(stderr, "Can't change user to %s\n", userName);
		exit(EXIT_FAILURE);
	}
}

static int createPidFile() {
	int fd = open(config.DaemonPidFile.c_str(), O_RDWR | O_CREAT, 0600);
	if (fd < 0) {
		fprintf(stderr, "Unable to open PID file %s\n", config.DaemonPidFile.c_str());
		exit(EXIT_FAILURE);
	}

	int res = lockf(fd, F_TLOCK, 0);
	if (res < 0) {
		fprintf(stderr, "Unable to lock PID file %s\n", config.DaemonPidFile.c_str());
		exit(EXIT_FAILURE);
	}

	char str[16];
	pid_t processPid = getpid();
	snprintf(str, sizeof(str), "%d", int(processPid));

	auto sz = write(fd, str, strlen(str));
	(void)sz;

	return fd;
}

static void daemonize() {
	pid_t pid = fork();
	if (pid < 0) {
		fprintf(stderr, "Can't fork the process\n");
		exit(EXIT_FAILURE);
	}

	if (pid > 0) {
		exit(0);
	}

	umask(0);
	setsid();
	if (chdir("/")) {
		fprintf(stderr, "Unable to change working directory\n");
		exit(EXIT_FAILURE);
	};

	close(STDIN_FILENO);
	close(STDOUT_FILENO);
	close(STDERR_FILENO);
}

void ChownDir(const string &path, gid_t gid, uid_t uid) {
	if (!path.empty()) {
		if (chown(path.c_str(), uid, gid) < 0) {
			fprintf(stderr, "Could not change ownership for directory '%s'. Reason: %s\n", path.c_str(), strerror(errno));
			exit(EXIT_FAILURE);
		}
	}
}
#endif

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
#ifndef _WIN32
		config.UserName = root["system"]["user"].As<std::string>(config.UserName);
		config.Daemonize = root["system"]["daemonize"].As<bool>(config.Daemonize);
		config.DaemonPidFile = root["system"]["pidfile"].As<std::string>(config.DaemonPidFile);
#endif
		config.DebugAllocs = root["debug"]["allocs"].As<bool>(config.DebugAllocs);
		config.DebugPprof = root["debug"]["pprof"].As<bool>(config.DebugPprof);
	} catch (const Yaml::Exception &ex) {
		fprintf(stderr, "Error with config file '%s': %s\n", filePath.c_str(), ex.Message());
		exit(EXIT_FAILURE);
	}
}

void parseCmdLine(int argc, char **argv) {
#ifndef LINK_RESOURCES
	config.WebRoot = GetDirPath(argv[0]);
#endif

	args::ArgumentParser parser("reindexer server");
	args::HelpFlag help(parser, "help", "Show this message", {'h', "help"});
	args::Flag securityF(parser, "", "Enable per-user security", {"security"});
	args::ValueFlag<string> configF(parser, "CONFIG", "Path to reindexer config file", {'c', "config"}, args::Options::Single);

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

#ifndef _WIN32
	args::Group unixDaemonGroup(parser, "Unix daemon options");
	args::ValueFlag<string> userF(unixDaemonGroup, "USER", "System user name", {'u', "user"}, config.UserName, args::Options::Single);
	args::Flag daemonizeF(unixDaemonGroup, "", "Run in daemon mode", {'d', "daemonize"});
	args::ValueFlag<string> daemonPidFileF(unixDaemonGroup, "", "Custom daemon pid file", {"pidfile"}, config.DaemonPidFile,
										   args::Options::Single);
#else
	args::Group winSvcGroup(parser, "Windows service options");
	args::Flag installF(winSvcGroup, "", "Install reindexer windows service", {"install"});
	args::Flag removeF(winSvcGroup, "", "Remove reindexer windows service", {"remove"});
	args::Flag serviceF(winSvcGroup, "", "Run in service mode", {"service"});
#endif

	try {
		parser.ParseCLI(argc, argv);
	} catch (const args::Help &) {
		std::cout << parser;
		exit(EXIT_SUCCESS);
	} catch (const args::Error &e) {
		std::cerr << e.what() << std::endl << parser;
		exit(EXIT_FAILURE);
	}

	if (configF) {
		parseConfigFile(args::get(configF));
	}

	if (storageF) config.StoragePath = args::get(storageF);
	if (logLevelF) config.LogLevel = args::get(logLevelF);
	if (httpAddrF) config.HTTPAddr = args::get(httpAddrF);
	if (rpcAddrF) config.RPCAddr = args::get(rpcAddrF);
	if (webRootF) config.WebRoot = args::get(webRootF);
#ifndef _WIN32
	if (userF) config.UserName = args::get(userF);
	if (daemonizeF) config.Daemonize = args::get(daemonizeF);
	if (daemonPidFileF) config.DaemonPidFile = args::get(daemonPidFileF);
#else
	if (installF) config.InstallSvc = args::get(installF);
	if (removeF) config.RemoveSvc = args::get(removeF);
	if (serviceF) config.SvcMode = args::get(serviceF);

#endif
	if (securityF) config.EnableSecurity = args::get(securityF);
	if (serverLogF) config.ServerLog = args::get(serverLogF);
	if (coreLogF) config.CoreLog = args::get(coreLogF);
	if (httpLogF) config.HttpLog = args::get(httpLogF);
	if (rpcLogF) config.RpcLog = args::get(rpcLogF);
}

static std::unordered_map<string, std::shared_ptr<spdlog::sinks::simple_file_sink_mt>> sinks;

static void loggerConfigure() {
	spdlog::drop_all();

	spdlog::set_async_mode(4096, spdlog::async_overflow_policy::block_retry, nullptr, std::chrono::seconds(2));

	vector<pair<string, string>> loggers = {
		{"server", config.ServerLog}, {"core", config.CoreLog}, {"http", config.HttpLog}, {"rpc", config.RpcLog}};

	for (auto &logger : loggers) {
		auto &fileName = logger.second;
		try {
			if (fileName == "stdout" || fileName == "-") {
				spdlog::stdout_color_mt(logger.first);
			} else if (!fileName.empty()) {
				auto sink = sinks.find(fileName);
				if (sink == sinks.end()) {
					sink = sinks.emplace(fileName, std::make_shared<spdlog::sinks::simple_file_sink_mt>(fileName)).first;
				}
				spdlog::create(logger.first, sink->second);
			}
		} catch (const spdlog::spdlog_ex &e) {
			fprintf(stderr, "Can't create logger for '%s' to file '%s': %s\n", logger.first.c_str(), logger.second.c_str(), e.what());
		}
	}
}

#ifndef _WIN32
static void loggerReopen() {
	for (auto &sync : sinks) {
		sync.second->reopen();
	}
}
#endif

int startServer() {
	logLevel = logLevelFromString(config.LogLevel);

	loggerConfigure();

	logger = LoggerWrapper("server");
	coreLogger = LoggerWrapper("core");

	if (config.DebugAllocs) {
		allocdebug_init();
#ifndef REINDEX_WITH_GPERFTOOLS
		logger.warn("debug.allocs is enabled in config, but reindexer complied without gperftools - Can't enable feature.");
#endif
	}

	if (config.DebugPprof) {
#ifndef REINDEX_WITH_GPERFTOOLS
		logger.warn("debug.pprof is enabled in config, but reindexer complied without gperftools - Can't enable feature.");
#else
		if (!std::getenv("HEAPPROFILE") && !std::getenv("TCMALLOC_SAMPLE_PARAMETER")) {
			logger.warn(
				"debug.pprof is enabled, but TCMALLOC_SAMPLE_PARAMETER or HEAPPROFILE environment varables are not set. Heap profiling is "
				"not possible.");
		}

#endif
	}

	reindexer::logInstallWriter(logWrite);

	try {
		ev::dynamic_loop loop;

		DBManager dbMgr(config.StoragePath, !config.EnableSecurity);
		auto status = dbMgr.Init();
		if (!status.ok()) {
			logger.error("Error init database manager: {0}", status.what());
			exit(EXIT_FAILURE);
		}

		logger.info("Starting reindexer_server ({0}) on {1} HTTP, {2} RPC, with db '{3}'", REINDEX_VERSION, config.HTTPAddr, config.RPCAddr,
					config.StoragePath);

		LoggerWrapper httpLogger("http");
		HTTPServer httpServer(dbMgr, config.WebRoot, httpLogger, config.DebugAllocs, config.DebugPprof);
		if (!httpServer.Start(config.HTTPAddr, loop)) {
			logger.error("Can't listen HTTP on '{0}'", config.HTTPAddr);
			exit(EXIT_FAILURE);
		}

		LoggerWrapper rpcLogger("rpc");
		RPCServer rpcServer(dbMgr, rpcLogger, config.DebugAllocs);
		if (!rpcServer.Start(config.RPCAddr, loop)) {
			logger.error("Can't listen RPC on '{0}'", config.RPCAddr);
			exit(EXIT_FAILURE);
		}

		bool terminate = false;
		auto sigCallback = [&](ev::sig &sig) {
			logger.info("Signal received. Terminating...");
			terminate = true;
			sig.loop.break_loop();
		};

		ev::sig sterm, sint, shup;
		sterm.set(loop);
		sterm.set(sigCallback);
		sterm.start(SIGTERM);
		sint.set(loop);
		sint.set(sigCallback);
		sint.start(SIGINT);
#ifndef _WIN32
		auto sigHupCallback = [&](ev::sig &sig) {
			(void)sig;
			loggerReopen();
		};
		shup.set(loop);
		shup.set(sigHupCallback);
		shup.start(SIGHUP);
#endif

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

	spdlog::drop_all();
	sinks.clear();
	return 0;
}

void TryCreateDirectory(const string &dir) {
	using reindexer::fs::MkDirAll;
	using reindexer::fs::DirectoryExists;
	using reindexer::fs::GetTempDir;
	if (!dir.empty()) {
		if (!DirectoryExists(dir) && dir != GetTempDir()) {
			if (MkDirAll(dir) < 0) {
				fprintf(stderr, "Could not create '%s'. Reason: %s\n", config.StoragePath.c_str(), strerror(errno));
				exit(EXIT_FAILURE);
			}
#ifdef _WIN32
		} else if (_access(dir.c_str(), 6) < 0) {
#else
		} else if (access(dir.c_str(), R_OK | W_OK) < 0) {
#endif
			fprintf(stderr, "Could not access dir '%s'. Reason: %s\n", dir.c_str(), strerror(errno));
			exit(EXIT_FAILURE);
		}
	}
}

int main(int argc, char **argv) {
	errno = 0;
	backtrace_init();
	setvbuf(stdout, 0, _IONBF, 0);
	setvbuf(stderr, 0, _IONBF, 0);

#ifdef LINK_RESOURCES
	CMRC_INIT(resources);
#endif

	parseCmdLine(argc, argv);

	string coreLogDir = GetDirPath(config.CoreLog);
	string httpLogDir = GetDirPath(config.HttpLog);
	string rpcLogDir = GetDirPath(config.RpcLog);
	string serverLogDir = GetDirPath(config.ServerLog);

	TryCreateDirectory(config.StoragePath);
	TryCreateDirectory(coreLogDir);
	TryCreateDirectory(httpLogDir);
	TryCreateDirectory(rpcLogDir);
	TryCreateDirectory(serverLogDir);

#ifndef _WIN32
	string pidDir = GetDirPath(config.DaemonPidFile);
	TryCreateDirectory(pidDir);

	uid_t uid = getuid(), targetUID = uid;
	gid_t gid = getgid(), targetGID = gid;

	signal(SIGPIPE, SIG_IGN);

	shared_ptr<passwd> pwdPtr;
	if (!config.UserName.empty()) {
		pwdPtr.reset(getpwnam(config.UserName.c_str()), free);
		if (!pwdPtr) {
			fprintf(stderr, "Could not get `uid` and `gid` for user '%s'. Reason: %s\n", config.UserName.c_str(), strerror(errno));
			exit(EXIT_FAILURE);
		}
		targetUID = pwdPtr->pw_uid;
		targetGID = pwdPtr->pw_gid;
	}

	if (gid != targetGID || uid != targetUID) {
		ChownDir(config.StoragePath, targetGID, targetUID);
		ChownDir(coreLogDir, targetGID, targetUID);
		ChownDir(httpLogDir, targetGID, targetUID);
		ChownDir(rpcLogDir, targetGID, targetUID);
		ChownDir(serverLogDir, targetGID, targetUID);
		ChownDir(pidDir, targetGID, targetUID);
		changeUser(config.UserName.c_str());
	}

	int pidFileFd = -1;
	if (config.Daemonize) {
		daemonize();
		pidFileFd = createPidFile();
	}

	startServer();
	if (config.Daemonize) {
		if (pidFileFd != -1) {
			close(pidFileFd);
			unlink(config.DaemonPidFile.c_str());
		}
	}
#else
	bool running = false;
	reindexer_server::WinService svc("reindexer", "Reindexer server",
									 [&]() {
										 running = true;
										 startServer();
										 running = false;
									 },
									 []() {  //
										 raise(SIGTERM);
									 },
									 [&]() {  //
										 return running;
									 });
	if (config.InstallSvc) {
		char cmdline[4096];
		strcpy(cmdline, argv[0]);
		for (int i = 1; i < argc; ++i) {
			strcat(cmdline, " ");
			if (strcmp(argv[i], "--install"))
				strcat(cmdline, argv[i]);
			else
				strcat(cmdline, "--service");
		}
		svc.Install(cmdline);
	} else if (config.RemoveSvc) {
		svc.Remove();
	} else if (config.SvcMode) {
		svc.Start();
	} else {
		startServer();
	}
#endif
}
