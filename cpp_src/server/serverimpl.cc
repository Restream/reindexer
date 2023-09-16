#include "server.h"

#if REINDEX_WITH_LIBDL
#include <dlfcn.h>
#endif

#include <vector>

#include "args/args.hpp"
#include "clientsstats.h"
#include "dbmanager.h"
#include "debug/allocdebug.h"
#include "debug/backtrace.h"
#include "httpserver.h"
#include "loggerwrapper.h"
#include "reindexer_version.h"
#include "rpcserver.h"
#include "serverimpl.h"
#include "statscollect/prometheus.h"
#include "statscollect/statscollector.h"
#include "tools/alloc_ext/je_malloc_extension.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/fsops.h"
#include "tools/stringstools.h"
#include "tools/tcmallocheapwathcher.h"
#include "yaml-cpp/yaml.h"
#ifdef _WIN32
#include "winservice.h"
#endif
#ifdef LINK_RESOURCES
#include <cmrc/cmrc.hpp>
static void init_resources() { CMRC_INIT(reindexer_server_resources); }
#else
static void init_resources() {}
#endif

#if defined(WITH_GRPC)
#include "grpc/grpcexport.h"
#endif

namespace reindexer {
extern std::atomic<bool> rxAllowNamespaceLeak;
}  // namespace reindexer

namespace reindexer_server {

using reindexer::fs::GetDirPath;
using reindexer::logLevelFromString;

ServerImpl::ServerImpl(ServerMode mode)
	:
#ifdef REINDEX_WITH_GPERFTOOLS
	  config_(alloc_ext::TCMallocIsAvailable()),
#else
	  config_(false),
#endif
	  coreLogLevel_(LogNone),
	  storageLoaded_(false),
	  running_(false),
	  mode_(mode) {
	async_.set(loop_);
}

Error ServerImpl::InitFromCLI(int argc, char *argv[]) {
	Error err = config_.ParseCmd(argc, argv);
	if (!err.ok()) {
		if (err.code() == errParams) {
			std::cerr << err.what() << std::endl;
			exit(EXIT_FAILURE);
		} else if (err.code() == errLogic) {
			std::cout << err.what() << std::endl;
			exit(EXIT_SUCCESS);
		}
	}
	return init();
}

Error ServerImpl::InitFromFile(const char *filePath) {
	Error err = config_.ParseFile(filePath);
	if (!err.ok()) {
		if (err.code() == errParams) {
			std::cerr << err.what() << std::endl;
			exit(EXIT_FAILURE);
		} else if (err.code() == errLogic) {
			std::cout << err.what() << std::endl;
			exit(EXIT_SUCCESS);
		}
	}
	return init();
}

Error ServerImpl::InitFromYAML(const std::string &yaml) {
	Error err = config_.ParseYaml(yaml);
	if (!err.ok()) {
		if (err.code() == errParams) {
			std::cerr << err.what() << std::endl;
			exit(EXIT_FAILURE);
		} else if (err.code() == errLogic) {
			std::cout << err.what() << std::endl;
			exit(EXIT_SUCCESS);
		}
	}
	return init();
}

Error ServerImpl::init() {
	using reindexer::fs::TryCreateDirectory;
	using reindexer::fs::ChownDir;
	using reindexer::fs::ChangeUser;
	Error err;

	init_resources();

	std::vector<std::string> dirs = {
#ifndef _WIN32
		GetDirPath(config_.DaemonPidFile),
#endif
		GetDirPath(config_.CoreLog),	   GetDirPath(config_.HttpLog), GetDirPath(config_.RpcLog),
		GetDirPath(config_.ServerLog),	   config_.StoragePath};

	for (const std::string &dir : dirs) {
		err = TryCreateDirectory(dir);
		if (!err.ok()) return err;
#ifndef _WIN32
		err = ChownDir(dir, config_.UserName);
		if (!err.ok()) return err;
#endif
	}

#ifndef _WIN32
	if (!config_.UserName.empty()) {
		err = ChangeUser(config_.UserName.c_str());
		if (!err.ok()) return err;
	}
	signal(SIGPIPE, SIG_IGN);
#endif

	coreLogLevel_ = logLevelFromString(config_.LogLevel);
	return {};
}

int ServerImpl::Start() {
#ifndef _WIN32
	if (config_.Daemonize) {
		Error err = daemonize();
		if (!err.ok()) {
			std::cerr << err.what() << std::endl;
			return EXIT_FAILURE;
		}
	}
#else
	using reindexer::iequals;
	bool running = false;
	reindexer_server::WinService svc(
		"reindexer", "Reindexer server",
		[&]() {
			running = true;
			run();
			running = false;
		},
		[]() {	//
			raise(SIGTERM);
		},
		[&]() {	 //
			return running;
		});

	if (config_.InstallSvc) {
		auto &args = config_.Args();
		std::string cmdline = args.front();
		for (size_t i = 1; i < args.size(); i++) {
			cmdline += " ";
			if (!iequals(args[i], "--install")) {
				cmdline += args[i];
			} else {
				cmdline += "--service";
			}
		}
		return svc.Install(cmdline.c_str()) ? EXIT_SUCCESS : EXIT_FAILURE;
	} else if (config_.RemoveSvc) {
		return svc.Remove() ? EXIT_SUCCESS : EXIT_FAILURE;
	} else if (config_.SvcMode) {
		return svc.Start();
	}
#endif

	return run();
}

void ServerImpl::Stop() {
	if (running_) {
		running_ = false;
		async_.send();
	}
}

void ServerImpl::ReopenLogFiles() {
#ifndef _WIN32
	for (auto &sync : sinks_) {
		sync.second->reopen();
	}
#endif
}

int ServerImpl::run() {
	loggerConfigure();

	reindexer::debug::backtrace_set_writer([](std::string_view out) {
		auto logger = spdlog::get("server");
		if (logger) {
			logger->flush();  // Extra flush to avoid backtrace message drop due to logger overflow
			logger->info("{}", out);
			logger->flush();
		} else {
			std::cerr << std::endl << out;
		}
	});
	if (running_) {
		logger_.warn("attempting to start server, but already started.");
		return -1;
	}

	if (config_.DebugAllocs) {
#ifdef __APPLE__
		// tcmalloc + osx is crashing with thread_local storage access from malloc hooks
		allocdebug_init_mt();
#else
		allocdebug_init();
#endif

#if !REINDEX_WITH_GPERFTOOLS
		logger_.warn("debug.allocs is enabled in config, but reindexer complied without gperftools - Can't enable feature.");
#endif
	}

	if (config_.DebugPprof) {
#if REINDEX_WITH_GPERFTOOLS
		if (!std::getenv("HEAPPROFILE") && !std::getenv("TCMALLOC_SAMPLE_PARAMETER")) {
			logger_.warn(
				"debug.pprof is enabled, but TCMALLOC_SAMPLE_PARAMETER or HEAPPROFILE environment varables are not set. Heap profiling is "
				"not possible.");
		}
#elif REINDEX_WITH_JEMALLOC
		if (alloc_ext::JEMallocIsAvailable()) {
			size_t val = 0, sz = sizeof(size_t);
			alloc_ext::mallctl("config.prof", &val, &sz, NULL, 0);
			if (!val) {
				logger_.warn("debug.pprof is enabled, but jemalloc compiled without profiling support. Heap profiling is not possible.");
			} else {
				alloc_ext::mallctl("opt.prof", &val, &sz, NULL, 0);
				if (!val) {
					logger_.warn(
						"debug.pprof is enabled, but jemmalloc profiler is off. Heap profiling is not possible. export "
						"MALLOC_CONF=\"prof:true\" "
						"to enable it");
				}
			}
		} else {
			logger_.warn("debug.pprof is enabled in config, but reindexer can't link jemalloc library");
		}
#else
		logger_.warn("debug.pprof is enabled in config, but reindexer complied without gperftools or jemalloc - Can't enable feature.");
#endif
	}

#if REINDEX_WITH_GPERFTOOLS
	ev::periodic tcmallocHeapWatchDog;
	TCMallocHeapWathcher heapWatcher;
	if (alloc_ext::TCMallocIsAvailable()) {
		heapWatcher =
			TCMallocHeapWathcher(alloc_ext::instance(), config_.AllocatorCacheLimit, config_.AllocatorCachePart, spdlog::get("server"));
		tcmallocHeapWatchDog.set(loop_);
		tcmallocHeapWatchDog.set([&heapWatcher](ev::timer &, int) { heapWatcher.CheckHeapUsagePeriodic(); });

		if (config_.AllocatorCacheLimit > 0 || config_.AllocatorCachePart > 0) {
			using fpSeconds = std::chrono::duration<double, std::chrono::seconds::period>;
			tcmallocHeapWatchDog.start(fpSeconds(10).count(), fpSeconds(std::chrono::milliseconds(100)).count());
			logger_.info(
				"TCMalloc heap wathcher started. (AllocatorCacheLimit: {0},\n AllocatorCachePart: {1},\n"
				"HeapInspectionPeriod(sec): {2},\nHeapChunkReleaseInterval(sec): {3})",
				config_.AllocatorCacheLimit, config_.AllocatorCachePart, fpSeconds(10).count(),
				fpSeconds(std::chrono::milliseconds(100)).count());
		}
	}
#endif

	initCoreLogger();
	logger_.info("Initializing databases...");
	std::unique_ptr<ClientsStats> clientsStats;
	if (config_.EnableConnectionsStats) clientsStats.reset(new ClientsStats());
	try {
		dbMgr_.reset(new DBManager(config_.StoragePath, !config_.EnableSecurity, clientsStats.get()));

		auto status = dbMgr_->Init(config_.StorageEngine, config_.StartWithErrors, config_.Autorepair);
		if (!status.ok()) {
			logger_.error("Error init database manager: {0}", status.what());
			return EXIT_FAILURE;
		}
		storageLoaded_ = true;

		logger_.info("Starting reindexer_server ({0}) on {1} HTTP, {2} RPC, with db '{3}'", REINDEX_VERSION, config_.HTTPAddr,
					 config_.RPCAddr, config_.StoragePath);

		std::unique_ptr<Prometheus> prometheus;
		std::unique_ptr<StatsCollector> statsCollector;
		if (config_.EnablePrometheus) {
			prometheus.reset(new Prometheus);
			statsCollector.reset(new StatsCollector(*dbMgr_, prometheus.get(), config_.PrometheusCollectPeriod, logger_));
		}

		LoggerWrapper httpLogger("http");
		HTTPServer httpServer(*dbMgr_, httpLogger, config_, prometheus.get(), statsCollector.get());
		if (!httpServer.Start(config_.HTTPAddr, loop_)) {
			logger_.error("Can't listen HTTP on '{0}'", config_.HTTPAddr);
			return EXIT_FAILURE;
		}

		LoggerWrapper rpcLogger("rpc");
		std::unique_ptr<RPCServer> rpcServer =
			std::make_unique<RPCServer>(*dbMgr_, rpcLogger, clientsStats.get(), config_, statsCollector.get());
		if (!rpcServer->Start(config_.RPCAddr, loop_)) {
			logger_.error("Can't listen RPC on '{0}'", config_.RPCAddr);
			return EXIT_FAILURE;
		}
#if defined(WITH_GRPC)
		void *hGRPCService = nullptr;
#if REINDEX_WITH_LIBDL
#ifdef __APPLE__
		auto hGRPCServiceLib = dlopen("libreindexer_grpc_library.dylib", RTLD_NOW);
#else
		auto hGRPCServiceLib = dlopen("libreindexer_grpc_library.so", RTLD_NOW);
#endif
		if (hGRPCServiceLib && config_.EnableGRPC) {
			auto start_grpc = reinterpret_cast<p_start_reindexer_grpc>(dlsym(hGRPCServiceLib, "start_reindexer_grpc"));

			hGRPCService = start_grpc(*dbMgr_, config_.TxIdleTimeout, loop_, config_.GRPCAddr);
			logger_.info("Listening gRPC service on {0}", config_.GRPCAddr);
		} else if (config_.EnableGRPC) {
			logger_.error("Can't load libreindexer_grpc_library. gRPC will not work: {}", dlerror());
			return EXIT_FAILURE;
		}
#else
		if (config_.EnableGRPC) {
			hGRPCService = start_reindexer_grpc(*dbMgr_, config_.TxIdleTimeout, loop_, config_.GRPCAddr);
			logger_.info("Listening gRPC service on {0}", config_.GRPCAddr);
		}

#endif
#endif
		auto sigCallback = [&](ev::sig &sig) {
			logger_.info("Signal received. Terminating...");
#ifndef REINDEX_WITH_ASAN
			if (config_.AllowNamespaceLeak && mode_ == ServerMode::Standalone) {
				rxAllowNamespaceLeak = true;
			}
#endif
			running_ = false;
			sig.loop.break_loop();
		};

		if (statsCollector) statsCollector->Start();

		ev::sig sterm, sint, shup;

		if (enableHandleSignals_) {
			sterm.set(loop_);
			sterm.set(sigCallback);
			sterm.start(SIGTERM);
			sint.set(loop_);
			sint.set(sigCallback);
			sint.start(SIGINT);
#ifndef _WIN32
			auto sigHupCallback = [&](ev::sig &sig) {
				(void)sig;
				ReopenLogFiles();
			};
			shup.set(loop_);
			shup.set(sigHupCallback);
			shup.start(SIGHUP);
#endif
		}

		async_.set([](ev::async &a) { a.loop.break_loop(); });
		async_.start();

		running_ = true;
		while (running_) {
			loop_.run();
		}
		logger_.info("Reindexer server terminating...");

		if (statsCollector) statsCollector->Stop();
		logger_.info("Stats collector shutdown completed.");
		rpcServer->Stop();
		logger_.info("RPC Server shutdown completed.");
		httpServer.Stop();
		logger_.info("HTTP Server shutdown completed.");
#if defined(WITH_GRPC)
#if REINDEX_WITH_LIBDL
		if (hGRPCServiceLib && config_.EnableGRPC) {
			auto stop_grpc = reinterpret_cast<p_stop_reindexer_grpc>(dlsym(hGRPCServiceLib, "stop_reindexer_grpc"));
			stop_grpc(hGRPCService);
			logger_.info("gRPC Server shutdown completed.");
		}
#else
		if (config_.EnableGRPC) {
			stop_reindexer_grpc(hGRPCService);
			logger_.info("gRPC Server shutdown completed.");
		}

#endif
#endif
	} catch (const Error &err) {
		logger_.error("Unhandled exception occurred: {0}", err.what());
	}
	logger_.info("Reindexer server shutdown completed.");
	dbMgr_.reset();
	logger_.info("Reindexer databases flush & shutdown completed.");

	logger_ = LoggerWrapper();
	spdlog::drop_all();

	return 0;
}

#ifndef _WIN32
Error ServerImpl::daemonize() {
	pid_t pid = ::fork();
	switch (pid) {
		// child process
		case 0:
			if (!pid_.Open(config_.DaemonPidFile.c_str())) return pid_.Status();
			umask(0);
			setsid();
			if (chdir("/")) {
				return Error(errLogic, "Could not change working directory. Reason: %s", strerror(errno));
			}

			close(STDIN_FILENO);
			close(STDOUT_FILENO);
			close(STDERR_FILENO);
			break;

		// fork error ...
		case -1:
			return Error(errLogic, "Could not fork process. Reason: %s", strerror(errno));

		// parent process
		default:
			exit(EXIT_SUCCESS);
			//	break;
	}
	return {};
}
#endif

Error ServerImpl::loggerConfigure() {
	static std::once_flag loggerConfigured;
	std::call_once(loggerConfigured, [] {
		spdlog::set_async_mode(16384, spdlog::async_overflow_policy::discard_log_msg, nullptr, std::chrono::seconds(2));
		spdlog::set_level(spdlog::level::trace);
		spdlog::set_pattern("[%L%d/%m %T.%e %t] %v");
	});

	std::vector<std::pair<std::string, std::string>> loggers = {
		{"server", config_.ServerLog}, {"core", config_.CoreLog}, {"http", config_.HttpLog}, {"rpc", config_.RpcLog}};

	for (auto &logger : loggers) {
		auto &fileName = logger.second;
		try {
			if (fileName == "stdout" || fileName == "-") {
				spdlog::stdout_color_mt(logger.first);
			} else if (!fileName.empty() && fileName != "none") {
				auto sink = sinks_.find(fileName);
				if (sink == sinks_.end()) {
					sink = sinks_.emplace(fileName, std::make_shared<spdlog::sinks::fast_file_sink>(fileName)).first;
				}
				spdlog::create(logger.first, sink->second);
			}
		} catch (const spdlog::spdlog_ex &e) {
			return Error(errLogic, "Can't create logger for '%s' to file '%s': %s\n", logger.first, logger.second, e.what());
		}
	}
	logger_ = LoggerWrapper("server");
	return {};
}

void ServerImpl::initCoreLogger() {
	std::weak_ptr<spdlog::logger> logger = spdlog::get("core");

	auto callback = [this, logger](int level, char *buf) {
		auto slogger = logger.lock();
		if (slogger && level <= coreLogLevel_) {
			switch (level) {
				case LogNone:
					break;
				case LogError:
					slogger->error(buf);
					break;
				case LogWarning:
					slogger->warn(buf);
					break;
				case LogTrace:
					slogger->trace(buf);
					break;
				case LogInfo:
					slogger->info(buf);
					break;
				default:
					slogger->debug(buf);
					break;
			}
		}
	};
	if (coreLogLevel_ && logger.lock()) {
		reindexer::logInstallWriter(callback, mode_ == ServerMode::Standalone ? LoggerPolicy::WithoutLocks : LoggerPolicy::WithLocks);
	}
}

ServerImpl::~ServerImpl() {
#ifndef REINDEX_WITH_ASAN
	if (config_.AllowNamespaceLeak && mode_ == ServerMode::Standalone) {
		rxAllowNamespaceLeak = true;
	}
#endif
	if (coreLogLevel_) {
		logInstallWriter(nullptr, mode_ == ServerMode::Standalone ? LoggerPolicy::WithoutLocks : LoggerPolicy::WithLocks);
	}
	async_.reset();
}

}  // namespace reindexer_server
