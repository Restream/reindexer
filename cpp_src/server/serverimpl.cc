#include "server.h"

#if REINDEX_WITH_LIBDL
#include <dlfcn.h>
#endif

#include <vector>

#include "clientsstats.h"
#include "dbmanager.h"
#include "debug/allocdebug.h"
#include "debug/backtrace.h"
#include "httpserver.h"
#include "loggerwrapper.h"
#include "reindexer_version.h"
#include "rpcserver.h"
#include "serverimpl.h"
#include "spdlog/async.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "statscollect/prometheus.h"
#include "statscollect/statscollector.h"
#if REINDEX_WITH_JEMALLOC
#include "tools/alloc_ext/je_malloc_extension.h"
#endif	// REINDEX_WITH_JEMALLOC
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/stringstools.h"
#include "tools/tcmallocheapwathcher.h"
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

Error ServerImpl::InitFromCLI(int argc, char* argv[]) {
	Error err = config_.ParseCmd(argc, argv);
	if (!err.ok()) {
		if ((err.code() == errParseYAML) || (err.code() == errParams)) {
			std::cerr << err.what() << std::endl;
			exit(EXIT_FAILURE);
		} else if (err.code() == errLogic) {
			std::cout << err.what() << std::endl;
			exit(EXIT_SUCCESS);
		}
	}
	return init();
}

Error ServerImpl::InitFromFile(const char* filePath) {
	Error err = config_.ParseFile(filePath);
	if (!err.ok()) {
		if ((err.code() == errParseYAML) || (err.code() == errParams)) {
			std::cerr << err.what() << std::endl;
			exit(EXIT_FAILURE);
		} else if (err.code() == errLogic) {
			std::cout << err.what() << std::endl;
			exit(EXIT_SUCCESS);
		}
	}
	return init();
}

Error ServerImpl::InitFromYAML(const std::string& yaml) {
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

	for (const std::string& dir : dirs) {
		err = TryCreateDirectory(dir);
		if (!err.ok()) {
			return err;
		}
#ifndef _WIN32
		err = ChownDir(dir, config_.UserName);
		if (!err.ok()) {
			return err;
		}
#endif
	}

#ifndef _WIN32
	if (!config_.UserName.empty()) {
		err = ChangeUser(config_.UserName.c_str());
		if (!err.ok()) {
			return err;
		}
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
		auto& args = config_.Args();
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

void ServerImpl::Stop() noexcept {
	if (running_) {
		running_ = false;
		async_.send();
	}
}

void ServerImpl::ReopenLogFiles() {
#ifndef _WIN32
	for (auto& sync : sinks_) {
		sync.second->reopen();
	}
#endif
}

std::string ServerImpl::GetCoreLogPath() const { return GetDirPath(config_.CoreLog); }

#if defined(WITH_GRPC) && defined(REINDEX_WITH_LIBDL)
static void* tryToOpenGRPCLib(bool enabled) noexcept {
#ifdef __APPLE__
	return enabled ? dlopen("libreindexer_grpc_library.dylib", RTLD_NOW) : nullptr;
#else	// __APPLE__
	return enabled ? dlopen("libreindexer_grpc_library.so", RTLD_NOW) : nullptr;
#endif	// __APPLE__
}
#endif	// defined(WITH_GRPC) && defined(REINDEX_WITH_LIBDL)

int ServerImpl::run() {
#if defined(WITH_GRPC) && defined(REINDEX_WITH_LIBDL)
	void* hGRPCServiceLib = tryToOpenGRPCLib(config_.EnableGRPC);
#endif	// defined(WITH_GRPC) && defined(REINDEX_WITH_LIBDL)

	auto err = loggerConfigure();
	(void)err;	// ingore; In case of the multiple builtin servers, we will get errors here

	reindexer::debug::backtrace_set_writer([](std::string_view out) {
		auto logger = spdlog::get("server");
		if (logger) {
			logger->flush();  // Extra flush to avoid backtrace message drop due to logger overflow
			logger->critical("{}", out);
			logger->flush();
		} else {
			std::cerr << out << std::endl;
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
			std::ignore = alloc_ext::mallctl("config.prof", &val, &sz, NULL, 0);
			if (!val) {
				logger_.warn("debug.pprof is enabled, but jemalloc compiled without profiling support. Heap profiling is not possible.");
			} else {
				std::ignore = alloc_ext::mallctl("opt.prof", &val, &sz, NULL, 0);
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
		tcmallocHeapWatchDog.set([&heapWatcher](ev::timer&, int) { heapWatcher.CheckHeapUsagePeriodic(); });

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
	if (config_.HasDefaultHttpWriteTimeout()) {
		logger_.info("HTTP write timeout was not set explicitly. The default value will be used: {0} seconds",
					 config_.HttpWriteTimeout().count());
	}
	const auto clientsStats = config_.EnableConnectionsStats ? std::make_unique<ClientsStats>() : std::unique_ptr<ClientsStats>();
	try {
		dbMgr_ = std::make_unique<DBManager>(config_, clientsStats.get());

		auto status = dbMgr_->Init();
		if (!status.ok()) {
			logger_.error("Error init database manager: {0}", status.what());
			return EXIT_FAILURE;
		}
		storageLoaded_ = true;

		bool withTLS = [this]() {
			const bool opensslAvailable = openssl::LibSSLAvailable() && openssl::LibCryptoAvailable();

			if (!config_.SslCertPath.size() && !config_.SslKeyPath.size()) {
				if (opensslAvailable) {
					logger_.info(
						"OpenSSL is available, but the certificate and the private key have not been passed. Reindexer will be launched "
						"without TLS support");
				}
				return false;
			}

			if (opensslAvailable) {
				std::string content;
				constexpr auto message = "The {0} file was not found on the path '{1}'. Reindexer will be launched without TLS support";
				if (fs::ReadFile(config_.SslCertPath, content) == -1) {
					logger_.error(message, "TLS certificate", config_.SslCertPath);
					return false;
				}
				if (fs::ReadFile(config_.SslKeyPath, content) == -1) {
					logger_.error(message, "private key", config_.SslKeyPath);
					return false;
				}
				return true;
			} else {
				logger_.error("The {} has been passed but {}",
							  config_.SslCertPath.size() ? "certificate file" : "file with the private key",
							  openssl::IsBuiltWithOpenSSL() ? "the OpenSSL library is currently unavailable on the system. For more "
															  "information about the error occurred during "
															  "the OpenSSL library loading see the log above."
															: "Reindexer was built without the support of the TLS library.");
				return false;
			}
		}();

		if (!withTLS) {
			config_.HTTPsAddr = "none";
			config_.RPCsAddr = "none";
		}

		const bool withHTTP = config_.HTTPAddr != "none";
		const bool withRPC = config_.RPCAddr != "none";
		const bool withHTTPs = config_.HTTPsAddr != "none";
		const bool withRPCs = config_.RPCsAddr != "none";
#ifndef _WIN32
		const bool withRPCUnix = !config_.RPCUnixAddr.empty() && config_.RPCUnixAddr != "none";
#else
		if (config_.RPCUnixAddr != "none" && !config_.RPCUnixAddr.empty()) {
			logger_.warn("Unable to startup RPC(Unix) on '{0}' (unix domain socket are not supported on Windows platforms)",
						 config_.RPCUnixAddr);
		}
		constexpr bool withRPCUnix = false;
#endif

		if (!withHTTP && !withRPC && !withHTTPs && !withRPCs && !withRPCUnix) {
			logger_.error("There are no running server instances");
			return EXIT_FAILURE;
		}

		logger_.info(
			"Starting reindexer_server ({0}) on {1}, with db '{2}'", REINDEX_VERSION,
			[&]() {
				using tuple = std::tuple<bool, std::string_view, std::string_view>;
				std::string res;
				for (const auto& [withPort, addr, name] :
					 {tuple{withHTTP, config_.HTTPAddr, "HTTP"}, tuple{withRPC, config_.RPCAddr, "RPC(TCP)"},
					  tuple{withHTTPs, config_.HTTPsAddr, "HTTPs"}, tuple{withRPCs, config_.RPCsAddr, "RPC-TLS(TCP)"},
					  tuple{withRPCUnix, config_.RPCUnixAddr, "RPC(Unix)"}}) {
					if (withPort) {
						res += fmt::format("{}{} {}", res.empty() ? "" : ", ", addr, name);
					}
				}
				return res;
			}(),
			config_.StoragePath);

		std::unique_ptr<Prometheus> prometheus;
		std::unique_ptr<StatsCollector> statsCollector;
		if (config_.EnablePrometheus) {
			prometheus.reset(new Prometheus);
			statsCollector.reset(new StatsCollector(*dbMgr_, prometheus.get(), config_.PrometheusCollectPeriod, logger_));
		}

		LoggerWrapper httpLogger("http");
		LoggerWrapper rpcLogger("rpc");

		std::unique_ptr<HTTPServer> httpServer;
		std::unique_ptr<HTTPServer> httpsServer;
		std::unique_ptr<RPCServer> rpcServerTCP;
		std::unique_ptr<RPCServer> rpcsServerTCP;
		std::unique_ptr<RPCServer> rpcServerUnix;

		if (withHTTP) {
			httpServer = std::make_unique<HTTPServer>(*dbMgr_, httpLogger, config_, prometheus.get(), statsCollector.get());
			try {
				httpServer->Start(config_.HTTPAddr, loop_);
			} catch (std::exception& e) {
				logger_.error("Can't listen HTTP on '{}': {}", config_.HTTPAddr, e.what());
				return EXIT_FAILURE;
			}
		}

		if (withHTTPs) {
			httpsServer = std::make_unique<HTTPServer>(*dbMgr_, httpLogger, config_, prometheus.get(), statsCollector.get());
			try {
				httpsServer->Start(config_.HTTPsAddr, loop_);
			} catch (std::exception& e) {
				logger_.error("Can't listen HTTPs on '{}': {}", config_.HTTPsAddr, e.what());
				return EXIT_FAILURE;
			}
		}

		if (withRPC) {
			rpcServerTCP = std::make_unique<RPCServer>(*dbMgr_, rpcLogger, clientsStats.get(), config_, statsCollector.get());
			try {
				rpcServerTCP->Start(config_.RPCAddr, loop_, RPCSocketT::TCP, config_.RPCThreadingMode);
			} catch (std::exception& e) {
				logger_.error("Can't listen RPC(TCP) on '{}': {}", config_.RPCAddr, e.what());
				return EXIT_FAILURE;
			}
		}

		if (withRPCs) {
			rpcsServerTCP = std::make_unique<RPCServer>(*dbMgr_, rpcLogger, clientsStats.get(), config_, statsCollector.get());
			try {
				rpcsServerTCP->Start(config_.RPCsAddr, loop_, RPCSocketT::TCP, config_.RPCThreadingMode);
			} catch (std::exception& e) {
				logger_.error("Can't listen RPC-TLS(TCP) on '{}': {}", config_.RPCsAddr, e.what());
				return EXIT_FAILURE;
			}
		}

		if (withRPCUnix) {
			rpcServerUnix = std::make_unique<RPCServer>(*dbMgr_, rpcLogger, clientsStats.get(), config_, statsCollector.get());
			try {
				rpcServerUnix->Start(config_.RPCUnixAddr, loop_, RPCSocketT::Unx, config_.RPCUnixThreadingMode);
			} catch (std::exception& e) {
				logger_.error("Can't listen RPC(Unix) on '{}': {}", config_.RPCUnixAddr, e.what());
				return EXIT_FAILURE;
			}
		}

#ifdef WITH_GRPC
		void* hGRPCService = nullptr;
		if (config_.EnableGRPC) {
#if REINDEX_WITH_LIBDL
			if (hGRPCServiceLib) {
				auto start_grpc = reinterpret_cast<p_start_reindexer_grpc>(dlsym(hGRPCServiceLib, "start_reindexer_grpc"));
				hGRPCService = start_grpc(*dbMgr_, config_.TxIdleTimeout, loop_, config_.GRPCAddr);
				logger_.info("Listening gRPC service on {0}", config_.GRPCAddr);
			} else {
				logger_.error("Can't load libreindexer_grpc_library. gRPC will not work: {}", dlerror());
				return EXIT_FAILURE;
			}
#else	// REINDEX_WITH_LIBDL
			hGRPCService = start_reindexer_grpc(*dbMgr_, config_.TxIdleTimeout, loop_, config_.GRPCAddr);
			logger_.info("Listening gRPC service on {0}", config_.GRPCAddr);
#endif	// REINDEX_WITH_LIBDL
		}
#endif	// WITH_GRPC

		auto sigCallback = [&](ev::sig& sig) {
			logger_.info("Signal received. Terminating...");
#ifndef REINDEX_WITH_ASAN
			if (config_.AllowNamespaceLeak && mode_ == ServerMode::Standalone) {
				rxAllowNamespaceLeak = true;
			}
#endif
			running_ = false;
			sig.loop.break_loop();
		};

		if (statsCollector) {
			statsCollector->Start();
		}

		ev::sig sterm, sint, shup;

		if (enableHandleSignals_) {
			sterm.set(loop_);
			sterm.set(sigCallback);
			sterm.start(SIGTERM);
			sint.set(loop_);
			sint.set(sigCallback);
			sint.start(SIGINT);
#ifndef _WIN32
			auto sigHupCallback = [&](ev::sig& sig) {
				(void)sig;
				ReopenLogFiles();
			};
			shup.set(loop_);
			shup.set(sigHupCallback);
			shup.start(SIGHUP);
#endif
		}

		async_.set([](ev::async& a) { a.loop.break_loop(); });
		async_.start();

		running_ = true;
		while (running_) {
			loop_.run();
		}
		logger_.info("Reindexer server terminating...");

		if (statsCollector) {
			statsCollector->Stop();
		}
		logger_.info("Stats collector shutdown completed.");
		dbMgr_->ShutdownClusters();
		logger_.info("ClusterOperation shutdown completed.");

		auto stop = [this](auto& serverPtr, std::string_view addr) {
			return std::async(
				std::launch::async,
				[this](auto serverRW, std::string_view addr) {
					auto& server = serverRW.get();
					if (server) {
						server->Stop();
						logger_.info("{} Server shutdown completed.", addr);
					}
				},
				std::ref(serverPtr), addr);
		};

		using namespace std::string_view_literals;
		for (auto& f : {
				 stop(httpServer, "HTTP"sv),
				 stop(httpsServer, "HTTPs"sv),
				 stop(rpcServerTCP, "RPC Server(TCP)"sv),
				 stop(rpcsServerTCP, "RPCs Server(TCP)"sv),
				 stop(rpcServerUnix, "RPC Server(Unix) "sv),
			 }) {
			f.wait();
		}
#ifdef WITH_GRPC
		if (config_.EnableGRPC) {
#if REINDEX_WITH_LIBDL
			if (hGRPCServiceLib) {
				auto stop_grpc = reinterpret_cast<p_stop_reindexer_grpc>(dlsym(hGRPCServiceLib, "stop_reindexer_grpc"));
				stop_grpc(hGRPCService);
				logger_.info("gRPC Server shutdown completed.");
			}
#else	// REINDEX_WITH_LIBDL
			stop_reindexer_grpc(hGRPCService);
			logger_.info("gRPC Server shutdown completed.");
#endif	// REINDEX_WITH_LIBDL
		}
#endif	// WITH_GRPC
	} catch (const std::exception& err) {
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
			if (!pid_.Open(config_.DaemonPidFile.c_str())) {
				return pid_.Status();
			}
			umask(0);
			setsid();
			if (chdir("/")) {
				return Error(errLogic, "Could not change working directory. Reason: {}", strerror(errno));
			}

			close(STDIN_FILENO);
			close(STDOUT_FILENO);
			close(STDERR_FILENO);
			break;

		// fork error ...
		case -1:
			return Error(errLogic, "Could not fork process. Reason: {}", strerror(errno));

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
		spdlog::init_thread_pool(16384, 1);	 // Using single background thread with st-sinks
		spdlog::flush_every(std::chrono::seconds(2));
		spdlog::set_level(spdlog::level::trace);
		spdlog::set_pattern("%^[%L%d/%m %T.%e %t] %v%$", spdlog::pattern_time_type::utc);
	});

	const std::vector<std::pair<std::string, std::string>> loggers = {
		{"server", config_.ServerLog}, {"core", config_.CoreLog}, {"http", config_.HttpLog}, {"rpc", config_.RpcLog}};

	for (auto& logger : loggers) {
		auto& fileName = logger.second;
		try {
			if (fileName == "stdout" || fileName == "-") {
				using LogFactoryT = spdlog::async_factory_impl<spdlog::async_overflow_policy::discard_new>;
				LogFactoryT::create<spdlog::sinks::stdout_color_sink_st>(logger.first);
			} else if (!fileName.empty() && fileName != "none") {
				auto sink = sinks_.find(fileName);
				if (sink == sinks_.end()) {
					auto sptr = std::make_shared<spdlog::sinks::reopen_file_sink_st>(fileName);
					sink = sinks_.emplace(fileName, std::move(sptr)).first;
				}
				auto lptr = std::make_shared<spdlog::async_logger>(logger.first, sink->second, spdlog::thread_pool(),
																   spdlog::async_overflow_policy::discard_new);
				spdlog::initialize_logger(std::move(lptr));
			}
		} catch (const spdlog::spdlog_ex& e) {
			return Error(errLogic, "Can't create logger for '{}' to file '{}': {}\n", logger.first, logger.second, e.what());
		}
	}
	logger_ = LoggerWrapper("server");
	return {};
}

void ServerImpl::initCoreLogger() {
	std::weak_ptr<spdlog::logger> logger = spdlog::get("core");

	auto callback = [this, logger](int level, char* buf) {
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
		reindexer::logInstallWriter(callback, mode_ == ServerMode::Standalone ? LoggerPolicy::WithoutLocks : LoggerPolicy::WithLocks,
									coreLogLevel_);
	}
}

ServerImpl::~ServerImpl() {
#ifndef REINDEX_WITH_ASAN
	if (config_.AllowNamespaceLeak && mode_ == ServerMode::Standalone) {
		rxAllowNamespaceLeak = true;
	}
#endif
#ifdef _WIN32
	// Windows must to call shutdown explicitly, otherwise it will stuck
	logInstallWriter(nullptr, mode_ == ServerMode::Standalone ? LoggerPolicy::WithoutLocks : LoggerPolicy::WithLocks, int(LogNone));
	spdlog::shutdown();
#else	// !_WIN32
	if (coreLogLevel_) {
		logInstallWriter(nullptr, mode_ == ServerMode::Standalone ? LoggerPolicy::WithoutLocks : LoggerPolicy::WithLocks, int(LogNone));
	}
#endif	// !_WIN32
	async_.reset();
}

}  // namespace reindexer_server
