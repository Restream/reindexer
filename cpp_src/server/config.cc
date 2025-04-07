#include "config.h"

#include "args/args.hpp"
#include "core/storage/storagefactory.h"
#include "reindexer_version.h"
#include "tools/fsops.h"
#include "yaml-cpp/yaml.h"

namespace reindexer_server {

void ServerConfig::Reset() {
	args_.clear();
	WebRoot.clear();
	StorageEngine = "leveldb";
	HTTPAddr = "0.0.0.0:9088";
	RPCAddr = "0.0.0.0:6534";
	HTTPsAddr = "0.0.0.0:9089";
	RPCsAddr = "0.0.0.0:6535";
	RPCUnixAddr = "none";
	GRPCAddr = "0.0.0.0:16534";
	RPCThreadingMode = kSharedThreading;
	RPCUnixThreadingMode = kSharedThreading;
	HttpThreadingMode = kSharedThreading;
	LogLevel = "info";
	ServerLog = "stdout";
	CoreLog = "stdout";
	HttpLog = "stdout";
	RpcLog = "stdout";
	AllowNamespaceLeak = true;
#ifndef _WIN32
	StoragePath = "/tmp/reindex";
	UserName.clear();
	DaemonPidFile = "reindexer.pid";
	Daemonize = false;
#else
	StoragePath = "\\reindexer";
	InstallSvc = false;
	RemoveSvc = false;
	SvcMode = false;
#endif
	StartWithErrors = false;
	EnableSecurity = false;
	DebugPprof = false;
	EnablePrometheus = false;
	PrometheusCollectPeriod = std::chrono::milliseconds(1000);
	DebugAllocs = false;
	EnableConnectionsStats = true;
	TxIdleTimeout = std::chrono::seconds(600);
	RPCQrIdleTimeout = std::chrono::seconds(600);
	HttpReadTimeout = std::chrono::seconds(0);
	httpWriteTimeout_ = kDefaultHttpWriteTimeout;
	MaxUpdatesSize = 1024 * 1024 * 1024;
	EnableGRPC = false;
	MaxHttpReqSize = 8 * 1024 * 1024;
	AllocatorCacheLimit = -1;
	AllocatorCachePart = -1;
}

reindexer::Error ServerConfig::ParseYaml(const std::string& yaml) {
	Error err;
	try {
		YAML::Node root = YAML::Load(yaml);
		err = fromYaml(root);
	} catch (const YAML::Exception& ex) {
		err = Error(errParseYAML, "Error with config string. Reason: '{}'", ex.what());
	}
	return err;
}

Error ServerConfig::ParseFile(const std::string& filePath) {
	Error err;
	try {
		YAML::Node root = YAML::LoadFile(filePath);
		err = fromYaml(root);
	} catch (const YAML::Exception& ex) {
		err = Error(errParseYAML, "Error with config file '{}'. Reason: {}", filePath, ex.what());
	}
	return err;
}

Error ServerConfig::ParseCmd(int argc, char* argv[]) {
	using reindexer::fs::GetDirPath;
#ifndef LINK_RESOURCES
	WebRoot = GetDirPath(argv[0]);
#endif
	args_.assign(argv, argv + argc);

	args::ArgumentParser parser("reindexer server");
	args::HelpFlag help(parser, "help", "Show this message", {'h', "help"});
	args::ActionFlag version(parser, "", "Reindexer version", {'v', "version"},
							 []() { throw Error(errLogic, fmt::format("Reindexer version: {}", REINDEX_VERSION)); });
	args::Flag securityF(parser, "", "Enable per-user security", {"security"});
	args::ValueFlag<std::string> configF(parser, "CONFIG", "Path to reindexer config file", {'c', "config"}, args::Options::Single);
	args::Flag startWithErrorsF(parser, "", "Allow to start reindexer with DB's load erros", {"startwitherrors"});

	args::Group dbGroup(parser, "Database options");
	args::ValueFlag<std::string> storageF(dbGroup, "PATH", "path to 'reindexer' storage", {'s', "db"}, StoragePath, args::Options::Single);
	auto availableStorageTypes = reindexer::datastorage::StorageFactory::getAvailableTypes();
	std::string availabledStorages;
	for (const auto& type : availableStorageTypes) {
		if (!availabledStorages.empty()) {
			availabledStorages.append(", ");
		}
		availabledStorages.append("'" + reindexer::datastorage::StorageTypeToString(type) + "'");
	}
	args::ValueFlag<std::string> storageEngineF(dbGroup, "NAME", "'reindexer' storage engine (" + availabledStorages + ")", {'e', "engine"},
												StorageEngine, args::Options::Single);
	args::Flag autorepairF(dbGroup, "", "Deprecated. Does nothing", {"autorepair"});
	args::Flag disableNamespaceLeakF(dbGroup, "", "Disable namespaces leak on database destruction (may slow down server's termination)",
									 {"disable-ns-leak"});

	args::Group netGroup(parser, "Network options");
	args::ValueFlag<std::string> httpAddrF(netGroup, "PORT", "http listen host:port", {'p', "httpaddr"}, HTTPAddr, args::Options::Single);
	args::ValueFlag<std::string> httpsAddrF(netGroup, "TLS-PORT", "https listen host:port", {"httpsaddr"}, HTTPsAddr,
											args::Options::Single);
	args::ValueFlag<std::string> rpcAddrF(netGroup, "RPORT", "RPC listen host:port", {'r', "rpcaddr"}, RPCAddr, args::Options::Single);
	args::ValueFlag<std::string> rpcsAddrF(netGroup, "RPC-TLS-PORT", "RPC with TLS support listen host:port", {"rpcsaddr"}, RPCsAddr,
										   args::Options::Single);
	args::ValueFlag<std::string> sslCertPathF(netGroup, "SSL Certificate PATH", "path to ssl certificate", {"ssl-cert"}, SslCertPath,
											  args::Options::Single);
	args::ValueFlag<std::string> sslKeyPathF(netGroup, "SSL Private Key PATH", "path to ssl key", {"ssl-key"}, SslKeyPath,
											 args::Options::Single);
#ifndef _WIN32
	args::ValueFlag<std::string> rpcUnixAddrF(netGroup, "URPORT", "RPC listen path (unix domain socket)", {"urpcaddr"}, RPCUnixAddr,
											  args::Options::Single);
#endif	// !_WIN32
	args::Flag enableClusterF(netGroup, "", "***deprecated***. Will be ignored by reindexer", {"enable-cluster"});
	args::ValueFlag<std::string> httpThreadingModeF(netGroup, "HTHREADING", "HTTP connections threading mode: shared or dedicated",
													{"http-threading"}, HttpThreadingMode, args::Options::Single);
	args::ValueFlag<std::string> rpcThreadingModeF(netGroup, "RTHREADING", "RPC connections threading mode: shared or dedicated",
												   {'X', "rpc-threading"}, RPCThreadingMode, args::Options::Single);
#ifndef _WIN32
	args::ValueFlag<std::string> rpcUnixThreadingModeF(netGroup, "URTHREADING",
													   "RPC connections threading mode: shared or dedicated (unix domain socket)",
													   {"urpc-threading"}, RPCUnixThreadingMode, args::Options::Single);
#endif	// _WIN32
	args::ValueFlag<size_t> MaxHttpReqSizeF(
		netGroup, "", "Max HTTP request size in bytes. Default value is 8 MB. 0 is 'unlimited', hovewer, stream mode is not supported",
		{"max-http-req"}, MaxHttpReqSize, args::Options::Single);
#if defined(WITH_GRPC)
	args::ValueFlag<std::string> grpcAddrF(netGroup, "GPORT", "GRPC listen host:port", {'g', "grpcaddr"}, RPCAddr, args::Options::Single);
	args::Flag grpcF(netGroup, "", "Enable gRpc service", {"grpc"});
#endif
	args::ValueFlag<std::string> webRootF(netGroup, "PATH", "web root. This path if set overrides linked-in resources", {'w', "webroot"},
										  WebRoot, args::Options::Single);
	args::ValueFlag<int> httpReadTimeoutF(netGroup, "", "timeout (s) for HTTP read operations (i.e. selects, get meta and others)",
										  {"http-read-timeout"}, HttpReadTimeout.count(), args::Options::Single);
	args::ValueFlag<int> httpWriteTimeoutF(
		netGroup, "", "timeout (s) for HTTP write operations (i.e. update, delete, put meta, add index and others). May not be set to 0",
		{"http-write-timeout"}, kDefaultHttpWriteTimeout.count(), args::Options::Single);
	args::ValueFlag<size_t> maxUpdatesSizeF(netGroup, "", "Maximum cached updates size", {"updatessize"}, MaxUpdatesSize,
											args::Options::Single);
	args::Flag pprofF(netGroup, "", "Enable pprof http handler", {'f', "pprof"});
	args::ValueFlag<int> txIdleTimeoutF(netGroup, "", "http transactions idle timeout (s)", {"tx-idle-timeout"}, TxIdleTimeout.count(),
										args::Options::Single);
	args::ValueFlag<int> rpcQrIdleTimeoutF(netGroup, "",
										   "RPC query results idle timeout (s). Expiration check timer has dynamic period, so this timeout "
										   "may float in range of ~20 seconds. 0 means 'disabled'. Default values is 600 seconds",
										   {"rpc-qr-idle-timeout"}, RPCQrIdleTimeout.count(), args::Options::Single);

	args::Group metricsGroup(parser, "Metrics options");
	args::Flag prometheusF(metricsGroup, "", "Enable prometheus handler", {"prometheus"});
	args::ValueFlag<int> prometheusPeriodF(metricsGroup, "", "Prometheus stats collect period (ms)", {"prometheus-period"},
										   PrometheusCollectPeriod.count(), args::Options::Single);
	args::Flag clientsConnectionsStatF(metricsGroup, "", "Enable client connection statistic", {"clientsstats"});

	args::Group logGroup(parser, "Logging options");
	args::ValueFlag<std::string> logLevelF(logGroup, "", "log level (none, warning, error, info, trace)", {'l', "loglevel"}, LogLevel,
										   args::Options::Single);
	args::ValueFlag<std::string> serverLogF(logGroup, "", "Server log file", {"serverlog"}, ServerLog, args::Options::Single);
	args::ValueFlag<std::string> coreLogF(logGroup, "", "Core log file", {"corelog"}, CoreLog, args::Options::Single);
	args::ValueFlag<std::string> httpLogF(logGroup, "", "Http log file", {"httplog"}, HttpLog, args::Options::Single);
	args::ValueFlag<std::string> rpcLogF(logGroup, "", "Rpc log file", {"rpclog"}, RpcLog, args::Options::Single);
	args::Flag logAllocsF(netGroup, "", "Log operations allocs statistics", {'a', "allocs"});

#ifndef _WIN32
	args::Group unixDaemonGroup(parser, "Unix daemon options");
	args::ValueFlag<std::string> userF(unixDaemonGroup, "USER", "System user name", {'u', "user"}, UserName, args::Options::Single);
	args::Flag daemonizeF(unixDaemonGroup, "", "Run in daemon mode", {'d', "daemonize"});
	args::ValueFlag<std::string> daemonPidFileF(unixDaemonGroup, "", "Custom daemon pid file", {"pidfile"}, DaemonPidFile,
												args::Options::Single);
#else
	args::Group winSvcGroup(parser, "Windows service options");
	args::Flag installF(winSvcGroup, "", "Install reindexer windows service", {"install"});
	args::Flag removeF(winSvcGroup, "", "Remove reindexer windows service", {"remove"});
	args::Flag serviceF(winSvcGroup, "", "Run in service mode", {"service"});
#endif

#if REINDEX_WITH_GPERFTOOLS
	std::string allocatorNote{""};
	if (!tcMallocIsAvailable_) {
		allocatorNote = "\n! NOTE !: Reindexer was compiled with tcmalloc, but tcmalloc shared library is not linked.";
	}
	args::Group systemGroup(parser, "Common system options");
	args::ValueFlag<int64_t> allocatorCacheLimit(
		systemGroup, "", "Recommended maximum free cache size of tcmalloc memory allocator in bytes" + allocatorNote,
		{"allocator-cache-limit"}, AllocatorCacheLimit, args::Options::Single);

	args::ValueFlag<float> allocatorCachePart(
		systemGroup, "",
		"Recommended maximum cache size of tcmalloc memory allocator in relation to total reindexer allocated memory size, in units" +
			allocatorNote,
		{"allocator-cache-part"}, AllocatorCachePart, args::Options::Single);
#endif

	try {
		parser.ParseCLI(argc, argv);
	} catch (const args::Help&) {
		return Error(errLogic, parser.Help());
	} catch (const Error& v) {
		return v;
	} catch (const args::Error& e) {
		return Error(errParams, "{}\n{}", e.what(), parser.Help());
	}

	if (configF) {
		auto err = ParseFile(args::get(configF));
		if (!err.ok()) {
			return err;
		}
	}

	if (storageF) {
		StoragePath = args::get(storageF);
	}

	if (sslCertPathF) {
		SslCertPath = args::get(sslCertPathF);
	}
	if (sslKeyPathF) {
		SslKeyPath = args::get(sslKeyPathF);
	}
	if (storageEngineF) {
		StorageEngine = args::get(storageEngineF);
	}
	if (startWithErrorsF) {
		StartWithErrors = args::get(startWithErrorsF);
	}
	if (disableNamespaceLeakF) {
		AllowNamespaceLeak = !args::get(disableNamespaceLeakF);
	}
	if (logLevelF) {
		LogLevel = args::get(logLevelF);
	}
	if (httpAddrF) {
		HTTPAddr = args::get(httpAddrF);
	}
	if (rpcAddrF) {
		RPCAddr = args::get(rpcAddrF);
	}
	if (httpsAddrF) {
		HTTPsAddr = args::get(httpsAddrF);
	}
	if (rpcsAddrF) {
		RPCsAddr = args::get(rpcsAddrF);
	}

	if (rpcThreadingModeF) {
		RPCThreadingMode = args::get(rpcThreadingModeF);
	}
	if (httpThreadingModeF) {
		HttpThreadingMode = args::get(httpThreadingModeF);
	}
	if (webRootF) {
		WebRoot = args::get(webRootF);
	}
	if (MaxHttpReqSizeF) {
		MaxHttpReqSize = args::get(MaxHttpReqSizeF);
	}
#ifndef _WIN32
	if (rpcUnixAddrF) {
		RPCUnixAddr = args::get(rpcUnixAddrF);
	}
	if (rpcUnixThreadingModeF) {
		RPCUnixThreadingMode = args::get(rpcUnixThreadingModeF);
	}
	if (userF) {
		UserName = args::get(userF);
	}
	if (daemonizeF) {
		Daemonize = args::get(daemonizeF);
	}
	if (daemonPidFileF) {
		DaemonPidFile = args::get(daemonPidFileF);
	}
#else
	if (installF) {
		InstallSvc = args::get(installF);
	}
	if (removeF) {
		RemoveSvc = args::get(removeF);
	}
	if (serviceF) {
		SvcMode = args::get(serviceF);
	}

#endif

#if REINDEX_WITH_GPERFTOOLS
	if (allocatorCacheLimit) {
		AllocatorCacheLimit = args::get(allocatorCacheLimit);
	}
	if (allocatorCachePart) {
		AllocatorCachePart = args::get(allocatorCachePart);
	}
#endif

	if (securityF) {
		EnableSecurity = args::get(securityF);
	}
#if defined(WITH_GRPC)
	if (grpcF) {
		EnableGRPC = args::get(grpcF);
	}
	if (grpcAddrF) {
		GRPCAddr = args::get(grpcAddrF);
	}
#endif
	if (serverLogF) {
		ServerLog = args::get(serverLogF);
	}
	if (coreLogF) {
		CoreLog = args::get(coreLogF);
	}
	if (httpLogF) {
		HttpLog = args::get(httpLogF);
	}
	if (rpcLogF) {
		RpcLog = args::get(rpcLogF);
	}
	if (pprofF) {
		DebugPprof = args::get(pprofF);
	}
	if (prometheusF) {
		EnablePrometheus = args::get(prometheusF);
	}
	if (prometheusPeriodF) {
		PrometheusCollectPeriod = std::chrono::milliseconds(args::get(prometheusPeriodF));
	}
	if (clientsConnectionsStatF) {
		EnableConnectionsStats = args::get(clientsConnectionsStatF);
	}
	if (httpReadTimeoutF) {
		HttpReadTimeout = std::chrono::seconds(args::get(httpReadTimeoutF));
	}
	if (httpWriteTimeoutF) {
		SetHttpWriteTimeout(std::chrono::seconds(args::get(httpWriteTimeoutF)));
	}
	if (logAllocsF) {
		DebugAllocs = args::get(logAllocsF);
	}
	if (txIdleTimeoutF) {
		TxIdleTimeout = std::chrono::seconds(args::get(txIdleTimeoutF));
	}
	if (rpcQrIdleTimeoutF) {
		RPCQrIdleTimeout = std::chrono::seconds(args::get(rpcQrIdleTimeoutF));
	}
	if (maxUpdatesSizeF) {
		MaxUpdatesSize = args::get(maxUpdatesSizeF);
	}

	return {};
}

void ServerConfig::SetHttpWriteTimeout(std::chrono::seconds val) noexcept {
	if (val.count() > 0) {
		httpWriteTimeout_ = val;
		defaultHttpWriteTimeout_ = false;
	} else {
		httpWriteTimeout_ = kDefaultHttpWriteTimeout;
		defaultHttpWriteTimeout_ = true;
	}
}

reindexer::Error ServerConfig::fromYaml(YAML::Node& root) {
	try {
		AllowNamespaceLeak = root["db"]["ns_leak"].as<bool>(AllowNamespaceLeak);
		StoragePath = root["storage"]["path"].as<std::string>(StoragePath);
		StorageEngine = root["storage"]["engine"].as<std::string>(StorageEngine);
		StartWithErrors = root["storage"]["startwitherrors"].as<bool>(StartWithErrors);
		LogLevel = root["logger"]["loglevel"].as<std::string>(LogLevel);
		ServerLog = root["logger"]["serverlog"].as<std::string>(ServerLog);
		CoreLog = root["logger"]["corelog"].as<std::string>(CoreLog);
		HttpLog = root["logger"]["httplog"].as<std::string>(HttpLog);
		RpcLog = root["logger"]["rpclog"].as<std::string>(RpcLog);
		SslCertPath = root["net"]["ssl_cert"].as<std::string>(SslCertPath);
		SslKeyPath = root["net"]["ssl_key"].as<std::string>(SslKeyPath);
		HTTPAddr = root["net"]["httpaddr"].as<std::string>(HTTPAddr);
		HTTPsAddr = root["net"]["httpsaddr"].as<std::string>(HTTPsAddr);
		RPCAddr = root["net"]["rpcaddr"].as<std::string>(RPCAddr);
		RPCsAddr = root["net"]["rpcsaddr"].as<std::string>(RPCsAddr);
		RPCThreadingMode = root["net"]["rpc_threading"].as<std::string>(RPCThreadingMode);
		HttpThreadingMode = root["net"]["http_threading"].as<std::string>(HttpThreadingMode);
		WebRoot = root["net"]["webroot"].as<std::string>(WebRoot);
		MaxUpdatesSize = root["net"]["maxupdatessize"].as<size_t>(MaxUpdatesSize);
		EnableSecurity = root["net"]["security"].as<bool>(EnableSecurity);
		EnableGRPC = root["net"]["grpc"].as<bool>(EnableGRPC);
		GRPCAddr = root["net"]["grpcaddr"].as<std::string>(GRPCAddr);
		TxIdleTimeout = std::chrono::seconds(root["net"]["tx_idle_timeout"].as<int>(TxIdleTimeout.count()));
		HttpReadTimeout = std::chrono::seconds(root["net"]["http_read_timeout"].as<int>(HttpReadTimeout.count()));
		RPCQrIdleTimeout = std::chrono::seconds(root["net"]["rpc_qr_idle_timeout"].as<int>(RPCQrIdleTimeout.count()));
		const auto httpWriteTimeout = root["net"]["http_write_timeout"].as<int>(-1);
		SetHttpWriteTimeout(std::chrono::seconds(httpWriteTimeout));
		MaxHttpReqSize = root["net"]["max_http_body_size"].as<std::size_t>(MaxHttpReqSize);
		EnablePrometheus = root["metrics"]["prometheus"].as<bool>(EnablePrometheus);
		PrometheusCollectPeriod = std::chrono::milliseconds(root["metrics"]["collect_period"].as<int>(PrometheusCollectPeriod.count()));
		EnableConnectionsStats = root["metrics"]["clientsstats"].as<bool>(EnableConnectionsStats);
#ifndef _WIN32
		RPCUnixAddr = root["net"]["urpcaddr"].as<std::string>(RPCUnixAddr);
		RPCUnixThreadingMode = root["net"]["urpc_threading"].as<std::string>(RPCUnixThreadingMode);
		UserName = root["system"]["user"].as<std::string>(UserName);
		Daemonize = root["system"]["daemonize"].as<bool>(Daemonize);
		DaemonPidFile = root["system"]["pidfile"].as<std::string>(DaemonPidFile);
#endif
		AllocatorCacheLimit = root["system"]["allocator_cache_limit"].as<int64_t>(AllocatorCacheLimit);
		AllocatorCachePart = root["system"]["allocator_cache_part"].as<float_t>(AllocatorCachePart);

		DebugAllocs = root["debug"]["allocs"].as<bool>(DebugAllocs);
		DebugPprof = root["debug"]["pprof"].as<bool>(DebugPprof);
	} catch (const YAML::Exception& ex) {
		return Error(errParseYAML, "Unable to parse YML server config: {}", ex.what());
	}
	return {};
}

}  // namespace reindexer_server
