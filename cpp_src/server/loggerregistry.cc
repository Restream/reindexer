#include "loggerregistry.h"

#include "estl/lock.h"
#include "estl/shared_mutex.h"
#include "estl/spin_lock.h"
#include "spdlog/async.h"
#include "spdlog/pattern_formatter.h"
#include "spdlog/sinks/reopen_file_sink.h"
#include "spdlog/sinks/sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "tools/logger.h"

using namespace reindexer;

namespace reindexer_server {
namespace {

// Runtime log reconfiguration in Reindexer runs concurrently with spdlog async queue.
// Since spdlog::sinks() isn't thread-safe, we have to provide spdlog with our thread-safe sink wrapper.
class [[nodiscard]] ComponentSink final : public spdlog::sinks::sink {
public:
	void update(spdlog::sink_ptr sink) {
		unique_lock lock{m_};
		sink_ = std::move(sink);
	}

	void prepare(spdlog::sinks::sink& sink) const {
		unique_lock lock{m_};
		if (formatter_) {
			sink.set_formatter(formatter_->clone());
		}
	}

	void log(const spdlog::details::log_msg& msg) override {
		shared_lock lock{m_};
		if (sink_ && sink_->should_log(msg.level)) {
			sink_->log(msg);
		}
	}

	void flush() override {
		shared_lock lock{m_};
		if (sink_) {
			sink_->flush();
		}
	}

	void set_pattern(const std::string& pattern) override { set_formatter(std::make_unique<spdlog::pattern_formatter>(pattern)); }

	void set_formatter(std::unique_ptr<spdlog::formatter> sinkFormatter) override {
		unique_lock lock{m_};
		formatter_ = std::move(sinkFormatter);
	}

private:
	mutable read_write_spinlock m_;
	spdlog::sink_ptr sink_;
	std::unique_ptr<spdlog::formatter> formatter_;
};

const char* kLoggerComponentCore = "core";
const char* kLoggerComponentServer = "server";
const char* kLoggerComponentHttp = "http";
const char* kLoggerComponentRpc = "rpc";
const char* kLoggerComponentGrpc = "grpc";

void installCoreLogWriter(const std::shared_ptr<spdlog::logger>& logger, LogLevel activeLevel, bool withLocks) {
	const auto policy{withLocks ? reindexer::LoggerPolicy::WithLocks : reindexer::LoggerPolicy::WithoutLocks};
	if (logger) {
		// NOLINTNEXTLINE(rx-perf-lambda-to-std-function-allocation)
		reindexer::logInstallWriter(
			[logger](int msgLevel, char* buf) {
				switch (msgLevel) {
					case LogNone:
						break;
					case LogError:
						logger->error(buf);
						break;
					case LogWarning:
						logger->warn(buf);
						break;
					case LogTrace:
						logger->trace(buf);
						break;
					case LogInfo:
						logger->info(buf);
						break;
					default:
						logger->debug(buf);
						break;
				}
			},
			policy, activeLevel);
	} else {
		reindexer::logInstallWriter(nullptr, policy, int(LogNone));
	}
}

const char* loggerName(LoggerComponent component) noexcept {
	switch (component) {
		case LoggerComponent::Server:
			return kLoggerComponentServer;
		case LoggerComponent::Core:
			return kLoggerComponentCore;
		case LoggerComponent::Http:
			return kLoggerComponentHttp;
		case LoggerComponent::Rpc:
			return kLoggerComponentRpc;
		case LoggerComponent::Grpc:
			return kLoggerComponentGrpc;
	}
	std::abort();
}

size_t loggerIndex(LoggerComponent component) noexcept { return static_cast<size_t>(component); }
}  // namespace

reindexer::Error LoggerRegistry::Init(const ServerConfig& config, ServerMode mode) {
	static std::once_flag spdlogInit;
	std::call_once(spdlogInit, [] {
		spdlog::init_thread_pool(16384, 1);	 // Using single background thread with st-sinks
		spdlog::flush_every(std::chrono::seconds(2));
		spdlog::set_level(spdlog::level::trace);
		spdlog::set_pattern("%^[%L%d/%m %T.%e %t] %v%$", spdlog::pattern_time_type::utc);
	});

	auto componentLogLevel = [&config](std::string_view level) {
		return reindexer::logLevelFromString(level.empty() ? config.LogLevel : level);
	};

	withLocks_ = (mode != ServerMode::Standalone);

	struct [[nodiscard]] LoggerSetup {
		LoggerComponent component;
		LoggerSettings settings;
	};
	const std::array<LoggerSetup, 5> loggers = {
		LoggerSetup{LoggerComponent::Core, LoggerSettings{componentLogLevel(config.CoreLogLevel), config.CoreLog}},
		LoggerSetup{LoggerComponent::Server, LoggerSettings{componentLogLevel(config.ServerLogLevel), config.ServerLog}},
		LoggerSetup{LoggerComponent::Http, LoggerSettings{componentLogLevel(config.HttpLogLevel), config.HttpLog}},
		LoggerSetup{LoggerComponent::Rpc, LoggerSettings{componentLogLevel(config.RpcLogLevel), config.RpcLog}},
		LoggerSetup{LoggerComponent::Grpc, LoggerSettings{componentLogLevel(config.GrpcLogLevel), config.GrpcLog}},
	};

	for (const auto& logger : loggers) {
		auto err{applySettings(logger.component, logger.settings, logger.component == LoggerComponent::Core)};
		if (!err.ok()) {
			return err;
		}
	}
	return {};
}

reindexer::Error LoggerRegistry::ApplySettings(LoggerComponent component, const LoggerSettings& settings) {
	return applySettings(component, settings, false);
}

LoggerSettings LoggerRegistry::GetSettings(LoggerComponent component) const {
	const size_t settingsIndex{loggerIndex(component)};
	std::shared_lock lck{settingsMtx_};
	assertrx(settingsIndex < settings_.size());
	return settings_[settingsIndex];
}

void LoggerRegistry::ReopenFiles() {
#ifndef _WIN32
	std::shared_lock lck{settingsMtx_};
	for (auto& sync : fileSinks_) {
		sync.second.sink->reopen();
	}
#endif
}

reindexer::Error LoggerRegistry::applySettings(LoggerComponent component, const LoggerSettings& settings, bool installCoreWriter) {
	auto toSpdlogLevel = [](LogLevel level) noexcept {
		switch (level) {
			case LogNone:
				return spdlog::level::off;
			case LogError:
				return spdlog::level::err;
			case LogWarning:
				return spdlog::level::warn;
			case LogInfo:
				return spdlog::level::info;
			case LogTrace:
				return spdlog::level::trace;
		}
		return spdlog::level::off;
	};

	const auto componentName{loggerName(component)};
	const size_t componentIndex{loggerIndex(component)};

	try {
		std::unique_lock lck{settingsMtx_};

		auto logger{spdlog::get(componentName)};
		auto componentSink{std::static_pointer_cast<ComponentSink>(componentSinks_[componentIndex])};
		const auto oldPath{settings_[componentIndex].path.value_or(std::string{})};
		const auto path{settings.path.value_or(oldPath)};
		const bool isLoggerDisabled{path.empty() || path == "none"};
		const LogLevel logLevel{isLoggerDisabled ? LogNone : settings.level};

		if (!componentSink) {
			componentSink = std::make_shared<ComponentSink>();
			componentSinks_[componentIndex] = componentSink;
		}

		if (!logger) {
			logger = std::make_shared<spdlog::async_logger>(componentName, spdlog::sinks_init_list{componentSink}, spdlog::thread_pool(),
															spdlog::async_overflow_policy::discard_new);
			spdlog::initialize_logger(logger);
		}

		spdlog::sink_ptr physicalSink;
		if (!isLoggerDisabled) {
			if (path == "stdout" || path == "-") {
				auto stdoutSink{std::make_shared<spdlog::sinks::stdout_color_sink_st>()};
				componentSink->prepare(*stdoutSink);
				physicalSink = std::move(stdoutSink);
			} else {
				auto sinkIt{fileSinks_.find(path)};
				if (sinkIt == fileSinks_.end()) {
					auto sptr{std::make_shared<spdlog::sinks::reopen_file_sink_st>(path)};
					componentSink->prepare(*sptr);
					sinkIt = fileSinks_.emplace(path, FileSinkEntry{std::move(sptr)}).first;
				}
				if (path != oldPath) {
					++sinkIt->second.users;
				}
				physicalSink = sinkIt->second.sink;
			}
		}

		componentSink->update(std::move(physicalSink));
		logger->set_level(toSpdlogLevel(logLevel));

		if (path != oldPath) {
			auto itPreviousSink{fileSinks_.find(oldPath)};
			if (itPreviousSink != fileSinks_.end()) {
				if (--itPreviousSink->second.users == 0) {
					fileSinks_.erase(itPreviousSink);
				}
			}
		}
		settings_[componentIndex].level = settings.level;
		settings_[componentIndex].path = path;
		if (component == LoggerComponent::Core) {
			if (installCoreWriter) {
				installCoreLogWriter(logger, logLevel, withLocks_);
			} else {
				reindexer::logSetLevel(logLevel);
			}
		}
	} catch (const spdlog::spdlog_ex& e) {
		return Error(errLogic, "Can't create logger for '{}': {}\n", componentName, e.what());
	}

	return {};
}

LoggerWrapper LoggerRegistry::Core() const { return LoggerWrapper(kLoggerComponentCore); }
LoggerWrapper LoggerRegistry::Http() const { return LoggerWrapper(kLoggerComponentHttp); }
LoggerWrapper LoggerRegistry::Rpc() const { return LoggerWrapper(kLoggerComponentRpc); }
LoggerWrapper LoggerRegistry::Grpc() const { return LoggerWrapper(kLoggerComponentGrpc); }
LoggerWrapper LoggerRegistry::Server() const { return LoggerWrapper(kLoggerComponentServer); }

void LoggerRegistry::DisableCoreLog() { installCoreLogWriter({}, LogNone, withLocks_); }

}  // namespace reindexer_server
