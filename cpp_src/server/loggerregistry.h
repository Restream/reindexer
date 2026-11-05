#pragma once

#include <optional>
#include <shared_mutex>
#include "config.h"
#include "loggerwrapper.h"
#include "tools/stringstools.h"

namespace spdlog::sinks {

class reopen_file_sink_st;
class sink;

}  // namespace spdlog::sinks

namespace reindexer_server {

enum class [[nodiscard]] LoggerComponent {
	Core,
	Server,
	Http,
	Rpc,
	Grpc,
};

struct [[nodiscard]] LoggerSettings {
	LoggerSettings() = default;
	LoggerSettings(LogLevel l, std::string_view p) : level{l}, path{p} {}
	LogLevel level = LogInfo;
	std::optional<std::string> path;
};

struct [[nodiscard]] ILoggerConfigurator {
	virtual reindexer::Error ApplySettings(LoggerComponent component, const LoggerSettings& settings) = 0;
	virtual LoggerSettings GetSettings(LoggerComponent component) const = 0;
	virtual ~ILoggerConfigurator() = default;
};

class [[nodiscard]] LoggerRegistry : public ILoggerConfigurator {
public:
	reindexer::Error Init(const ServerConfig&, ServerMode);

	reindexer::Error ApplySettings(LoggerComponent component, const LoggerSettings& settings) override;
	LoggerSettings GetSettings(LoggerComponent component) const override;

	LoggerWrapper Core() const;
	LoggerWrapper Http() const;
	LoggerWrapper Rpc() const;
	LoggerWrapper Grpc() const;
	LoggerWrapper Server() const;

	void DisableCoreLog();
	void ReopenFiles();

private:
	reindexer::Error applySettings(LoggerComponent component, const LoggerSettings& settings, bool installCoreWriter);

	struct [[nodiscard]] FileSinkEntry {
		std::shared_ptr<spdlog::sinks::reopen_file_sink_st> sink;
		size_t users = 0;
	};

	std::unordered_map<std::string, FileSinkEntry> fileSinks_;
	std::array<std::shared_ptr<spdlog::sinks::sink>, 5> componentSinks_;

	std::array<LoggerSettings, 5> settings_;
	mutable std::shared_mutex settingsMtx_;

	bool withLocks_ = true;
};

}  // namespace reindexer_server
