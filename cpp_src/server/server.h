#pragma once

#include <memory>
#include <string>
#include "config.h"
#include "tools/errors.h"

namespace reindexer_server {

using reindexer::Error;

class ServerImpl;
class DBManager;

class [[nodiscard]] Server {
public:
	Server(ServerMode mode = ServerMode::Builtin);
	~Server();

	Error InitFromCLI(int argc, char* argv[]);
	Error InitFromYAML(const std::string& yaml);
	Error InitFromFile(const char* filepath);
	int Start();
	void Stop();
	void EnableHandleSignals(bool enable = true) noexcept;
	DBManager& GetDBManager() noexcept;
	bool IsReady() const noexcept;
	bool IsRunning() const noexcept;
	void ReopenLogFiles();
	std::string GetCoreLogPath() const;

protected:
	std::unique_ptr<ServerImpl> impl_;
};

}  // namespace reindexer_server
