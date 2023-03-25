#pragma once

#include <memory>
#include <string>
#include "config.h"
#include "tools/errors.h"

namespace reindexer_server {

using reindexer::Error;

class ServerImpl;
class DBManager;

class Server {
public:
	Server(ServerMode mode = ServerMode::Builtin);
	~Server();

	Error InitFromCLI(int argc, char* argv[]);
	Error InitFromYAML(const std::string& yaml);
	Error InitFromFile(const char* filepath);
	int Start();
	void Stop();
	void EnableHandleSignals(bool enable = true);
	DBManager& GetDBManager();
	bool IsReady();
	bool IsRunning();
	void ReopenLogFiles();

protected:
	std::unique_ptr<ServerImpl> impl_;
};

}  // namespace reindexer_server
