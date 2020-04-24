#pragma once

#include <memory>
#include <string>
#include "tools/errors.h"

namespace reindexer_server {

using reindexer::Error;
using std::string;

class ServerImpl;
class DBManager;

class Server {
public:
	Server();
	~Server();

	Error InitFromCLI(int argc, char* argv[]);
	Error InitFromYAML(const string& yaml);
	Error InitFromFile(const char* filepath);
	int Start();
	void Stop();
	void EnableHandleSignals(bool enable = true);
	DBManager& GetDBManager();
	bool IsReady();
	void ReopenLogFiles();

protected:
	std::unique_ptr<ServerImpl> impl_;
};

}  // namespace reindexer_server
