#pragma once

#include <chrono>
#include <string>

namespace reindexer {
namespace client {

using std::chrono::milliseconds;

struct [[nodiscard]] ReindexerConfig {
	int FetchAmount = 10000;
	int ReconnectAttempts = 0;
	milliseconds NetTimeout = milliseconds(0);
	bool EnableCompression = false;
	bool RequestDedicatedThread = false;
	std::string AppName = "CPP-client";
	unsigned int SyncRxCoroCount = 10;
	std::string ReplToken;
};

}  // namespace client
}  // namespace reindexer
