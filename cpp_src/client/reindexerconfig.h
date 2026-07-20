#pragma once

#include <chrono>
#include <string>

namespace reindexer {
namespace client {

using std::chrono::milliseconds;

struct [[nodiscard]] ReindexerConfig {
	explicit ReindexerConfig(int _FetchAmount = 10000, int _ReconnectAttempts = 0, milliseconds _NetTimeout = milliseconds(0),
							 bool _EnableCompression = false, bool _RequestDedicatedThread = false, std::string _appName = "CPP-client",
							 unsigned int _syncRxCoroCount = 10, std::string _replToken = {})
		: FetchAmount(_FetchAmount),
		  ReconnectAttempts(_ReconnectAttempts),
		  NetTimeout(_NetTimeout),
		  EnableCompression(_EnableCompression),
		  RequestDedicatedThread(_RequestDedicatedThread),
		  AppName(std::move(_appName)),
		  SyncRxCoroCount(_syncRxCoroCount),
		  ReplToken(std::move(_replToken)) {}

	int FetchAmount;
	int ReconnectAttempts;
	milliseconds NetTimeout;
	bool EnableCompression;
	bool RequestDedicatedThread;
	std::string AppName;
	unsigned int SyncRxCoroCount;
	std::string ReplToken;
};

}  // namespace client
}  // namespace reindexer
