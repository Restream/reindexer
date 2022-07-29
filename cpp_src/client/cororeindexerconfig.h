#pragma once

#include <chrono>
#include <string>
#include "connectopts.h"

namespace reindexer {
namespace client {

using std::chrono::milliseconds;

struct CoroReindexerConfig {
	explicit CoroReindexerConfig(int _FetchAmount = 10000, int _ReconnectAttempts = 0, milliseconds _NetTimeout = milliseconds(0),
								 bool _EnableCompression = false, std::string _appName = "CPP-client", unsigned int _syncRxCoroCount = 10)
		: FetchAmount(_FetchAmount),
		  ReconnectAttempts(_ReconnectAttempts),
		  NetTimeout(_NetTimeout),
		  EnableCompression(_EnableCompression),
		  AppName(std::move(_appName)),
		  SyncRxCoroCount(_syncRxCoroCount) {}

	int FetchAmount;
	int ReconnectAttempts;
	milliseconds NetTimeout;
	bool EnableCompression;
	std::string AppName;
	unsigned int SyncRxCoroCount;
};

}  // namespace client
}  // namespace reindexer
