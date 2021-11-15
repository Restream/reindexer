#pragma once

#include <chrono>
#include <string>
#include "connectopts.h"

namespace reindexer {
namespace client {

using std::chrono::seconds;

struct ReindexerConfig {
	ReindexerConfig(int _ConnPoolSize = 4, int _WorkerThreads = 1, int _FetchAmount = 10000, int _ReconnectAttempts = 0,
					seconds _ConnectTimeout = seconds(0), seconds _RequestTimeout = seconds(0), bool _EnableCompression = false,
					std::string _appName = "CPP-client")
		: ConnPoolSize(_ConnPoolSize),
		  WorkerThreads(_WorkerThreads),
		  FetchAmount(_FetchAmount),
		  ReconnectAttempts(_ReconnectAttempts),
		  ConnectTimeout(_ConnectTimeout),
		  RequestTimeout(_RequestTimeout),
		  EnableCompression(_EnableCompression),
		  AppName(std::move(_appName)) {}

	int ConnPoolSize;
	int WorkerThreads;
	int FetchAmount;
	int ReconnectAttempts;
	seconds ConnectTimeout;
	seconds RequestTimeout;
	bool EnableCompression;
	std::string AppName;
};

}  // namespace client
}  // namespace reindexer
