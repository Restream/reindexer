#pragma once

#include <chrono>

namespace reindexer {
namespace client {

using std::chrono::seconds;

struct ReindexerConfig {
	ReindexerConfig(int _ConnPoolSize = 4, int _WorkerThreads = 1, int _FetchAmount = 10000, seconds _ConnectTimeout = seconds(0),
					seconds _RequestTimeout = seconds(0))
		: ConnPoolSize(_ConnPoolSize),
		  WorkerThreads(_WorkerThreads),
		  FetchAmount(_FetchAmount),
		  ConnectTimeout(_ConnectTimeout),
		  RequestTimeout(_RequestTimeout) {}

	int ConnPoolSize;
	int WorkerThreads;
	int FetchAmount;
	seconds ConnectTimeout;
	seconds RequestTimeout;
};

}  // namespace client
}  // namespace reindexer
