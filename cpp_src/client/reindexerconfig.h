#pragma once

namespace reindexer {
namespace client {

struct ReindexerConfig {
	ReindexerConfig(int _ConnPoolSize = 4, int _WorkerThreads = 1, int _FetchAmount = 10000)
		: ConnPoolSize(_ConnPoolSize), WorkerThreads(_WorkerThreads), FetchAmount(_FetchAmount){};
	int ConnPoolSize;
	int WorkerThreads;
	int FetchAmount;
};

}  // namespace client
}  // namespace reindexer
