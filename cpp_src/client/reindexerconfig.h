#pragma once

namespace reindexer {
namespace client {

struct ReindexerConfig {
	ReindexerConfig(int _ConnPoolSize = 4, int _WorkerThreads = 1) : ConnPoolSize(_ConnPoolSize), WorkerThreads(_WorkerThreads){};
	int ConnPoolSize;
	int WorkerThreads;
};

}  // namespace client
}  // namespace reindexer
