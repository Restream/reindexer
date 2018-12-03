#pragma once

namespace reindexer {
namespace client {

struct ReindexerConfig {
	int ConnPoolSize = 4;
	int WorkerThreads = 1;
};

}  // namespace client
}  // namespace reindexer
