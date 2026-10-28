#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace reindexer {

class IClientsStats;
class IExternalEventsListener;

/// Runtime configuration of the embedding module. An out of range value is reported by the operation that passes it and is not replaced by
/// a default.
struct [[nodiscard]] EmbeddingRuntimeConfig {
	/// Number of logical processors, capped at 16 like the background thread pool. Out of line so that this header stays free of <thread>.
	static uint32_t DefaultThreadCount() noexcept;

	/// Threads the module may use inside one operator, 0..INT32_MAX, where 0 means DefaultThreadCount(). Process-wide: the value of the
	/// operation that started the module is the one in force, and a later one only produces a warning.
	uint32_t intraOpNumThreads = DefaultThreadCount();
	/// Encode calls running at the same time on one model, 1..INT32_MAX.
	uint32_t maxConcurrency = DefaultThreadCount();
	/// Upper limit for the Content-Length of one model container, in bytes, 1..UINT64_MAX.
	uint64_t maxModelSizeBytes = 10ULL * 1024 * 1024 * 1024;
	/// Advisory free memory check before a model is loaded, and the ratio of available memory to container size it requires. The ratio is
	/// 1.0..16.0 and must be finite.
	bool memoryPreflightEnabled = false;
	double memoryPreflightMultiplier = 2.0;
};

// NOLINTBEGIN(performance-unnecessary-value-param) // TODO: Temprorary comment to avoid false-positive on warning on moved params

struct [[nodiscard]] ReindexerConfig {
	ReindexerConfig& WithClientStats(IClientsStats* cs) noexcept {
		clientsStats = cs;
		return *this;
	}
	// NOLINTNEXTLINE(performance-unnecessary-value-param)
	ReindexerConfig& WithDBName(std::string _dbName) noexcept {
		dbName = std::move(_dbName);
		return *this;
	}
	ReindexerConfig& WithUpdatesSize(size_t val) noexcept {
		if (val) {
			maxReplUpdatesSize = val;
		}
		return *this;
	}
	ReindexerConfig& WithAllocatorCacheLimits(int64_t cacheLimit, float maxCachePart) noexcept {
		allocatorCacheLimit = cacheLimit;
		allocatorCachePart = maxCachePart;
		return *this;
	}
	ReindexerConfig& WithBackgroundThreads(size_t val) noexcept {
		backgroundThreads = val;
		return *this;
	}
	// NOLINTNEXTLINE(performance-unnecessary-value-param)
	ReindexerConfig& WithEmbeddingRuntimeConfig(EmbeddingRuntimeConfig val) noexcept {
		embedding = val;
		return *this;
	}

	/// Object for receiving clients statistics
	IClientsStats* clientsStats = nullptr;
	/// DB name to register in the events manager
	std::string dbName;
	/// Max pending replication updates size in bytes
	size_t maxReplUpdatesSize = 1024 * 1024 * 1024;
	/// Recommended maximum free cache size of tcmalloc memory allocator in bytes
	int64_t allocatorCacheLimit = -1;
	/// Recommended maximum free cache size of tcmalloc memory allocator in relation to total reindexer allocated memory size, in units
	float allocatorCachePart = -1.0;
	/// Number of threads in the process-wide background thread pool (fulltext and other indexes).
	/// 0 means the thread count is chosen automatically based on the number of CPU cores.
	/// The value of the first created Reindexer instance is used.
	size_t backgroundThreads = 0;
	/// Runtime configuration of the embedding module.
	/// Its thread count follows the same rule as backgroundThreads: the value of the first created Reindexer instance is used.
	EmbeddingRuntimeConfig embedding;
};

// NOLINTEND(performance-unnecessary-value-param) // Temprorary comment to avoid false-positive on warning on moved params

}  // namespace reindexer
