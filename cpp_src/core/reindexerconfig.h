#pragma once

#include <cstddef>
#include <cstdint>

namespace reindexer {

class IClientsStats;

struct ReindexerConfig {
	ReindexerConfig& WithClientStats(IClientsStats* cs) noexcept {
		clientsStats = cs;
		return *this;
	}
	ReindexerConfig& WithUpdatesSize(size_t val) noexcept {
		maxReplUpdatesSize = val;
		return *this;
	}
	ReindexerConfig& WithAllocatorCacheLimits(int64_t cacheLimit, float maxCachePart) noexcept {
		allocatorCacheLimit = cacheLimit;
		allocatorCachePart = maxCachePart;
		return *this;
	}

	/// Object for receiving clients statistics
	IClientsStats* clientsStats = nullptr;
	/// Max pended replication updates size in bytes
	size_t maxReplUpdatesSize = 1024 * 1024 * 1024;
	/// Recommended maximum free cache size of tcmalloc memory allocator in bytes
	int64_t allocatorCacheLimit = -1;
	/// Recommended maximum free cache size of tcmalloc memory allocator in relation to total reindexer allocated memory size, in units
	float allocatorCachePart = -1.0;
};

}  // namespace reindexer
