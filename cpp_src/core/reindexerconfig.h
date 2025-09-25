#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace reindexer {

class IClientsStats;
class IExternalEventsListener;

struct [[nodiscard]] ReindexerConfig {
	ReindexerConfig& WithClientStats(IClientsStats* cs) noexcept {
		clientsStats = cs;
		return *this;
	}
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
};

}  // namespace reindexer
