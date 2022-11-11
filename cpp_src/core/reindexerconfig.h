#pragma once

#include <cstddef>

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

	/// Object for receiving clients statistics
	IClientsStats* clientsStats = nullptr;
	/// Max pended replication updates size in bytes
	size_t maxReplUpdatesSize = 1024 * 1024 * 1024;
};

}  // namespace reindexer
