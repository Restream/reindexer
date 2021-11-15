#pragma once

#include <cstddef>

namespace reindexer {

class IClientsStats;

struct ReindexerConfig {
	ReindexerConfig& WithClientStats(IClientsStats* cs) noexcept {
		clientsStats = cs;
		return *this;
	}
	ReindexerConfig& EnableRaftCluster(bool val = true) noexcept {
		raftCluster = val;
		return *this;
	}
	ReindexerConfig& WithUpdatesSize(size_t val) noexcept {
		maxReplUpdatesSize = val;
		return *this;
	}

	/// Object for receiving clients statistics
	IClientsStats* clientsStats = nullptr;
	/// If true, allows this DB to receive raft cluster config and operate in cluster
	bool raftCluster = false;
	/// Max pended replication updates size in bytes
	size_t maxReplUpdatesSize = 1024 * 1024 * 1024;
};

}  // namespace reindexer
