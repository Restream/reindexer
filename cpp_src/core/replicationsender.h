#pragma once

namespace reindexer {

/// Sender of the replicated requests. It is set for the whole client's connection on login
struct [[nodiscard]] ReplicationSender {
	/// Async replication to the cluster target, i.e. to the leader of the RAFT cluster. The sender may be a single node or a cluster
	bool clusterTarget = false;
	/// Server ID of the RAFT leader, replicating to its follower. Identifies the leader for the namespaces, replicated from the primary
	/// cluster: their records keep the primary's LSNs. The other leader's requests are identified by their origin LSN
	int clusterLeaderId = -1;
};

}  // namespace reindexer
