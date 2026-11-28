#pragma once

#include <span>
#include "estl/expected.h"
#include "net/ev/ev.h"
#include "tools/dsn.h"

namespace reindexer::client {
class CoroReindexer;
}  // namespace reindexer::client

namespace reindexer::cluster {

struct ReplThreadConfig;

/// Checks the secondary RAFT cluster before the async replication: servers compatibility, cluster membership,
/// nodes availability, consensus and synchronized leader. Returns index of the leader's DSN
Expected<size_t> ValidateClusterTarget(net::ev::dynamic_loop& loop, const ReplThreadConfig& config, std::span<const DSN> dsns) noexcept;
/// Checks, that the server of the client's current session supports async replication to the RAFT cluster
Error CheckClusterTargetCompatibility(client::CoroReindexer& client, const DSN& dsn) noexcept;

}  // namespace reindexer::cluster
