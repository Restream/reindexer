#pragma once

#include <vector>
#include "tools/dsn.h"
#include "tools/errors.h"

namespace reindexer {
namespace cluster {

struct [[nodiscard]] RolesSyncNodeStatus {
	DSN dsn;
	size_t namespacesAttempted{0};
	size_t namespacesReset{0};
	Error lastError;
};

struct [[nodiscard]] RolesSyncResult {
	std::vector<RolesSyncNodeStatus> nodes;
};

}  // namespace cluster
}  // namespace reindexer
