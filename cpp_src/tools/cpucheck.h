#pragma once

namespace reindexer {

// Throws system error if SSE support is required, but CPU does not have it
void CheckRequiredSSESupport();

}  // namespace reindexer
