#include "core/reindexerconfig.h"

#include "tools/hardware_concurrency.h"

namespace reindexer {

uint32_t EmbeddingRuntimeConfig::DefaultThreadCount() noexcept { return GetMaxBackgroundThreads(); }

}  // namespace reindexer
