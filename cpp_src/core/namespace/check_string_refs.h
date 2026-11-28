#pragma once

#include <memory>
#include <span>
#include "core/payload/payloadvalue.h"
#include "tools/flagguard.h"

namespace reindexer {

class Index;
class PayloadType;

namespace debug {

#if REINDEX_WITH_STRHOLDER_CHECKS
void CheckStringRefs(std::span<const PayloadValue> items, std::span<const std::unique_ptr<Index>> indexes, const PayloadType& pt);
// Nested disable; the counter is decremented when the guard is destroyed.
[[nodiscard]] CounterGuardAIR32 DisableStringRefsChecks() noexcept;
#else	// !REINDEX_WITH_STRHOLDER_CHECKS
inline void CheckStringRefs(std::span<const PayloadValue>, std::span<const std::unique_ptr<Index>>, const PayloadType&) noexcept {}
[[nodiscard]] inline CounterGuardAIR32 DisableStringRefsChecks() noexcept { return {}; }
#endif	// REINDEX_WITH_STRHOLDER_CHECKS

}  // namespace debug
}  // namespace reindexer
