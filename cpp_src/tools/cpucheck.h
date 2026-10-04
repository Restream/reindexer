#pragma once

namespace reindexer {

// Throws system error if SSE support is required, but CPU does not have it
void CheckRequiredSSESupport();

bool CPUHasAVX() noexcept;
bool CPUHasAVX2() noexcept;
bool CPUHasAVX512() noexcept;

bool IsAVX512Allowed() noexcept;
bool IsAVX2Allowed() noexcept;
bool IsAVXAllowed() noexcept;

}  // namespace reindexer
