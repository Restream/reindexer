#pragma once

#include <bit>
#include <cstdint>

namespace reindexer {
struct [[nodiscard]] PayloadChecksum {
	uint64_t hashV1 = 0;  // Deprecated. TODO: Remove somewhere around v5.18.0. Issue #2417
	uint64_t hashV2 = 0;  // TODO: Rename this to checksum after V1 deletion

	void operator*=(uint64_t v) noexcept {
		hashV1 *= v;
		hashV2 *= v;
	}
	void operator^=(uint64_t v) noexcept {
		hashV1 ^= v;
		hashV2 ^= v;
	}
	PayloadChecksum Rotl(unsigned v) const noexcept { return {std::rotl(hashV1, v), std::rotl(hashV2, v)}; }

	bool operator==(const PayloadChecksum&) const noexcept = default;
};

}  // namespace reindexer
