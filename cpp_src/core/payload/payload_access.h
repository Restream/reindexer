#pragma once

#include <cstddef>
#include <cstdint>

#include "core/payload/payloadfieldvalue.h"
#include "estl/defines.h"
#include "tools/unaligned.h"

namespace reindexer::payload_access {

using ArrayMeta = PayloadFieldValue::Array;

RX_ALWAYS_INLINE ArrayMeta readArrayMeta(const uint8_t* payload, size_t metaOffset) noexcept {
	return unaligned::read<ArrayMeta>(payload + metaOffset);
}

RX_ALWAYS_INLINE void writeArrayMeta(uint8_t* payload, size_t metaOffset, const ArrayMeta& arr) noexcept {
	unaligned::write(payload + metaOffset, arr);
}

// Low-level array access over packed payload bytes. Prefer PayloadIface::GetView for typed field reads.
template <typename T>
RX_ALWAYS_INLINE unaligned::view<T> arrayElems(const uint8_t* payload, size_t metaOffset) noexcept {
	const auto arr = readArrayMeta(payload, metaOffset);
	assertrx_dbg(arr.len >= 0);
	return {payload + arr.offset, static_cast<size_t>(arr.len)};
}

}  // namespace reindexer::payload_access
