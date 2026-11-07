#pragma once

#include "core/keyvalue/key_string.h"
#include "core/payload/payloadiface.h"
#include "vendor/sparse-map/sparse_hash.h"

namespace tsl {
namespace sh {

template <>
struct [[nodiscard]] default_hash_prefix_bits<reindexer::key_string> : std::integral_constant<std::size_t, 2> {};

template <>
struct [[nodiscard]] default_hash_prefix_bits<reindexer::PayloadValue> : std::integral_constant<std::size_t, 2> {};

}  // namespace sh
}  // namespace tsl
