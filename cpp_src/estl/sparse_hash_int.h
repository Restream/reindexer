#pragma once

#include <cstddef>
#include <cstdint>

namespace reindexer {

// sparsemap needs special hash for intergers, due to
// performance issue https://github.com/greg7mdp/sparsepp#integer-keys-and-other-hash-function-considerations
template <typename T>
struct [[nodiscard]] hash_int {};

template <>
struct [[nodiscard]] hash_int<int64_t> {
	size_t operator()(int64_t k) const noexcept { return (k ^ 14695981039346656037ULL) * 1099511628211ULL; }
};

template <>
struct [[nodiscard]] hash_int<uint64_t> {
	size_t operator()(uint64_t k) const noexcept { return (k ^ 14695981039346656037ULL) * 1099511628211ULL; }
};

template <>
struct [[nodiscard]] hash_int<int32_t> {
	size_t operator()(int32_t k) const noexcept { return (k ^ 2166136261U) * 16777619UL; }
};

}  // namespace reindexer
