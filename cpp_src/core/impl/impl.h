#pragma once

#include "estl/concepts.h"

namespace reindexer::impl {

template <concepts::IsRef>
class [[nodiscard]] Impl;

template <typename T>
explicit Impl(const T&) -> Impl<const T&>;

template <typename T>
explicit Impl(T&) -> Impl<T&>;

template <typename T>
explicit Impl(T&&) -> Impl<T&&>;

}  // namespace reindexer::impl
