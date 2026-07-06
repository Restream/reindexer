#pragma once

#include <concepts>

#include "core/nsselecter/joins/preselect.h"

namespace reindexer {

template <typename JoinPreSelCtx>
struct [[nodiscard]] SelectAndPreSelectCtx;

using MainSelectCtx = SelectAndPreSelectCtx<void>;
using JoinSelectCtx = SelectAndPreSelectCtx<joins::PreSelectExecuteCtx>;
using JoinPreSelectCtx = SelectAndPreSelectCtx<joins::PreSelectBuildCtx>;

template <typename T>
inline constexpr bool IsMainSelectCtx = std::same_as<std::remove_cvref_t<T>, MainSelectCtx>;

template <typename T>
inline constexpr bool IsJoinSelectCtx = std::same_as<std::remove_cvref_t<T>, JoinSelectCtx>;

template <typename T>
inline constexpr bool IsJoinPreSelectCtx = std::same_as<std::remove_cvref_t<T>, JoinPreSelectCtx>;

}  // namespace reindexer