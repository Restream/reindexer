#pragma once

#include "type_consts.h"

inline constexpr int format_as(QueryItemType v) noexcept { return int(v); }
inline constexpr int format_as(IndexType v) noexcept { return int(v); }
inline constexpr int format_as(CondType v) noexcept { return int(v); }
inline constexpr int format_as(FieldModifyMode v) noexcept { return int(v); }
inline constexpr int format_as(AggType v) noexcept { return int(v); }
inline constexpr int format_as(OpType v) noexcept { return int(v); }
inline constexpr int format_as(QueryType v) noexcept { return int(v); }
inline constexpr int format_as(TagType v) noexcept { return int(v); }
inline constexpr int format_as(DataFormat v) noexcept { return int(v); }
inline constexpr int format_as(ErrorCode v) noexcept { return int(v); }
