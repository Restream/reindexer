#pragma once

#include "core/type_consts.h"

#include <string_view>

namespace reindexer::joins {

CondType InvertJoinCondition(CondType);
std::string_view JoinTypeName(JoinType type) noexcept;

}  // namespace reindexer::joins

template <typename T>
auto& operator<<(T& os, JoinType jt) {
	return os << reindexer::joins::JoinTypeName(jt);
}
