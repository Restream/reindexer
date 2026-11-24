#pragma once

#include <optional>
#include "core/type_consts.h"

namespace reindexer {

std::optional<CondType> TryInvertCondition(CondType) noexcept;
CondType InvertCondition(CondType);

}  // namespace reindexer
