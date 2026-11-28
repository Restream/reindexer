#pragma once

#include <optional>
#include "core/type_consts.h"

namespace reindexer {

std::optional<CondType> TryInvertCondition(CondType) noexcept;
CondType InvertCondition(CondType);

/// With a single value on the right, values ALLSET scalar is equivalent to scalar IN values.
CondType InvertConditionWithScalarRhs(CondType);

}  // namespace reindexer
