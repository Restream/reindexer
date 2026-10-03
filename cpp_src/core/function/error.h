#pragma once

#include <string_view>
#include "core/type_consts.h"

namespace reindexer::functions::errors {

[[noreturn]] void throwTypeIsNotSupportedError(ErrorCode errCode, int type);
[[noreturn]] void throwExpectsOneArgumentError(ErrorCode errCode, std::string_view name, size_t argsProvided);
[[noreturn]] void throwExpectsOneOrZeroArgumentsError(ErrorCode errCode, std::string_view name, size_t argsProvided);
[[noreturn]] void throwIncorrectFieldsError(ErrorCode errCode, std::string_view name, size_t fieldsProvided);

}  // namespace reindexer::functions::errors
