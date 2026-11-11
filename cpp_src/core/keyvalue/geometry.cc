#include "geometry.h"
#include "tools/errors.h"

namespace reindexer {

[[noreturn]] void Point::throwInfError(std::string_view name) { throw Error(errParams, "Point coordinate '{}' can not be inf", name); }

[[noreturn]] void Point::throwNanError(std::string_view name) { throw Error(errParams, "Point coordinate '{}' can not be nan", name); }

}  // namespace reindexer
