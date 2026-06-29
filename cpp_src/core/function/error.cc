#include "error.h"
#include "tools/errors.h"

namespace reindexer::functions::errors {

void throwTypeIsNotSupportedError(ErrorCode errCode, int type) { throw Error(errCode, "Function type {} is not supported", type); }

void throwExpectsOneArgumentError(ErrorCode errCode, std::string_view name, size_t argsProvided) {
	throw Error(errCode, "'{}' expects only 1 argument, but {} were provided", name, argsProvided);
}

void throwExpectsOneOrZeroArgumentsError(ErrorCode errCode, std::string_view name, size_t argsProvided) {
	throw Error(errCode, "'{}' expects 0 or 1 argument, but {} were provided", name, argsProvided);
}

void throwIncorrectFieldsError(ErrorCode errCode, std::string_view name, size_t fieldsProvided) {
	throw Error(errCode, "'{}' can only be applied to a single field, but {} were provided", name, fieldsProvided);
}

}  // namespace reindexer::functions::errors
