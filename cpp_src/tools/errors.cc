#include "errors.h"

namespace reindexer {

const Error::WhatPtr Error::defaultErrorText_{make_intrusive<Error::WhatT>("Error text generation failed")};

std::ostream& operator<<(std::ostream& os, const Error& error) {
	return os << "{ code: " << error.code() << "; what: \"" << error.what() << "\"}";
}

}  // namespace reindexer
