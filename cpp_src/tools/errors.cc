#include "errors.h"

namespace reindexer {

const Error::WhatPtr Error::defaultErrorText_{make_intrusive<Error::WhatT>("Error text generation failed.")};

}  // namespace reindexer
