#include "errors.h"
#include <stdio.h>

namespace reindexer {

Error::Error(int code, const string &what) : code_(code), what_(what) {}

Error::Error(int code) : code_(code) {}

const string &Error::what() const { return what_; }
int Error::code() const { return code_; }

}  // namespace reindexer
