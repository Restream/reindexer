#include "key_string.h"
#include "tools/errors.h"

namespace reindexer {

void key_string::throwMaxLenOverflow(size_t len) {
	throw Error(errParams, "Key_string length overflow: {} > max key_string length ({})", len, kMaxLen);
}

}  // namespace reindexer
