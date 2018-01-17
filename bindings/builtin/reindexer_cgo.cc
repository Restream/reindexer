
#include "reindexer_cgo.h"
#include <string.h>
#include "core/cbinding/reindexer_c.h"
#include "tools/logger.h"

extern "C" {
#include <_cgo_export.h>
}
using namespace reindexer;

void reindexer_enable_go_logger() {
	reindexer_enable_logger([](int level, char *msg) { CGoLogger((GoInt)level, GoString{msg, (GoInt)strlen(msg)}); });
}

void reindexer_disable_go_logger() { reindexer_disable_logger(); }
