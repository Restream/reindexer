
#include "reindexer_cgo.h"
#include <string.h>
#include "core/cbinding/reindexer_c.h"

extern "C" {
#include <_cgo_export.h>
}

void reindexer_enable_go_logger() {
	reindexer_enable_logger([](int level, char *msg) { CGoLogger((GoInt)level, GoString{msg, (GoInt)strlen(msg)}); });
}

void reindexer_disable_go_logger() { reindexer_disable_logger(); }
