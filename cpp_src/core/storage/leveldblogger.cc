#include "leveldblogger.h"

#include <leveldb/env.h>
#include <leveldb/options.h>

// Using separate cc-file to be able to compile it with different options.
// Static LevelDB v1.23 is built with -fno-rtti by default and to inherit NoOpLogger from leveldb's logger, this file must be built with
// -fno-rtti to

namespace reindexer {
namespace datastorage {

class [[nodiscard]] NoOpLogger : public leveldb::Logger {
	void Logv(const char* /*format*/, va_list /*ap*/) override final {}
};

static NoOpLogger dummyLevelDBLogger;

void SetDummyLogger(leveldb::Options& options) { options.info_log = &dummyLevelDBLogger; }

}  // namespace datastorage
}  // namespace reindexer
