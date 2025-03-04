#include "cluster/logger.h"
#include "tools/logger.h"

namespace reindexer {
namespace cluster {

void Logger::print(LogLevel l, std::string& str) const { logPrint(l, &str[0]); }

}  // namespace cluster
}  // namespace reindexer
