#include "mutex.h"
#include <map>
#include <string_view>

namespace reindexer {

std::string_view DescribeMutexMark(MutexMark mark) {
	static const std::map<MutexMark, std::string_view> descriptions{{MutexMark::DbManager, "Database Manager"},
																	{MutexMark::IndexText, "Fulltext Index"},
																	{MutexMark::Namespace, "Namespace"},
																	{MutexMark::Reindexer, "Database"},
																	{MutexMark::ReindexerStorage, "Database Storage"}};
	return descriptions.at(mark);
}

}  // namespace reindexer
