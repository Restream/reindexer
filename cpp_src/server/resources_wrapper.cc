#include "resources_wrapper.h"

#ifdef LINK_RESOURCES
#include <cmrc/cmrc.hpp>
#endif

namespace reindexer {

#ifdef LINK_RESOURCES

fs::FileStatus web::stat(const std::string& target) {
	auto& table = cmrc::detail::table_instance();

	if (table.find(target) != table.end()) return fs::StatFile;

	for (auto it = table.begin(); it != table.end(); ++it) {
		if (target.length() < it->first.length() && it->first.find(target) == 0 && it->first[target.length()] == '/') {
			return fs::StatDir;
		}
	}

	return fs::StatError;
}

int web::file(Context& ctx, HttpStatusCode code, const std::string& target) {
	auto it = cmrc::detail::table_instance().find(target);
	auto end = cmrc::detail::table_instance().end();

	if (it == end) {
		return ctx.String(net::http::StatusNotFound, "File not found");
	}

	auto file_entry = cmrc::open(target);
	string_view slice(file_entry.begin(), std::distance(file_entry.begin(), file_entry.end()));
	return ctx.File(code, target.c_str(), slice);
}

#else

fs::FileStatus web::stat(const std::string &target) { return fs::Stat(target); }
int web::file(Context &ctx, HttpStatusCode code, const std::string &target) { return ctx.File(code, target.c_str()); }

#endif

}  // namespace reindexer
