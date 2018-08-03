#include "resources_wrapper.h"

#ifdef LINK_RESOURCES
#include <cmrc/cmrc.hpp>
#endif

namespace reindexer {

#ifdef LINK_RESOURCES

string common_path(const string& path1, const string& path2) {
	const string& shorter = path1.size() < path2.size() ? path1 : path2;
	const string& longer = path1.size() < path2.size() ? path2 : path1;
	auto result = std::mismatch(shorter.begin(), shorter.end(), longer.begin());
	return string(shorter.begin(), result.first);
}

fs::FileStatus web::stat(const std::string& target) {
	auto& table = cmrc::detail::table_instance();

	if (table.find(target) != table.end()) return fs::StatFile;

	for (auto it = table.begin(); it != table.end(); ++it) {
		string p = common_path((*it).first, target);
		if (!p.empty()) return fs::StatDir;
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
