#include "resources_wrapper.h"

#ifdef LINK_RESOURCES
#include <cmrc/cmrc.hpp>
#endif

namespace reindexer {

DocumentStatus web::fsStatus(const std::string& target) {
	if (webRoot_.empty()) return DocumentStatus{};
	DocumentStatus status;
	status.fstatus = fs::Stat(webRoot_ + target);
	if (status.fstatus == fs::StatError) {
		status.fstatus = fs::Stat(webRoot_ + target + net::http::kGzSuffix);
		if (status.fstatus == fs::StatFile) {
			status.isGzip = true;
		}
	}
	return status;
}

#ifdef LINK_RESOURCES
DocumentStatus web::stat(const std::string& target) {
	auto& table = cmrc::detail::table_instance();

	if (table.find(target) != table.end()) {
		return fs::StatFile;
	} else if (table.find(target + net::http::kGzSuffix) != table.end()) {
		return {fs::StatFile, true};
	}

	for (auto it = table.begin(); it != table.end(); ++it) {
		if (target.length() < it->first.length() && it->first.find(target) == 0 && it->first[target.length()] == '/') {
			return fs::StatDir;
		}
	}
	return fsStatus(target);
}

int web::file(Context& ctx, HttpStatusCode code, const std::string& target, bool isGzip) {
	const auto& table = cmrc::detail::table_instance();
	auto it = table.find(isGzip ? target + net::http::kGzSuffix : target);

	if (it == table.end()) {
		return webRoot_.empty() ? ctx.String(net::http::StatusNotFound, "File not found")
								: ctx.File(code, webRoot_ + target, string_view(), isGzip);
	}

	auto file_entry = cmrc::open(isGzip ? target + net::http::kGzSuffix : target);
	string_view slice(file_entry.begin(), std::distance(file_entry.begin(), file_entry.end()));
	return ctx.File(code, target, slice, isGzip);
}

#else

DocumentStatus web::stat(const std::string &target) { return fsStatus(target); }
int web::file(Context &ctx, HttpStatusCode code, const std::string &target, bool isGzip) {
	return ctx.File(code, webRoot_ + target, string_view(), isGzip);
}

#endif

}  // namespace reindexer
