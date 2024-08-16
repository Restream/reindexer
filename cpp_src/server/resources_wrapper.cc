#include "resources_wrapper.h"

#ifdef LINK_RESOURCES
#include <cmrc/cmrc.hpp>
#endif

namespace reindexer_server {

DocumentStatus web::fsStatus(const std::string& target) {
	if (webRoot_.empty()) {
		return DocumentStatus{};
	}
	DocumentStatus status;
	status.fstatus = reindexer::fs::Stat(webRoot_ + target);
	if (status.fstatus == reindexer::fs::StatError) {
		using reindexer::net::http::kGzSuffix;
		status.fstatus = reindexer::fs::Stat(std::string(webRoot_).append(target).append(kGzSuffix));
		if (status.fstatus == reindexer::fs::StatFile) {
			status.isGzip = true;
		}
	}
	return status;
}

DocumentStatus web::stat(const std::string& target) {
	auto fsRes = fsStatus(target);
#ifdef LINK_RESOURCES
	if (fsRes.fstatus == reindexer::fs::StatError) {
		using reindexer::net::http::kGzSuffix;
		auto& table = cmrc::detail::table_instance();

		if (table.find(target) != table.end()) {
			return reindexer::fs::StatFile;
		} else if (table.find(std::string(target).append(kGzSuffix)) != table.end()) {
			return {reindexer::fs::StatFile, true};
		}

		for (auto it = table.begin(); it != table.end(); ++it) {
			if (target.length() < it->first.length() && it->first.find(target) == 0 && it->first[target.length()] == '/') {
				return reindexer::fs::StatDir;
			}
		}
	}
#endif
	return fsRes;
}

int web::file(Context& ctx, HttpStatusCode code, const std::string& target, bool isGzip, bool withCache) {
#ifdef LINK_RESOURCES
	auto fsRes = fsStatus(target);
	if (fsRes.fstatus == reindexer::fs::StatError) {
		using reindexer::net::http::kGzSuffix;

		const auto& table = cmrc::detail::table_instance();
		auto it = table.find(isGzip ? std::string(target).append(kGzSuffix) : target);

		if (it == table.end()) {
			return ctx.String(reindexer::net::http::StatusNotFound, "File not found");
		}

		auto file_entry = cmrc::open(isGzip ? std::string(target).append(kGzSuffix) : target);
		std::string_view slice(file_entry.begin(), std::distance(file_entry.begin(), file_entry.end()));
		return ctx.File(code, target, slice, isGzip, withCache);
	}
#endif
	return ctx.File(code, webRoot_ + target, std::string_view(), isGzip, withCache);
}

}  // namespace reindexer_server
