#include "dsn.h"
#include "masking.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer {

void DSN::processDSN() {
	masked_ = dsn_;
	parsed_ = httpparser::UrlParser(masked_);
	if (!parsed_.isValid()) {
		throw Error(errParams, "Errors occurred when parsing the URL to mask user credentials");
	}
	std::string_view login = parsed_.username();
	std::string_view password = parsed_.password();

	// Already masked credentials is not masked
	if (!login.empty() && !password.empty() && login.find("...") == std::string::npos) {
		masked_.replace(masked_.find(login), login.length(), maskLogin(login))
			.replace(masked_.find(password), password.length(), maskPassword(password));
	}
}

DSN& DSN::WithDb(std::string&& db) & {
	using namespace std::string_view_literals;
	bool dbEmpty = false;
	if (parsed_.scheme() == "ucproto"sv) {
		std::vector<std::string_view> pathParts;
		std::ignore = reindexer::split(std::string_view(parsed_.path()), ":", true, pathParts);
		dbEmpty = pathParts.size() < 2 || pathParts.back() == "/"sv;
	} else if (parsed_.scheme() == "cproto"sv || parsed_.scheme() == "cprotos"sv) {
		dbEmpty = parsed_.db().empty();
	}

	if (dbEmpty) {
		*this = DSN(dsn_ + std::move(db));
		return *this;
	}

	throw Error(errParams, "Attempt to redefine the db for the DSN");
}

DSN&& DSN::WithDb(std::string&& db) && { return std::move(WithDb(std::move(db))); }

}  // namespace reindexer
