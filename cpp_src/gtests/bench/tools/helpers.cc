#include "helpers.h"

#include <cmath>
#include "core/reindexer.h"
#include "tools/fsops.h"

std::string HumanReadableNumber(size_t number, bool si, const std::string& unitLabel) {
	const std::string siPrefix = "kMGTPE";
	const std::string prefix = "KMGTPE";

	size_t unit = si ? 1000 : 1024;
	if (number < unit) {
		return std::to_string(number) + " " + unitLabel;
	}
	int exp = static_cast<int>(std::log(number) / std::log(unit));
	std::string pre;

	if (si) {
		pre = siPrefix[exp - 1];
	} else {
		pre = prefix[exp - 1];
		pre += "i";
	}

	double result = number / std::pow(unit, exp);
	std::string format = "%.2f %s" + unitLabel;

	size_t size = snprintf(nullptr, 0, format.c_str(), result, pre.c_str()) + 1;
	std::unique_ptr<char[]> buf(new char[size]);
	snprintf(buf.get(), size, format.c_str(), result, pre.c_str());
	return std::string(buf.get(), buf.get() + size - 1);
}

std::string FormatString(const char* msg, va_list args) {
	// we might need a second shot at this, so pre-emptivly make a copy
	va_list args_cp;
	va_copy(args_cp, args);

	std::size_t size = 256;
	char local_buff[256];
	auto ret = vsnprintf(local_buff, size, msg, args_cp);  // NOLINT(*valist.Uninitialized) False positive

	va_end(args_cp);

	if (ret == 0) {	 // handle empty expansion
		return {};
	} else if (static_cast<size_t>(ret) < size) {
		return local_buff;
	} else {
		// we did not provide a long enough buffer on our first attempt.
		size = static_cast<size_t>(ret) + 1;  // + 1 for the null byte
		std::unique_ptr<char[]> buff(new char[size]);
		ret = vsnprintf(buff.get(), size, msg, args);
		(void)ret;
		return buff.get();
	}
}

std::string FormatString(const char* msg, ...) {
	va_list args;
	va_start(args, msg);
	auto tmp = FormatString(msg, args);
	va_end(args);
	return tmp;
}

std::shared_ptr<reindexer::Reindexer> InitBenchDB(std::string_view dbDir) {
	const auto storagePath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex", dbDir);
	if (reindexer::fs::RmDirAll(storagePath) < 0 && errno != ENOENT) {
		std::cerr << "Could not clean working dir '" << storagePath << "'.";
		std::cerr << "Reason: " << strerror(errno) << std::endl;

		throw reindexer::Error(errForbidden, "Could not clean working dir '{}'.", storagePath);
	}

	auto DB = std::make_shared<reindexer::Reindexer>();
	auto err = DB->Connect("builtin://" + storagePath);
	if (!err.ok()) {
		throw err;
	}
	return DB;
}
