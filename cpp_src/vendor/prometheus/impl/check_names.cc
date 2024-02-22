
#include "prometheus/check_names.h"

#if defined(__GLIBCXX__) && __GLIBCXX__ <= 20150623
#define STD_REGEX_IS_BROKEN
#endif
#if defined(__GNUC__) && (__GNUC__ == 12) && (__GNUC_MINOR__ == 2) && defined(REINDEX_WITH_ASAN)
// regex header is broken in GCC 12.2 with ASAN
#define STD_REGEX_IS_BROKEN
#endif
#if defined(_MSC_VER) && _MSC_VER < 1900
#define STD_REGEX_IS_BROKEN
#endif

#ifndef STD_REGEX_IS_BROKEN
#include <regex>
#endif

namespace prometheus {
bool CheckMetricName(const std::string& name) {
	// see https://prometheus.io/docs/concepts/data_model/
	auto reserved_for_internal_purposes = name.compare(0, 2, "__") == 0;
	if (reserved_for_internal_purposes) return false;
#ifdef STD_REGEX_IS_BROKEN
	return !name.empty();
#else
	static const std::regex metric_name_regex("[a-zA-Z_:][a-zA-Z0-9_:]*");
	return std::regex_match(name, metric_name_regex);
#endif
}

bool CheckLabelName(const std::string& name) {
	// see https://prometheus.io/docs/concepts/data_model/
	auto reserved_for_internal_purposes = name.compare(0, 2, "__") == 0;
	if (reserved_for_internal_purposes) return false;
#ifdef STD_REGEX_IS_BROKEN
	return !name.empty();
#else
	static const std::regex label_name_regex("[a-zA-Z_][a-zA-Z0-9_]*");
	return std::regex_match(name, label_name_regex);
#endif
}
}  // namespace prometheus
