
#include "baseconfig.h"
#include <string.h>
#include "core/ft/stopwords/stop.h"
#include "tools/errors.h"

namespace reindexer {

BaseFTConfig::BaseFTConfig() {
	for (const char **p = stop_words_en; *p != nullptr; p++) stopWords.insert(*p);
	for (const char **p = stop_words_ru; *p != nullptr; p++) stopWords.insert(*p);
}

template <typename T>
void BaseFTConfig::parseJsonField(const char *name, T &ref, const JsonNode *elem, double min, double max) {
	if (strcmp(name, elem->key)) return;
	if (elem->value.getTag() == JSON_NUMBER) {
		T v = elem->value.toNumber();
		if (v < min || v > max) {
			throw Error(errParseJson, "Value of setting '%s' is out of range [%g,%g]", name, min, max);
		}
		ref = v;
	} else
		throw Error(errParseJson, "Expected type number for setting '%s' of ft1 config", name);
}

void BaseFTConfig::parseJsonField(const char *name, bool &ref, const JsonNode *elem) {
	if (strcmp(name, elem->key)) return;
	if (elem->value.getTag() == JSON_TRUE) {
		ref = true;
	} else if (elem->value.getTag() == JSON_FALSE) {
		ref = false;
	} else
		throw Error(errParseJson, "Expected value `true` of `false` for setting '%s' of ft1 config", name);
}

void BaseFTConfig::parseBase(const JsonNode *elem) {
	parseJsonField("enable_translit", enableTranslit, elem);
	parseJsonField("enable_kb_layout", enableKbLayout, elem);
	parseJsonField("merge_limit", mergeLimit, elem, 0, 65535);
	parseJsonField("log_level", logLevel, elem, 0, 5);
	if (!strcmp("stop_words", elem->key)) {
		if (elem->value.getTag() != JSON_ARRAY) {
			throw Error(errParseJson, "Expected array value of setting 'stop_words' of ft1 config");
		}
		stopWords.clear();
		for (auto ee : elem->value) {
			if (ee->value.getTag() != JSON_STRING) {
				throw Error(errParseJson, "Expected string value in array setting 'stop_words' of ft1 config");
			}
			stopWords.insert(ee->value.toString());
		}
	}
	if (!strcmp("stemmers", elem->key)) {
		if (elem->value.getTag() != JSON_ARRAY) {
			throw Error(errParseJson, "Expected array value of setting 'stemmers' of ft1 config");
		}
		stemmers.clear();
		for (auto ee : elem->value) {
			if (ee->value.getTag() != JSON_STRING) {
				throw Error(errParseJson, "Expected string value in array setting 'stemmers' of ft1 config");
			}
			stemmers.push_back(ee->value.toString());
		}
	}
}

template void BaseFTConfig::parseJsonField<size_t>(const char *, size_t &, const JsonNode *, double, double);
template void BaseFTConfig::parseJsonField<double>(const char *, double &, const JsonNode *, double, double);

}  // namespace reindexer
