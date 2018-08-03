
#include "baseftconfig.h"
#include <string.h>
#include "core/ft/stopwords/stop.h"
#include "tools/errors.h"
#include "tools/jsontools.h"

namespace reindexer {

BaseFTConfig::BaseFTConfig() {
	for (const char **p = stop_words_en; *p != nullptr; p++) stopWords.insert(*p);
	for (const char **p = stop_words_ru; *p != nullptr; p++) stopWords.insert(*p);
}

void BaseFTConfig::parseBase(const JsonNode *elem) {
	parseJsonField("enable_translit", enableTranslit, elem);
	parseJsonField("enable_numbers_search", enableNumbersSearch, elem);
	parseJsonField("enable_kb_layout", enableKbLayout, elem);
	parseJsonField("merge_limit", mergeLimit, elem, 0, 65535);
	parseJsonField("log_level", logLevel, elem, 0, 5);
	parseJsonField("extra_word_symbols", extraWordSymbols, elem);

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

}  // namespace reindexer
