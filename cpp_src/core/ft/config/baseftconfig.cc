
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

void BaseFTConfig::parseBase(const gason::JsonNode &root) {
	enableTranslit = root["enable_translit"].As<>(enableTranslit);
	enableNumbersSearch = root["enable_numbers_search"].As<>(enableNumbersSearch);
	enableKbLayout = root["enable_kb_layout"].As<>(enableKbLayout);
	mergeLimit = root["merge_limit"].As<>(mergeLimit, 0, 65000);
	logLevel = root["log_level"].As<>(logLevel, 0, 5);
	extraWordSymbols = root["extra_word_symbols"].As<>(extraWordSymbols);

	auto &stopWordsNode = root["stop_words"];
	if (!stopWordsNode.empty()) {
		stopWords.clear();
		for (auto &sw : stopWordsNode) stopWords.insert(sw.As<string>());
	}

	auto &stemmersNode = root["stemmers"];
	if (!stemmersNode.empty()) {
		stemmers.clear();
		for (auto &st : stemmersNode) stemmers.push_back(st.As<string>());
	}
}

}  // namespace reindexer
