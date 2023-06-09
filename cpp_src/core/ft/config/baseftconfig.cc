
#include "baseftconfig.h"
#include <string.h>
#include "core/cjson/jsonbuilder.h"
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
	enableWarmupOnNsCopy = root["enable_warmup_on_ns_copy"].As<>(enableWarmupOnNsCopy);
	mergeLimit = root["merge_limit"].As<>(mergeLimit, kMinMergeLimitValue, kMaxMergeLimitValue);
	logLevel = root["log_level"].As<>(logLevel, 0, 5);
	extraWordSymbols = root["extra_word_symbols"].As<>(extraWordSymbols);

	auto &stopWordsNode = root["stop_words"];
	if (!stopWordsNode.empty()) {
		stopWords.clear();
		for (auto &sw : stopWordsNode) stopWords.insert(sw.As<std::string>());
	}

	auto &stemmersNode = root["stemmers"];
	if (!stemmersNode.empty()) {
		stemmers.clear();
		for (auto &st : stemmersNode) stemmers.push_back(st.As<std::string>());
	}
	synonyms.clear();
	for (auto &se : root["synonyms"]) {
		Synonym synonym;
		for (auto &ae : se["alternatives"]) synonym.alternatives.push_back(ae.As<std::string>());
		for (auto &te : se["tokens"]) synonym.tokens.push_back(te.As<std::string>());
		synonyms.push_back(std::move(synonym));
	}
}

void BaseFTConfig::getJson(JsonBuilder &jsonBuilder) const {
	jsonBuilder.Put("enable_translit", enableTranslit);
	jsonBuilder.Put("enable_numbers_search", enableNumbersSearch);
	jsonBuilder.Put("enable_kb_layout", enableKbLayout);
	jsonBuilder.Put("enable_warmup_on_ns_copy", enableWarmupOnNsCopy);
	jsonBuilder.Put("merge_limit", mergeLimit);
	jsonBuilder.Put("log_level", logLevel);
	jsonBuilder.Put("extra_word_symbols", extraWordSymbols);
	jsonBuilder.Array<std::string>("stemmers", stemmers);
	{
		auto synonymsNode = jsonBuilder.Array("synonyms");
		for (const auto &synonym : synonyms) {
			auto synonymObj = synonymsNode.Object();
			{
				auto tokensNode = synonymObj.Array("tokens");
				for (const auto &token : synonym.tokens) tokensNode.Put(nullptr, token);
			}
			{
				auto alternativesNode = synonymObj.Array("alternatives");
				for (const auto &token : synonym.alternatives) alternativesNode.Put(nullptr, token);
			}
		}
	}
	{
		auto stopWordsNode = jsonBuilder.Array("stop_words");
		for (const auto &sw : stopWords) {
			stopWordsNode.Put(nullptr, sw);
		}
	}
}

}  // namespace reindexer
