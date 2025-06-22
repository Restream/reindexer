#include "baseftconfig.h"
#include "core/cjson/jsonbuilder.h"
#include "core/ft/limits.h"
#include "core/ft/stopwords/stop.h"
#include "vendor/gason/gason.h"
#include "vendor/utf8cpp/utf8/unchecked.h"

namespace reindexer {

BaseFTConfig::BaseFTConfig() {
	for (const char** p = stop_words_en; *p != nullptr; p++) {
		stopWords.insert({*p, StopWord::Type::Morpheme});
	}
	for (const char** p = stop_words_ru; *p != nullptr; p++) {
		stopWords.insert({*p, StopWord::Type::Morpheme});
	}
}

std::string BaseFTConfig::removeAccentsAndDiacritics(const std::string& str) const {
	if (!removeDiacriticsMask) {
		return str;
	}

	std::string buf;
	buf.resize(str.length());
	auto bufBegin = buf.begin();
	auto bufIt = buf.begin();

	for (auto it = str.begin(), endIt = str.end(); it != endIt;) {
		uint32_t ch = utf8::unchecked::next(it);
		if (FitsMask(ch, removeDiacriticsMask)) {
			ch = RemoveDiacritic(ch);
		}
		if (ch != 0) {
			bufIt = utf8::unchecked::append(ch, bufIt);
		}
	}

	buf.resize(std::distance(bufBegin, bufIt));
	return buf;
}

void BaseFTConfig::parseBase(const gason::JsonNode& root) {
	enableTranslit = root["enable_translit"].As<>(enableTranslit);
	enableNumbersSearch = root["enable_numbers_search"].As<>(enableNumbersSearch);
	enableKbLayout = root["enable_kb_layout"].As<>(enableKbLayout);
	mergeLimit = root["merge_limit"].As<>(mergeLimit, kMinMergeLimitValue, kMaxMergeLimitValue);
	logLevel = root["log_level"].As<>(logLevel, 0, 5);
	extraWordSymbols = root["extra_word_symbols"].As<>(extraWordSymbols);

	auto& removeDiacriticsNode = root["keep_diacritics"];
	if (!removeDiacriticsNode.empty()) {
		for (auto& st : removeDiacriticsNode) {
			SymbolType symbolType = GetSymbolType(st.As<std::string>());
			removeDiacriticsMask ^= GetSymbolTypeMask(symbolType);
		}
	}

	auto& stopWordsNode = root["stop_words"];
	if (!stopWordsNode.empty()) {
		stopWords.clear();
		for (auto& sw : stopWordsNode) {
			std::string word;
			StopWord::Type type = StopWord::Type::Stop;
			if (sw.value.getTag() == gason::JsonTag::STRING) {
				word = removeAccentsAndDiacritics(sw.As<std::string>());
			} else if (sw.value.getTag() == gason::JsonTag::OBJECT) {
				word = removeAccentsAndDiacritics(sw["word"].As<std::string>());
				type = sw["is_morpheme"].As<bool>() ? StopWord::Type::Morpheme : StopWord::Type::Stop;
			}

			if (std::find_if(word.begin(), word.end(), [](const auto& symbol) { return std::isspace(symbol); }) != word.end()) {
				throw Error(errParams, "Stop words can't contain spaces: {}", word);
			}

			auto [it, inserted] = stopWords.emplace(std::move(word), type);
			if (!inserted && it->type != type) {
				throw Error(errParams, "Duplicate stop-word with different morpheme attribute: {}", *it);
			}
		}
	}

	auto& stemmersNode = root["stemmers"];
	if (!stemmersNode.empty()) {
		stemmers.clear();
		for (auto& st : stemmersNode) {
			stemmers.emplace_back(st.As<std::string>());
		}
	}
	synonyms.clear();
	for (auto& se : root["synonyms"]) {
		Synonym synonym;
		for (auto& ae : se["alternatives"]) {
			synonym.alternatives.emplace_back(removeAccentsAndDiacritics(ae.As<std::string>()));
		}
		for (auto& te : se["tokens"]) {
			synonym.tokens.emplace_back(removeAccentsAndDiacritics(te.As<std::string>()));
		}
		synonyms.emplace_back(std::move(synonym));
	}
	const auto& baseRankingConfigNode = root["base_ranking"];
	if (!baseRankingConfigNode.empty()) {
		rankingConfig.fullMatch = baseRankingConfigNode["full_match_proc"].As<>(rankingConfig.fullMatch, 0, 500);
		rankingConfig.prefixMin = baseRankingConfigNode["prefix_min_proc"].As<>(rankingConfig.prefixMin, 0, 500);
		rankingConfig.suffixMin = baseRankingConfigNode["suffix_min_proc"].As<>(rankingConfig.suffixMin, 0, 500);
		rankingConfig.typo = baseRankingConfigNode["base_typo_proc"].As<>(rankingConfig.typo, 0, 500);
		rankingConfig.typoPenalty = baseRankingConfigNode["typo_proc_penalty"].As<>(rankingConfig.typoPenalty, 0, 500);
		rankingConfig.stemmerPenalty = baseRankingConfigNode["stemmer_proc_penalty"].As<>(rankingConfig.stemmerPenalty, 0, 500);
		rankingConfig.kblayout = baseRankingConfigNode["kblayout_proc"].As<>(rankingConfig.kblayout, 0, 500);
		rankingConfig.translit = baseRankingConfigNode["translit_proc"].As<>(rankingConfig.translit, 0, 500);
		rankingConfig.synonyms = baseRankingConfigNode["synonyms_proc"].As<>(rankingConfig.synonyms, 0, 500);
	}
}

void BaseFTConfig::getJson(JsonBuilder& jsonBuilder) const {
	jsonBuilder.Put("enable_translit", enableTranslit);
	jsonBuilder.Put("enable_numbers_search", enableNumbersSearch);
	jsonBuilder.Put("enable_kb_layout", enableKbLayout);
	jsonBuilder.Put("merge_limit", mergeLimit);
	jsonBuilder.Put("log_level", logLevel);
	jsonBuilder.Put("extra_word_symbols", extraWordSymbols);
	jsonBuilder.Array<std::string>("stemmers", stemmers);
	{
		auto synonymsNode = jsonBuilder.Array("synonyms");
		for (const auto& synonym : synonyms) {
			auto synonymObj = synonymsNode.Object();
			{
				auto tokensNode = synonymObj.Array("tokens");
				for (const auto& token : synonym.tokens) {
					tokensNode.Put(TagName::Empty(), token);
				}
			}
			{
				auto alternativesNode = synonymObj.Array("alternatives");
				for (const auto& token : synonym.alternatives) {
					alternativesNode.Put(TagName::Empty(), token);
				}
			}
		}
	}
	{
		auto stopWordsNode = jsonBuilder.Array("stop_words");
		for (const auto& sw : stopWords) {
			auto wordNode = stopWordsNode.Object();
			wordNode.Put("word", sw);
			wordNode.Put("is_morpheme", sw.type == StopWord::Type::Morpheme);
		}
	}
	{
		auto baseRankingConfigNode = jsonBuilder.Object("base_ranking");
		baseRankingConfigNode.Put("full_match_proc", rankingConfig.fullMatch);
		baseRankingConfigNode.Put("prefix_min_proc", rankingConfig.prefixMin);
		baseRankingConfigNode.Put("suffix_min_proc", rankingConfig.suffixMin);
		baseRankingConfigNode.Put("base_typo_proc", rankingConfig.typo);
		baseRankingConfigNode.Put("typo_proc_penalty", rankingConfig.typoPenalty);
		baseRankingConfigNode.Put("stemmer_proc_penalty", rankingConfig.stemmerPenalty);
		baseRankingConfigNode.Put("kblayout_proc", rankingConfig.kblayout);
		baseRankingConfigNode.Put("translit_proc", rankingConfig.translit);
		baseRankingConfigNode.Put("synonyms_proc", rankingConfig.synonyms);
	}
}

}  // namespace reindexer
