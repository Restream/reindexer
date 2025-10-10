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

void BaseFTConfig::parseBase(const gason::JsonNode& root) {
	using namespace std::string_view_literals;
	enableTranslit = root["enable_translit"sv].As<>(enableTranslit);
	enableNumbersSearch = root["enable_numbers_search"sv].As<>(enableNumbersSearch);
	enableKbLayout = root["enable_kb_layout"sv].As<>(enableKbLayout);
	mergeLimit = root["merge_limit"sv].As<>(mergeLimit, kMinMergeLimitValue, kMaxMergeLimitValue);
	logLevel = root["log_level"sv].As<>(logLevel, 0, 5);

	std::string extraWordSymbols = kDefaultExtraWordsSymbols;
	std::string wordPartDelimiters = kDefaultWordPartDelimiters;

	extraWordSymbols = root["extra_word_symbols"sv].As<>(extraWordSymbols);
	wordPartDelimiters = root["word_part_delimiters"sv].As<>(wordPartDelimiters);
	splitOptions.SetSymbols(extraWordSymbols, wordPartDelimiters);

	size_t minPartSize = root["min_word_part_size"sv].As<>(3, 1, 100);
	splitOptions.SetMinPartSize(minPartSize);

	splitOptions.SetRemoveDiacriticsMask(kRemoveAllDiacriticsMask);

	auto& removeDiacriticsNode = root["keep_diacritics"sv];
	if (!removeDiacriticsNode.empty()) {
		for (auto& st : removeDiacriticsNode) {
			SymbolType symbolType = GetSymbolType(st.As<std::string>());
			splitOptions.SetRemoveDiacriticsMask(splitOptions.GetRemoveDiacriticsMask() ^ GetSymbolTypeMask(symbolType));
		}
	}

	auto& stopWordsNode = root["stop_words"sv];
	if (!stopWordsNode.empty()) {
		stopWords.clear();
		for (auto& sw : stopWordsNode) {
			std::string word;
			StopWord::Type type = StopWord::Type::Stop;
			if (sw.value.getTag() == gason::JsonTag::STRING) {
				word = splitOptions.RemoveAccentsAndDiacritics(sw.As<std::string>());
			} else if (sw.value.getTag() == gason::JsonTag::OBJECT) {
				word = splitOptions.RemoveAccentsAndDiacritics(sw["word"].As<std::string>());
				type = sw["is_morpheme"sv].As<bool>() ? StopWord::Type::Morpheme : StopWord::Type::Stop;
			}

			if (!splitOptions.IsWord(word)) {
				throw Error(errParams, "Stop words can't contain spaces: {}"sv, word);
			}

			std::string wordWithoutDelims = splitOptions.RemoveDelims(word);
			if (wordWithoutDelims.size() < word.size()) {
				auto [it, inserted] = stopWords.emplace(std::move(wordWithoutDelims), type);
				if (!inserted && it->type != type) {
					throw Error(errParams, "Duplicate stop-word with different morpheme attribute: {}"sv, *it);
				}
			}

			auto [it, inserted] = stopWords.emplace(std::move(word), type);

			if (!inserted && it->type != type) {
				throw Error(errParams, "Duplicate stop-word with different morpheme attribute: {}"sv, *it);
			}
		}
	}

	auto& stemmersNode = root["stemmers"sv];
	if (!stemmersNode.empty()) {
		stemmers.clear();
		for (auto& st : stemmersNode) {
			stemmers.emplace_back(st.As<std::string>());
		}
	}
	synonyms.clear();
	for (auto& se : root["synonyms"sv]) {
		Synonym synonym;

		for (auto& ae : se["alternatives"sv]) {
			std::string alternative = splitOptions.RemoveAccentsAndDiacritics(ae.As<std::string>());
			if (splitOptions.ContainsDelims(alternative)) {
				std::string alternativeWithoutDelims = splitOptions.RemoveDelims(alternative);
				synonym.alternatives.emplace_back(std::move(alternativeWithoutDelims));
			}

			synonym.alternatives.emplace_back(std::move(alternative));
		}

		for (auto& te : se["tokens"sv]) {
			std::string token = splitOptions.RemoveAccentsAndDiacritics(te.As<std::string>());
			if (splitOptions.ContainsDelims(token)) {
				std::string tokenWithoutDelims = splitOptions.RemoveDelims(token);
				synonym.tokens.emplace_back(std::move(tokenWithoutDelims));
			}
			synonym.tokens.emplace_back(std::move(token));
		}

		synonyms.emplace_back(std::move(synonym));
	}
	const auto& baseRankingConfigNode = root["base_ranking"sv];
	if (!baseRankingConfigNode.empty()) {
		rankingConfig.fullMatch = baseRankingConfigNode["full_match_proc"sv].As<>(rankingConfig.fullMatch, 0, 500);
		rankingConfig.prefixMin = baseRankingConfigNode["prefix_min_proc"sv].As<>(rankingConfig.prefixMin, 0, 500);
		rankingConfig.suffixMin = baseRankingConfigNode["suffix_min_proc"sv].As<>(rankingConfig.suffixMin, 0, 500);
		rankingConfig.typo = baseRankingConfigNode["base_typo_proc"sv].As<>(rankingConfig.typo, 0, 500);
		rankingConfig.typoPenalty = baseRankingConfigNode["typo_proc_penalty"sv].As<>(rankingConfig.typoPenalty, 0, 500);
		rankingConfig.stemmerPenalty = baseRankingConfigNode["stemmer_proc_penalty"sv].As<>(rankingConfig.stemmerPenalty, 0, 500);
		rankingConfig.kblayout = baseRankingConfigNode["kblayout_proc"sv].As<>(rankingConfig.kblayout, 0, 500);
		rankingConfig.translit = baseRankingConfigNode["translit_proc"sv].As<>(rankingConfig.translit, 0, 500);
		rankingConfig.synonyms = baseRankingConfigNode["synonyms_proc"sv].As<>(rankingConfig.synonyms, 0, 500);
		rankingConfig.delimited = baseRankingConfigNode["delimited_proc"sv].As<>(rankingConfig.delimited, 0, 500);
	}
}

void BaseFTConfig::getJson(JsonBuilder& jsonBuilder) const {
	using namespace std::string_view_literals;
	jsonBuilder.Put("enable_translit"sv, enableTranslit);
	jsonBuilder.Put("enable_numbers_search"sv, enableNumbersSearch);
	jsonBuilder.Put("enable_kb_layout"sv, enableKbLayout);
	jsonBuilder.Put("merge_limit"sv, mergeLimit);
	jsonBuilder.Put("log_level"sv, logLevel);
	jsonBuilder.Put("extra_word_symbols"sv, splitOptions.GetExtraWordSymbols());
	jsonBuilder.Put("word_part_delimiters"sv, splitOptions.GetWordPartDelimiters());
	jsonBuilder.Array<std::string>("stemmers"sv, stemmers);
	{
		auto synonymsNode = jsonBuilder.Array("synonyms"sv);
		for (const auto& synonym : synonyms) {
			auto synonymObj = synonymsNode.Object();
			{
				auto tokensNode = synonymObj.Array("tokens"sv);
				for (const auto& token : synonym.tokens) {
					tokensNode.Put(TagName::Empty(), token);
				}
			}
			{
				auto alternativesNode = synonymObj.Array("alternatives"sv);
				for (const auto& token : synonym.alternatives) {
					alternativesNode.Put(TagName::Empty(), token);
				}
			}
		}
	}
	{
		auto stopWordsNode = jsonBuilder.Array("stop_words"sv);
		for (const auto& sw : stopWords) {
			auto wordNode = stopWordsNode.Object();
			wordNode.Put("word"sv, sw);
			wordNode.Put("is_morpheme"sv, sw.type == StopWord::Type::Morpheme);
		}
	}
	{
		auto baseRankingConfigNode = jsonBuilder.Object("base_ranking"sv);
		baseRankingConfigNode.Put("full_match_proc"sv, rankingConfig.fullMatch);
		baseRankingConfigNode.Put("prefix_min_proc"sv, rankingConfig.prefixMin);
		baseRankingConfigNode.Put("suffix_min_proc"sv, rankingConfig.suffixMin);
		baseRankingConfigNode.Put("base_typo_proc"sv, rankingConfig.typo);
		baseRankingConfigNode.Put("typo_proc_penalty"sv, rankingConfig.typoPenalty);
		baseRankingConfigNode.Put("stemmer_proc_penalty"sv, rankingConfig.stemmerPenalty);
		baseRankingConfigNode.Put("kblayout_proc"sv, rankingConfig.kblayout);
		baseRankingConfigNode.Put("translit_proc"sv, rankingConfig.translit);
		baseRankingConfigNode.Put("synonyms_proc"sv, rankingConfig.synonyms);
		baseRankingConfigNode.Put("delimited_proc"sv, rankingConfig.delimited);
	}
}

}  // namespace reindexer
