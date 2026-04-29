#include "ftconfig.h"
#include "core/cjson/jsonbuilder.h"
#include "core/ft/limits.h"
#include "core/ft/stopwords/stop.h"
#include "vendor/gason/gason.h"
#include "vendor/utf8cpp/utf8/unchecked.h"

namespace reindexer {

namespace {

template <typename C>
static bool isAllEqual(const C& c) {
	for (size_t i = 1, size = c.size(); i < size; ++i) {
		if (!(c[0] == c[i])) {
			return false;
		}
	}
	return true;
}

}  // namespace

bool FTFieldConfig::operator==(const FTFieldConfig& o) const noexcept {
	return fp::ExactlyEqual(bm25Boost, o.bm25Boost) && fp::ExactlyEqual(bm25Weight, o.bm25Weight) &&
		   fp::ExactlyEqual(termLenBoost, o.termLenBoost) && fp::ExactlyEqual(termLenWeight, o.termLenWeight) &&
		   fp::ExactlyEqual(positionBoost, o.positionBoost) && fp::ExactlyEqual(positionWeight, o.positionWeight);
}

FTConfig::FTConfig(size_t fieldsCount) : fieldsCfg(fieldsCount ? fieldsCount : 1) {
	for (const char** p = stop_words_en; *p != nullptr; p++) {
		stopWords.insert({*p, StopWord::Type::Morpheme});
	}
	for (const char** p = stop_words_ru; *p != nullptr; p++) {
		stopWords.insert({*p, StopWord::Type::Morpheme});
	}
}

void FTRankingConfig::parse(const gason::JsonNode& node) {
	using namespace std::string_view_literals;
	fullMatch = node["full_match_proc"sv].As<>(kDefaultFullMatch, 0, 500);
	concat = node["concat_proc"sv].As<>(kDefaultConcat, 0, 500);
	prefixMin = node["prefix_min_proc"sv].As<>(kDefaultPrefixMin, 0, 500);
	suffixMin = node["suffix_min_proc"sv].As<>(kDefaultSuffixMin, 0, 500);
	typo = node["base_typo_proc"sv].As<>(kDefaultTypo, 0, 500);
	typoPenalty = node["typo_proc_penalty"sv].As<>(kDefaultTypoPenalty, 0, 500);
	stemmerPenalty = node["stemmer_proc_penalty"sv].As<>(kDefaultStemmerPenalty, 0, 500);
	kblayout = node["kblayout_proc"sv].As<>(kDefaultKblayout, 0, 500);
	translit = node["translit_proc"sv].As<>(kDefaultTranslit, 0, 500);
	synonyms = node["synonyms_proc"sv].As<>(kDefaultSynonyms, 0, 500);
	delimited = node["delimited_proc"sv].As<>(kDefaultDelimited, 0, 500);
}

void FTRankingConfig::getJson(JsonBuilder& jsonBuilder) const {
	using namespace std::string_view_literals;
	jsonBuilder.Put("full_match_proc"sv, fullMatch);
	jsonBuilder.Put("concat_proc"sv, concat);
	jsonBuilder.Put("prefix_min_proc"sv, prefixMin);
	jsonBuilder.Put("suffix_min_proc"sv, suffixMin);
	jsonBuilder.Put("base_typo_proc"sv, typo);
	jsonBuilder.Put("typo_proc_penalty"sv, typoPenalty);
	jsonBuilder.Put("stemmer_proc_penalty"sv, stemmerPenalty);
	jsonBuilder.Put("kblayout_proc"sv, kblayout);
	jsonBuilder.Put("translit_proc"sv, translit);
	jsonBuilder.Put("synonyms_proc"sv, synonyms);
	jsonBuilder.Put("delimited_proc"sv, delimited);
}

void FTConfig::Bm25Config::parse(const gason::JsonNode& node) {
	bm25k1 = node["bm25_k1"].As<double>(bm25k1, 0.0);
	bm25b = node["bm25_b"].As<double>(bm25b, 0.0, 1.0);
	const std::string bm25TypeStr = toLower(node["bm25_type"].As<std::string>("rx_bm25"));
	if (bm25TypeStr == "rx_bm25") {
		bm25Type = Bm25Type::rx;
	} else if (bm25TypeStr == "bm25") {
		bm25Type = Bm25Type::classic;
	} else if (bm25TypeStr == "word_count") {
		bm25Type = Bm25Type::wordCount;
	} else {
		throw Error(errParseJson, "FTConfig: unknown bm25Type value: {}", bm25TypeStr);
	}
}

void FTConfig::Bm25Config::getJson(JsonBuilder& jsonBuilder) const {
	jsonBuilder.Put("bm25_k1", bm25k1);
	jsonBuilder.Put("bm25_b", bm25b);
	switch (bm25Type) {
		case Bm25Type::classic:
			jsonBuilder.Put("bm25_type", "bm25");
			break;
		case Bm25Type::rx:
			jsonBuilder.Put("bm25_type", "rx_bm25");
			break;
		case Bm25Type::wordCount:
			jsonBuilder.Put("bm25_type", "word_count");
			break;
	}
}

void FTConfig::parse(std::string_view json, const RHashMap<std::string, FtIndexFieldPros>& fields) {
	fieldsCfg.clear();
	if (json.empty()) {
		fieldsCfg.resize(fields.size() ? fields.size() : 1);
		return;
	}

	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);

		distanceBoost = root["distance_boost"].As<>(distanceBoost, 0.0, 10.0);
		distanceWeight = root["distance_weight"].As<>(distanceWeight, 0.0, 1.0);
		fullMatchBoost = root["full_match_boost"].As<>(fullMatchBoost, 0.0, 10.0);
		partialMatchDecrease = root["partial_match_decrease"].As<>(partialMatchDecrease, 0, 100);

		if (!root["min_rank"].isEmpty()) {
			minRank = root["min_rank"].As<>(minRank, 0, 255);
		} else {
			// minRelevancy is deprecated, but still can be used to init minRank
			float minRelevancy = root["min_relevancy"].As<>(0.05f, 0.0f, 1.0f);
			minRank = static_cast<uint32_t>(100.0 * minRelevancy);
		}

		if (!root["max_typos_in_word"].isEmpty()) {
			if (!root["max_typos"].isEmpty()) {
				throw Error(errParseDSL,
							"Fulltext configuration cannot contain 'max_typos' and 'max_typos_in_word' fields at the same time");
			}
			maxTypos = 2 * root["max_typos_in_word"].As<>(MaxTyposInWord(), 0, kMaxTyposInWord);
		} else {
			const auto& maxTyposNode = root["max_typos"];
			if (!maxTyposNode.isEmpty() && maxTyposNode.value.getTag() != gason::JsonTag::NUMBER) {
				throw Error(errParseDSL, "Fulltext configuration field 'max_typos' should be integer");
			}
			maxTypos = maxTyposNode.As<>(maxTypos, 0, 2 * kMaxTyposInWord);
		}
		maxTypoLen = root["max_typo_len"].As<uint8_t>(maxTypoLen, 0, kMaxTypoLenLimit);
		if (!root["typos_detailed_config"].isEmpty()) {
			auto typos = root["typos_detailed_config"];
			maxTypoDistance = typos["max_typo_distance"].As<>(maxTypoDistance, -1, kMaxTypoLenLimit);
			maxSymbolPermutationDistance =
				typos["max_symbol_permutation_distance"].As<>(maxSymbolPermutationDistance, -1, kMaxTypoLenLimit);
			maxMissingLetters = typos["max_missing_letters"].As<>(maxMissingLetters, -1, kMaxTyposInWord);
			maxExtraLetters = typos["max_extra_letters"].As<>(maxExtraLetters, -1, kMaxTyposInWord);
		}

		maxRebuildSteps = root["max_rebuild_steps"].As<>(maxRebuildSteps, 1, 500);
		// Override value without error to avoid situations, where maxRebuildSteps was set on the older version with incorrect limit
		maxRebuildSteps = std::min(uint32_t(maxRebuildSteps), kMaxStepsCount);
		maxStepSize = root["max_step_size"].As<>(maxStepSize, 5);
		maxAreasInDoc = root["max_areas_in_doc"].As<int>(maxAreasInDoc);
		maxTotalAreasToCache = root["max_total_areas_to_cache"].As<int>(maxTotalAreasToCache);

		if (!root["bm25_config"].isEmpty()) {
			auto conf = root["bm25_config"];
			bm25Config.parse(conf);
		}

		summationRanksByFieldsRatio = root["sum_ranks_by_fields_ratio"].As<>(summationRanksByFieldsRatio, 0.0, 1.0);

		FTFieldConfig defaultFieldCfg;
		defaultFieldCfg.bm25Boost = root["bm25_boost"].As<>(defaultFieldCfg.bm25Boost, 0.0, 10.0);
		defaultFieldCfg.bm25Weight = root["bm25_weight"].As<>(defaultFieldCfg.bm25Weight, 0.0, 1.0);
		defaultFieldCfg.termLenBoost = root["term_len_boost"].As<>(defaultFieldCfg.termLenBoost, 0.0, 10.0);
		defaultFieldCfg.termLenWeight = root["term_len_weight"].As<>(defaultFieldCfg.termLenWeight, 0.0, 1.0);
		defaultFieldCfg.positionBoost = root["position_boost"].As<>(defaultFieldCfg.positionBoost, 0.0, 10.0);
		defaultFieldCfg.positionWeight = root["position_weight"].As<>(defaultFieldCfg.positionWeight, 0.0, 1.0);

		std::ignore = fieldsCfg.insert(fieldsCfg.cend(), fields.size() ? fields.size() : 1, defaultFieldCfg);

		const auto& fieldsCfgNode = root["fields"];
		if (!fieldsCfgNode.isEmpty() && begin(fieldsCfgNode.value) != end(fieldsCfgNode.value)) {
			if (fields.empty()) {
				throw Error(errParseDSL, "Configuration for single field fulltext index can't contain field specifications");
			}
			std::set<size_t> modifiedFields;
			for (const auto& fldCfg : fieldsCfgNode.value) {
				const std::string fieldName = fldCfg["field_name"].As<std::string>();
				const auto fldIt = fields.find(fieldName);
				if (fldIt == fields.end()) {
					throw Error(errParseDSL, "Field '{}' is not included to full text index", fieldName);
				}
				assertrx(fldIt->second.fieldNumber < fieldsCfg.size());
				if (modifiedFields.contains(fldIt->second.fieldNumber)) {
					throw Error(errParseDSL, "Field '{}' is duplicated in fulltext configuration", fieldName);
				}
				modifiedFields.insert(fldIt->second.fieldNumber);
				FTFieldConfig& curFieldCfg = fieldsCfg[fldIt->second.fieldNumber];
				curFieldCfg.bm25Boost = fldCfg["bm25_boost"].As<>(defaultFieldCfg.bm25Boost);
				curFieldCfg.bm25Weight = fldCfg["bm25_weight"].As<>(defaultFieldCfg.bm25Weight);
				curFieldCfg.termLenBoost = fldCfg["term_len_boost"].As<>(defaultFieldCfg.termLenBoost);
				curFieldCfg.termLenWeight = fldCfg["term_len_weight"].As<>(defaultFieldCfg.termLenWeight);
				curFieldCfg.positionBoost = fldCfg["position_boost"].As<>(defaultFieldCfg.positionBoost);
				curFieldCfg.positionWeight = fldCfg["position_weight"].As<>(defaultFieldCfg.positionWeight);
			}
		}

		const std::string opt = toLower(root["optimization"].As<std::string>("memory"));
		if (opt == "memory") {
			optimization = Optimization::Memory;
		} else if (opt == "cpu") {
			optimization = Optimization::CPU;
		} else {
			throw Error(errParseJson, "FTConfig: unknown optimization value: {}", opt);
		}
		enablePreselectBeforeFt = root["enable_preselect_before_ft"].As<>(enablePreselectBeforeFt);

		const std::string splitterStr = toLower(root["splitter"].As<std::string>("fast"));
		if (splitterStr == "fast") {
			splitterType = Splitter::Fast;
		} else if (splitterStr == "friso" || splitterStr == "mmseg_cn") {
			splitterType = Splitter::MMSegCN;
		} else {
			throw Error(errParseJson, "FTConfig: unknown splitter value: {}", splitterStr);
		}

		using namespace std::string_view_literals;
		enableTermsConcat = root["enable_terms_concat"sv].As<>(enableTermsConcat);
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
		if (!removeDiacriticsNode.isEmpty()) {
			for (auto& st : removeDiacriticsNode) {
				SymbolType symbolType = GetSymbolType(st.As<std::string>());
				splitOptions.SetRemoveDiacriticsMask(splitOptions.GetRemoveDiacriticsMask() ^ GetSymbolTypeMask(symbolType));
			}
		}

		auto& stopWordsNode = root["stop_words"sv];
		if (!stopWordsNode.isEmpty()) {
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
		if (!stemmersNode.isEmpty()) {
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

		termsBoost.clear();
		for (auto& tb : root["terms_boost"sv]) {
			float boost = tb["boost"sv].As<float>(1.0, 0.0, 5.0);

			for (auto& term : tb["terms"sv]) {
				std::string termStr = term.As<std::string>();
				std::wstring termWstr = utf8_to_utf16(termStr);
				ToLower(termWstr);
				termStr = utf16_to_utf8(termWstr);
				if (!splitOptions.IsWord(termStr)) {
					throw Error(errParams, "Incorrect term to boost: {} (must be single word)"sv, termStr);
				}
				termsBoost[termStr] = std::max(termsBoost[termStr], boost);
			}
		}

		const auto& baseRankingConfigNode = root["base_ranking"sv];
		if (!baseRankingConfigNode.isEmpty()) {
			rankingConfig.parse(baseRankingConfigNode);
		}
	} catch (const gason::Exception& ex) {
		throw Error(errParseJson, "FTConfig: {}", ex.what());
	}
}

std::string FTConfig::GetJSON(const fast_hash_map<std::string, int>& fields) const {
	using namespace std::string_view_literals;
	WrSerializer wrser;
	JsonBuilder jsonBuilder(wrser);

	jsonBuilder.Put("enable_terms_concat"sv, enableTermsConcat);
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
		auto termsBoostNode = jsonBuilder.Array("terms_boost"sv);

		for (const auto& [term, boost] : termsBoost) {
			auto termObj = termsBoostNode.Object();
			{ termObj.Array("terms"sv).Put(TagName::Empty(), term); }
			termObj.Put("boost"sv, boost);
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
		rankingConfig.getJson(baseRankingConfigNode);
	}

	jsonBuilder.Put("distance_boost", distanceBoost);
	jsonBuilder.Put("distance_weight", distanceWeight);
	jsonBuilder.Put("full_match_boost", fullMatchBoost);
	jsonBuilder.Put("partial_match_decrease", partialMatchDecrease);
	jsonBuilder.Put("min_relevancy", minRank / 100.0f);
	jsonBuilder.Put("min_rank", minRank);
	jsonBuilder.Put("max_typos", maxTypos);

	{
		auto typos = jsonBuilder.Object("typos_detailed_config");
		typos.Put("max_typo_distance", maxTypoDistance);
		typos.Put("max_symbol_permutation_distance", maxSymbolPermutationDistance);
		typos.Put("max_missing_letters", maxMissingLetters);
		typos.Put("max_extra_letters", maxExtraLetters);
	}

	jsonBuilder.Put("max_typo_len", maxTypoLen);
	jsonBuilder.Put("max_rebuild_steps", maxRebuildSteps);
	jsonBuilder.Put("max_step_size", maxStepSize);
	jsonBuilder.Put("sum_ranks_by_fields_ratio", summationRanksByFieldsRatio);
	jsonBuilder.Put("max_areas_in_doc", maxAreasInDoc);
	jsonBuilder.Put("max_total_areas_to_cache", maxTotalAreasToCache);

	{
		auto conf = jsonBuilder.Object("bm25_config");
		bm25Config.getJson(conf);
	}

	switch (optimization) {
		case Optimization::Memory:
			jsonBuilder.Put("optimization", "Memory");
			break;
		case Optimization::CPU:
			jsonBuilder.Put("optimization", "CPU");
			break;
	}

	switch (splitterType) {
		case Splitter::Fast:
			jsonBuilder.Put("splitter", "fast");
			break;
		case Splitter::MMSegCN:
			jsonBuilder.Put("splitter", "mmseg_cn");
			break;
	}

	jsonBuilder.Put("enable_preselect_before_ft", enablePreselectBeforeFt);

	if (fields.empty() || isAllEqual(fieldsCfg)) {
		assertrx_throw(!fieldsCfg.empty());
		jsonBuilder.Put("bm25_boost", fieldsCfg[0].bm25Boost);
		jsonBuilder.Put("bm25_weight", fieldsCfg[0].bm25Weight);
		jsonBuilder.Put("term_len_boost", fieldsCfg[0].termLenBoost);
		jsonBuilder.Put("term_len_weight", fieldsCfg[0].termLenWeight);
		jsonBuilder.Put("position_boost", fieldsCfg[0].positionBoost);
		jsonBuilder.Put("position_weight", fieldsCfg[0].positionWeight);
	} else {
		auto fieldsNode = jsonBuilder.Array("fields");
		for (const auto& f : fields) {
			auto fldNode = fieldsNode.Object();
			assertrx_throw(0 <= f.second);
			assertrx_throw(f.second < static_cast<int>(fieldsCfg.size()));
			fldNode.Put("field_name", f.first);
			fldNode.Put("bm25_boost", fieldsCfg[f.second].bm25Boost);
			fldNode.Put("bm25_weight", fieldsCfg[f.second].bm25Weight);
			fldNode.Put("term_len_boost", fieldsCfg[f.second].termLenBoost);
			fldNode.Put("term_len_weight", fieldsCfg[f.second].termLenWeight);
			fldNode.Put("position_boost", fieldsCfg[f.second].positionBoost);
			fldNode.Put("position_weight", fieldsCfg[f.second].positionWeight);
		}
	}
	jsonBuilder.End();
	return std::string(wrser.Slice());
}

}  // namespace reindexer
