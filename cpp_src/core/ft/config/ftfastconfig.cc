#include "ftfastconfig.h"
#include <string.h>
#include <set>
#include "core/cjson/jsonbuilder.h"
#include "core/ft/limits.h"
#include "tools/errors.h"
#include "tools/float_comparison.h"
#include "tools/jsontools.h"

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

namespace reindexer {

bool FtFastFieldConfig::operator==(const FtFastFieldConfig& o) const noexcept {
	return fp::ExactlyEqual(bm25Boost, o.bm25Boost) && fp::ExactlyEqual(bm25Weight, o.bm25Weight) &&
		   fp::ExactlyEqual(termLenBoost, o.termLenBoost) && fp::ExactlyEqual(termLenWeight, o.termLenWeight) &&
		   fp::ExactlyEqual(positionBoost, o.positionBoost) && fp::ExactlyEqual(positionWeight, o.positionWeight);
}

void FtFastConfig::parse(std::string_view json, const RHashMap<std::string, FtIndexFieldPros>& fields) {
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
		minRelevancy = root["min_relevancy"].As<>(minRelevancy, 0.0, 1.0);
		if (!root["max_typos_in_word"].empty()) {
			if (!root["max_typos"].empty()) {
				throw Error(errParseDSL,
							"Fulltext configuration cannot contain 'max_typos' and 'max_typos_in_word' fields at the same time");
			}
			maxTypos = 2 * root["max_typos_in_word"].As<>(MaxTyposInWord(), 0, kMaxTyposInWord);
		} else {
			const auto& maxTyposNode = root["max_typos"];
			if (!maxTyposNode.empty() && maxTyposNode.value.getTag() != gason::JsonTag::NUMBER) {
				throw Error(errParseDSL, "Fulltext configuration field 'max_typos' should be integer");
			}
			maxTypos = maxTyposNode.As<>(maxTypos, 0, 2 * kMaxTyposInWord);
		}
		maxTypoLen = root["max_typo_len"].As<>(maxTypoLen, 0, kMaxTypoLenLimit);
		if (!root["typos_detailed_config"].empty()) {
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

		if (!root["bm25_config"].empty()) {
			auto conf = root["bm25_config"];
			bm25Config.parse(conf);
		}

		summationRanksByFieldsRatio = root["sum_ranks_by_fields_ratio"].As<>(summationRanksByFieldsRatio, 0.0, 1.0);

		FtFastFieldConfig defaultFieldCfg;
		defaultFieldCfg.bm25Boost = root["bm25_boost"].As<>(defaultFieldCfg.bm25Boost, 0.0, 10.0);
		defaultFieldCfg.bm25Weight = root["bm25_weight"].As<>(defaultFieldCfg.bm25Weight, 0.0, 1.0);
		defaultFieldCfg.termLenBoost = root["term_len_boost"].As<>(defaultFieldCfg.termLenBoost, 0.0, 10.0);
		defaultFieldCfg.termLenWeight = root["term_len_weight"].As<>(defaultFieldCfg.termLenWeight, 0.0, 1.0);
		defaultFieldCfg.positionBoost = root["position_boost"].As<>(defaultFieldCfg.positionBoost, 0.0, 10.0);
		defaultFieldCfg.positionWeight = root["position_weight"].As<>(defaultFieldCfg.positionWeight, 0.0, 1.0);

		std::ignore = fieldsCfg.insert(fieldsCfg.cend(), fields.size() ? fields.size() : 1, defaultFieldCfg);

		const auto& fieldsCfgNode = root["fields"];
		if (!fieldsCfgNode.empty() && begin(fieldsCfgNode.value) != end(fieldsCfgNode.value)) {
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
				FtFastFieldConfig& curFieldCfg = fieldsCfg[fldIt->second.fieldNumber];
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
			throw Error(errParseJson, "FtFastConfig: unknown optimization value: {}", opt);
		}
		enablePreselectBeforeFt = root["enable_preselect_before_ft"].As<>(enablePreselectBeforeFt);

		const std::string splitterStr = toLower(root["splitter"].As<std::string>("fast"));
		if (splitterStr == "fast") {
			splitterType = Splitter::Fast;
		} else if (splitterStr == "friso" || splitterStr == "mmseg_cn") {
			splitterType = Splitter::MMSegCN;
		} else {
			throw Error(errParseJson, "FtFastConfig: unknown splitter value: {}", splitterStr);
		}

		parseBase(root);
	} catch (const gason::Exception& ex) {
		throw Error(errParseJson, "FtFastConfig: {}", ex.what());
	}
}

std::string FtFastConfig::GetJSON(const fast_hash_map<std::string, int>& fields) const {
	WrSerializer wrser;
	JsonBuilder jsonBuilder(wrser);
	BaseFTConfig::getJson(jsonBuilder);
	jsonBuilder.Put("distance_boost", distanceBoost);
	jsonBuilder.Put("distance_weight", distanceWeight);
	jsonBuilder.Put("full_match_boost", fullMatchBoost);
	jsonBuilder.Put("partial_match_decrease", partialMatchDecrease);
	jsonBuilder.Put("min_relevancy", minRelevancy);
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

void FtFastConfig::Bm25Config::getJson(JsonBuilder& jsonBuilder) const {
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

void FtFastConfig::Bm25Config::parse(const gason::JsonNode& node) {
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
		throw Error(errParseJson, "FtFastConfig: unknown bm25Type value: {}", bm25TypeStr);
	}
}

}  // namespace reindexer
