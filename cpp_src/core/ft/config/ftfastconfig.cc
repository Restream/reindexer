#include "ftfastconfig.h"
#include <string.h>
#include <limits>
#include <set>
#include "core/ft/stopwords/stop.h"
#include "tools/errors.h"
#include "tools/jsontools.h"

namespace reindexer {

void FtFastConfig::parse(std::string_view json, const fast_hash_map<std::string, int>& fields) {
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
			maxTypos = 2 * root["max_typos_in_word"].As<>(MaxTyposInWord(), 0, 2);
		} else {
			const auto& maxTyposNode = root["max_typos"];
			if (!maxTyposNode.empty() && maxTyposNode.value.getTag() != gason::JSON_NUMBER) {
				throw Error(errParseDSL, "Fulltext configuration field 'max_typos' should be integer");
			}
			maxTypos = maxTyposNode.As<>(maxTypos, 0, 4);
		}
		maxTypoLen = root["max_typo_len"].As<>(maxTypoLen, 0, 100);
		maxRebuildSteps = root["max_rebuild_steps"].As<>(maxRebuildSteps, 1, 500);
		maxStepSize = root["max_step_size"].As<>(maxStepSize, 5);
		summationRanksByFieldsRatio = root["sum_ranks_by_fields_ratio"].As<>(summationRanksByFieldsRatio, 0.0, 1.0);

		FtFastFieldConfig defaultFieldCfg;
		defaultFieldCfg.bm25Boost = root["bm25_boost"].As<>(defaultFieldCfg.bm25Boost, 0.0, 10.0);
		defaultFieldCfg.bm25Weight = root["bm25_weight"].As<>(defaultFieldCfg.bm25Weight, 0.0, 1.0);
		defaultFieldCfg.termLenBoost = root["term_len_boost"].As<>(defaultFieldCfg.termLenBoost, 0.0, 10.0);
		defaultFieldCfg.termLenWeight = root["term_len_weight"].As<>(defaultFieldCfg.termLenWeight, 0.0, 1.0);
		defaultFieldCfg.positionBoost = root["position_boost"].As<>(defaultFieldCfg.positionBoost, 0.0, 10.0);
		defaultFieldCfg.positionWeight = root["position_weight"].As<>(defaultFieldCfg.positionWeight, 0.0, 1.0);

		fieldsCfg.insert(fieldsCfg.end(), fields.size() ? fields.size() : 1, defaultFieldCfg);

		const auto& fieldsCfgNode = root["fields"];
		if (!fieldsCfgNode.empty() && begin(fieldsCfgNode.value) != end(fieldsCfgNode.value)) {
			if (fields.empty()) {
				throw Error(errParseDSL, "Configuration for single field fulltext index cannot contain field specifications");
			}
			std::set<size_t> modifiedFields;
			for (const auto fldCfg : fieldsCfgNode.value) {
				const std::string fieldName = (*fldCfg)["field_name"].As<std::string>();
				const auto fldIt = fields.find(fieldName);
				if (fldIt == fields.end()) {
					throw Error(errParseDSL, "Field '%s' is not included to full text index", fieldName);
				}
				assertrx(fldIt->second < static_cast<int>(fieldsCfg.size()));
				if (modifiedFields.count(fldIt->second) != 0) {
					throw Error(errParseDSL, "Field '%s' is dublicated in fulltext configuration", fieldName);
				}
				modifiedFields.insert(fldIt->second);
				FtFastFieldConfig& curFieldCfg = fieldsCfg[fldIt->second];
				curFieldCfg.bm25Boost = (*fldCfg)["bm25_boost"].As<>(defaultFieldCfg.bm25Boost);
				curFieldCfg.bm25Weight = (*fldCfg)["bm25_weight"].As<>(defaultFieldCfg.bm25Weight);
				curFieldCfg.termLenBoost = (*fldCfg)["term_len_boost"].As<>(defaultFieldCfg.termLenBoost);
				curFieldCfg.termLenWeight = (*fldCfg)["term_len_weight"].As<>(defaultFieldCfg.termLenWeight);
				curFieldCfg.positionBoost = (*fldCfg)["position_boost"].As<>(defaultFieldCfg.positionBoost);
				curFieldCfg.positionWeight = (*fldCfg)["position_weight"].As<>(defaultFieldCfg.positionWeight);
			}
		}

		parseBase(root);
	} catch (const gason::Exception& ex) {
		throw Error(errParseJson, "FtFastConfig: %s", ex.what());
	}
}

}  // namespace reindexer
