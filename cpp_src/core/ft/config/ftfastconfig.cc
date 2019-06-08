#include "ftfastconfig.h"
#include <string.h>
#include <limits>
#include "core/ft/stopwords/stop.h"
#include "tools/errors.h"
#include "tools/jsontools.h"

namespace reindexer {

void FtFastConfig::parse(string_view json) {
	if (json.empty()) return;

	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);

		bm25Boost = root["bm25_boost"].As<>(bm25Boost, 0.0, 10.0);
		bm25Weight = root["bm25_weight"].As<>(bm25Weight, 0.0, 1.0);
		distanceBoost = root["distance_boost"].As<>(distanceBoost, 0.0, 10.0);
		distanceWeight = root["distance_weight"].As<>(distanceWeight, 0.0, 1.0);
		termLenBoost = root["term_len_boost"].As<>(termLenBoost, 0.0, 10.0);
		termLenWeight = root["term_len_weight"].As<>(termLenWeight, 0.0, 1.0);
		minRelevancy = root["min_relevancy"].As<>(minRelevancy, 0.0, 1.0);
		maxTyposInWord = root["max_typos_in_word"].As<>(maxTyposInWord, 0, 2);
		maxTypoLen = root["max_typo_len"].As<>(maxTypoLen, 0, 100);
		maxRebuildSteps = root["max_rebuild_steps"].As<>(maxRebuildSteps, 1, 500);
		maxStepSize = root["max_step_size"].As<>(maxStepSize, 5);
		parseBase(root);
	} catch (const gason::Exception &ex) {
		throw Error(errParseJson, "FtFastConfig: %s", ex.what());
	}
}

}  // namespace reindexer
