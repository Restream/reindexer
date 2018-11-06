#include "ftfastconfig.h"
#include <string.h>
#include <limits>
#include "core/ft/stopwords/stop.h"
#include "tools/errors.h"
#include "tools/jsontools.h"

namespace reindexer {

void FtFastConfig::parse(char *json) {
	JsonAllocator jalloc;
	JsonValue jvalue;
	char *endp;

	if (!*json) return;

	int status = jsonParse(json, &endp, &jvalue, jalloc);

	if (status != JSON_OK) {
		throw Error(errParseJson, "Malformed JSON with ft1 config");
	}
	if (jvalue.getTag() == JSON_NULL) return;

	if (jvalue.getTag() != JSON_OBJECT) throw Error(errParseJson, "Expected json object in ft1 config");

	for (auto elem : jvalue) {
		if (elem->value.getTag() == JSON_NULL) continue;

		parseJsonField("bm25_boost", bm25Boost, elem, 0, 10);
		parseJsonField("bm25_weight", bm25Weight, elem, 0, 1);
		parseJsonField("distance_boost", distanceBoost, elem, 0, 10);
		parseJsonField("distance_weight", distanceWeight, elem, 0, 1);
		parseJsonField("term_len_boost", termLenBoost, elem, 0, 10);
		parseJsonField("term_len_weight", termLenWeight, elem, 0, 1);
		parseJsonField("min_relevancy", minRelevancy, elem, 0, 1);
		parseJsonField("max_typos_in_word", maxTyposInWord, elem, 0, 2);
		parseJsonField("max_typo_len", maxTypoLen, elem, 0, 100);

		parseJsonField("max_rebuild_steps", maxRebuildSteps, elem, 1, 500);
		parseJsonField("max_step_size", maxStepSize, elem, 5, std::numeric_limits<double>::max());

		parseBase(elem);
	}
}

}  // namespace reindexer
