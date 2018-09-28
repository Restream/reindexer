

#include "ftfuzzyconfig.h"
#include <string.h>
#include "core/ft/stopwords/stop.h"
#include "tools/errors.h"
#include "tools/jsontools.h"
namespace reindexer {

void FtFuzzyConfig::parse(char *json) {
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
		parseJsonField("max_src_proc", maxSrcProc, elem, 0, 200);
		parseJsonField("max_dst_proc", maxDstProc, elem, 0, 200);
		parseJsonField("pos_source_boost", posSourceBoost, elem, 0, 2);
		parseJsonField("pos_source_dist_min", posSourceDistMin, elem, 0, 2);
		parseJsonField("pos_source_dist_boost", posSourceDistBoost, elem, 0, 2);
		parseJsonField("pos_dst_boost", posDstBoost, elem, 0, 2);
		parseJsonField("start_decreese_boost", startDecreeseBoost, elem, 0, 2);
		parseJsonField("start_default_decreese", startDefaultDecreese, elem, 0, 2);
		parseJsonField("min_ok_proc", minOkProc, elem, 0, 100);
		parseJsonField("buffer_size", bufferSize, elem, 2, 10);
		parseJsonField("space_size", spaceSize, elem, 0, 9);
		parseBase(elem);
	}
}

}  // namespace reindexer
