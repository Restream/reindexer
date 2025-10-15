#include "ftfuzzyconfig.h"
#include <string.h>
#include "core/cjson/jsonbuilder.h"
#include "tools/errors.h"
#include "tools/jsontools.h"

namespace reindexer {

void FtFuzzyConfig::parse(std::string_view json, const RHashMap<std::string, FtIndexFieldPros>&) {
	if (json.empty()) {
		return;
	}

	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		maxSrcProc = root["max_src_proc"].As<>(maxSrcProc, 0.0, 200.0);
		maxDstProc = root["max_dst_proc"].As<>(maxDstProc, 0.0, 200.0);
		posSourceBoost = root["pos_source_boost"].As<>(posSourceBoost, 0.0, 2.0);
		posSourceDistMin = root["pos_source_dist_min"].As<>(posSourceDistMin, 0.0, 2.0);
		posSourceDistBoost = root["pos_source_dist_boost"].As<>(posSourceDistBoost, 0.0, 2.0);
		posDstBoost = root["pos_dst_boost"].As<>(posDstBoost, 0.0, 2.0);
		startDecreeseBoost = root["start_decreese_boost"].As<>(startDecreeseBoost, 0.0, 2.0);
		startDefaultDecreese = root["start_default_decreese"].As<>(startDefaultDecreese, 0.0, 2.0);
		minOkProc = root["min_ok_proc"].As<>(minOkProc, 0.0, 100.);
		bufferSize = root["buffer_size"].As<size_t>(bufferSize, 2, 10);
		spaceSize = root["space_size"].As<size_t>(spaceSize, 0, 9);

		parseBase(root);

	} catch (const gason::Exception& ex) {
		throw Error(errParseJson, ex.what());
	}
}

std::string FtFuzzyConfig::GetJSON(const fast_hash_map<std::string, int>&) const {
	WrSerializer wrser;
	JsonBuilder jsonBuilder(wrser);
	BaseFTConfig::getJson(jsonBuilder);
	jsonBuilder.Put("max_src_proc", maxSrcProc);
	jsonBuilder.Put("max_dst_proc", maxDstProc);
	jsonBuilder.Put("pos_source_boost", posSourceBoost);
	jsonBuilder.Put("pos_source_dist_min", posSourceDistMin);
	jsonBuilder.Put("pos_source_dist_boost", posSourceDistBoost);
	jsonBuilder.Put("pos_dst_boost", posDstBoost);
	jsonBuilder.Put("start_decreese_boost", startDecreeseBoost);
	jsonBuilder.Put("start_default_decreese", startDefaultDecreese);
	jsonBuilder.Put("min_ok_proc", minOkProc);
	jsonBuilder.Put("buffer_size", bufferSize);
	jsonBuilder.Put("space_size", spaceSize);
	jsonBuilder.End();
	return wrser.c_str();
}

}  // namespace reindexer
