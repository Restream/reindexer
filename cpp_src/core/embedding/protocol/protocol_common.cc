#include "protocol_common.h"

#if defined(__GNUC__) && ((__GNUC__ == 12) || (__GNUC__ == 13)) && defined(REINDEX_WITH_ASAN)
// regex header is broken in GCC 12.0-13.3 with ASAN
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#include <regex>
#pragma GCC diagnostic pop
#else  // REINDEX_WITH_ASAN
#include <regex>
#endif	// REINDEX_WITH_ASAN
#include "core/cjson/jsonbuilder.h"
#include "core/enums.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer::embedding {
namespace {

void putDocFields(JsonBuilder& json, const DocSource& docSource) {
	for (const auto& itemSource : docSource) {
		if (!itemSource.second.IsArrayValue() && itemSource.second.size() == 1) {
			json.Put(itemSource.first, itemSource.second.front());
		} else {
			auto arrNode = json.Array(itemSource.first);
			for (const auto& item : itemSource.second) {
				arrNode.Put(TagName::Empty(), item);
			}
		}
	}
}

void assignFromSerializer(WrSerializer& ser, std::string& out) {
	const auto slice = ser.Slice();
	out.assign(slice.data(), slice.size());
}

}  // namespace

void BuildQueryView(std::string_view text, std::string& out) {
	WrSerializer ser;
	{  // [text0]
		JsonBuilder json{ser, ObjType::TypePlain};
		json.Put(TagName::Empty(), text);
	}
	assignFromSerializer(ser, out);
}

void BuildUpsertView(std::span<const DocSource> sources, std::string& out) {
	WrSerializer ser;
	{  // {'fld0':text,'fld1':[Val0,Val1,...],...}
		JsonBuilder json{ser, ObjType::TypePlain};
		for (const auto& docSource : sources) {
			auto arrNodeItem = json.Object(TagName::Empty());
			putDocFields(arrNodeItem, docSource);
			arrNodeItem.End();
		}
	}
	assignFromSerializer(ser, out);
}

bool MatchHttpUrl(std::string_view url) {
	const static std::regex re(R"(^(http[s]?)://[0-9a-z\.-]+(:[1-9][0-9]*)?(/[^\s]*)*$)");
	return std::regex_match(url.begin(), url.end(), re);
}

}  // namespace reindexer::embedding
