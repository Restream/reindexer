#include "synonyms.h"
#include "core/ft/config/baseftconfig.h"
#include "tools/stringstools.h"

namespace reindexer {

Synonyms::Synonyms() {}
void Synonyms::GetVariants(const wstring& data, std::vector<std::pair<std::wstring, int>>& result) {
	if (synonyms_.empty()) return;

	auto it = synonyms_.find(data);
	if (it == synonyms_.end()) {
		return;
	}
	for (auto ait : *it->second) {
		result.push_back({ait, 95});
	}
}

void Synonyms::SetConfig(BaseFTConfig* cfg) {
	for (auto& synonym : cfg->synonyms) {
		auto alternatives = std::make_shared<vector<wstring>>();
		for (auto alt : synonym.alternatives) {
			alternatives->push_back(utf8_to_utf16(toLower(alt)));
		}
		for (auto& token : synonym.tokens) {
			auto res = synonyms_.emplace(utf8_to_utf16(toLower(token)), alternatives);
			if (!res.second) {
				res.first->second->insert(res.first->second->end(), alternatives->begin(), alternatives->end());
			}
		}
	}
}

}  // namespace reindexer
