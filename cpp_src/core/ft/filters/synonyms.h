#pragma once

#include "estl/fast_hash_map.h"
#include "itokenfilter.h"
#include "tools/stringstools.h"

namespace reindexer {

class Synonyms : public ITokenFilter {
public:
	Synonyms();
	virtual void GetVariants(const wstring& data, std::vector<std::pair<std::wstring, int>>& result) override final;
	void SetConfig(BaseFTConfig* cfg) override final;

private:
	fast_hash_map<wstring, std::shared_ptr<vector<wstring>>> synonyms_;
};
}  // namespace reindexer
