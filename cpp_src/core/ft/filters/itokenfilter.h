#pragma once

#include <memory>
#include <string>
#include <vector>

#include "core/ft/ftdsl.h"

namespace reindexer {

class BaseFTConfig;

class [[nodiscard]] ITokenFilter {
public:
	using Ptr = std::unique_ptr<ITokenFilter>;
	typedef h_vector<FtDSLVariant, 32> ResultsStorage;

	virtual void GetVariants(const std::wstring& data, ResultsStorage& result, int proc,
							 fast_hash_map<std::wstring, size_t>& patternsUsed) = 0;
	virtual void SetConfig(BaseFTConfig*) {}
	virtual ~ITokenFilter() = default;
};

void AddOrUpdateVariant(ITokenFilter::ResultsStorage& variants, fast_hash_map<std::wstring, size_t>& patternsUsed, FtDSLVariant variant);

}  // namespace reindexer
