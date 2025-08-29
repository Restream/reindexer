#pragma once

#include "itokenfilter.h"

namespace reindexer {

class [[nodiscard]] CompositeWordsSplitter final : public ITokenFilter {
public:
	CompositeWordsSplitter(SplitOptions opts) : opts_(opts) {}
	void GetVariants(const std::wstring& data, ITokenFilter::ResultsStorage& result, int proc,
					 fast_hash_map<std::wstring, size_t>& patternsUsed) override final;

private:
	const SplitOptions opts_;
};

}  // namespace reindexer
