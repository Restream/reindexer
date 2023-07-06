#pragma once

#include <memory>
#include <string>
#include <vector>

#include "core/ft/ftdsl.h"

namespace reindexer {

class BaseFTConfig;

struct SynonymsDsl {
	SynonymsDsl(FtDSLQuery&& dsl_, const std::vector<size_t>& termsIdx_) : dsl{std::move(dsl_)}, termsIdx{termsIdx_} {}
	FtDSLQuery dsl;
	std::vector<size_t> termsIdx;
};

class ITokenFilter {
public:
	using Ptr = std::unique_ptr<ITokenFilter>;

	virtual void GetVariants(const std::wstring& data, std::vector<FtDSLVariant>& result, int proc) = 0;
	virtual void SetConfig(BaseFTConfig*) {}
	virtual void PreProcess(const FtDSLQuery&, std::vector<SynonymsDsl>&, int /*proc*/) const {}
	virtual void PostProcess(const FtDSLEntry&, const FtDSLQuery&, size_t /*termIdx*/, std::vector<SynonymsDsl>&, int /*proc*/) const {}
	virtual ~ITokenFilter() {}
};

}  // namespace reindexer
