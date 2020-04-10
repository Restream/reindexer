#pragma once

#include <memory>
#include <string>
#include <vector>

namespace reindexer {

class BaseFTConfig;
class FtDSLQuery;
struct FtDSLEntry;
class ITokenFilter {
public:
	using Ptr = std::unique_ptr<ITokenFilter>;

	virtual void GetVariants(const std::wstring& data, std::vector<std::pair<std::wstring, int>>& result) = 0;
	virtual void SetConfig(BaseFTConfig*) {}
	virtual void PreProcess(FtDSLQuery&) const {}
	virtual void PostProcess(const FtDSLEntry&, FtDSLQuery&) const {}
	virtual ~ITokenFilter() {}
};

}  // namespace reindexer
