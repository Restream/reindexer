#pragma once

#include <memory>
#include <string>
#include <vector>

namespace reindexer {

class BaseFTConfig;
class ITokenFilter {
public:
	using Ptr = std::unique_ptr<ITokenFilter>;

	virtual void GetVariants(const std::wstring& data, std::vector<std::pair<std::wstring, int>>& result) = 0;
	virtual void SetConfig(BaseFTConfig*){};
	virtual ~ITokenFilter() {}
};

}  // namespace reindexer
