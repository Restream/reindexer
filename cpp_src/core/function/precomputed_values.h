#pragma once

#include <optional>
#include "function.h"

namespace reindexer::functions {

class [[nodiscard]] PrecomputedValues {
public:
	PrecomputedValues() = default;

	void Put(FunctionType functionType, Variant&& v);
	void Put(const functions::Now& f);

	std::optional<Variant> Get(const FunctionVariant& function) const;
	std::optional<int64_t> GetNowNsec() const;

private:
	std::optional<Variant> get(FunctionType type) const;

	std::unordered_map<FunctionType, Variant> values_;
};

}  // namespace reindexer::functions
