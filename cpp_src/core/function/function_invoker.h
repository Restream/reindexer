#pragma once

#include "precomputed_values.h"
#include "updates/updaterecord.h"

namespace reindexer {
class NamespaceImpl;
class PayloadValue;
class NsContext;
}  // namespace reindexer

namespace reindexer::functions {

class [[nodiscard]] FunctionInvoker {
public:
	FunctionInvoker(const PrecomputedValues& pv, NamespaceImpl& ns, const PayloadType& pt, const TagsMatcher& tm,
					UpdatesContainer& replUpdates) noexcept
		: ns_(ns), pt_(pt), tm_(tm), replUpdates_(replUpdates), precomputedValues_(pv) {}

	FunctionInvoker(const FunctionInvoker&) = delete;
	FunctionInvoker& operator=(const FunctionInvoker&) = delete;
	FunctionInvoker(FunctionInvoker&&) = delete;
	FunctionInvoker& operator=(FunctionInvoker&&) = delete;

	Variant Invoke(const FunctionVariant& function, const NsContext& ctx, const PayloadValue& pv) const;

private:
	NamespaceImpl& ns_;
	const PayloadType& pt_;
	const TagsMatcher& tm_;
	UpdatesContainer& replUpdates_;
	const PrecomputedValues& precomputedValues_;
};

}  // namespace reindexer::functions
