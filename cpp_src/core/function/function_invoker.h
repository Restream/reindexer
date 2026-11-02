#pragma once

#include "core/item_tree/data_extractor.h"
#include "precomputed_values.h"
#include "updates/updaterecord.h"

namespace reindexer {
class NamespaceImpl;
class PayloadValue;
class NsContext;
class IItemValuesExtractor;
}  // namespace reindexer

namespace reindexer::functions {

class [[nodiscard]] FunctionInvoker {
public:
	FunctionInvoker(const PrecomputedValues& pv, NamespaceImpl& ns, const PayloadType& pt, UpdatesContainer& replUpdates) noexcept
		: ns_(ns), pt_(pt), replUpdates_(replUpdates), precomputedValues_(pv) {}

	FunctionInvoker(const FunctionInvoker&) = delete;
	FunctionInvoker& operator=(const FunctionInvoker&) = delete;
	FunctionInvoker(FunctionInvoker&&) = delete;
	FunctionInvoker& operator=(FunctionInvoker&&) = delete;

	Variant Invoke(const FunctionVariant& function, const NsContext& ctx, const IItemValuesExtractor& de) const;

private:
	NamespaceImpl& ns_;
	const PayloadType& pt_;
	UpdatesContainer& replUpdates_;
	const PrecomputedValues& precomputedValues_;
};

}  // namespace reindexer::functions
