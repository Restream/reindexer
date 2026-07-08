#include "function_invoker.h"
#include "core/namespace/namespaceimpl.h"
#include "core/payload/payloadvalue.h"

namespace reindexer {
namespace functions {

Variant FunctionInvoker::Invoke(const FunctionVariant& function, const NsContext& ctx, const PayloadValue& pv) const {
	if (auto v = precomputedValues_.Get(function); v.has_value()) {
		return v.value();
	}
	return std::visit(
		overloaded{[&](const FlatArrayLen& flatArrayLen) { return Variant(static_cast<int>(flatArrayLen.Evaluate(pv, pt_, tm_))); },
				   [&](const Serial& serial) { return Variant(serial.Evaluate(ns_, replUpdates_, ctx)); },
				   [&](const Now& now) { return Variant(now.Evaluate()); }},
		function);
}

}  // namespace functions
}  // namespace reindexer
