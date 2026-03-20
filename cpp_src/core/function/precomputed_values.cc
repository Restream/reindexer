#include "precomputed_values.h"

namespace reindexer::functions {

void PrecomputedValues::Put(FunctionType functionType, Variant&& v) {
	if (values_.count(functionType) == 0) {
		values_[functionType] = std::move(v);
	}
}

void PrecomputedValues::Put(const functions::Now&) {
	if (values_.count(FunctionType::FunctionNow) == 0) {
		// Saving Now() values in nanoseconds only (for future conversions to any unit).
		values_[FunctionType::FunctionNow] = Variant(Now(TimeUnit::nsec).Evaluate());
	}
}

std::optional<Variant> PrecomputedValues::Get(const FunctionVariant& function) const {
	return std::visit(overloaded{[&](const Serial& s) { return get(s.Type()); }, [&](const FlatArrayLen& f) { return get(f.Type()); },
								 [&](const Now& now) -> std::optional<Variant> {
									 auto t{get(FunctionNow)};
									 if (t.has_value()) {
										 return Variant{ConvertTime(t.value().As<int64_t>(), TimeUnit::nsec, now.Unit())};
									 }
									 return {};
								 }},
					  function);
}

std::optional<Variant> PrecomputedValues::get(FunctionType type) const {
	auto it = values_.find(type);
	if (it != values_.end()) {
		return it->second;
	}
	return {};
}

}  // namespace reindexer::functions
