#include "functions_comparator.h"
#include "helpers.h"

namespace reindexer {

namespace {

template <CondType Cond>
std::string ConditionToString(const VariantArray& values) {
	std::string value;
	if (!values.empty()) {
		value = values[0].As<std::string>();
	}
	return fmt::format("{} {}", comparators::CondToStr<Cond>(), value);
}

}  // namespace

std::string FunctionsComparatorImplBase::ConditionStr() const {
	switch (cond_) {
		case CondGt:
			return ConditionToString<CondGt>(values_);
		case CondGe:
			return ConditionToString<CondGe>(values_);
		case CondLt:
			return ConditionToString<CondLt>(values_);
		case CondLe:
			return ConditionToString<CondLe>(values_);
		case CondSet:
		case CondEq:
			if (values_.empty()) {
				return "IN []";
			} else {
				if (values_.size() == 1) {
					return ConditionToString<CondEq>(values_);
				}
				return fmt::format("IN [{} ...]", values_.cbegin()->As<std::string>());
			}
		case CondRange:
			if (values_.size() == 2) {
				return fmt::format("RANGE({} {})", values_[0].As<std::string>(), values_[1].As<std::string>());
			}
			break;
		case CondAny:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
		case CondKnn:
			// Not supported by functions comparator yet.
			break;
	}
	return {};
}

}  // namespace reindexer
