#include "function_executor.h"
#include "core/namespace/namespaceimpl.h"
#include "function_parser.h"
#include "tools/timetools.h"

namespace reindexer {

QueryFunction::QueryFunction(const ParsedQueryFunction& parsed) : QueryFunction(ParsedQueryFunction{parsed}) {}

QueryFunction::QueryFunction(ParsedQueryFunction&& parsed) : Base{QueryFunctionSerial{}} {
	using namespace std::string_view_literals;

	if (iequals(parsed.funcName, "serial"sv)) {
		fieldName_ = std::move(parsed.field);
	} else if (iequals(parsed.funcName, "now"sv)) {
		if (!parsed.funcArgs.empty() && !parsed.funcArgs.front().empty()) {
			AsVariant().emplace<QueryFunctionNow>(ToTimeUnit(parsed.funcArgs.front()));
		} else {
			AsVariant().emplace<QueryFunctionNow>(TimeUnit::sec);
		}
	} else {
		throw Error(errParams, "Unknown function '{}'. Field: '{}'", parsed.funcName, parsed.field);
	}
}

Variant FunctionExecutor::Execute(const QueryFunction& func, const NsContext& ctx) {
	return std::visit(overloaded{[&](const QueryFunctionSerial&) {
									 int index = 0;
									 std::string_view fieldName = func.FieldName();
									 if (ns_.tryGetIndexByNameOrJsonPath(func.FieldName(), index)) {
										 fieldName = ns_.indexes_[index]->Name();
									 }
									 return Variant(ns_.GetSerial(fieldName, replUpdates_, ctx));
								 },
								 [&](const QueryFunctionNow& now) { return Variant(getTimeNow(now.Unit())); }},
					  func.AsVariant());
}

}  // namespace reindexer
