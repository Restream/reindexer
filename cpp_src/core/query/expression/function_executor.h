#pragma once

#include "tools/timetools.h"
#include "updates/updaterecord.h"

namespace reindexer {

class NamespaceImpl;
class NsContext;
struct ParsedQueryFunction;

class [[nodiscard]] QueryFunctionNow {
public:
	explicit QueryFunctionNow(TimeUnit unit) noexcept : unit_{unit} {}
	TimeUnit Unit() const noexcept { return unit_; }

private:
	TimeUnit unit_;
};

struct [[nodiscard]] QueryFunctionSerial {};

class [[nodiscard]] QueryFunction : private std::variant<QueryFunctionSerial, QueryFunctionNow> {
	using Base = std::variant<QueryFunctionSerial, QueryFunctionNow>;

public:
	explicit QueryFunction(const ParsedQueryFunction&);
	explicit QueryFunction(ParsedQueryFunction&&);

	Base& AsVariant() & { return *this; }
	const Base& AsVariant() const& { return *this; }

	std::string_view FieldName() const& noexcept { return fieldName_; }

	auto AsVariant() const&& = delete;
	auto FieldName() const&& = delete;

private:
	std::string fieldName_;
};

class [[nodiscard]] FunctionExecutor {
public:
	explicit FunctionExecutor(NamespaceImpl& ns, UpdatesContainer& replUpdates) noexcept : ns_(ns), replUpdates_(replUpdates) {}
	Variant Execute(const QueryFunction& funcData, const NsContext& ctx);

private:
	NamespaceImpl& ns_;
	UpdatesContainer& replUpdates_;
};

}  // namespace reindexer
