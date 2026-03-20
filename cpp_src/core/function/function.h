#pragma once

#include "core/function/error.h"
#include "core/keyvalue/variant.h"
#include "core/type_consts.h"
#include "estl/concepts.h"
#include "estl/h_vector.h"
#include "tools/timetools.h"

#include <string>
#include <variant>

namespace gason {
struct JsonNode;
}

namespace reindexer {

class TagsMatcher;
class Tokenizer;
class NamespaceImpl;
class NsContext;

namespace builders {
class JsonBuilder;
}
}  // namespace reindexer

namespace reindexer::functions {

class FlatArrayLen;
class Now;
class Serial;
using FunctionVariant = std::variant<FlatArrayLen, Now, Serial>;

struct ParsedFunction;

class [[nodiscard]] Function {
public:
	explicit Function(FunctionType type) : type_(type) {}

	template <concepts::ConvertibleToString Field>
	Function(FunctionType type, Field&& field) : type_(type), fields_{{std::forward<Field>(field)}} {}

	template <concepts::StringContainer Fields>
	Function(FunctionType type, Fields&& fields)
		: type_(type), fields_{std::make_move_iterator(std::begin(fields)), std::make_move_iterator(std::end(fields))} {}

	template <concepts::StringContainer Fields>
	Function(FunctionType type, const Fields& fields) : type_(type), fields_{fields.begin(), fields.end()} {}

	template <concepts::StringContainer Fields>
	Function(FunctionType type, Fields&& fields, VariantArray&& args)
		: type_(type),
		  fields_{std::make_move_iterator(std::begin(fields)), std::make_move_iterator(std::end(fields))},
		  arguments_{std::move(args)} {}

	template <concepts::StringContainer Fields>
	Function(FunctionType type, const Fields& fields, const VariantArray& args)
		: type_(type), fields_{fields.begin(), fields.end()}, arguments_{args} {}

	Function(FunctionType type, VariantArray&& args) noexcept : type_(type), arguments_{std::move(args)} {}
	Function(FunctionType type, const VariantArray& args) : type_(type), arguments_{args} {}

	template <std::input_iterator InputIt>
	Function(FunctionType type, InputIt firstArg, InputIt lastArg) : type_(type) {
		if constexpr (std::forward_iterator<InputIt>) {
			arguments_.reserve(std::distance(firstArg, lastArg));
		}
		for (; firstArg != lastArg; ++firstArg) {
			using ArgType = decltype(*firstArg);
			if constexpr (std::is_same_v<std::decay_t<ArgType>, Variant>) {
				arguments_.emplace_back(std::move(*firstArg));
			} else {
				arguments_.emplace_back(Variant(std::move(*firstArg)));
			}
		}
	}

	virtual ~Function() = default;

	Function(const Function&) = default;
	Function& operator=(const Function&) = default;
	Function(Function&&) = default;
	Function& operator=(Function&&) = default;

	bool operator==(const Function& other) const noexcept = default;

	static FunctionVariant FromSQL(std::string_view name, Tokenizer& tokenizer);
	static FunctionVariant FromExpression(std::string_view expr);
	static FunctionVariant FromJSON(const gason::JsonNode& node);
	void GetJSON(builders::JsonBuilder& builder) const;

	void Serialize(WrSerializer& ser) const;
	static FunctionVariant Deserialize(Serializer& ser);

	const h_vector<std::string, 1>& FieldNames() const& noexcept { return fields_; }
	const VariantArray& Arguments() const& noexcept { return arguments_; }
	FunctionType Type() const noexcept { return type_; }
	std::string_view Name() const noexcept;

	virtual std::string ToString() const;

	const h_vector<std::string, 1>& FieldNames() const&& = delete;
	const VariantArray& Arguments() const&& = delete;

protected:
	FunctionType type_;
	h_vector<std::string, 1> fields_;
	VariantArray arguments_;
};

class [[nodiscard]] FlatArrayLen : public Function {
public:
	template <concepts::ConvertibleToString Str>
	explicit FlatArrayLen(Str&& field) : Function(FunctionFlatArrayLen, std::forward<Str>(field)) {}

	template <concepts::StringContainer Fields>
	explicit FlatArrayLen(Fields&& fields) : Function(FunctionFlatArrayLen, std::forward<Fields>(fields)) {
		if (FieldNames().size() != 1) {
			errors::throwExpectsOneArgumentError(errParams, Name(), FieldNames().size());
		}
	}

	static FlatArrayLen FromSQL(Tokenizer& tokenizer);
	static FlatArrayLen FromJSON(const gason::JsonNode& node);

	size_t Evaluate(const PayloadValue& pv, const PayloadType& pt, const TagsMatcher& tm) const;
	bool Compare(CondType condition, const h_vector<int, 1>& expectedValues, size_t fieldSize) const;
};

class [[nodiscard]] Now : public Function {
public:
	Now() : Function(FunctionNow, {Variant(TimeUnitToString(TimeUnit::sec))}), timeUnit_(TimeUnit::sec) {}

	explicit Now(TimeUnit timeUnit) : Function(FunctionNow, {Variant(TimeUnitToString(timeUnit))}), timeUnit_(timeUnit) {}

	explicit Now(std::string_view timeUnit) : Function(FunctionNow, {Variant(timeUnit)}), timeUnit_(ToTimeUnit(timeUnit)) {}

	template <concepts::StringContainer Args>
	explicit Now(Args&& args) : Now(std::begin(args), std::end(args)) {}

	explicit Now(VariantArray&& args) : Now(std::make_move_iterator(std::begin(args)), std::make_move_iterator(std::end(args))) {}

	template <std::input_iterator InputIt>
	Now(InputIt firstArg, InputIt lastArg) : Function(FunctionNow, firstArg, lastArg) {
		if (Arguments().size() > 1) {
			errors::throwExpectsOneOrZeroArgumentsError(errParams, Name(), Arguments().size());
		}
		if (Arguments().empty()) {
			timeUnit_ = TimeUnit::sec;
		} else {
			timeUnit_ = ToTimeUnit(Arguments().front().As<std::string>());
		}
	}

	explicit Now(ParsedFunction&& parsedFunction);

	Now(const Now&) = default;
	Now& operator=(const Now&) = default;
	Now(Now&&) = default;
	Now& operator=(Now&&) = default;

	static Now FromSQL(Tokenizer& tokenizer);
	static Now FromJSON(const gason::JsonNode& node);

	int64_t Evaluate() const noexcept { return getTimeNow(timeUnit_); }
	TimeUnit Unit() const noexcept { return timeUnit_; }

	std::string ToString() const override;

private:
	TimeUnit timeUnit_;
};

class [[nodiscard]] Serial : public Function {
public:
	explicit Serial(ParsedFunction&& parsedFunction);

	template <concepts::ConvertibleToString Str>
	explicit Serial(Str&& forField) : Function(FunctionSerial), comparisonField_(std::forward<Str>(forField)) {}

	template <concepts::StringContainer Fields>
	explicit Serial(Fields&& fields) : Function(FunctionSerial) {
		if (fields.size() != 1 || std::ranges::empty(fields[0])) {
			errors::throwIncorrectFieldsError(errParams, Name(), fields.size());
		}
		comparisonField_ = fields[0];
	}

	Serial(const Serial&) = default;
	Serial& operator=(const Serial&) = default;
	Serial(Serial&&) = default;
	Serial& operator=(Serial&&) = default;

	template <typename UpdatesContainer>
	int64_t Evaluate(NamespaceImpl& ns, UpdatesContainer& replUpdates, const NsContext& ctx) const;

private:
	std::string comparisonField_;
};

template <concepts::StringContainer Fields>
FunctionVariant Create(FunctionType type, Fields&& fields, VariantArray&& args) {
	switch (type) {
		case FunctionType::FunctionFlatArrayLen:
			return FlatArrayLen(std::forward<Fields>(fields));
		case FunctionType::FunctionNow:
			return Now(std::move(args));
		case FunctionType::FunctionSerial:
			return Serial(std::forward<Fields>(fields));
		default:
			errors::throwTypeIsNotSupportedError(errParams, int(type));
	}
}

FunctionVariant Create(ParsedFunction&& parsedFunction);

FunctionType FunctionVariantType(const FunctionVariant& f);
std::string_view TypeToName(FunctionType type) noexcept;

}  // namespace reindexer::functions

namespace reindexer {
namespace concepts {
template <typename T>
concept Function = std::derived_from<typename std::remove_cvref<T>::type, functions::Function>;
}  // namespace concepts
}  // namespace reindexer
