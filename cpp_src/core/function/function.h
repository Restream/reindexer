#pragma once

#include <string>
#include <variant>
#include "core/type_consts.h"
#include "estl/concepts.h"
#include "estl/forward_like.h"
#include "estl/h_vector.h"
#include "tools/errors.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

class Tokenizer;

namespace builders {
class JsonBuilder;
}

namespace functions {

class FlatArrayLen;
using FunctionVariant = std::variant<FlatArrayLen>;

class [[nodiscard]] Function {
public:
	template <concepts::ConvertibleToString Str>
	Function(FunctionType type, Str&& field) : type_(type), fields_{{std::forward<Str>(field)}} {}

	template <concepts::StringContainer Fields>
	Function(FunctionType type, Fields&& fields)
		: type_(type), fields_{std::make_move_iterator(std::begin(fields)), std::make_move_iterator(std::end(fields))} {}

	template <concepts::StringContainer Fields>
	Function(FunctionType type, const Fields& fields) : type_(type), fields_{fields.begin(), fields.end()} {}

	Function(const Function&) = default;
	Function& operator=(const Function&) = default;
	Function(Function&&) = default;
	Function& operator=(Function&&) = default;

	bool operator==(const Function& other) const noexcept = default;

	static FunctionVariant FromSQL(std::string_view name, Tokenizer& parser);
	static FunctionVariant FromJSON(const gason::JsonNode& node);
	void GetJSON(builders::JsonBuilder& builder) const;

	const h_vector<std::string, 1>& FieldNames() const noexcept { return fields_; }
	FunctionType Type() const noexcept { return type_; }
	std::string_view Name() const noexcept;

	std::string ToString() const;

private:
	FunctionType type_;
	h_vector<std::string, 1> fields_;
};

class [[nodiscard]] FlatArrayLen : public Function {
public:
	template <concepts::ConvertibleToString Str>
	explicit FlatArrayLen(Str&& field) : Function(FunctionFlatArrayLen, std::forward<Str>(field)) {}

	FlatArrayLen(const FlatArrayLen&) = default;
	FlatArrayLen& operator=(const FlatArrayLen&) = default;
	FlatArrayLen(FlatArrayLen&&) = default;
	FlatArrayLen& operator=(FlatArrayLen&&) = default;

	static FlatArrayLen FromSQL(Tokenizer& parser);
	static FlatArrayLen FromJSON(const gason::JsonNode& node);

	bool Evaluate(CondType condition, const h_vector<int, 1>& expectedValues, size_t fieldSize) const;
};

template <concepts::StringContainer Fields>
FunctionVariant Create(FunctionType type, Fields&& fields) {
	if (type == FunctionType::FunctionFlatArrayLen) {
		if (fields.size() != 1) {
			throw Error(errParams, "'flat_array_len' expects only 1 argument, but found {}", fields.size());
		}
		return FlatArrayLen(forward_like<Fields>(*std::begin(fields)));
	}
	throw Error(errLogic, "Function type {} is not supported yet.", int(type));
}

}  // namespace functions

namespace concepts {
template <typename T>
concept Function = std::derived_from<T, functions::Function>;
}

}  // namespace reindexer
