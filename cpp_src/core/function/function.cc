#include "function.h"

#include "core/cjson/jsonbuilder.h"
#include "estl/tokenizer.h"
#include "tools/frozen_str_tools.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"
#include "vendor/frozen/unordered_map.h"
#include "vendor/gason/gason.h"

namespace reindexer {

namespace functions {

namespace {
template <std::size_t N>
constexpr auto MakeFunctionsMap(const std::pair<std::string_view, FunctionType> (&items)[N]) {
	return frozen::make_unordered_map<std::string_view, FunctionType>(items, frozen::nocase_hash_str{}, frozen::nocase_equal_str{});
}

constexpr static auto supportedFunctions = MakeFunctionsMap({{"flat_array_len", FunctionType::FunctionFlatArrayLen}});

std::string_view TypeToName(FunctionType type) noexcept {
	switch (type) {
		case FunctionType::FunctionFlatArrayLen:
			return "flat_array_len";
	}
	return {};
}

FunctionType NameToType(std::string_view name) {
	auto it = supportedFunctions.find(name);
	if (it == supportedFunctions.end()) {
		throw Error(errParams, "Function '{}' is not supported.", name);
	}
	return it->second;
}
}  // namespace

FunctionVariant Function::FromSQL(std::string_view name, Tokenizer& parser) {
	if (NameToType(name) == FunctionType::FunctionFlatArrayLen) {
		return FlatArrayLen::FromSQL(parser);
	}
	throw Error(errParseSQL, "Function '{}' is not supported.", name);
}

FunctionVariant Function::FromJSON(const gason::JsonNode& node) {
	const auto name = node.findCaseInsensitive("name").As<std::string_view>();
	if (NameToType(name) == FunctionType::FunctionFlatArrayLen) {
		return FlatArrayLen::FromJSON(node);
	}
	throw Error(errParseDSL, "Function '{}' is not supported.", name);
}

void Function::GetJSON(builders::JsonBuilder& builder) const {
	auto function = builder.Object("function");
	function.Put("name", Name());

	auto fields = function.Array("fields");
	for (const auto& field : FieldNames()) {
		fields.Put(TagName::Empty(), field);
	}
	fields.End();
}

std::string_view Function::Name() const noexcept { return TypeToName(type_); }

std::string Function::ToString() const {
	WrSerializer ser;
	ser << Name() << '(';
	for (size_t i = 0; i < FieldNames().size(); ++i) {
		if (i) {
			ser << ',';
		}
		ser << FieldNames()[i];
	}
	ser << ')';
	return std::string{ser.Slice()};
}

FlatArrayLen FlatArrayLen::FromSQL(Tokenizer& parser) {
	auto tok = parser.NextToken();
	if (tok.Text() != "(") {
		throw Error(errParseDSL, "Expected '(' after function  name, but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	tok = parser.NextToken();
	if (tok.Type() != TokenName) {
		throw Error(errParseSQL, "Expected field name, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
	}
	std::string field{tok.Text()};
	tok = parser.NextToken();
	if (tok.Text() != ")") {
		throw Error(errParseDSL, "Expected ')' in function call, but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	return FlatArrayLen{std::move(field)};
}

FlatArrayLen FlatArrayLen::FromJSON(const gason::JsonNode& node) {
	std::string field;
	const auto fieldsNode = node.findCaseInsensitive("fields");
	if (!fieldsNode.empty()) {
		if (fieldsNode.value.getTag() == gason::JsonTag::ARRAY) {
			size_t itemsCount = 0;
			for (const auto& item : fieldsNode) {
				if (++itemsCount > 1) {
					throw Error(errParseDSL, "'flat_array_len' expects only 1 field as function argument, but found {}", itemsCount);
				}
				field = item.As<std::string>();
			}
		} else if (fieldsNode.value.getTag() == gason::JsonTag::STRING) {
			field = fieldsNode.As<std::string>();
		}
	}
	if (const auto name = node.findCaseInsensitive("name").As<std::string_view>(); NameToType(name) != FunctionType::FunctionFlatArrayLen) {
		throw Error(errParseDSL, "'flat_array_len' is expected as function name, but found {}", name);
	}
	return FlatArrayLen(std::move(field));
}

bool FlatArrayLen::Evaluate(CondType condition, const h_vector<int, 1>& expectedSize, size_t fieldSize) const {
	const int size{static_cast<int>(fieldSize)};
	switch (condition) {
		case CondGt:
			return (expectedSize.size() == 1 && size > expectedSize[0]);
		case CondGe:
			return (expectedSize.size() == 1 && size >= expectedSize[0]);
		case CondLt:
			return (expectedSize.size() == 1 && size < expectedSize[0]);
		case CondLe:
			return (expectedSize.size() == 1 && size <= expectedSize[0]);
		case CondRange:
			return (expectedSize.size() == 2 && size >= expectedSize[0] && size <= expectedSize[1]);
		case CondSet:
		case CondEq:
			for (const auto& s : expectedSize) {
				if (size == s) {
					return true;
				}
			}
			return false;
		case CondAny:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
		case CondKnn:
		case CondAllSet:
			throw Error(errQueryExec, "Condition {} is not supported by flat_array_len()", CondTypeToStrShort(condition));
	}
	return false;
}

}  // namespace functions
}  // namespace reindexer
