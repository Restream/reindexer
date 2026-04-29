#include "function.h"
#include "function_parser.h"

#include "core/cjson/jsonbuilder.h"
#include "core/namespace/namespaceimpl.h"
#include "core/payload/payloadiface.h"
#include "estl/tokenizer.h"
#include "tools/frozen_str_tools.h"
#include "tools/jsontools.h"
#include "tools/serilize/serializer.h"
#include "tools/serilize/wrserializer.h"
#include "vendor/frozen/unordered_map.h"
#include "vendor/gason/gason.h"

namespace reindexer {

namespace functions {

namespace {
template <std::size_t N>
constexpr auto MakeFunctionsMap(const std::pair<std::string_view, FunctionType> (&items)[N]) {
	return frozen::make_unordered_map<std::string_view, FunctionType>(items, frozen::nocase_hash_str{}, frozen::nocase_equal_str{});
}

constexpr static auto supportedFunctions = MakeFunctionsMap({
	{"flat_array_len", FunctionType::FunctionFlatArrayLen},
	{"now", FunctionType::FunctionNow},
	{"serial", FunctionType::FunctionSerial},
});

FunctionType NameToType(std::string_view name) {
	auto it = supportedFunctions.find(name);
	if (it == supportedFunctions.end()) {
		throw Error(errParams, "Function '{}' is not supported", name);
	}
	return it->second;
}
}  // namespace

FunctionVariant Function::FromSQL(std::string_view name, Tokenizer& tokenizer) {
	const FunctionType type{NameToType(name)};
	switch (type) {
		case FunctionType::FunctionFlatArrayLen:
			return FlatArrayLen::FromSQL(tokenizer);
		case FunctionType::FunctionNow:
			return Now::FromSQL(tokenizer);
		case FunctionType::FunctionSerial:
		default:
			throw Error(errParseSQL, "Function '{}' is not supported", name);
	}
}

FunctionVariant Function::FromExpression(std::string_view expr) {
	Tokenizer tokenizer{expr};
	auto name{tokenizer.NextToken()};
	return FromSQL(name.Text(), tokenizer);
}

FunctionVariant Function::FromJSON(const gason::JsonNode& node) {
	const auto name = node.findCaseInsensitive("name").As<std::string_view>();
	switch (NameToType(name)) {
		case FunctionType::FunctionFlatArrayLen:
			return FlatArrayLen::FromJSON(node);
		case FunctionType::FunctionNow:
			return Now::FromJSON(node);
		case FunctionType::FunctionSerial:
		default:
			throw Error(errParseSQL, "Function '{}' is not supported", name);
	}
}

void Function::GetJSON(builders::JsonBuilder& builder) const {
	auto function = builder.Object("function");
	function.Put("name", Name());

	if (!FieldNames().empty()) {
		auto fields = function.Array("fields");
		for (const auto& field : FieldNames()) {
			fields.Put(TagName::Empty(), field);
		}
	}

	if (!Arguments().empty()) {
		auto arguments = function.Array("arguments");
		for (const auto& arg : Arguments()) {
			arguments.Put(TagName::Empty(), arg);
		}
	}
}

void Function::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(FieldNames().size());
	for (const auto& field : FieldNames()) {
		ser.PutVString(field);
	}
	ser.PutVarUint(Arguments().size());
	for (const auto& arg : Arguments()) {
		ser.PutVariant(arg);
	}
	ser.PutVarUint(Type());
}

FunctionVariant Function::Deserialize(Serializer& ser) {
	auto numFields{ser.GetVarUInt()};
	h_vector<std::string, 1> fields;
	fields.reserve(numFields);
	while (numFields--) {
		fields.emplace_back(ser.GetVString());
	}
	VariantArray args;
	auto numValues{ser.GetVarUInt()};
	args.reserve(numValues);
	while (numValues--) {
		args.emplace_back(ser.GetVariant().EnsureHold());
	}
	const FunctionType type{FunctionType(ser.GetVarUInt())};
	return functions::Create(type, std::move(fields), std::move(args));
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

FlatArrayLen FlatArrayLen::FromSQL(Tokenizer& tokenizer) {
	auto tok = tokenizer.NextToken();
	if (tok.Text() != "(") {
		throw Error(errParseDSL, "Expected '(' after function  name, but found '{}', {}", tok.Text(), tokenizer.Where(tok));
	}
	tok = tokenizer.NextToken();
	if (tok.Type() != TokenName) {
		throw Error(errParseSQL, "Expected field name, but found '{}' in query, {}", tok.Text(), tokenizer.Where(tok));
	}
	std::string field{tok.Text()};
	tok = tokenizer.NextToken();
	if (tok.Text() != ")") {
		throw Error(errParseDSL, "Expected ')' in function call, but found '{}', {}", tok.Text(), tokenizer.Where(tok));
	}
	return FlatArrayLen{std::move(field)};
}

FlatArrayLen FlatArrayLen::FromJSON(const gason::JsonNode& node) {
	if (const auto name = node.findCaseInsensitive("name").As<std::string_view>(); NameToType(name) != FunctionType::FunctionFlatArrayLen) {
		throw Error(errParseDSL, "'flat_array_len' is expected as function name, but '{}' was provided", name);
	}
	return FlatArrayLen(readNodeValues<std::string, h_vector, 1>(node.findCaseInsensitive("fields")));
}

size_t FlatArrayLen::Evaluate(const PayloadValue& pv, const PayloadType& pt, const TagsMatcher& tm) const {
	ConstPayload item{pt, pv};
	std::string_view fieldName{Function::FieldNames()[0]};

	int fieldIndex = 0;
	if (pt.FieldByName(fieldName, fieldIndex)) {
		return item.GetFieldSize(FieldsSet{fieldIndex});
	}

	return item.GetFieldSize(fieldName, tm);
}

bool FlatArrayLen::Compare(CondType condition, const h_vector<int, 1>& expectedSize, size_t fieldSize) const {
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

Now::Now(ParsedFunction&& parsedFunction) : Function(FunctionNow) {
	if (!parsedFunction.funcArgs.empty() && !parsedFunction.funcArgs[0].empty()) {
		std::string_view timeUnit{parsedFunction.funcArgs[0]};
		arguments_ = {Variant(timeUnit)};
		timeUnit_ = ToTimeUnit(timeUnit);
	} else {
		timeUnit_ = TimeUnit::sec;
	}
}

Now Now::FromSQL(Tokenizer& tokenizer) {
	std::string timeUnit;
	auto tok = tokenizer.NextToken();
	if (tok.Text() != "(") {
		throw Error(errParseDSL, "Expected '(' after function  name, but found '{}', {}", tok.Text(), tokenizer.Where(tok));
	}
	tok = tokenizer.NextToken();
	if (tok.Type() == TokenName) {
		timeUnit = tok.Text();
		tok = tokenizer.NextToken();
	}
	if (tok.Text() != ")") {
		throw Error(errParseDSL, "Expected ')' in function call, but found '{}', {}", tok.Text(), tokenizer.Where(tok));
	}
	if (timeUnit.empty()) {
		return Now{};
	}
	return Now{timeUnit};
}

Now Now::FromJSON(const gason::JsonNode& node) {
	if (const auto name = node.findCaseInsensitive("name").As<std::string_view>(); NameToType(name) != FunctionType::FunctionNow) {
		throw Error(errParseDSL, "'now' is expected as function name, but '{}' was provided", name);
	}
	return Now{readNodeValues<std::string, h_vector, 1>(node.findCaseInsensitive("arguments"))};
}

std::string Now::ToString() const {
	WrSerializer ser;
	ser << Name() << '(';
	ser << TimeUnitToString(timeUnit_);
	ser << ')';
	return std::string{ser.Slice()};
}

Serial::Serial(ParsedFunction&& parsedFunction) : Serial(std::move(parsedFunction.field)) {}

template <typename UpdatesContainer>
int64_t Serial::Evaluate(NamespaceImpl& ns, UpdatesContainer& replUpdates, const NsContext& ctx) const {
	int indexField{0};
	std::string_view fieldName{comparisonField_};
	if (ns.tryGetIndexByNameOrJsonPath(comparisonField_, indexField)) {
		fieldName = ns.indexes_[indexField]->Name();
	}
	return ns.GetSerial(fieldName, replUpdates, ctx);
}

FunctionVariant Create(ParsedFunction&& parsedFunction) {
	const FunctionType type{NameToType(parsedFunction.funcName)};
	switch (type) {
		case FunctionType::FunctionFlatArrayLen:
			return FlatArrayLen(parsedFunction.funcArgs);
		case FunctionType::FunctionNow:
			return Now{std::move(parsedFunction)};
		case FunctionType::FunctionSerial:
			return Serial{std::move(parsedFunction)};
		default:
			throw Error{errParams, "Function '{}' is not supported", parsedFunction.funcName};
	}
}

std::string_view TypeToName(FunctionType type) noexcept {
	switch (type) {
		case FunctionType::FunctionFlatArrayLen:
			return "flat_array_len";
		case FunctionType::FunctionNow:
			return "now";
		case FunctionType::FunctionSerial:
			return "serial";
	}
	return {};
}

FunctionType FunctionVariantType(const FunctionVariant& f) {
	return std::visit([](const auto& f) { return f.Type(); }, f);
}

template int64_t Serial::Evaluate<reindexer::UpdatesContainer>(NamespaceImpl& ns, reindexer::UpdatesContainer& replUpdates,
															   const NsContext& ctx) const;

}  // namespace functions
}  // namespace reindexer
