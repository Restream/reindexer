#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace reindexer {

class token;
class tokenizer;
class VariantArray;

struct [[nodiscard]] ParsedQueryFunction {
	std::string field;
	std::string value;
	std::string funcName;
	std::vector<std::string> funcArgs;
	std::unordered_map<std::string, std::string> namedArgs;
	bool isFunction{false};
};

class [[nodiscard]] QueryFunctionParser {
public:
	static ParsedQueryFunction Parse(std::string_view query);
	static ParsedQueryFunction ParseFunction(tokenizer& parser, token& tok);
	[[nodiscard]] static bool IsFunction(std::string_view val) noexcept;
	[[nodiscard]] static bool IsFunction(const VariantArray& val) noexcept;

private:
	struct [[nodiscard]] Args {
		explicit Args(unsigned int p, std::unordered_set<std::string_view>&& n) : posArgsCount(p), namedArgs(std::move(n)) {}
		unsigned int posArgsCount;
		std::unordered_set<std::string_view> namedArgs;
	};

	static void parseFunction(tokenizer& parser, ParsedQueryFunction&, token& tok);
	static void parseFunctionImpl(tokenizer& parser, ParsedQueryFunction&, token& tok);
	static void parsePositionalAndNamedArgs(tokenizer& parser, ParsedQueryFunction&, const Args& args);
};

}  // namespace reindexer
