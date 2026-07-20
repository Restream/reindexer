#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace reindexer {
class Token;
class Tokenizer;
class VariantArray;
}  // namespace reindexer

namespace reindexer::functions {

struct [[nodiscard]] ParsedFunction {
	std::string field;
	std::string value;
	std::string funcName;
	std::vector<std::string> funcArgs;
	std::unordered_map<std::string, std::string> namedArgs;
	bool isFunction{false};
};

class [[nodiscard]] FunctionParser {
public:
	static ParsedFunction Parse(std::string_view query);
	static ParsedFunction ParseFunction(Tokenizer& parser, Token& tok);
	static bool IsFunction(std::string_view val) noexcept;
	static bool IsFunction(const VariantArray& val) noexcept;

private:
	struct [[nodiscard]] Args {
		explicit Args(unsigned int p, std::unordered_set<std::string_view>&& n) : posArgsCount(p), namedArgs(std::move(n)) {}
		unsigned int posArgsCount;
		std::unordered_set<std::string_view> namedArgs;
	};

	static void parseFunction(Tokenizer& parser, ParsedFunction&, Token& tok);
	static void parseFunctionImpl(Tokenizer& parser, ParsedFunction&, Token& tok);
	static void parsePositionalAndNamedArgs(Tokenizer& parser, ParsedFunction&, const Args& args);
};

}  // namespace reindexer::functions
