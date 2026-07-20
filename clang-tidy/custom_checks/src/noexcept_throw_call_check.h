#pragma once

#include <clang-tidy/ClangTidyCheck.h>
#include <clang/ASTMatchers/ASTMatchFinder.h>

#include <string>
#include <vector>

namespace clang {
namespace tidy {
namespace reindexer_checks {

/// Warns when a `noexcept` function calls (directly or through in-TU callees)
/// a non-`noexcept` function whose name contains a configured substring
/// (case-insensitive, default: "throw"). Matches that fall entirely inside one of
/// the configured \c ExcludeSubstrings (default: "nothrow") are ignored.
class NoexceptThrowCallCheck : public ClangTidyCheck {
public:
	NoexceptThrowCallCheck(StringRef Name, ClangTidyContext* Context);
	void storeOptions(ClangTidyOptions::OptionMap& Opts) override;
	bool isLanguageVersionSupported(const LangOptions& LangOpts) const override {
		return LangOpts.CPlusPlus;
	}
	void registerMatchers(ast_matchers::MatchFinder* Finder) override;
	void check(const ast_matchers::MatchFinder::MatchResult& Result) override;

private:
	std::vector<std::string> KeywordsLower;
	std::string KeywordsOptionRaw;
	std::vector<std::string> ExcludeSubstringsLower;
	std::string ExcludeSubstringsOptionRaw;
};

}  // namespace reindexer_checks
}  // namespace tidy
}  // namespace clang
