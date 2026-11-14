#pragma once

#include <clang-tidy/ClangTidy.h>
#include <clang-tidy/ClangTidyCheck.h>
#include <clang/ASTMatchers/ASTMatchFinder.h>

namespace clang {
namespace tidy {

namespace reindexer_checks {

class NoDiscardDefinitionCheck : public ClangTidyCheck {
public:
	NoDiscardDefinitionCheck(StringRef Name, ClangTidyContext* Context) : ClangTidyCheck(Name, Context) {}
	void registerMatchers(::clang::ast_matchers::MatchFinder* Finder) override;
	void check(const ::clang::ast_matchers::MatchFinder::MatchResult& Result) override;
};

}  // namespace reindexer_checks
}  // namespace tidy
}  // namespace clang
