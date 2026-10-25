#include <clang-tidy/ClangTidyModule.h>
#include <clang/AST/ASTContext.h>
#include <clang/AST/DeclCXX.h>
#include <clang/AST/ExprCXX.h>
#include <clang/AST/Type.h>
#include <clang/ASTMatchers/ASTMatchers.h>
#include <clang/Basic/Diagnostic.h>
#include <clang/Basic/SourceLocation.h>
#include "clang/AST/Attr.h"

#include "nodiscard_check.h"

using namespace clang;
using namespace clang::ast_matchers;

namespace clang {
namespace tidy {
namespace reindexer_checks {

void NoDiscardDefinitionCheck::registerMatchers(MatchFinder* Finder) {
	Finder->addMatcher(cxxRecordDecl(isDefinition(), unless(isImplicit())).bind("record"), this);
	Finder->addMatcher(enumDecl(isDefinition(), unless(isImplicit())).bind("enum"), this);
}

static bool isInExternalNamespace(const Decl* D) {
	const DeclContext* DC = D->getDeclContext();
	while (DC) {
		if (const auto* NS = dyn_cast<NamespaceDecl>(DC)) {
			if (NS->getIdentifier() && (NS->getName() == "std" || NS->getName() == "fmt")) {
				return true;
			}
		}
		DC = DC->getParent();
	}
	return false;
}

static bool isInSkippedDirectory(const SourceManager& SM, SourceLocation Loc) {
	if (Loc.isInvalid()) {
		return false;
	}
	SourceLocation SpellingLoc = SM.getSpellingLoc(Loc);
	StringRef Filename = SM.getFilename(SpellingLoc);
	if (Filename.empty()) {
		return false;
	}
	return Filename.contains("/vendor/") || Filename.contains("/_cmrc/") || Filename.contains("/cbinding/");
}

void NoDiscardDefinitionCheck::check(const MatchFinder::MatchResult& Result) {
	if (const auto* Record = Result.Nodes.getNodeAs<CXXRecordDecl>("record")) {
		if (!Record->getIdentifier() || Record->isAnonymousStructOrUnion() || isInExternalNamespace(Record)) {
			return;
		}
		if (isInExternalNamespace(Record)) {
			return;
		}
		if (isInSkippedDirectory(*Result.SourceManager, Record->getLocation())) {
			return;
		}
		if (!Record->hasAttr<WarnUnusedResultAttr>() && !Record->hasAttr<UnusedAttr>()) {
			diag(Record->getLocation(), "struct/class definition should be marked with either [[nodiscard]] or [[maybe_unused]]")
				<< Record->getDeclName();
		}
	}

	if (const auto* Enum = Result.Nodes.getNodeAs<EnumDecl>("enum")) {
		if (!Enum->getIdentifier()) {
			return;
		}
		if (isInExternalNamespace(Enum)) {
			return;
		}
		if (isInSkippedDirectory(*Result.SourceManager, Enum->getLocation())) {
			return;
		}

		if (!Enum->hasAttr<WarnUnusedResultAttr>() && !Enum->hasAttr<UnusedAttr>()) {
			diag(Enum->getLocation(), "enum definition should be marked with either [[nodiscard]] or [[maybe_unused]]")
				<< Enum->getDeclName();
		}
	}
}

}  // namespace reindexer_checks
}  // namespace tidy
}  // namespace clang
