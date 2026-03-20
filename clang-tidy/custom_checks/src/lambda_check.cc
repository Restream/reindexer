#include <clang/AST/ASTContext.h>
#include <clang/AST/DeclCXX.h>
#include <clang/AST/ExprCXX.h>
#include <clang/AST/Type.h>
#include <clang/ASTMatchers/ASTMatchers.h>
#include <clang/Basic/Diagnostic.h>
#include <clang/Basic/SourceLocation.h>

#include "lambda_check.h"

using namespace clang;
using namespace clang::ast_matchers;

namespace clang {
namespace tidy {
namespace reindexer_checks {

void LambdaToStdFunctionAllocationCheck::registerMatchers(MatchFinder* Finder) {
	// Match the constructor call of std::function that takes exactly one argument
	// whose type (after stripping references) is a lambda closure type.
	auto LambdaType = qualType(hasCanonicalType(hasDeclaration(cxxRecordDecl(isLambda()))));
	auto StdFunctionCtor =
		cxxConstructExpr(hasDeclaration(cxxConstructorDecl(ofClass(classTemplateSpecializationDecl(hasName("::std::function"))))),
						 argumentCountIs(1), hasArgument(0, hasType(qualType(LambdaType))))
			.bind("ctor");

	Finder->addMatcher(StdFunctionCtor, this);
}

void LambdaToStdFunctionAllocationCheck::check(const MatchFinder::MatchResult& Result) {
	const auto* Ctor = Result.Nodes.getNodeAs<CXXConstructExpr>("ctor");
	if (!Ctor) {
		return;
	}

	// Get the type of the lambda argument.
	const Expr* Arg = Ctor->getArg(0);
	QualType ArgType = Arg->getType();

	// Strip references and cv-qualifiers to get the underlying lambda type.
	ArgType = ArgType.getCanonicalType().getNonReferenceType().getUnqualifiedType();

	const auto* RD = ArgType->getAsCXXRecordDecl();
	if (!RD || !RD->isLambda()) {
		return;
	}

	// Compute the size of the lambda closure type in bytes.
	ASTContext& Ctx = *Result.Context;
	uint64_t Size = Ctx.getTypeSize(ArgType) / 8;  // bits → bytes

	const unsigned Threshold = 16;
	if (Size > Threshold) {
		diag(Ctor->getBeginLoc(), "lambda capture size is %0 bytes (>%1), converting to std::function may allocate memory dynamically")
			<< Size << Threshold;
	}
}

}  // namespace reindexer_checks
}  // namespace tidy
}  // namespace clang
