#include "noexcept_throw_call_check.h"

#include <clang/AST/ASTContext.h>
#include <clang/AST/DeclCXX.h>
#include <clang/AST/ExprCXX.h>
#include <clang/AST/RecursiveASTVisitor.h>
#include <clang/AST/StmtCXX.h>
#include <clang/ASTMatchers/ASTMatchers.h>
#include <clang/Basic/DiagnosticIDs.h>
#include <clang/Basic/SourceManager.h>

#include "llvm/ADT/PointerUnion.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/StringRef.h"

using llvm::SmallPtrSet;
using llvm::SmallPtrSetImpl;

#include <cctype>
#include <string>
#include <vector>

using namespace clang;
using namespace clang::ast_matchers;

namespace clang {
namespace tidy {
namespace reindexer_checks {

namespace {

std::string toLowerAscii(StringRef S) {
	std::string R(S.data(), S.size());
	for (char& C : R) {
		if (static_cast<unsigned char>(C) <= 127) {
			C = static_cast<char>(std::tolower(static_cast<unsigned char>(C)));
		}
	}
	return R;
}

static void parseCommaSeparatedLower(StringRef Opt, std::vector<std::string>& Out) {
	Out.clear();
	StringRef Remain = Opt.trim();
	while (!Remain.empty()) {
		StringRef Token;
		std::tie(Token, Remain) = Remain.split(',');
		Token = Token.trim();
		if (!Token.empty()) {
			Out.push_back(toLowerAscii(Token));
		}
		Remain = Remain.trim();
	}
}

static void parseKeywords(StringRef Opt, std::vector<std::string>& Out) {
	parseCommaSeparatedLower(Opt, Out);
	if (Out.empty()) {
		Out.push_back("throw");
	}
}

static bool typeIsNothrow(const FunctionDecl* FD) {
	if (!FD) {
		return false;
	}
	const auto* Proto = FD->getType()->getAs<FunctionProtoType>();
	return Proto && Proto->isNothrow();
}

/// True if [KwPos, KwPos + KwLen) lies inside some occurrence of an exclude substring.
static bool keywordMatchInsideExcludedSubstring(StringRef FullLower, size_t KwPos, size_t KwLen, ArrayRef<std::string> ExcludeLower) {
	if (KwLen == 0 || ExcludeLower.empty()) {
		return false;
	}
	const size_t KwEnd = KwPos + KwLen;
	for (const std::string& Ex : ExcludeLower) {
		if (Ex.empty()) {
			continue;
		}
		for (size_t Start = 0; (Start = FullLower.find(Ex, Start)) != StringRef::npos; ++Start) {
			const size_t ExEnd = Start + Ex.size();
			if (KwPos >= Start && KwEnd <= ExEnd) {
				return true;
			}
		}
	}
	return false;
}

static bool nameContainsAnyKeyword(const NamedDecl* ND, ArrayRef<std::string> KeywordsLower, ArrayRef<std::string> ExcludeLower) {
	if (!ND || KeywordsLower.empty()) {
		return false;
	}
	const std::string Lower = toLowerAscii(ND->getQualifiedNameAsString());
	StringRef LR(Lower);
	for (const std::string& Kw : KeywordsLower) {
		if (Kw.empty()) {
			continue;
		}
		for (size_t Pos = 0;;) {
			Pos = LR.find(Kw, Pos);
			if (Pos == StringRef::npos) {
				break;
			}
			if (keywordMatchInsideExcludedSubstring(LR, Pos, Kw.size(), ExcludeLower)) {
				++Pos;
				continue;
			}
			return true;
		}
	}
	return false;
}

/// Call / construct is directly in the try compound of a C++ try statement (not in a catch clause).
static bool isDefinitionInSystemHeader(const FunctionDecl* Def, const SourceManager& SM) {
	if (!Def) {
		return false;
	}
	const FunctionDecl* Canon = Def->getDefinition();
	if (!Canon) {
		return false;
	}
	SourceLocation Spell = SM.getSpellingLoc(Canon->getBeginLoc());
	return Spell.isValid() && SM.isInSystemHeader(Spell);
}

static bool isUnderCXXTryBody(const Stmt* S, ASTContext& Ctx) {
	llvm::PointerUnion<const Stmt*, const Decl*> Up = S;
	for (;;) {
		if (const auto* St = Up.dyn_cast<const Stmt*>()) {
			DynTypedNodeList PL = Ctx.getParents(*St);
			if (PL.empty()) {
				return false;
			}
			const DynTypedNode& P = *PL.begin();
			if (const auto* CS = P.get<CompoundStmt>()) {
				for (const DynTypedNode& GP : Ctx.getParents(*CS)) {
					if (const auto* Try = GP.get<CXXTryStmt>()) {
						if (Try->getTryBlock() == CS) {
							return true;
						}
					}
				}
			}
			if (const Stmt* N = P.get<Stmt>()) {
				Up = N;
				continue;
			}
			if (const Decl* D = P.get<Decl>()) {
				Up = D;
				continue;
			}
			return false;
		}
		if (const auto* D = Up.dyn_cast<const Decl*>()) {
			DynTypedNodeList PL = Ctx.getParents(*D);
			if (PL.empty()) {
				return false;
			}
			const DynTypedNode& P = *PL.begin();
			if (const Stmt* N = P.get<Stmt>()) {
				Up = N;
				continue;
			}
			return false;
		}
		return false;
	}
}

struct CallPathStep {
	SourceLocation Loc;
	std::string Message;
};

static std::string qualifiedName(const NamedDecl* D) {
	if (!D) {
		return "?";
	}
	return D->getQualifiedNameAsString();
}

static std::string noteInCalls(const FunctionDecl* Enclosing, const FunctionDecl* Callee) {
	std::string R = "in '";
	R += qualifiedName(Enclosing);
	R += "', calls '";
	R += qualifiedName(Callee);
	R += '\'';
	return R;
}

static std::string noteNameMatches(const FunctionDecl* D) {
	std::string R;
	R.push_back('\'');
	R += qualifiedName(D);
	R += "' — name matches a configured keyword";
	return R;
}

struct CalleeSite {
	/// CallExpr or CXXConstructExpr that resolves to \p Callee (for try / parent analysis).
	const Stmt* Trigger = nullptr;
	SourceLocation Loc;
	const FunctionDecl* Callee = nullptr;
};

class CallAndCtorCollector : public RecursiveASTVisitor<CallAndCtorCollector> {
public:
	/// When \p TraverseLambdas is false, lambda bodies are not walked so call sites
	/// inside a closure are only analyzed when a call to \c operator() is seen.
	explicit CallAndCtorCollector(std::vector<CalleeSite>& Out, bool TraverseLambdas) : Out(Out), TraverseLambdaBodies(TraverseLambdas) {}

	bool TraverseLambdaExpr(LambdaExpr* E) {
		if (!TraverseLambdaBodies) {
			return true;
		}
		return RecursiveASTVisitor::TraverseLambdaExpr(E);
	}

	bool TraverseCXXRecordDecl(CXXRecordDecl* RD) {
		if (RD->isLambda()) {
			return RecursiveASTVisitor::TraverseCXXRecordDecl(RD);
		}
		if (isa<FunctionDecl>(RD->getDeclContext())) {
			return true;
		}
		return RecursiveASTVisitor::TraverseCXXRecordDecl(RD);
	}

	bool VisitCallExpr(const CallExpr* CE) {
		if (const FunctionDecl* FD = CE->getDirectCallee()) {
			Out.push_back({CE, CE->getExprLoc(), FD});
		}
		return true;
	}

	bool VisitCXXConstructExpr(const CXXConstructExpr* E) {
		if (const CXXConstructorDecl* Ctor = E->getConstructor()) {
			Out.push_back({E, E->getBeginLoc(), Ctor});
		}
		return true;
	}

private:
	std::vector<CalleeSite>& Out;
	bool TraverseLambdaBodies;
};

static SourceLocation declNoteLoc(const FunctionDecl* FD) {
	if (!FD) {
		return {};
	}
	SourceLocation L = FD->getLocation();
	return L.isValid() ? L : FD->getBeginLoc();
}

static bool bodyReachesNonNoexceptKeywordTarget(const FunctionDecl* Def, ArrayRef<std::string> KeywordsLower,
												ArrayRef<std::string> ExcludeLower, SmallPtrSetImpl<const FunctionDecl*>& Stack,
												ASTContext& Ctx, std::vector<CallPathStep>& PathOut) {
	PathOut.clear();
	if (!Def || !Def->hasBody()) {
		return false;
	}
	// Do not walk standard library / system implementation: many routines call
	// __throw_* helpers; noexcept user code often guards preconditions (e.g. bitset::test).
	if (isDefinitionInSystemHeader(Def, Ctx.getSourceManager())) {
		return false;
	}
	const FunctionDecl* Canon = Def->getCanonicalDecl();
	if (!Stack.insert(Canon).second) {
		return false;
	}

	std::vector<CalleeSite> Sites;
	CallAndCtorCollector Collector(Sites, true);
	Collector.TraverseStmt(Def->getBody());

	for (const CalleeSite& Site : Sites) {
		const FunctionDecl* Callee = Site.Callee;
		if (!Callee) {
			continue;
		}
		if (Site.Trigger && isUnderCXXTryBody(Site.Trigger, Ctx)) {
			continue;
		}
		Callee = Callee->getCanonicalDecl();

		if (typeIsNothrow(Callee)) {
			continue;
		}

		if (nameContainsAnyKeyword(Callee, KeywordsLower, ExcludeLower)) {
			PathOut.push_back({Site.Loc, noteInCalls(Def, Callee)});
			PathOut.push_back({declNoteLoc(Callee), noteNameMatches(Callee)});
			Stack.erase(Canon);
			return true;
		}

		const FunctionDecl* CalleeDef = Callee->getDefinition();
		if (CalleeDef && CalleeDef->hasBody()) {
			std::vector<CallPathStep> Sub;
			if (bodyReachesNonNoexceptKeywordTarget(CalleeDef, KeywordsLower, ExcludeLower, Stack, Ctx, Sub)) {
				PathOut.push_back({Site.Loc, noteInCalls(Def, Callee)});
				PathOut.insert(PathOut.end(), Sub.begin(), Sub.end());
				Stack.erase(Canon);
				return true;
			}
		}
	}

	Stack.erase(Canon);
	return false;
}

static bool callViolates(const CalleeSite& Site, ArrayRef<std::string> KeywordsLower, ArrayRef<std::string> ExcludeLower,
						 SmallPtrSetImpl<const FunctionDecl*>& Stack, ASTContext& Ctx, std::vector<CallPathStep>& PathOut) {
	PathOut.clear();
	const FunctionDecl* Callee = Site.Callee;
	if (!Callee) {
		return false;
	}
	Callee = Callee->getCanonicalDecl();

	if (typeIsNothrow(Callee)) {
		return false;
	}

	if (nameContainsAnyKeyword(Callee, KeywordsLower, ExcludeLower)) {
		PathOut.push_back({declNoteLoc(Callee), noteNameMatches(Callee)});
		return true;
	}

	const FunctionDecl* Def = Callee->getDefinition();
	if (!Def || !Def->hasBody()) {
		return false;
	}

	return bodyReachesNonNoexceptKeywordTarget(Def, KeywordsLower, ExcludeLower, Stack, Ctx, PathOut);
}

}  // namespace

NoexceptThrowCallCheck::NoexceptThrowCallCheck(StringRef Name, ClangTidyContext* Context)
	: ClangTidyCheck(Name, Context),
	  KeywordsOptionRaw(std::string(Options.get("Keywords", "throw"))),
	  ExcludeSubstringsOptionRaw(std::string(Options.get("ExcludeSubstrings", "nothrow"))) {
	parseKeywords(KeywordsOptionRaw, KeywordsLower);
	parseCommaSeparatedLower(ExcludeSubstringsOptionRaw, ExcludeSubstringsLower);
}

void NoexceptThrowCallCheck::storeOptions(ClangTidyOptions::OptionMap& Opts) {
	Options.store(Opts, "Keywords", KeywordsOptionRaw);
	Options.store(Opts, "ExcludeSubstrings", ExcludeSubstringsOptionRaw);
}

void NoexceptThrowCallCheck::registerMatchers(MatchFinder* Finder) {
	Finder->addMatcher(functionDecl(isDefinition(), unless(isImplicit()), unless(isDeleted()), hasBody(stmt())).bind("func"), this);
}

void NoexceptThrowCallCheck::check(const MatchFinder::MatchResult& Result) {
	const auto* FD = Result.Nodes.getNodeAs<FunctionDecl>("func");
	if (!FD) {
		return;
	}

	const auto* Proto = FD->getType()->getAs<FunctionProtoType>();
	if (!Proto || !Proto->isNothrow()) {
		return;
	}

	Stmt* Body = FD->getBody();
	if (!Body) {
		return;
	}

	std::vector<CalleeSite> Sites;
	CallAndCtorCollector Collector(Sites, false);
	Collector.TraverseStmt(Body);

	for (const CalleeSite& Site : Sites) {
		if (!Site.Loc.isValid()) {
			continue;
		}
		if (Site.Trigger && isUnderCXXTryBody(Site.Trigger, *Result.Context)) {
			continue;
		}
		SmallPtrSet<const FunctionDecl*, 16> Stack;
		std::vector<CallPathStep> Path;
		if (!callViolates(Site, KeywordsLower, ExcludeSubstringsLower, Stack, *Result.Context, Path)) {
			continue;
		}
		diag(Site.Loc, "noexcept function calls a non-noexcept callee that can reach a function whose name "
					  "contains a configured keyword");
		for (const CallPathStep& Step : Path) {
			if (Step.Loc.isValid()) {
				diag(Step.Loc, Step.Message, DiagnosticIDs::Note);
			}
		}
	}
}

}  // namespace reindexer_checks
}  // namespace tidy
}  // namespace clang
