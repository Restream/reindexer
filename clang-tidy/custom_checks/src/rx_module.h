#pragma once

#include <clang-tidy/ClangTidy.h>
#include <clang-tidy/ClangTidyModule.h>

namespace clang {
namespace tidy {
namespace reindexer_checks {

class ReindexerChecksModule : public ClangTidyModule {
public:
	void addCheckFactories(ClangTidyCheckFactories& CheckFactories) override;
};

}  // namespace reindexer_checks
}  // namespace tidy
}  // namespace clang
