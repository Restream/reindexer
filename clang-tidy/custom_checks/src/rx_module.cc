#include "rx_module.h"
#include <clang-tidy/ClangTidyModuleRegistry.h>
#include "lambda_check.h"
#include "noexcept_throw_call_check.h"
#include "nodiscard_check.h"

namespace clang {
namespace tidy {
namespace reindexer_checks {

void ReindexerChecksModule::addCheckFactories(ClangTidyCheckFactories& CheckFactories) {
	CheckFactories.registerCheck<LambdaToStdFunctionAllocationCheck>("rx-perf-lambda-to-std-function-allocation");
	CheckFactories.registerCheck<NoDiscardDefinitionCheck>("rx-declarations-nodiscard");
	CheckFactories.registerCheck<NoexceptThrowCallCheck>("rx-safety-noexcept-throw-call");
}

}  // namespace reindexer_checks

static ClangTidyModuleRegistry::Add<reindexer_checks::ReindexerChecksModule> X("rx-custom-checks-module", "Adds custom reindexer checks");
}  // namespace tidy
}  // namespace clang
