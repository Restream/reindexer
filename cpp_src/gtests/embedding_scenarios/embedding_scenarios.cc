// Scenario helper for the process-wide behaviour of the embedding module loader.
//
// The loader fixes its state once per process, so these cases cannot share one and cannot live in the common gtest binary. CTest runs this
// executable once per scenario, each with its own RX_CUSTOM_EMBEDDING_LIB_NAME, and production code gets no reset hook. Everything goes
// through the public surface of Module.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>
#ifndef _WIN32
#include <dlfcn.h>
#else
#include <windows.h>
#endif
#include "core/embedding/embedding_module.h"
#include "core/embedding/rx_embed.h"
#include "tools/logger.h"

namespace {

std::vector<std::string> g_log;
int g_failures = 0;

void check(bool condition, const char* what) {
	std::printf("  %-62s %s\n", what, condition ? "ok" : "FAILED");
	if (!condition) {
		++g_failures;
	}
}

// Counts the lines the loader wrote about its own state; the warning about a differing configuration is not one of them.
size_t statusLines() {
	size_t count = 0;
	for (const auto& line : g_log) {
		if (line.find("already fixed") == std::string::npos) {
			++count;
		}
	}
	return count;
}

bool logContains(std::string_view text) {
	for (const auto& line : g_log) {
		if (line.find(text) != std::string::npos) {
			return true;
		}
	}
	return false;
}

using reindexer::EmbeddingRuntimeConfig;
using reindexer::Error;
using reindexer::embedding::Module;

std::string modelPath(const char* root, std::string_view name) { return std::string(root) + "/../" + std::string(name); }

#ifdef _WIN32
// Library paths are UTF-8, like every path in this ABI, so the scenario opens them the way the core does.
[[nodiscard]] std::wstring toWidePath(const std::string& path) {
	const int wideLen = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path.c_str(), int(path.size()), nullptr, 0);
	if (wideLen <= 0) {
		return {};
	}
	std::wstring wide(size_t(wideLen), L'\0');
	if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path.c_str(), int(path.size()), wide.data(), wideLen) != wideLen) {
		return {};
	}
	return wide;
}
#endif

// Shared by every scenario that expects the module to be unusable: before the first operation nothing is known and nothing is logged, and
// after it the reason is in the log once and in Status().
void expectUnavailable(std::string_view reason, const char* modelDir) {
	check(Module::Status().ok(), "nothing is known to be wrong before the module is used");
	check(statusLines() == 0, "and nothing is written to the log");

	Module::ModelHandle handle;
	const Error loaded = Module::LoadModel(modelDir, EmbeddingRuntimeConfig{}, handle);
	check(!loaded.ok(), "loading a model fails");
	check(loaded.whatStr().find(reason) != std::string::npos, "and fails with the reason");
	check(!handle.IsValid(), "the handle is left empty");

	check(!Module::Status().ok(), "afterwards the reason is available without performing an operation");
	check(logContains(reason), "the log names the reason");
	Module::ModelHandle again;
	check(!Module::LoadModel(modelDir, EmbeddingRuntimeConfig{}, again).ok(), "a second operation fails the same way");
	std::ignore = Module::Status();
	check(statusLines() == 1, "and neither it nor a query adds a line");
}

int scenarioAbsent(const char* dir) {
	expectUnavailable("could not be loaded", dir);
	return 0;
}

// A file that exists and is not a library fails like a missing one.
int scenarioBrokenLibrary(const char* dir) {
	expectUnavailable("could not be loaded", dir);
	return 0;
}

// A configuration mistake and a missing library at once: the mistake wins, and the library is not even opened.
int scenarioInvalidConfigWithoutLibrary(const char* modelDir) {
	EmbeddingRuntimeConfig broken;
	broken.intraOpNumThreads = 3000000000u;	 // above INT32_MAX

	Module::ModelHandle handle;
	const Error refused = Module::LoadModel(modelDir, broken, handle);
	check(refused.code() == errParams, "the configuration error is reported, not the missing library");
	check(refused.whatStr().find("intra_op_num_threads") != std::string::npos, "and names the offending field");
	check(!handle.IsValid(), "no model is produced");

	check(Module::Status().ok(), "nothing was opened, so there is nothing to report");
	check(Module::VersionString().empty(), "and nothing is known about the library");
	check(statusLines() == 0, "and the log is silent");
	return 0;
}

int scenarioEmptyOverride(const char* dir) {
	expectUnavailable("set to an empty value", dir);
	check(logContains(Module::PlatformLibName()), "the log names the platform library it would have used");
	return 0;
}

int scenarioPlatformDefault(const char* dir) {
	// Cleared here rather than left to the test runner, in case a developer has the override exported in their shell.
#ifdef _WIN32
	std::ignore = _putenv_s("RX_CUSTOM_EMBEDDING_LIB_NAME", "");
#else
	std::ignore = ::unsetenv("RX_CUSTOM_EMBEDDING_LIB_NAME");
#endif
	expectUnavailable(Module::PlatformLibName(), dir);
	return 0;
}

int scenarioUnsupportedAbi(const char* dir) {
	expectUnavailable("was built for a different ABI", dir);
	check(!Module::VersionString().empty(), "the version string of the library is still readable");
	return 0;
}

int scenarioShortTable(const char* dir) {
	expectUnavailable("reports a function table of", dir);
	return 0;
}

int scenarioNullMember(const char* dir) {
	expectUnavailable("required entry of the table empty", dir);
	return 0;
}

int scenarioNoVersionString(const char* dir) {
	expectUnavailable("returned no version string", dir);
	return 0;
}

// Both the plain stub and the one with a larger table run this: the caller must keep working through the prefix it knows.
int scenarioValid(const char* modelDir) {
	EmbeddingRuntimeConfig config;
	config.intraOpNumThreads = 2;
	config.maxConcurrency = 2;

	check(Module::Status().ok(), "status is clean");
	check(statusLines() == 0, "the library is not opened before it is needed");
	check(Module::VersionString().empty(), "so nothing is known about the module yet");

	Module::ModelHandle handle;
	const Error loaded = Module::LoadModel(modelDir, config, handle);
	check(loaded.ok(), "LoadModel on the fixture succeeds");
	if (!loaded.ok()) {
		std::printf(" LoadModel said: %s\n", loaded.what());
		return 1;
	}
	check(handle.IsValid(), "the handle owns a model");
	check(!Module::VersionString().empty(), "the module reports a version string");
	check(statusLines() == 1, "the first operation wrote the one line about the module");
	check(logContains(Module::VersionString()), "and that line names the version");
	std::ignore = Module::Status();
	check(statusLines() == 1, "neither a second instance nor a query adds a line");

	Module::ModelInfo info;
	check(Module::GetModelInfo(handle, info).ok(), "GetModelInfo succeeds");
	check(info.dimension > 0, "the model reports a dimension");

	const std::string_view texts[] = {"hello", "world"};
	std::vector<float> vectors(2 * info.dimension);
	size_t written = 0;
	const Error encoded = Module::Encode(handle, texts, "query: ", vectors, written);
	check(encoded.ok(), "Encode succeeds");
	check(written == 2 * size_t(info.dimension), "Encode reports the expected number of floats");

	// A prefix that breaks the precondition of the ABI is refused here, before the module is reached.
	size_t ignored = 12345;
	const std::string withZero("a\0b", 3);
	check(!Module::Encode(handle, texts, withZero, vectors, ignored).ok(), "a prefix with a zero character is refused");
	check(ignored == 12345, "and the caller's counter is untouched");
	check(Module::Encode(handle, texts, std::string(Module::kMaxPrefixBytes + 1, 'x'), vectors, ignored).code() == errParams,
		  "a prefix past the limit is refused");

	// An empty batch does not have to be special-cased by the caller.
	size_t empty = 7;
	check(Module::Encode(handle, {}, "", vectors, empty).ok(), "an empty batch succeeds");
	check(empty == 0, "and writes nothing");

	// Move assignment releases the model the target held; a double destroy would be caught by the sanitizer builds.
	Module::ModelHandle replacement;
	check(Module::LoadModel(modelDir, EmbeddingRuntimeConfig{}, replacement).ok(), "a second model loads");
	handle = std::move(replacement);
	check(handle.IsValid(), "move assignment hands the model over and releases the one that was there");
	return 0;
}

// Nothing serializes the callers of LoadModel, so several threads race for the first one here: all of them must get a model and the module
// must be started once. Under the thread sanitizer this also checks that the two lazy steps publish what they wrote.
int scenarioConcurrentFirstLoad(const char* modelDir) {
	constexpr size_t kThreads = 8;
	std::vector<Module::ModelHandle> handles(kThreads);
	std::vector<Error> errors(kThreads);
	std::vector<std::thread> threads;
	threads.reserve(kThreads);
	for (size_t i = 0; i < kThreads; ++i) {
		threads.emplace_back([&, i] { errors[i] = Module::LoadModel(modelDir, EmbeddingRuntimeConfig{}, handles[i]); });
	}
	for (auto& thread : threads) {
		thread.join();
	}

	size_t loaded = 0;
	for (size_t i = 0; i < kThreads; ++i) {
		if (errors[i].ok() && handles[i].IsValid()) {
			++loaded;
		} else if (!errors[i].ok()) {
			std::printf(" thread %zu said: %s\n", i, errors[i].what());
		}
	}
	check(loaded == kThreads, "every thread that raced for the first load got a model");
	check(Module::Status().ok(), "the module is usable afterwards");
	check(!Module::VersionString().empty(), "and the version of the library is readable");
	check(statusLines() == 1, "the state of the module reached the log exactly once");
	return 0;
}

int scenarioInitHappensOnce(const char* modelDir) {
	Module::ModelHandle first;
	Module::ModelHandle second;
	check(Module::LoadModel(modelDir, EmbeddingRuntimeConfig{}, first).ok(), "the first model loads");
	check(Module::LoadModel(modelDir, EmbeddingRuntimeConfig{}, second).ok(),
		  "the second model loads, so the environment was not created again");
	check(statusLines() == 1, "and nothing extra is written to the log");
	return 0;
}

// The library loads and the module refuses to start. The reason is written by the operation that asked for it, like every other reason.
int scenarioInitFails(const char* modelDir) {
	check(Module::Status().ok(), "nothing is known to be wrong before the module is used");
	check(statusLines() == 0, "and nothing is written to the log");

	Module::ModelHandle handle;
	const Error loaded = Module::LoadModel(modelDir, EmbeddingRuntimeConfig{}, handle);
	check(!loaded.ok(), "loading a model fails because the module cannot start");
	check(!handle.IsValid(), "the handle is left empty");
	check(!Module::Status().ok(), "the module stays unavailable");
	check(logContains("the module could not start"), "and the reason is written to the log");
	check(statusLines() == 1, "exactly one line");

	check(!Module::LoadModel(modelDir, EmbeddingRuntimeConfig{}, handle).ok(), "a second attempt fails the same way");
	check(statusLines() == 1, "and does not log again");
	return 0;
}

int scenarioFirstLoadWins(const char* modelDir) {
	// The first load fixes the process-wide value. The fixture refuses to load unless init received exactly 3, so this proves what reached
	// the ABI and not only what the warning says.
	EmbeddingRuntimeConfig first;
	first.intraOpNumThreads = 3;
	Module::ModelHandle checked;
	const Error started = Module::LoadModel(modelPath(modelDir, "expects_three_threads"), first, checked);
	check(started.ok(), "the module confirms init received the thread count of the first load");
	if (!started.ok()) {
		std::printf(" the module said: %s\n", started.what());
	}

	EmbeddingRuntimeConfig second;
	second.intraOpNumThreads = 9;
	Module::ModelHandle later;
	check(Module::LoadModel(modelDir, second, later).ok(), "a later load with a different value still works");
	Module::ModelHandle again;
	check(Module::LoadModel(modelDir, second, again).ok(), "and so does the one after it");

	size_t warnings = 0;
	for (const auto& line : g_log) {
		if (line.find("already fixed") != std::string::npos) {
			++warnings;
		}
	}
	check(warnings == 1, "a differing value is reported once, not once per load");
	check(logContains("intra_op_num_threads=3"), "the warning names the value in force");
	check(logContains("requested value 9"), "and the value that was ignored");
	return 0;
}

// A process-wide value out of range is refused by the call that passes it. Nothing is remembered, so the next call with a correct value
// works.
int scenarioInvalidThreadCount(const char* modelDir) {
	EmbeddingRuntimeConfig broken;
	broken.intraOpNumThreads = 3000000000u;	 // above INT32_MAX

	Module::ModelHandle rejected;
	const Error refused = Module::LoadModel(modelDir, broken, rejected);
	check(refused.code() == errParams, "the call is refused with errParams");
	check(refused.whatStr().find("intra_op_num_threads") != std::string::npos, "and names the offending field");
	check(!rejected.IsValid(), "and produces no model");
	check(Module::Status().ok(), "nothing was started, so nothing is wrong with the module");
	check(statusLines() == 0, "and nothing was logged");

	Module::ModelHandle handle;
	check(Module::LoadModel(modelDir, EmbeddingRuntimeConfig{}, handle).ok(), "a correct value works from then on");
	check(handle.IsValid(), "the handle owns a model");
	check(statusLines() == 1, "and the module reports its state once");
	return 0;
}

// Opens the library the scenario was given without going through Module, for the rules that are about what happens before anything has
// started the module. The calling convention is part of the contract, so the pointer carries it.
[[nodiscard]] const rx_embed_api* openStubDirectly() {
	using EntryPointT = const rx_embed_api_base*(RX_EMBED_CALL*)();

	const char* const libName = std::getenv(Module::kLibNameEnvVar);
	check(libName != nullptr && libName[0] != '\0', "the scenario was given a library to open");
	if (!libName || libName[0] == '\0') {
		return nullptr;
	}
	// Copied out of the environment before use, the same way the loader does it.
	const std::string libPath(libName);
#ifndef _WIN32
	void* const handle = dlopen(libPath.c_str(), RTLD_NOW | RTLD_LOCAL);
	const auto entry = handle ? reinterpret_cast<EntryPointT>(dlsym(handle, "rx_embed_get_api_base")) : nullptr;
#else
	// LoadLibraryW, like the loader in the core: the ANSI entry point would read the path in the active code page.
	const std::wstring widePath = toWidePath(libPath);
	const HMODULE handle = widePath.empty() ? nullptr : LoadLibraryW(widePath.c_str());
	const auto entry = handle ? reinterpret_cast<EntryPointT>(GetProcAddress(handle, "rx_embed_get_api_base")) : nullptr;
#endif
	check(entry != nullptr, "the entry point resolves");
	if (!entry) {
		return nullptr;
	}
	const rx_embed_api_base* const base = entry();
	const rx_embed_api* const api = base ? base->GetApi(RX_EMBED_API_VERSION) : nullptr;
	check(api != nullptr, "the table is there");
	return api;
}

// The common gtest binary cannot check this rule: by the time its cases run, something has already started the module.
int scenarioAbiLoadBeforeInit(const char* modelDir) {
	const rx_embed_api* const api = openStubDirectly();
	if (!api) {
		return 0;
	}

	const rx_embed_load_options opts = {.struct_size = uint32_t(RX_EMBED_LOAD_OPTIONS_MIN_SIZE), .max_concurrency = 1};
	const std::string dir = std::string(modelDir);
	char err[512] = {};
	rx_embed_model* model = nullptr;
	check(api->model_load(dir.c_str(), &opts, &model, err, sizeof(err)) == RX_EMBED_ERR_INVALID_ARG, "model_load before init is refused");
	check(model == nullptr, "and produces no handle");
	check(std::strlen(err) > 0, "and says why");

	// The refusal above did not start anything, so the module still starts normally afterwards.
	const rx_embed_init_options envOpts = {.struct_size = uint32_t(RX_EMBED_INIT_OPTIONS_MIN_SIZE), .intra_op_num_threads = 2};
	check(api->init(&envOpts, err, sizeof(err)) == RX_EMBED_OK, "init still succeeds");
	check(api->model_load(dir.c_str(), &opts, &model, err, sizeof(err)) == RX_EMBED_OK, "and the model loads after it");
	if (model) {
		api->model_destroy(model);
	}
	return 0;
}

// init with no options at all: Reindexer never takes that path, and the one successful init of a process has to be spent on it.
int scenarioAbiInitDefaultOptions(const char* modelDir) {
	const rx_embed_api* const api = openStubDirectly();
	if (!api) {
		return 0;
	}

	char err[512] = {};
	check(api->init(nullptr, err, sizeof(err)) == RX_EMBED_OK, "init with no options succeeds");
	check(api->init(nullptr, err, sizeof(err)) == RX_EMBED_ERR_INVALID_ARG, "and a second such call is refused like any other");

	const rx_embed_load_options opts = {.struct_size = uint32_t(RX_EMBED_LOAD_OPTIONS_MIN_SIZE), .max_concurrency = 1};
	const std::string dir(modelDir);
	rx_embed_model* model = nullptr;
	check(api->model_load(dir.c_str(), &opts, &model, err, sizeof(err)) == RX_EMBED_OK, "the model loads after a default start");
	if (model) {
		api->model_destroy(model);
	}
	return 0;
}

// Every module status has to arrive as a Reindexer error code; the mapping lives in one place and this pins it down.
int scenarioStatusMapping(const char* modelDir) {
	const struct {
		const char* fixture;
		ErrorCode expected;
	} cases[] = {
		{"fail_load_not_found", errNotFound},	{"fail_load_invalid", errParams}, {"fail_load_unsupported", errNotValid},
		{"fail_load_oom", errSystem},			{"fail_load_internal", errLogic}, {"invalid_pooling", errParams},
		{"cls_with_left_padding", errNotValid},
	};
	for (const auto& item : cases) {
		Module::ModelHandle handle;
		const Error err = Module::LoadModel(modelPath(modelDir, item.fixture), EmbeddingRuntimeConfig{}, handle);
		check(err.code() == item.expected, item.fixture);
		check(!handle.IsValid(), "the handle stays empty on failure");
	}

	// A failed inference is a failed operation, while an internal failure of the module is a broken invariant.
	const struct {
		const char* fixture;
		ErrorCode expected;
	} encodeCases[] = {{"fail_encode_inference", errQueryExec}, {"fail_encode_internal", errLogic}};
	for (const auto& item : encodeCases) {
		Module::ModelHandle handle;
		check(Module::LoadModel(modelPath(modelDir, item.fixture), EmbeddingRuntimeConfig{}, handle).ok(), "the model loads");
		Module::ModelInfo info;
		check(Module::GetModelInfo(handle, info).ok(), "and reports its properties");
		const std::string_view texts[] = {"text"};
		std::vector<float> vectors(info.dimension);
		size_t written = 12345;
		check(Module::Encode(handle, texts, "", vectors, written).code() == item.expected, item.fixture);
		check(written == 12345, "and the counter of the caller is untouched");
	}

	// Properties a model may not have: the metadata format keeps both numbers inside 1..65535.
	for (const char* const fixture : {"info_zero_dimension", "info_huge_tokens"}) {
		Module::ModelHandle handle;
		check(Module::LoadModel(modelPath(modelDir, fixture), EmbeddingRuntimeConfig{}, handle).ok(), "the model loads");
		Module::ModelInfo info;
		const Error reported = Module::GetModelInfo(handle, info);
		check(reported.code() == errLogic, fixture);
		check(reported.whatStr().find("allowed range is 1..65535") != std::string::npos, "and the message names the range");
	}

	// A module that reports more floats than the buffer holds. Taking that count would make the caller read past the end of its buffer.
	{
		Module::ModelHandle handle;
		check(Module::LoadModel(modelPath(modelDir, "overreported_floats"), EmbeddingRuntimeConfig{}, handle).ok(), "the model loads");
		Module::ModelInfo info;
		check(Module::GetModelInfo(handle, info).ok(), "and reports its properties");
		const std::string_view texts[] = {"text"};
		std::vector<float> vectors(info.dimension);
		size_t written = 999;
		check(Module::Encode(handle, texts, "", vectors, written).code() == errLogic, "an impossible count is refused");
		check(written == 999, "and the counter of the caller is untouched");
	}
	return 0;
}

// The per-model part of the configuration is refused by Reindexer before the ABI is reached, so that the message names the configuration
// field rather than an ABI status.
int scenarioModelConfigRejected(const char* modelDir) {
	EmbeddingRuntimeConfig zeroConcurrency;
	zeroConcurrency.maxConcurrency = 0;	 // the ABI allows it, the configuration of Reindexer does not
	Module::ModelHandle handle;
	const Error refused = Module::LoadModel(modelDir, zeroConcurrency, handle);
	check(refused.code() == errParams, "max_concurrency 0 is refused by the configuration");
	check(refused.whatStr().find("max_concurrency") != std::string::npos, "and the message names the field");
	check(!handle.IsValid(), "the handle stays empty");

	check(Module::LoadModel(modelDir, EmbeddingRuntimeConfig{}, handle).ok(), "a valid per-model configuration still loads");
	return 0;
}

}  // namespace

int main(int argc, char** argv) {
	if (argc < 2) {
		std::fprintf(stderr, "usage: %s <scenario> [model directory]\n", argv[0]);
		return 2;
	}
	reindexer::logInstallWriter([](int, char* message) { g_log.emplace_back(message); }, reindexer::LoggerPolicy::WithoutLocks, LogTrace);

	const std::string scenario = argv[1];
	const char* const modelDir = argc > 2 ? argv[2] : "";
	std::printf("scenario %s\n", scenario.c_str());

	int result = 0;
	if (scenario == "absent") {
		result = scenarioAbsent(modelDir);
	} else if (scenario == "broken_library") {
		result = scenarioBrokenLibrary(modelDir);
	} else if (scenario == "invalid_config_without_library") {
		result = scenarioInvalidConfigWithoutLibrary(modelDir);
	} else if (scenario == "empty_override") {
		result = scenarioEmptyOverride(modelDir);
	} else if (scenario == "platform_default") {
		result = scenarioPlatformDefault(modelDir);
	} else if (scenario == "unsupported_abi") {
		result = scenarioUnsupportedAbi(modelDir);
	} else if (scenario == "short_table") {
		result = scenarioShortTable(modelDir);
	} else if (scenario == "null_member") {
		result = scenarioNullMember(modelDir);
	} else if (scenario == "no_version_string") {
		result = scenarioNoVersionString(modelDir);
	} else if (scenario == "valid") {
		result = scenarioValid(modelDir);
	} else if (scenario == "extended_abi") {
		result = scenarioValid(modelDir);
	} else if (scenario == "concurrent_first_load") {
		result = scenarioConcurrentFirstLoad(modelDir);
	} else if (scenario == "init_happens_once") {
		result = scenarioInitHappensOnce(modelDir);
	} else if (scenario == "init_fails") {
		result = scenarioInitFails(modelDir);
	} else if (scenario == "first_load_wins") {
		result = scenarioFirstLoadWins(modelDir);
	} else if (scenario == "invalid_thread_count") {
		result = scenarioInvalidThreadCount(modelDir);
	} else if (scenario == "abi_load_before_init") {
		result = scenarioAbiLoadBeforeInit(modelDir);
	} else if (scenario == "abi_init_default_options") {
		result = scenarioAbiInitDefaultOptions(modelDir);
	} else if (scenario == "status_mapping") {
		result = scenarioStatusMapping(modelDir);
	} else if (scenario == "model_config_rejected") {
		result = scenarioModelConfigRejected(modelDir);
	} else {
		std::fprintf(stderr, "unknown scenario '%s'\n", scenario.c_str());
		return 2;
	}

	if (g_failures) {
		std::printf("\nlog written by the loader:\n");
		for (const auto& line : g_log) {
			std::printf("  | %s\n", line.c_str());
		}
	}
	std::printf("%s: %d check(s) failed\n", g_failures ? "FAILED" : "passed", g_failures);
	return g_failures ? 1 : result;
}
