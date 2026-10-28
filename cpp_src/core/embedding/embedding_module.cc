#include "core/embedding/embedding_module.h"
#include "core/embedding/rx_embed.h"

#ifndef _WIN32
#include <dlfcn.h>
#else
#include <windows.h>

#include <errhandlingapi.h>
#include <libloaderapi.h>
#endif

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <mutex>
#include <string>
#include <tuple>
#include "estl/h_vector.h"
#include "tools/catch_and_return.h"
#include "tools/logger.h"

namespace reindexer::embedding {

static_assert(Module::kMaxPrefixBytes == RX_EMBED_MAX_PREFIX_BYTES,
			  "the prefix limit of the public surface must match the one the module is told about");

namespace {

// Platform primitives for dynamic loading, taken from the OpenSSL wrapper in cpp_src/tools/openssl_singletons.cc. One flag differs:
// RTLD_LOCAL, because ONNX Runtime links protobuf into itself statically and those symbols must not leak.
#if defined(__linux__) || defined(__APPLE__)
#define LIB_TYPE void*
#define LOAD_SYM dlsym
#elif defined(_WIN32)
#define LIB_TYPE HMODULE
#define LOAD_SYM GetProcAddress
#endif

#if defined(_WIN32)
// Library names are UTF-8 and the ANSI entry points would read them in the active code page. wchar_t and every allocation stay inside
// these two calls; an empty result means the conversion failed.
[[nodiscard]] std::wstring toWide(const char* name) noexcept {
	const int nameLen = int(std::strlen(name));
	const int wideLen = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, name, nameLen, nullptr, 0);
	if (wideLen <= 0) {
		return {};
	}
	std::wstring wide;
	try {
		wide.resize(size_t(wideLen));
	} catch (...) {
		return {};
	}
	// The length is passed explicitly, so the result has no terminator of its own and the string holds exactly the name.
	if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, name, nameLen, wide.data(), wideLen) != wideLen) {
		return {};
	}
	return wide;
}

// Back to UTF-8, for messages and the log. The library itself is opened from the UTF-16 form.
[[nodiscard]] std::string toUtf8(const std::wstring& wide) {
	if (wide.empty()) {
		return {};
	}
	const int len = WideCharToMultiByte(CP_UTF8, 0, wide.c_str(), int(wide.size()), nullptr, 0, nullptr, nullptr);
	if (len <= 0) {
		return {};
	}
	std::string utf8(size_t(len), '\0');
	if (WideCharToMultiByte(CP_UTF8, 0, wide.c_str(), int(wide.size()), utf8.data(), len, nullptr, nullptr) != len) {
		return {};
	}
	return utf8;
}

// The same name as Module::kLibNameEnvVar, as a wide literal for the UTF-16 environment API.
constexpr wchar_t kLibNameEnvVarW[] = L"RX_CUSTOM_EMBEDDING_LIB_NAME";
static_assert(sizeof(kLibNameEnvVarW) / sizeof(wchar_t) == sizeof(Module::kLibNameEnvVar),
			  "the wide spelling of the variable must name the same variable");
#endif

// TODO: the module is built for Linux only so far, so the two branches below are exercised by the test stub and not by a real module.
#if defined(__linux__)
constexpr char kPlatformLibName[] = "libreindexer-embedding.so";
#elif defined(__APPLE__)
constexpr char kPlatformLibName[] = "libreindexer-embedding.dylib";
#elif defined(_WIN32)
constexpr char kPlatformLibName[] = "reindexer-embedding.dll";
#else
#error "Unsupported platform for the embedding module loader"
#endif

constexpr char kEntryPointName[] = "rx_embed_get_api_base";

// Size of the error text buffer handed to the module. The ABI sets none, and one constant keeps messages truncated the same way.
constexpr size_t kErrorBufferSize = 512;

// Typical batch size of one embedding call. Larger batches allocate; smaller ones stay on the stack.
constexpr size_t kTypicalBatch = 16;

// Largest value the int32 fields of the ABI can carry, so a uint32 from the configuration cannot arrive negative on the other side.
constexpr uint32_t kMaxAbiThreads = uint32_t(std::numeric_limits<int32_t>::max());

// The range the metadata format allows for both numbers. The module reports them, so they are checked here against the same bounds.
constexpr uint32_t kMaxModelDimension = 65535;
constexpr uint32_t kMaxModelInputTokens = 65535;

using GetApiBaseFnT = const rx_embed_api_base*(RX_EMBED_CALL*)(void);

// What the environment asks for, in the forms this platform needs. On Windows the variable is read as UTF-16: the narrow view the CRT
// offers is in the active code page, and a path outside it would be mangled or fail to convert.
struct [[nodiscard]] NameOverride {
	bool present = false;
	bool empty = false;
	std::string utf8;
#if defined(_WIN32)
	std::wstring wide;
#endif
};

// Allocates, and that is allowed: load() is not noexcept and the callable around it catches.
[[nodiscard]] NameOverride readNameOverride() {
	NameOverride result;
#if defined(_WIN32)
	const DWORD needed = GetEnvironmentVariableW(kLibNameEnvVarW, nullptr, 0);
	if (needed == 0) {
		// Either the variable is absent, or it is set to an empty string. Only GetLastError tells the two apart.
		if (GetLastError() == ERROR_ENVVAR_NOT_FOUND) {
			return result;
		}
		result.present = true;
		result.empty = true;
		return result;
	}
	// The size asked for includes the terminator; the second call reports the length without it.
	std::wstring wide(size_t(needed), L'\0');
	const DWORD written = GetEnvironmentVariableW(kLibNameEnvVarW, wide.data(), needed);
	if (written == 0 || written >= needed) {
		result.present = true;
		result.empty = true;
		return result;
	}
	wide.resize(size_t(written));
	result.present = true;
	result.wide = wide;
	result.utf8 = toUtf8(wide);
#else
	const char* const value = std::getenv(Module::kLibNameEnvVar);
	if (!value) {
		return result;
	}
	result.present = true;
	result.empty = value[0] == '\0';
	result.utf8 = value;
#endif
	return result;
}

// Text of the last dynamic loader failure. dlerror() may return nullptr, and Windows gives only a numeric code.
[[nodiscard]] std::string lastLoadError() {
#if defined(_WIN32)
	return "error " + std::to_string(GetLastError());
#else
	const char* const err = dlerror();
	return err ? std::string(err) : std::string("no additional information");
#endif
}

// Makes the buffer usable as a string even if the module broke the contract and left it unterminated.
[[nodiscard]] const char* terminated(char (&buf)[kErrorBufferSize]) noexcept {
	buf[kErrorBufferSize - 1] = '\0';
	return buf;
}

// The usable part of the library, or nothing at all: if there is a table, the library is open.
struct [[nodiscard]] LoadedLibrary {
	LIB_TYPE lib = nullptr;
	const rx_embed_api* api = nullptr;
};

// The one place where a module status becomes a Reindexer error. A second mapping would give two answers to the same question.
[[nodiscard]] ErrorCode errorCodeFor(rx_embed_status status) noexcept {
	switch (status) {
		case RX_EMBED_ERR_INVALID_ARG:
		case RX_EMBED_ERR_MODEL_INVALID:
			return errParams;
		case RX_EMBED_ERR_MODEL_NOT_FOUND:
			return errNotFound;
		case RX_EMBED_ERR_MODEL_UNSUPPORTED:
			return errNotValid;
		case RX_EMBED_ERR_OUT_OF_MEMORY:
			return errSystem;
		case RX_EMBED_ERR_INFERENCE:
			return errQueryExec;
		// errLogic covers what is wrong on our side of the boundary: a buffer Reindexer sized itself, and a module that misbehaves.
		case RX_EMBED_ERR_BUFFER_TOO_SMALL:
		case RX_EMBED_ERR_INTERNAL:
		default:
			return errLogic;
	}
}

[[nodiscard]] Error errorFor(rx_embed_status status, std::string_view what, char (&err)[kErrorBufferSize]) noexcept {
	return Error(errorCodeFor(status), "Embedders: {} failed with status {}: {}", what, int(status), terminated(err));
}

// Process-wide access to the library. The first operation that needs it opens it; the first operation that has a configuration starts the
// module. Each step runs once under its own std::once_flag, which also publishes what it wrote, so nothing below takes a lock. Neither
// callable returns without doing its work, so consuming the flag is safe.
class [[nodiscard]] ModuleImpl {
public:
	static ModuleImpl& Instance() noexcept {
		static ModuleImpl inst;
		return inst;
	}

	// Nothing is opened here, so before the first operation this is empty even when the library is missing.
	[[nodiscard]] Error Status() const noexcept {
		if (startDone_.load(std::memory_order_acquire)) {
			return startStatus_;
		}
		if (loadDone_.load(std::memory_order_acquire)) {
			return loadStatus_;
		}
		return {};
	}

	[[nodiscard]] std::string_view VersionString() const noexcept {
		return loadDone_.load(std::memory_order_acquire) ? version_ : std::string_view();
	}

	Error LoadModel(std::string_view modelDir, const EmbeddingRuntimeConfig& config, rx_embed_model*& out) noexcept;
	Error GetModelInfo(const rx_embed_model* model, Module::ModelInfo& out) noexcept;
	Error Encode(const rx_embed_model* model, std::span<const std::string_view> texts, std::string_view prefix, std::span<float> out,
				 size_t& written) noexcept;
	void DestroyModel(rx_embed_model* model) noexcept;

private:
	ModuleImpl() noexcept = default;

	// Not noexcept: the callable around it catches, and noexcept would send an allocation failure to std::terminate instead.
	void load();
	void start(uint32_t threads);

	[[nodiscard]] Error ensureLoaded() noexcept;
	[[nodiscard]] Error ensureStarted(uint32_t threads) noexcept;
	void reportOnce(const Error& status) noexcept;

	// The table once the module has been started, and nothing before that. The acquire pairs with the release at the end of the start.
	[[nodiscard]] const rx_embed_api* startedApi() const noexcept {
		return startDone_.load(std::memory_order_acquire) ? loaded_.api : nullptr;
	}

	std::once_flag loadOnce_;
	std::once_flag startOnce_;

	// Written by the two callables and final once the matching flag is set. Readers outside call_once check that flag with acquire.
	std::string libName_;
	std::string_view version_;
	LoadedLibrary loaded_;
	Error loadStatus_;
	Error startStatus_;
	uint32_t startedThreads_ = 0;

	std::atomic<bool> loadDone_{false};
	std::atomic<bool> startDone_{false};
	std::atomic<bool> statusReported_{false};
	std::atomic<bool> mismatchReported_{false};
};

void ModuleImpl::load() {
	const NameOverride nameOverride = readNameOverride();
	if (nameOverride.present && nameOverride.empty) {
		// An override that is set but empty is a configuration mistake. It must not fall back to the platform name.
		loadStatus_ = Error(
			errParams, "Embedders are disabled: {} is set to an empty value. Set it to a library name or path, or unset it to use '{}'",
			Module::kLibNameEnvVar, kPlatformLibName);
		return;
	}
	// A non-empty override is the only candidate: there is no fallback to the platform name.
	libName_ = nameOverride.present ? nameOverride.utf8 : std::string(kPlatformLibName);

#if defined(_WIN32)
	const std::wstring wideName = nameOverride.present ? nameOverride.wide : toWide(kPlatformLibName);
	LIB_TYPE lib = wideName.empty() ? nullptr : LoadLibraryW(wideName.c_str());
#else
	LIB_TYPE lib = dlopen(libName_.c_str(), RTLD_NOW | RTLD_LOCAL);
#endif
	if (!lib) {
		const std::string reason = lastLoadError();
		loadStatus_ = Error(errNotFound, "Embedders are not available: library '{}' could not be loaded: {}", libName_, reason);
		return;
	}

	auto* const getApiBase = reinterpret_cast<GetApiBaseFnT>(LOAD_SYM(lib, kEntryPointName));
	if (!getApiBase) {
		loadStatus_ =
			Error(errNotFound, "Embedders are disabled: symbol '{}' was not found in '{}': {}", kEntryPointName, libName_, lastLoadError());
		return;
	}

	const rx_embed_api_base* const base = getApiBase();
	if (!base || !base->GetApi || !base->GetVersionString) {
		loadStatus_ = Error(errLogic, "Embedders are disabled: library '{}' returned an incomplete base API table", libName_);
		return;
	}

	// The ABI says this string is always non-NULL, so a library that returns NULL has broken the contract.
	const char* const version = base->GetVersionString();
	if (!version) {
		loadStatus_ = Error(errLogic, "Embedders are disabled: library '{}' returned no version string", libName_);
		return;
	}
	version_ = std::string_view(version);

	const rx_embed_api* const api = base->GetApi(RX_EMBED_API_VERSION);
	if (!api) {
		loadStatus_ = Error(errNotFound,
							"Embedders are disabled: library '{}' (version '{}') was built for a different ABI than {}. Install a module "
							"built for this one",
							libName_, version_, RX_EMBED_API_VERSION);
		return;
	}
	// Everything Reindexer calls has to be covered by the declared size before a single pointer is read.
	if (api->struct_size < RX_EMBED_API_MIN_SIZE) {
		loadStatus_ = Error(errLogic, "Embedders are disabled: library '{}' reports a function table of {} bytes, {} are required",
							libName_, api->struct_size, uint32_t(RX_EMBED_API_MIN_SIZE));
		return;
	}
	if (!api->init || !api->model_load || !api->model_destroy || !api->model_info || !api->encode) {
		loadStatus_ = Error(errLogic, "Embedders are disabled: library '{}' left a required entry of the table empty", libName_);
		return;
	}
	loaded_ = LoadedLibrary{.lib = lib, .api = api};
}

void ModuleImpl::start(uint32_t threads) {
	startStatus_ = loadStatus_;
	startedThreads_ = threads;
	if (!loaded_.api) {
		return;
	}

	rx_embed_init_options opts = {};
	opts.struct_size = uint32_t(RX_EMBED_INIT_OPTIONS_MIN_SIZE);
	opts.intra_op_num_threads = int32_t(threads);

	char err[kErrorBufferSize] = {};
	// init is serialized against everything else, as the ABI asks: every other caller waits inside call_once meanwhile.
	const rx_embed_status status = loaded_.api->init(&opts, err, sizeof(err));
	if (status != RX_EMBED_OK) {
		startStatus_ =
			Error(errorCodeFor(status), "Embedders are disabled: the module could not start, status {}: {}", int(status), terminated(err));
	}
}

Error ModuleImpl::ensureLoaded() noexcept {
	std::call_once(loadOnce_, [this] {
		try {
			load();
		} catch (std::exception& e) {
			loadStatus_ = Error(errSystem, "Embedders: unexpected error while loading the module: {}", e.what());
		} catch (...) {
			loadStatus_ = Error(errSystem, "Embedders: unexpected error while loading the module");
		}
		loadDone_.store(true, std::memory_order_release);
	});
	return loadStatus_;
}

Error ModuleImpl::ensureStarted(uint32_t threads) noexcept {
	std::ignore = ensureLoaded();
	std::call_once(startOnce_, [this, threads] {
		try {
			start(threads);
		} catch (std::exception& e) {
			startStatus_ = Error(errSystem, "Embedders: unexpected error while starting the module: {}", e.what());
		} catch (...) {
			startStatus_ = Error(errSystem, "Embedders: unexpected error while starting the module");
		}
		startDone_.store(true, std::memory_order_release);
	});
	reportOnce(startStatus_);
	if (!startStatus_.ok()) {
		return startStatus_;
	}
	// The thread pool belongs to the process, so a later caller asking for a different size is told once that its value is ignored.
	if (threads != startedThreads_ && !mismatchReported_.exchange(true)) {
		logFmt(LogWarning,
			   "Embedders: the process-wide thread count is already fixed by the operation that started the module "
			   "(intra_op_num_threads={}). The requested value {} is ignored.",
			   startedThreads_, threads);
	}
	return {};
}

// The one line about the state of the module, written by whichever caller gets there first.
void ModuleImpl::reportOnce(const Error& status) noexcept {
	if (statusReported_.exchange(true)) {
		return;
	}
	try {
		if (status.ok()) {
			logFmt(LogInfo, "Embedders: module '{}' version '{}' has been loaded, ABI version {}", libName_, version_,
				   RX_EMBED_API_VERSION);
		} else {
			logPrint(LogError, status.whatStr());
		}
		// NOLINTBEGIN(bugprone-empty-catch)
	} catch (...) {
		// Nothing can be done here: the one thing this function does is write to the log, and that is what failed.
	}
	// NOLINTEND(bugprone-empty-catch)
}

Error ModuleImpl::LoadModel(std::string_view modelDir, const EmbeddingRuntimeConfig& config, rx_embed_model*& out) noexcept {
	try {
		if (modelDir.empty()) {
			return Error(errParams, "Embedders: the model directory must not be empty");
		}
		if (modelDir.find('\0') != std::string_view::npos) {
			// The ABI takes a NUL terminated path, so a zero inside this one would silently become a different path.
			return Error(errParams, "Embedders: the model directory must not contain a zero character");
		}
		// Checked here rather than at the ABI, so that the message names a field of the Reindexer configuration.
		if (config.maxConcurrency < 1 || config.maxConcurrency > kMaxAbiThreads) {
			return Error(errParams, "Embedders: 'max_concurrency' is {}, the allowed range is 1..{}", config.maxConcurrency,
						 kMaxAbiThreads);
		}
		if (config.intraOpNumThreads > kMaxAbiThreads) {
			return Error(errParams, "Embedders: 'intra_op_num_threads' is {}, the allowed range is 0..{}", config.intraOpNumThreads,
						 kMaxAbiThreads);
		}
		// 0 means a default chosen here, not the zero of the ABI, which is the processor count without the cap Reindexer applies.
		const uint32_t threads = config.intraOpNumThreads != 0 ? config.intraOpNumThreads : EmbeddingRuntimeConfig::DefaultThreadCount();
		if (const Error err = ensureStarted(threads); !err.ok()) {
			return err;
		}

		const rx_embed_load_options opts = {.struct_size = uint32_t(RX_EMBED_LOAD_OPTIONS_MIN_SIZE),
											.max_concurrency = int32_t(config.maxConcurrency)};
		// The ABI takes a NUL terminated path and a string_view is not one.
		const std::string dir(modelDir);
		char err[kErrorBufferSize] = {};
		rx_embed_model* model = nullptr;
		const rx_embed_status status = loaded_.api->model_load(dir.c_str(), &opts, &model, err, sizeof(err));
		if (status != RX_EMBED_OK) {
			return errorFor(status, "loading the model", err);
		}
		if (!model) {
			return Error(errLogic, "Embedders: the module reported success but returned no model");
		}
		out = model;
		return {};
	} CATCH_AND_RETURN
}

Error ModuleImpl::GetModelInfo(const rx_embed_model* model, Module::ModelInfo& out) noexcept {
	try {
		if (!model) {
			return Error(errLogic, "Embedders: the model handle is empty");
		}
		const rx_embed_api* const api = startedApi();
		if (!api) {
			return Error(errLogic, "Embedders: the module is not loaded");
		}
		rx_embed_model_info info = {};
		info.struct_size = uint32_t(RX_EMBED_MODEL_INFO_MIN_SIZE);
		const rx_embed_status status = api->model_info(model, &info);
		if (status != RX_EMBED_OK) {
			return Error(errorCodeFor(status), "Embedders: reading the model information failed with status {}", int(status));
		}
		// Both numbers come from the module; a zero dimension would otherwise surface later as an error about the output buffer.
		if (info.dimension < 1 || info.dimension > kMaxModelDimension) {
			return Error(errLogic, "Embedders: the module reports a dimension of {}, the allowed range is 1..{}", info.dimension,
						 kMaxModelDimension);
		}
		if (info.max_input_tokens < 1 || info.max_input_tokens > kMaxModelInputTokens) {
			return Error(errLogic, "Embedders: the module reports a token limit of {}, the allowed range is 1..{}", info.max_input_tokens,
						 kMaxModelInputTokens);
		}
		out = Module::ModelInfo{
			.dimension = info.dimension, .memoryUsageBytes = info.memory_usage_bytes, .maxInputTokens = info.max_input_tokens};
		return {};
	} CATCH_AND_RETURN
}

Error ModuleImpl::Encode(const rx_embed_model* model, std::span<const std::string_view> texts, std::string_view prefix,
						 std::span<float> out, size_t& written) noexcept {
	try {
		if (!model) {
			return Error(errLogic, "Embedders: the model handle is empty");
		}
		const rx_embed_api* const api = startedApi();
		if (!api) {
			return Error(errLogic, "Embedders: the module is not loaded");
		}
		if (prefix.size() > RX_EMBED_MAX_PREFIX_BYTES) {
			return Error(errParams, "Embedders: the prefix is {} bytes, the limit is {}", prefix.size(), size_t(RX_EMBED_MAX_PREFIX_BYTES));
		}
		if (prefix.find('\0') != std::string_view::npos) {
			// The ABI passes the prefix without a length, so the module would read only up to the first zero.
			return Error(errParams, "Embedders: the prefix must not contain a zero character");
		}

		// The real capacity is texts.size() * dimension and the dimension is unknown here; this catches a caller that forgot to multiply.
		if (!texts.empty() && out.size() < texts.size()) {
			// errLogic, like the BUFFER_TOO_SMALL the module would return: Reindexer sizes this buffer itself.
			return Error(errLogic, "Embedders: the output buffer holds {} floats, which cannot fit {} vectors", out.size(), texts.size());
		}

		char prefixBuf[RX_EMBED_MAX_PREFIX_BYTES + 1] = {};
		if (!prefix.empty()) {
			std::memcpy(prefixBuf, prefix.data(), prefix.size());
		}
		prefixBuf[prefix.size()] = '\0';

		char err[kErrorBufferSize] = {};
		if (texts.empty()) {
			// The ABI wants a zero capacity with an empty batch. The buffer of the caller is simply not passed on.
			size_t producedFloats = 0;
			const rx_embed_status status =
				api->encode(model, RX_EMBED_INPUT_TEXT, nullptr, nullptr, 0, prefixBuf, nullptr, 0, &producedFloats, err, sizeof(err));
			if (status != RX_EMBED_OK) {
				return errorFor(status, "encoding an empty batch", err);
			}
			if (producedFloats != 0) {
				return Error(errLogic, "Embedders: the module reported {} floats for an empty batch", producedFloats);
			}
			written = producedFloats;
			return {};
		}

		h_vector<const char*, kTypicalBatch> inputs;
		h_vector<size_t, kTypicalBatch> sizes;
		inputs.reserve(texts.size());
		sizes.reserve(texts.size());
		for (const auto& text : texts) {
			if (!text.data() && !text.empty()) {
				return Error(errLogic, "Embedders: an input of the batch has no data");
			}
			// An empty input still needs a non-null pointer, because the ABI requires every inputs[i] to be non-NULL.
			inputs.emplace_back(text.data() ? text.data() : "");
			sizes.emplace_back(text.size());
		}

		size_t producedFloats = 0;
		const rx_embed_status status = api->encode(model, RX_EMBED_INPUT_TEXT, inputs.data(), sizes.data(), texts.size(), prefixBuf,
												   out.data(), out.size(), &producedFloats, err, sizeof(err));
		if (status != RX_EMBED_OK) {
			return errorFor(status, "encoding a batch", err);
		}
		// The count comes from the module and tells the caller how much of the buffer to read, so it is checked instead of trusted.
		if (producedFloats > out.size()) {
			return Error(errLogic, "Embedders: the module reported {} floats for a buffer that holds {}", producedFloats, out.size());
		}
		written = producedFloats;
		return {};
	} CATCH_AND_RETURN
}

void ModuleImpl::DestroyModel(rx_embed_model* model) noexcept {
	const rx_embed_api* const api = startedApi();
	if (model && api) {
		api->model_destroy(model);
	}
}

}  // namespace

void Module::ModelHandle::reset() noexcept {
	if (model_) {
		ModuleImpl::Instance().DestroyModel(model_);
		model_ = nullptr;
	}
}

Module::ModelHandle::~ModelHandle() { reset(); }

Module::ModelHandle::ModelHandle(ModelHandle&& other) noexcept : model_(other.model_) { other.model_ = nullptr; }

Module::ModelHandle& Module::ModelHandle::operator=(ModelHandle&& other) noexcept {
	if (this != &other) {
		reset();
		model_ = other.model_;
		other.model_ = nullptr;
	}
	return *this;
}

Error Module::Status() noexcept { return ModuleImpl::Instance().Status(); }

std::string_view Module::VersionString() noexcept { return ModuleImpl::Instance().VersionString(); }

std::string_view Module::PlatformLibName() noexcept { return kPlatformLibName; }

Error Module::LoadModel(std::string_view modelDir, const EmbeddingRuntimeConfig& config, ModelHandle& out) noexcept {
	rx_embed_model* model = nullptr;
	if (const Error err = ModuleImpl::Instance().LoadModel(modelDir, config, model); !err.ok()) {
		return err;
	}
	out = ModelHandle(model);
	return {};
}

Error Module::GetModelInfo(const ModelHandle& model, ModelInfo& out) noexcept {
	return ModuleImpl::Instance().GetModelInfo(model.model_, out);
}

Error Module::Encode(const ModelHandle& model, std::span<const std::string_view> texts, std::string_view prefix, std::span<float> out,
					 size_t& written) noexcept {
	return ModuleImpl::Instance().Encode(model.model_, texts, prefix, out, written);
}

}  // namespace reindexer::embedding
