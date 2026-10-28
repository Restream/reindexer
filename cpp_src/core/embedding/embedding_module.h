#pragma once

#include <cstddef>
#include <span>
#include <string_view>
#include "core/reindexerconfig.h"
#include "tools/errors.h"

struct rx_embed_model;
namespace reindexer::embedding {

/// Process-wide access point to the optional embedding module, a separately packaged shared library. One instance per process, and nothing
/// of the C ABI crosses this class.
///
/// The first operation that needs the module opens it and starts it with the process-wide part of the configuration that operation passes;
/// that value is then fixed for the process, and a later operation asking for another one gets a warning. A missing library, an unsupported
/// ABI version or a module that fails to start leave embedders unavailable and Reindexer working, with one line in the log. An opened
/// library is never unloaded.
///
/// The file name is the platform one unless RX_CUSTOM_EMBEDDING_LIB_NAME overrides it. A non-empty value is then the only candidate, and a
/// value that is set but empty is a configuration error.
class [[nodiscard]] Module {
public:
	static constexpr char kLibNameEnvVar[] = "RX_CUSTOM_EMBEDDING_LIB_NAME";

	/// Longest prefix Encode accepts, in bytes; the same limit the ABI carries.
	static constexpr size_t kMaxPrefixBytes = 1024;

	struct [[nodiscard]] ModelInfo {
		uint32_t dimension = 0;
		/// Allocator snapshot of one inference session, not the memory of the model.
		uint64_t memoryUsageBytes = 0;
		uint32_t maxInputTokens = 0;
	};

	/// Owns one loaded model. The caller must not destroy a handle while an operation on it is running: this class cannot keep the model
	/// alive for the duration of a call.
	class [[nodiscard]] ModelHandle {
	public:
		ModelHandle() noexcept = default;
		~ModelHandle();
		ModelHandle(ModelHandle&& other) noexcept;
		ModelHandle& operator=(ModelHandle&& other) noexcept;
		ModelHandle(const ModelHandle&) = delete;
		ModelHandle& operator=(const ModelHandle&) = delete;

		[[nodiscard]] bool IsValid() const noexcept { return model_ != nullptr; }

	private:
		friend class Module;
		explicit ModelHandle(rx_embed_model* model) noexcept : model_(model) {}

		/// Destroys the model and empties the handle.
		void reset() noexcept;

		rx_embed_model* model_ = nullptr;
	};

	/// Empty while nothing is known to be wrong, otherwise the reason the module cannot be used; that reason never goes away. Opens
	/// nothing, so a missing or broken library shows up here only after something tried to use the module. Does not log.
	static Error Status() noexcept;

	/// Version the module reports, or empty while nothing has opened the library.
	static std::string_view VersionString() noexcept;

	/// File name of the library for this platform, without the override applied.
	static std::string_view PlatformLibName() noexcept;

	/// Loads one model from an unpacked directory. The first call opens the library and starts the module with the process-wide part of
	/// `config`. A value out of range is refused before the ABI, so that the message names a configuration field. On error `out` is kept.
	static Error LoadModel(std::string_view modelDir, const EmbeddingRuntimeConfig& config, ModelHandle& out) noexcept;

	/// Reports the properties of a loaded model. On error `out` stays as it was.
	static Error GetModelInfo(const ModelHandle& model, ModelInfo& out) noexcept;

	/// Turns a batch of texts into vectors. `out` holds at least texts.size() * dimension floats, with dimension from GetModelInfo; vector
	/// i occupies out[i * dimension .. (i + 1) * dimension). `written` receives the number produced, and any other number means a broken
	/// model. `prefix` goes in front of every text, must not contain a zero character and must be at most kMaxPrefixBytes long; an empty
	/// view means no prefix, an empty batch writes 0. On error `written` is kept and the contents of `out` are unspecified.
	static Error Encode(const ModelHandle& model, std::span<const std::string_view> texts, std::string_view prefix, std::span<float> out,
						size_t& written) noexcept;
};

}  // namespace reindexer::embedding
