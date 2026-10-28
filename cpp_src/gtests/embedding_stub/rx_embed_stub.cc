// Test stub of the embedding module. It implements the whole ABI from rx_embed.h and returns deterministic fake vectors instead of running
// inference. It reads the same metadata.json as the real module, and the x_stub object of that file drives its failures.
//
// What cannot be expressed there happens before model_load, when no model directory is known. Each such case is a separate target built
// from these same sources:
//
//   RX_EMBED_STUB_UNSUPPORTED_ABI=1    GetApi() returns NULL for every version;
//   RX_EMBED_STUB_EXTENDED_ABI=1       the table appends one function after the required prefix and reports the larger size, which is how
//                                      a module built from a newer header looks;
//   RX_EMBED_STUB_BROKEN_ABI=1         the table claims to be shorter than the required prefix;
//   RX_EMBED_STUB_BROKEN_ABI=2         the size is right and a required member is NULL;
//   RX_EMBED_STUB_NO_VERSION_STRING=1  GetVersionString() returns NULL, which the ABI forbids;
//   RX_EMBED_STUB_INIT_FAILS=1         init always fails.
//
// No exception may leave this file: every entry point catches everything and returns a status.

#include "core/embedding/rx_embed.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <tuple>
#include <vector>
#if defined(_WIN32)
#include <windows.h>

#include <stringapiset.h>
#endif
#include "vendor/gason/gason.h"

namespace {

// Unused in the variant that returns no version string at all.
[[maybe_unused]] constexpr char kVersionString[] = "reindexer-embedding-stub 1.0.0";
constexpr char kMetadataFileName[] = "metadata.json";
constexpr size_t kMaxMetadataBytes = 64 * 1024;
constexpr uint32_t kMaxDimension = 65535;
constexpr uint32_t kMaxInputTokens = 65535;

[[nodiscard]] uint32_t defaultConcurrency() noexcept { return std::max(std::thread::hardware_concurrency(), 1u); }

// Copies as much of the text as fits and always terminates it. err == nullptr or errSize == 0 means the caller does not want the text.
void setError(char* err, size_t errSize, std::string_view text) noexcept {
	if (!err || errSize == 0) {
		return;
	}
	const size_t copied = std::min(text.size(), errSize - 1);
	if (copied) {
		std::memcpy(err, text.data(), copied);
	}
	err[copied] = '\0';
}

// Accepts exactly the well formed UTF-8 of Unicode: no overlong forms, no surrogates, nothing above U+10FFFF. A zero byte inside the range
// is ordinary data, because the length is passed separately from the pointer.
[[nodiscard]] bool isValidUtf8(const char* data, size_t size) noexcept {
	const auto* p = reinterpret_cast<const unsigned char*>(data);
	const auto* const end = p + size;
	while (p < end) {
		const unsigned char lead = *p;
		size_t extra = 0;
		uint32_t code = 0;
		if (lead < 0x80) {
			++p;
			continue;
		} else if ((lead & 0xE0) == 0xC0) {
			extra = 1;
			code = lead & 0x1Fu;
		} else if ((lead & 0xF0) == 0xE0) {
			extra = 2;
			code = lead & 0x0Fu;
		} else if ((lead & 0xF8) == 0xF0) {
			extra = 3;
			code = lead & 0x07u;
		} else {
			return false;
		}
		if (size_t(end - p) <= extra) {
			return false;
		}
		for (size_t i = 1; i <= extra; ++i) {
			const unsigned char cont = p[i];
			if ((cont & 0xC0) != 0x80) {
				return false;
			}
			code = (code << 6) | (cont & 0x3Fu);
		}
		if ((extra == 1 && code < 0x80) || (extra == 2 && code < 0x800) || (extra == 3 && code < 0x10000)) {
			return false;  // overlong
		}
		if (code > 0x10FFFF || (code >= 0xD800 && code <= 0xDFFF)) {
			return false;
		}
		p += extra + 1;
	}
	return true;
}

// FNV-1a over the bytes. Only integer arithmetic, so the result is the same on every platform.
[[nodiscard]] uint64_t hashBytes(const void* data, size_t size, uint64_t seed) noexcept {
	uint64_t hash = seed;
	const auto* p = static_cast<const unsigned char*>(data);
	for (size_t i = 0; i < size; ++i) {
		hash ^= uint64_t(p[i]);
		hash *= 0x100000001B3ull;
	}
	return hash;
}

// One element of the fake vector, derived from the seed by splitmix64 and mapped into [-1, 1).
[[nodiscard]] float vectorValue(uint64_t seed, uint32_t index) noexcept {
	uint64_t x = seed + 0x9E3779B97F4A7C15ull * (uint64_t(index) + 1);
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
	x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
	x ^= x >> 31;
	const uint32_t bits = uint32_t(x >> 40);  // 24 bits
	return float(double(bits) / double(1 << 23) - 1.0);
}

// Seed of one batch element. The prefix length is hashed before the bytes, so that splitting the same character sequence differently
// between the prefix and the text gives different vectors.
[[nodiscard]] uint64_t inputSeed(const char* prefix, size_t prefixSize, const char* text, size_t textSize) noexcept {
	const uint64_t sizeLe = uint64_t(prefixSize);
	unsigned char sizeBytes[8];
	for (size_t i = 0; i < sizeof(sizeBytes); ++i) {
		sizeBytes[i] = static_cast<unsigned char>((sizeLe >> (8 * i)) & 0xFFu);
	}
	uint64_t hash = hashBytes(sizeBytes, sizeof(sizeBytes), 0xCBF29CE484222325ull);
	hash = hashBytes(prefix, prefixSize, hash);
	return hashBytes(text, textSize, hash);
}

// Blocks while max_concurrency calls already run inference on one handle. The ABI has no BUSY status and no immediate refusal: an extra
// call waits for a slot and then runs. A limit of 0 means no limit.
class [[nodiscard]] ConcurrencyLimiter {
public:
	explicit ConcurrencyLimiter(int32_t limit) noexcept : limit_(limit), available_(limit) {}

	void Acquire() {
		if (limit_ <= 0) {
			return;
		}
		std::unique_lock<std::mutex> lck(mtx_);
		cv_.wait(lck, [this] { return available_ > 0; });
		--available_;
	}

	void Release() noexcept {
		if (limit_ <= 0) {
			return;
		}
		{
			std::lock_guard<std::mutex> lck(mtx_);
			++available_;
		}
		cv_.notify_one();
	}

private:
	const int32_t limit_;
	int32_t available_;
	std::mutex mtx_;
	std::condition_variable cv_;
};

class [[nodiscard]] LimiterGuard {
public:
	explicit LimiterGuard(ConcurrencyLimiter& limiter) : limiter_(limiter) { limiter_.Acquire(); }
	~LimiterGuard() { limiter_.Release(); }
	LimiterGuard(const LimiterGuard&) = delete;
	LimiterGuard& operator=(const LimiterGuard&) = delete;

private:
	ConcurrencyLimiter& limiter_;
};

// The part of metadata.json this stub reads. The real module reads more and ignores x_stub entirely.
struct [[nodiscard]] StubMetadata {
	uint32_t dimension = 0;
	uint32_t maxInputTokens = 0;
	uint64_t memoryUsageBytes = 0;
	rx_embed_status failLoad = RX_EMBED_OK;
	rx_embed_status failEncode = RX_EMBED_OK;
	int64_t reportDimension = -1;  // -1 means the declared dimension is reported
	uint32_t encodeDelayMs = 0;
	bool overreportFloats = false;
	int64_t expectIntraOpThreads = -1;	// -1 means the value init received is not checked
	// Values model_info may not report: the metadata format keeps both numbers inside 1..65535.
	bool reportZeroDimension = false;
	bool reportHugeMaxInputTokens = false;
};

// Opens a file whose path is UTF-8. On Windows std::fopen reads the bytes in the active ANSI code page and would not find a model
// directory with non-ASCII characters, so the path goes through UTF-16 and _wfopen. wchar_t stays inside this call.
[[nodiscard]] std::FILE* openUtf8Path(const std::string& path) noexcept {
#if defined(_WIN32)
	const int wideLen = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path.c_str(), int(path.size()), nullptr, 0);
	if (wideLen <= 0) {
		return nullptr;
	}
	try {
		// The buffer allocates, and this function is noexcept, so a failed allocation has to become a failed open.
		std::wstring widePath(size_t(wideLen), L'\0');
		if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path.c_str(), int(path.size()), widePath.data(), wideLen) != wideLen) {
			return nullptr;
		}
		return _wfopen(widePath.c_str(), L"rb");
	} catch (...) {
		return nullptr;
	}
#else
	return std::fopen(path.c_str(), "rb");
#endif
}

[[nodiscard]] bool readWholeFile(const std::string& path, std::string& content, std::string& error) {
	std::FILE* const file = openUtf8Path(path);
	if (!file) {
		error = "cannot open '" + path + "'";
		return false;
	}
	content.clear();
	char buf[4096];
	bool complete = false;
	// A short read means end of file or an error, so the loop stops there rather than on a zero count.
	while (!complete) {
		const size_t got = std::fread(buf, 1, sizeof(buf), file);
		complete = got < sizeof(buf);
		if (content.size() + got > kMaxMetadataBytes) {
			std::fclose(file);
			error = "'" + path + "' is larger than the 64 KiB limit";
			return false;
		}
		content.append(buf, got);
	}
	const bool failed = std::ferror(file) != 0;
	std::fclose(file);
	if (failed) {
		error = "cannot read '" + path + "'";
		return false;
	}
	return true;
}

// Two rules the format puts on the whole document, not only on the fields this stub reads: no duplicate keys inside one object, and no
// U+0000 in any string or key. Arrays are walked as well, because an object that breaks a rule can sit inside one.
[[nodiscard]] bool findMetadataViolation(const gason::JsonNode& node, std::string& problem) {
	if (node.value.getTag() == gason::JsonTag::STRING) {
		if (node.value.toString().find('\0') != std::string_view::npos) {
			problem = "a string value contains a zero character";
			return true;
		}
		return false;
	}
	if (!node.isObject() && !node.isArray()) {
		return false;
	}
	std::vector<std::string_view> seen;
	for (const auto& child : node) {
		if (node.isObject()) {
			const std::string_view key = child.key;
			if (key.find('\0') != std::string_view::npos) {
				problem = "a key contains a zero character";
				return true;
			}
			if (std::find(seen.begin(), seen.end(), key) != seen.end()) {
				problem = "duplicate key '" + std::string(key) + "'";
				return true;
			}
			seen.emplace_back(key);
		}
		if (findMetadataViolation(child, problem)) {
			return true;
		}
	}
	return false;
}

[[nodiscard]] bool statusFromName(std::string_view name, bool forLoad, rx_embed_status& status) noexcept {
	if (forLoad) {
		if (name == "model_not_found") {
			status = RX_EMBED_ERR_MODEL_NOT_FOUND;
		} else if (name == "model_invalid") {
			status = RX_EMBED_ERR_MODEL_INVALID;
		} else if (name == "model_unsupported") {
			status = RX_EMBED_ERR_MODEL_UNSUPPORTED;
		} else if (name == "out_of_memory") {
			status = RX_EMBED_ERR_OUT_OF_MEMORY;
		} else if (name == "internal") {
			status = RX_EMBED_ERR_INTERNAL;
		} else {
			return false;
		}
		return true;
	}
	if (name == "inference") {
		status = RX_EMBED_ERR_INFERENCE;
	} else if (name == "internal") {
		status = RX_EMBED_ERR_INTERNAL;
	} else {
		return false;
	}
	return true;
}

// Parses the metadata and reports the failure through status and error, so that a broken fixture is distinguishable from a fixture that
// asks the stub to fail.
[[nodiscard]] bool parseMetadata(const std::string& dir, StubMetadata& out, rx_embed_status& status, std::string& error) {
	std::string path = dir;
	if (!path.empty() && path.back() != '/' && path.back() != '\\') {
		path += '/';
	}
	path += kMetadataFileName;

	std::string content;
	if (!readWholeFile(path, content, error)) {
		status = RX_EMBED_ERR_MODEL_NOT_FOUND;
		return false;
	}

	gason::JsonParser parser;
	gason::JsonNode root = gason::JsonNode::EmptyNode();
	try {
		root = parser.Parse(std::span<char>(content.data(), content.size()));
	} catch (const gason::Exception& e) {
		status = RX_EMBED_ERR_MODEL_INVALID;
		error = std::string("cannot parse '") + path + "': " + e.what();
		return false;
	}

	std::string problem;
	if (findMetadataViolation(root, problem)) {
		status = RX_EMBED_ERR_MODEL_INVALID;
		error = problem + " in '" + path + "'";
		return false;
	}

	const auto readString = [](const gason::JsonNode& parent, std::string_view key) { return parent[key].As<std::string>(); };

	try {
		const int64_t version = root["metadata_version"].As<int64_t>(-1);
		if (version != 1) {
			status = RX_EMBED_ERR_MODEL_INVALID;
			error = "unsupported metadata_version in '" + path + "'";
			return false;
		}

		const int64_t dimension = root["graph"]["dimension"].As<int64_t>(-1);
		if (dimension < 1 || dimension > int64_t(kMaxDimension)) {
			status = RX_EMBED_ERR_MODEL_INVALID;
			error = "graph.dimension is missing or out of the range 1.." + std::to_string(kMaxDimension);
			return false;
		}
		out.dimension = uint32_t(dimension);

		const int64_t maxLength = root["tokenizer"]["max_length"].As<int64_t>(-1);
		if (maxLength < 1 || maxLength > int64_t(kMaxInputTokens)) {
			status = RX_EMBED_ERR_MODEL_INVALID;
			error = "tokenizer.max_length is missing or out of the range 1.." + std::to_string(kMaxInputTokens);
			return false;
		}
		out.maxInputTokens = uint32_t(maxLength);

		// The stub must refuse the metadata the real module would refuse, otherwise a test could pass here and fail against the module.
		const auto& graph = root["graph"];
		if (readString(graph, "file").empty()) {
			status = RX_EMBED_ERR_MODEL_INVALID;
			error = "graph.file is missing or empty";
			return false;
		}
		if (readString(root["tokenizer"], "file").empty()) {
			status = RX_EMBED_ERR_MODEL_INVALID;
			error = "tokenizer.file is missing or empty";
			return false;
		}
		if (readString(graph, "output").empty()) {
			status = RX_EMBED_ERR_MODEL_INVALID;
			error = "graph.output is missing or empty";
			return false;
		}

		// Roles of the graph inputs. input_ids and attention_mask are required, token_type_ids is optional, and no two roles may point at
		// the same input of the graph.
		const auto& inputs = graph["inputs"];
		const std::string inputIds = readString(inputs, "input_ids");
		const std::string attentionMask = readString(inputs, "attention_mask");
		const std::string tokenTypeIds = readString(inputs, "token_type_ids");
		if (inputIds.empty() || attentionMask.empty()) {
			status = RX_EMBED_ERR_MODEL_INVALID;
			error = "graph.inputs must name input_ids and attention_mask";
			return false;
		}
		if (inputIds == attentionMask || (!tokenTypeIds.empty() && (tokenTypeIds == inputIds || tokenTypeIds == attentionMask))) {
			status = RX_EMBED_ERR_MODEL_INVALID;
			error = "graph.inputs maps two roles onto the same input";
			return false;
		}

		const std::string truncation = readString(root["tokenizer"], "truncation");
		const std::string padding = readString(root["tokenizer"], "padding");
		if (truncation != "right" && truncation != "left") {
			status = RX_EMBED_ERR_MODEL_INVALID;
			error = "tokenizer.truncation must be 'right' or 'left'";
			return false;
		}
		if (padding != "right" && padding != "left") {
			status = RX_EMBED_ERR_MODEL_INVALID;
			error = "tokenizer.padding must be 'right' or 'left'";
			return false;
		}

		const auto& inference = root["inference"];
		const std::string pooling = readString(inference, "pooling");
		if (pooling != "mean" && pooling != "cls" && pooling != "last_token" && pooling != "none") {
			status = RX_EMBED_ERR_MODEL_INVALID;
			error = "inference.pooling must be one of mean, cls, last_token, none";
			return false;
		}
		if (inference["normalize"].isEmpty()) {
			status = RX_EMBED_ERR_MODEL_INVALID;
			error = "inference.normalize is missing";
			return false;
		}
		std::ignore = inference["normalize"].As<bool>();
		// cls takes position 0, which is a padding position when the padding is on the left. The module refuses the combination instead of
		// quietly returning the vector of a padding token.
		if (pooling == "cls" && padding == "left") {
			status = RX_EMBED_ERR_MODEL_UNSUPPORTED;
			error = "inference.pooling 'cls' cannot be combined with tokenizer.padding 'left'";
			return false;
		}

		const auto& stub = root["x_stub"];
		if (!stub.isEmpty()) {
			const std::string failLoad = stub["fail_load"].As<std::string>();
			if (!failLoad.empty() && !statusFromName(failLoad, true, out.failLoad)) {
				status = RX_EMBED_ERR_MODEL_INVALID;
				error = "unknown x_stub.fail_load value '" + failLoad + "'";
				return false;
			}
			const std::string failEncode = stub["fail_encode"].As<std::string>();
			if (!failEncode.empty() && !statusFromName(failEncode, false, out.failEncode)) {
				status = RX_EMBED_ERR_MODEL_INVALID;
				error = "unknown x_stub.fail_encode value '" + failEncode + "'";
				return false;
			}
			const auto& reported = stub["report_dimension"];
			if (!reported.isEmpty()) {
				const int64_t value = reported.As<int64_t>(-1);
				if (value < 0 || value > int64_t(kMaxDimension)) {
					status = RX_EMBED_ERR_MODEL_INVALID;
					error = "x_stub.report_dimension is out of the range 0.." + std::to_string(kMaxDimension);
					return false;
				}
				out.reportDimension = value;
			}
			const int64_t delay = stub["encode_delay_ms"].As<int64_t>(0);
			if (delay < 0 || delay > 600000) {
				status = RX_EMBED_ERR_MODEL_INVALID;
				error = "x_stub.encode_delay_ms is out of the range 0..600000";
				return false;
			}
			out.encodeDelayMs = uint32_t(delay);
			const int64_t memory = stub["memory_usage_bytes"].As<int64_t>(0);
			if (memory < 0) {
				status = RX_EMBED_ERR_MODEL_INVALID;
				error = "x_stub.memory_usage_bytes must not be negative";
				return false;
			}
			out.memoryUsageBytes = uint64_t(memory);
			out.overreportFloats = stub["overreport_floats"].As<bool>(false);
			out.expectIntraOpThreads = stub["expect_intra_op_threads"].As<int64_t>(-1);
			out.reportZeroDimension = stub["report_zero_dimension"].As<bool>(false);
			out.reportHugeMaxInputTokens = stub["report_huge_max_input_tokens"].As<bool>(false);
		}
	} catch (const gason::Exception& e) {
		status = RX_EMBED_ERR_MODEL_INVALID;
		error = std::string("invalid metadata in '") + path + "': " + e.what();
		return false;
	}
	return true;
}

}  // namespace

// Set by init. A model cannot be loaded before it, because a real module would have nothing to load it into.
std::atomic<bool> g_initialized{false};

// The one opaque handle of the ABI. Its layout belongs to the module, and the header only forward declares it.
struct [[nodiscard]] rx_embed_model {
	explicit rx_embed_model(int32_t maxConcurrency) noexcept : limiter(maxConcurrency) {}

	StubMetadata meta;
	// encode receives a const handle, but admission control is call state rather than model state.
	mutable ConcurrencyLimiter limiter;
};

extern "C" {

// The stub has no runtime to start, but it validates the options as a real module would and keeps the thread count, so that a model can
// assert what init received (x_stub.expect_intra_op_threads).
std::atomic<int32_t> g_initIntraOpThreads{-1};
static rx_embed_status RX_EMBED_CALL stubInit(const rx_embed_init_options* opts, char* err, size_t errSize) {
	try {
		// Options are validated before anything else, so that a rejected call does not count as the one call the contract allows.
		if (opts) {
			if (opts->struct_size < RX_EMBED_INIT_OPTIONS_MIN_SIZE) {
				setError(err, errSize, "rx_embed_init_options.struct_size is smaller than the required size");
				return RX_EMBED_ERR_INVALID_ARG;
			}
			if (opts->intra_op_num_threads < 0) {
				setError(err, errSize, "intra_op_num_threads must not be negative");
				return RX_EMBED_ERR_INVALID_ARG;
			}
		}
#ifdef RX_EMBED_STUB_INIT_FAILS
		// Only the start of the runtime fails. metadata.json cannot express it: init runs before any model directory is known.
		setError(err, errSize, "the stub was built to fail init");
		return RX_EMBED_ERR_INTERNAL;
#else
		// The same flag answers both questions: whether a second call has to be refused, and whether model_load may run at all.
		if (g_initialized.exchange(true)) {
			setError(err, errSize, "init was called again after it had succeeded");
			return RX_EMBED_ERR_INVALID_ARG;
		}
		g_initIntraOpThreads.store(opts ? opts->intra_op_num_threads : 0);
		return RX_EMBED_OK;
#endif
	} catch (const std::exception& e) {
		setError(err, errSize, e.what());
		return RX_EMBED_ERR_INTERNAL;
	} catch (...) {
		setError(err, errSize, "unknown error in init");
		return RX_EMBED_ERR_INTERNAL;
	}
}

static rx_embed_status RX_EMBED_CALL stubModelLoad(const char* modelDir, const rx_embed_load_options* opts, rx_embed_model** out, char* err,
												   size_t errSize) {
	try {
		if (!g_initialized.load()) {
			setError(err, errSize, "model_load was called before init");
			return RX_EMBED_ERR_INVALID_ARG;
		}
		if (!out) {
			setError(err, errSize, "out must not be NULL");
			return RX_EMBED_ERR_INVALID_ARG;
		}
		if (!modelDir || modelDir[0] == '\0') {
			setError(err, errSize, "model_dir must be a non-empty path");
			return RX_EMBED_ERR_INVALID_ARG;
		}
		if (!isValidUtf8(modelDir, std::strlen(modelDir))) {
			setError(err, errSize, "model_dir is not valid UTF-8");
			return RX_EMBED_ERR_INVALID_ARG;
		}

		int32_t maxConcurrency = int32_t(defaultConcurrency());
		if (opts) {
			if (opts->struct_size < RX_EMBED_LOAD_OPTIONS_MIN_SIZE) {
				setError(err, errSize, "rx_embed_load_options.struct_size is smaller than the required size");
				return RX_EMBED_ERR_INVALID_ARG;
			}
			if (opts->max_concurrency < 0) {
				setError(err, errSize, "max_concurrency must not be negative");
				return RX_EMBED_ERR_INVALID_ARG;
			}
			maxConcurrency = opts->max_concurrency;
		}

		StubMetadata meta;
		rx_embed_status status = RX_EMBED_ERR_MODEL_INVALID;
		std::string error;
		if (!parseMetadata(modelDir, meta, status, error)) {
			setError(err, errSize, error);
			return status;
		}
		if (meta.failLoad != RX_EMBED_OK) {
			setError(err, errSize, "x_stub.fail_load requested this failure");
			return meta.failLoad;
		}
		if (meta.expectIntraOpThreads >= 0 && meta.expectIntraOpThreads != int64_t(g_initIntraOpThreads.load())) {
			setError(err, errSize,
					 "init received intra_op_num_threads=" + std::to_string(g_initIntraOpThreads.load()) + ", the model expects " +
						 std::to_string(meta.expectIntraOpThreads));
			return RX_EMBED_ERR_MODEL_INVALID;
		}

		auto* const model = new (std::nothrow) rx_embed_model(maxConcurrency);
		if (!model) {
			setError(err, errSize, "cannot allocate the model");
			return RX_EMBED_ERR_OUT_OF_MEMORY;
		}
		model->meta = meta;
		*out = model;
		return RX_EMBED_OK;
	} catch (const std::exception& e) {
		setError(err, errSize, e.what());
		return RX_EMBED_ERR_INTERNAL;
	} catch (...) {
		setError(err, errSize, "unknown error in model_load");
		return RX_EMBED_ERR_INTERNAL;
	}
}

static void RX_EMBED_CALL stubModelDestroy(rx_embed_model* model) { delete model; }

static rx_embed_status RX_EMBED_CALL stubModelInfo(const rx_embed_model* model, rx_embed_model_info* out) {
	if (!model || !out) {
		return RX_EMBED_ERR_INVALID_ARG;
	}
	// Not one byte of the structure of the caller may change on error, struct_size included.
	if (out->struct_size < RX_EMBED_MODEL_INFO_MIN_SIZE) {
		return RX_EMBED_ERR_INVALID_ARG;
	}
	// Breaks the contract on request: both numbers are inside 1..65535 for every model the format allows.
	out->dimension = model->meta.reportZeroDimension ? 0 : model->meta.dimension;
	out->memory_usage_bytes = model->meta.memoryUsageBytes;
	out->max_input_tokens = model->meta.reportHugeMaxInputTokens ? 70000 : model->meta.maxInputTokens;
	return RX_EMBED_OK;
}

// The broken-table variant that leaves a required member NULL still compiles this function, it just does not put it into the table.
#if defined(RX_EMBED_STUB_BROKEN_ABI) && RX_EMBED_STUB_BROKEN_ABI == 2
#define RX_STUB_ABSENT_FROM_TABLE [[maybe_unused]]
#else
#define RX_STUB_ABSENT_FROM_TABLE
#endif

RX_STUB_ABSENT_FROM_TABLE static rx_embed_status RX_EMBED_CALL stubEncode(const rx_embed_model* model, rx_embed_input_type inputType,
																		  const char* const* inputs, const size_t* inputSizes, size_t count,
																		  const char* prefix, float* outVectors, size_t outCapacityFloats,
																		  size_t* outFloatsWritten, char* err, size_t errSize) {
	try {
		if (!model || !outFloatsWritten) {
			setError(err, errSize, "model and out_floats_written must not be NULL");
			return RX_EMBED_ERR_INVALID_ARG;
		}
		if (!prefix) {
			setError(err, errSize, "prefix must not be NULL, an empty string means no prefix");
			return RX_EMBED_ERR_INVALID_ARG;
		}
		if (inputType != RX_EMBED_INPUT_TEXT) {
			setError(err, errSize, "input_type must be RX_EMBED_INPUT_TEXT");
			return RX_EMBED_ERR_INVALID_ARG;
		}
		const size_t prefixSize = std::strlen(prefix);
		if (prefixSize > RX_EMBED_MAX_PREFIX_BYTES) {
			setError(err, errSize, "prefix is longer than the allowed limit");
			return RX_EMBED_ERR_INVALID_ARG;
		}
		if (!isValidUtf8(prefix, prefixSize)) {
			setError(err, errSize, "prefix is not valid UTF-8");
			return RX_EMBED_ERR_INVALID_ARG;
		}

		if (count == 0) {
			if (outCapacityFloats != 0) {
				setError(err, errSize, "out_capacity_floats must be 0 when count is 0");
				return RX_EMBED_ERR_INVALID_ARG;
			}
			*outFloatsWritten = 0;
			return RX_EMBED_OK;
		}

		if (!inputs || !inputSizes || !outVectors) {
			setError(err, errSize, "inputs, input_sizes and out_vectors are required when count is not 0");
			return RX_EMBED_ERR_INVALID_ARG;
		}

		// A fixture can ask for a dimension that differs from the declared one, so that the caller can prove it detects the mismatch.
		const uint32_t dimension = model->meta.reportDimension >= 0 ? uint32_t(model->meta.reportDimension) : model->meta.dimension;
		// The overflow check comes before the loop and not after it. A count that cannot be multiplied by the dimension is larger than any
		// array, so walking the inputs first would read past the end of the array of the caller.
		if (dimension != 0 && count > SIZE_MAX / size_t(dimension)) {
			setError(err, errSize, "count multiplied by the dimension does not fit into size_t");
			return RX_EMBED_ERR_INVALID_ARG;
		}

		for (size_t i = 0; i < count; ++i) {
			if (!inputs[i]) {
				setError(err, errSize, "an element of inputs is NULL");
				return RX_EMBED_ERR_INVALID_ARG;
			}
			if (inputSizes[i] > SIZE_MAX - prefixSize) {
				setError(err, errSize, "the length of the prefix plus an input does not fit into size_t");
				return RX_EMBED_ERR_INVALID_ARG;
			}
			if (!isValidUtf8(inputs[i], inputSizes[i])) {
				setError(err, errSize, "an element of inputs is not valid UTF-8");
				return RX_EMBED_ERR_INVALID_ARG;
			}
		}

		const size_t needed = count * size_t(dimension);
		if (needed > outCapacityFloats) {
			setError(err, errSize, "out_vectors is too small for the result");
			return RX_EMBED_ERR_BUFFER_TOO_SMALL;
		}
		if (model->meta.failEncode != RX_EMBED_OK) {
			setError(err, errSize, "x_stub.fail_encode requested this failure");
			return model->meta.failEncode;
		}

		{
			LimiterGuard guard(model->limiter);
			if (model->meta.encodeDelayMs) {
				std::this_thread::sleep_for(std::chrono::milliseconds(model->meta.encodeDelayMs));
			}
			for (size_t i = 0; i < count; ++i) {
				const uint64_t seed = inputSeed(prefix, prefixSize, inputs[i], inputSizes[i]);
				float* const vector = outVectors + i * size_t(dimension);
				for (uint32_t j = 0; j < dimension; ++j) {
					vector[j] = vectorValue(seed, j);
				}
			}
		}
		// Breaks the contract: the reported count is larger than what was written and larger than the buffer. The caller must not trust it.
		*outFloatsWritten = model->meta.overreportFloats ? outCapacityFloats + 1 : needed;
		return RX_EMBED_OK;
	} catch (const std::exception& e) {
		setError(err, errSize, e.what());
		return RX_EMBED_ERR_INTERNAL;
	} catch (...) {
		setError(err, errSize, "unknown error in encode");
		return RX_EMBED_ERR_INTERNAL;
	}
}

static const char* RX_EMBED_CALL stubGetVersionString(void) {
#ifdef RX_EMBED_STUB_NO_VERSION_STRING
	// Breaks the contract: the ABI says this is always a non-NULL NUL terminated string.
	return nullptr;
#else
	return kVersionString;
#endif
}

#if defined(RX_EMBED_STUB_EXTENDED_ABI)

// Stands in for a module built from a newer header: the required prefix byte for byte, with one function appended after it. Reindexer
// must accept such a table and keep using only the members it knows, which is what makes appending a member a compatible change.
static void RX_EMBED_CALL stubFutureCall(void) {}

typedef struct {
	rx_embed_api required;
	void(RX_EMBED_CALL* appended_call)(void);
} rx_embed_api_extended;

static const rx_embed_api_extended kApiExtended = {
	{uint32_t(sizeof(rx_embed_api_extended)), stubInit, stubModelLoad, stubModelDestroy, stubModelInfo, stubEncode}, stubFutureCall};

#elif defined(RX_EMBED_STUB_BROKEN_ABI) && RX_EMBED_STUB_BROKEN_ABI == 1

// A table that claims to be shorter than the required prefix. The loader must refuse it before reading any member.
static const rx_embed_api kApi = {
	uint32_t(RX_EMBED_API_MIN_SIZE) - 1, stubInit, stubModelLoad, stubModelDestroy, stubModelInfo, stubEncode};

#elif defined(RX_EMBED_STUB_BROKEN_ABI) && RX_EMBED_STUB_BROKEN_ABI == 2

// The size is right and the last required member is missing. The loader must refuse the library rather than call through NULL.
static const rx_embed_api kApi = {uint32_t(sizeof(rx_embed_api)), stubInit, stubModelLoad, stubModelDestroy, stubModelInfo, nullptr};

#else

// The module reports the actual size of the table it carries, not the required minimum. The variant that supports no ABI version never
// hands this table out, but it is still what references the functions below.
[[maybe_unused]] static const rx_embed_api kApi = {
	uint32_t(sizeof(rx_embed_api)), stubInit, stubModelLoad, stubModelDestroy, stubModelInfo, stubEncode};

#endif

static const rx_embed_api* RX_EMBED_CALL stubGetApi(uint32_t version) {
#if defined(RX_EMBED_STUB_UNSUPPORTED_ABI)
	// This target covers the branch where the library is present but does not support the requested ABI version.
	(void)version;
	return nullptr;
#elif defined(RX_EMBED_STUB_EXTENDED_ABI)
	// The prefix sub-object is returned directly, so no cast is involved and the layout is the one an appended member would have.
	return version == RX_EMBED_API_VERSION ? &kApiExtended.required : nullptr;
#else
	return version == RX_EMBED_API_VERSION ? &kApi : nullptr;
#endif
}

static const rx_embed_api_base kApiBase = {stubGetApi, stubGetVersionString};

RX_EMBED_EXPORT const rx_embed_api_base* RX_EMBED_CALL rx_embed_get_api_base(void) { return &kApiBase; }

}  // extern "C"
