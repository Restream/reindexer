// Contract tests of the embedding ABI (rx_embed.h) against the test stub.
//
// The stub is loaded directly, not through reindexer::embedding::Module: the ABI has no process-wide state, so these cases share one
// process, while the loader is covered by the scenario helper. The path arrives as RX_EMBEDDING_STUB_PATH, and a missing library fails.

#include <gtest/gtest.h>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
#include "core/embedding/rx_embed.h"

#ifndef _WIN32
#include <dlfcn.h>
#else
#include <windows.h>
#endif

namespace {
#ifdef _WIN32
// The stub path arrives as UTF-8, like every path in this ABI, so the test opens it the way the core does.
[[nodiscard]] std::wstring toWidePath(const char* path) {
	const int length = int(std::strlen(path));
	const int wideLen = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, length, nullptr, 0);
	if (wideLen <= 0) {
		return {};
	}
	std::wstring wide(size_t(wideLen), L'\0');
	if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, length, wide.data(), wideLen) != wideLen) {
		return {};
	}
	return wide;
}
#endif
}  // namespace

namespace {

// The numeric layout lives in rx_embed.h, so that a module built from the vendored copy checks it too. What is left here is the consumer
// side: the status values and the growth property.
static_assert(RX_EMBED_OK == 0 && RX_EMBED_ERR_INVALID_ARG == 1 && RX_EMBED_ERR_MODEL_NOT_FOUND == 2 && RX_EMBED_ERR_MODEL_INVALID == 3 &&
				  RX_EMBED_ERR_MODEL_UNSUPPORTED == 4 && RX_EMBED_ERR_OUT_OF_MEMORY == 5 && RX_EMBED_ERR_BUFFER_TOO_SMALL == 6 &&
				  RX_EMBED_ERR_INFERENCE == 7 && RX_EMBED_ERR_INTERNAL == 8,
			  "status values are part of the contract and may not be renumbered");
static_assert(RX_EMBED_INPUT_TEXT == 0, "the only input type this contract defines");
static_assert(RX_EMBED_MAX_PREFIX_BYTES == 1024, "the prefix limit is shared with the metadata schema and the index configuration");

// Appending a field must grow sizeof and leave the required boundary alone, which is how a module built from a newer header looks.
struct [[nodiscard]] EnvOptionsWithTail {
	rx_embed_init_options required;
	int32_t appended_field;
};
struct [[nodiscard]] ApiWithTail {
	rx_embed_api required;
	void(RX_EMBED_CALL* appended_call)(void);
};
static_assert(sizeof(EnvOptionsWithTail) > sizeof(rx_embed_init_options), "an appended field grows the structure");
static_assert(offsetof(EnvOptionsWithTail, required) == 0 && RX_EMBED_INIT_OPTIONS_MIN_SIZE == 8,
			  "and leaves the required boundary where it was");
static_assert(sizeof(ApiWithTail) > sizeof(rx_embed_api), "an appended function grows the table");
static_assert(offsetof(ApiWithTail, appended_call) == sizeof(rx_embed_api),
			  "the appended function sits exactly where a member added by a newer header would");

// Appending to the load options the same way EnvOptionsWithTail appends to the init options.
struct [[nodiscard]] LoadOptionsWithTail {
	rx_embed_load_options required;
	int32_t appended_field;
};
static_assert(sizeof(LoadOptionsWithTail) > sizeof(rx_embed_load_options), "an appended field grows the structure");

// A value no implementation can produce. "The output handle is left unchanged" must not be satisfied by writing nullptr over nullptr.
rx_embed_model* const kHandleSentinel = reinterpret_cast<rx_embed_model*>(std::uintptr_t(0xD15EA5E));

constexpr size_t kErrSize = 512;
constexpr uint32_t kFixtureDimension = 4;

std::string modelPath(std::string_view name) { return std::string(REINDEXER_TESTS_DATA_PATH) + "/embedders/models/" + std::string(name); }

// Loaded once for the whole binary: the ABI guarantees the tables are static and the lookups need no environment.
class [[nodiscard]] StubLibrary {
public:
	static const StubLibrary& Instance() {
		static StubLibrary instance;
		return instance;
	}

	// The calling convention is part of the contract, so the pointer this test calls through has to carry it as well.
	using EntryPointT = const rx_embed_api_base*(RX_EMBED_CALL*)();

	// The entry point is kept so that tests can call it again: the contract calls it safe for parallel calls.
	EntryPointT entryPoint = nullptr;
	const rx_embed_api_base* base = nullptr;
	const rx_embed_api* api = nullptr;
	std::string error;

private:
	StubLibrary() {
#ifndef _WIN32
		void* const handle = dlopen(RX_EMBEDDING_STUB_PATH, RTLD_NOW | RTLD_LOCAL);
		if (!handle) {
			const char* const text = dlerror();
			error = std::string("cannot load ") + RX_EMBEDDING_STUB_PATH + ": " + (text ? text : "unknown error");
			return;
		}
		auto* const entry = reinterpret_cast<EntryPointT>(dlsym(handle, "rx_embed_get_api_base"));
#else
		// LoadLibraryW, like the loader in the core: the ANSI entry point would read the path in the active code page.
		const std::wstring widePath = toWidePath(RX_EMBEDDING_STUB_PATH);
		const HMODULE handle = widePath.empty() ? nullptr : LoadLibraryW(widePath.c_str());
		if (!handle) {
			error = std::string("cannot load ") + RX_EMBEDDING_STUB_PATH + ": error " + std::to_string(GetLastError());
			return;
		}
		auto* const entry = reinterpret_cast<EntryPointT>(GetProcAddress(handle, "rx_embed_get_api_base"));
#endif
		if (!entry) {
			error = std::string("rx_embed_get_api_base is not exported by ") + RX_EMBEDDING_STUB_PATH;
			return;
		}
		entryPoint = entry;
		base = entry();
		if (base) {
			api = base->GetApi(RX_EMBED_API_VERSION);
		}
		if (!api) {
			error = std::string("the stub does not provide ABI version ") + std::to_string(RX_EMBED_API_VERSION);
		}
	}
};

// Holds the model of one test.
class [[nodiscard]] EmbeddingAbi : public ::testing::Test {
protected:
	void SetUp() override {
		const auto& library = StubLibrary::Instance();
		ASSERT_TRUE(library.error.empty()) << library.error
										   << "\nThis target depends on the stub, so a normal build produces it. Reaching this means the"
											  " file was removed or the tree is stale; rebuild.";
		api_ = library.api;
		ASSERT_NE(api_, nullptr);
	}

	void TearDown() override {
		if (model_) {
			api_->model_destroy(model_);
		}
	}

	rx_embed_init_options envOptions(int32_t intraOp = 2) const noexcept {
		return rx_embed_init_options{.struct_size = uint32_t(RX_EMBED_INIT_OPTIONS_MIN_SIZE), .intra_op_num_threads = intraOp};
	}

	rx_embed_load_options loadOptions(int32_t maxConcurrency = 4) const noexcept {
		return rx_embed_load_options{.struct_size = uint32_t(RX_EMBED_LOAD_OPTIONS_MIN_SIZE), .max_concurrency = maxConcurrency};
	}

	// init may succeed once per process, so the first test that needs it does it for all of them.
	void initialize() {
		static const bool done = [this] {
			// The one successful call the contract allows is spent on a structure larger than the minimum. The tail really exists, because
			// a receiver may read what the declared size promises.
			EnvOptionsWithTail options = {};
			options.required = envOptions();
			options.required.struct_size = uint32_t(sizeof(EnvOptionsWithTail));
			char err[kErrSize] = {};
			return api_->init(&options.required, err, sizeof(err)) == RX_EMBED_OK;
		}();
		ASSERT_TRUE(done) << "the module refused to start";
	}

	void loadModel(std::string_view fixture = "good", int32_t maxConcurrency = 4) {
		ASSERT_NO_FATAL_FAILURE(initialize());
		const rx_embed_load_options options = loadOptions(maxConcurrency);
		const std::string path = modelPath(fixture);
		ASSERT_EQ(api_->model_load(path.c_str(), &options, &model_, err_, sizeof(err_)), RX_EMBED_OK) << err_;
		ASSERT_NE(model_, nullptr);
	}

	std::vector<float> encodeOne(std::string_view text, std::string_view prefix, uint32_t dimension = kFixtureDimension) {
		std::vector<float> out(dimension);
		const char* const inputs[1] = {text.data()};
		const size_t sizes[1] = {text.size()};
		size_t written = 0;
		const std::string prefixCopy(prefix);
		EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, prefixCopy.c_str(), out.data(), out.size(), &written, err_,
							   sizeof(err_)),
				  RX_EMBED_OK)
			<< err_;
		EXPECT_EQ(written, dimension);
		return out;
	}

	const rx_embed_api* api_ = nullptr;
	rx_embed_model* model_ = nullptr;
	char err_[kErrSize] = {};
};

// ---------------------------------------------------------------- static tables

TEST_F(EmbeddingAbi, StaticTablesAreStable) {
	const auto& library = StubLibrary::Instance();
	ASSERT_NE(library.base, nullptr);
	EXPECT_NE(library.base->GetApi, nullptr);
	EXPECT_NE(library.base->GetVersionString, nullptr);

	const char* const version = library.base->GetVersionString();
	ASSERT_NE(version, nullptr);
	EXPECT_GT(std::strlen(version), 0u);
	// The pointers refer to static data, so repeated lookups give the same addresses.
	EXPECT_EQ(library.base->GetVersionString(), version);
	// The version is compared for equality, so anything but the exact number is refused, in both directions.
	EXPECT_EQ(library.base->GetApi(RX_EMBED_API_VERSION), api_);
	EXPECT_EQ(library.base->GetApi(RX_EMBED_API_VERSION + 1), nullptr);
	EXPECT_EQ(library.base->GetApi(RX_EMBED_API_VERSION - 1), nullptr);
	EXPECT_EQ(library.base->GetApi(0), nullptr);
}

TEST_F(EmbeddingAbi, ApiTableReportsItsOwnSizeAndHasNoEmptyMembers) {
	EXPECT_GE(api_->struct_size, RX_EMBED_API_MIN_SIZE);
	EXPECT_NE(api_->init, nullptr);
	EXPECT_NE(api_->model_load, nullptr);
	EXPECT_NE(api_->model_destroy, nullptr);
	EXPECT_NE(api_->model_info, nullptr);
	EXPECT_NE(api_->encode, nullptr);
}

TEST_F(EmbeddingAbi, LookupsAreSafeConcurrentlyBeforeAnyEnvironment) {
	const auto& library = StubLibrary::Instance();
	std::atomic<int> mismatches{0};
	std::vector<std::thread> threads;
	threads.reserve(8);
	for (int i = 0; i < 8; ++i) {
		threads.emplace_back([&] {
			for (int j = 0; j < 200; ++j) {
				// All three lookups, because the contract calls all three safe without an initialized module.
				const rx_embed_api_base* const base = library.entryPoint();
				if (base != library.base || base->GetApi(RX_EMBED_API_VERSION) != api_ || base->GetVersionString() == nullptr) {
					++mismatches;
				}
			}
		});
	}
	for (auto& thread : threads) {
		thread.join();
	}
	EXPECT_EQ(mismatches.load(), 0);
}

// ---------------------------------------------------------------- struct_size

TEST_F(EmbeddingAbi, EnvOptionsStructSizeBoundary) {
	// A size below the minimum is refused whether or not the module has already started, because the options are checked first.
	rx_embed_init_options options = envOptions();
	options.struct_size = uint32_t(RX_EMBED_INIT_OPTIONS_MIN_SIZE) - 1;
	EXPECT_EQ(api_->init(&options, err_, sizeof(err_)), RX_EMBED_ERR_INVALID_ARG);
	EXPECT_GT(std::strlen(err_), 0u);

	// A rejected call does not count as the one the contract allows, so the module can still be started.
	ASSERT_NO_FATAL_FAILURE(initialize());
}

TEST_F(EmbeddingAbi, EnvInitHappensOncePerProcess) {
	ASSERT_NO_FATAL_FAILURE(initialize());
	const rx_embed_init_options options = envOptions();
	EXPECT_EQ(api_->init(&options, err_, sizeof(err_)), RX_EMBED_ERR_INVALID_ARG) << "the settings belong to the process";
	EXPECT_GT(std::strlen(err_), 0u);
}

TEST_F(EmbeddingAbi, LoadOptionsStructSizeBoundary) {
	ASSERT_NO_FATAL_FAILURE(initialize());
	const std::string path = modelPath("good");

	rx_embed_load_options tooSmall = loadOptions();
	tooSmall.struct_size = uint32_t(RX_EMBED_LOAD_OPTIONS_MIN_SIZE) - 1;
	rx_embed_model* untouched = kHandleSentinel;
	EXPECT_EQ(api_->model_load(path.c_str(), &tooSmall, &untouched, err_, sizeof(err_)), RX_EMBED_ERR_INVALID_ARG);
	EXPECT_EQ(untouched, kHandleSentinel);

	// A larger size, with the tail it declares.
	LoadOptionsWithTail larger = {};
	larger.required = loadOptions();
	larger.required.struct_size = uint32_t(sizeof(LoadOptionsWithTail));
	rx_embed_model* loaded = nullptr;
	ASSERT_EQ(api_->model_load(path.c_str(), &larger.required, &loaded, err_, sizeof(err_)), RX_EMBED_OK) << err_;
	api_->model_destroy(loaded);
}

TEST_F(EmbeddingAbi, ModelInfoStructSizeBoundaryAndUntouchedOnError) {
	ASSERT_NO_FATAL_FAILURE(loadModel());

	rx_embed_model_info info;
	std::memset(&info, 0xAB, sizeof(info));
	info.struct_size = uint32_t(RX_EMBED_MODEL_INFO_MIN_SIZE) - 1;
	EXPECT_EQ(api_->model_info(model_, &info), RX_EMBED_ERR_INVALID_ARG);
	// Not one byte of the caller's structure may change on error, struct_size included.
	rx_embed_model_info reference;
	std::memset(&reference, 0xAB, sizeof(reference));
	reference.struct_size = uint32_t(RX_EMBED_MODEL_INFO_MIN_SIZE) - 1;
	// Copied into byte arrays before the comparison: the padding is part of what must not change, and comparing the structures directly
	// would compare a type whose padding makes its object representation non-unique.
	unsigned char infoBytes[sizeof(info)];
	unsigned char referenceBytes[sizeof(reference)];
	std::memcpy(infoBytes, &info, sizeof(info));
	std::memcpy(referenceBytes, &reference, sizeof(reference));
	EXPECT_EQ(std::memcmp(infoBytes, referenceBytes, sizeof(infoBytes)), 0);

	info = rx_embed_model_info{};
	info.struct_size = uint32_t(RX_EMBED_MODEL_INFO_MIN_SIZE);
	ASSERT_EQ(api_->model_info(model_, &info), RX_EMBED_OK);
	EXPECT_EQ(info.struct_size, uint32_t(RX_EMBED_MODEL_INFO_MIN_SIZE)) << "the module must not overwrite struct_size";
	EXPECT_EQ(info.dimension, kFixtureDimension);
	EXPECT_EQ(info.max_input_tokens, 512u);
	EXPECT_EQ(info.memory_usage_bytes, 123456789ull);
}

TEST_F(EmbeddingAbi, NullArgumentsAreRejected) {
	ASSERT_NO_FATAL_FAILURE(initialize());
	const std::string path = modelPath("good");
	const rx_embed_load_options options = loadOptions();
	rx_embed_model* model = kHandleSentinel;

	EXPECT_EQ(api_->model_load(nullptr, &options, &model, err_, sizeof(err_)), RX_EMBED_ERR_INVALID_ARG);
	EXPECT_EQ(api_->model_load("", &options, &model, err_, sizeof(err_)), RX_EMBED_ERR_INVALID_ARG);
	EXPECT_EQ(api_->model_load(path.c_str(), &options, nullptr, err_, sizeof(err_)), RX_EMBED_ERR_INVALID_ARG);
	EXPECT_EQ(model, kHandleSentinel);

	rx_embed_model_info info{};
	info.struct_size = uint32_t(RX_EMBED_MODEL_INFO_MIN_SIZE);
	EXPECT_EQ(api_->model_info(nullptr, &info), RX_EMBED_ERR_INVALID_ARG);

	// Destroying a null handle is a defined no-op.
	api_->model_destroy(nullptr);
}

// The load options the contract allows next to the ordinary ones: no options at all, no limit, and a negative limit.
TEST_F(EmbeddingAbi, LoadOptionRangesAndDefaults) {
	ASSERT_NO_FATAL_FAILURE(initialize());
	const std::string path = modelPath("good");

	// opts == NULL means the module picks the limit itself.
	rx_embed_model* defaulted = nullptr;
	ASSERT_EQ(api_->model_load(path.c_str(), nullptr, &defaulted, err_, sizeof(err_)), RX_EMBED_OK) << err_;
	api_->model_destroy(defaulted);

	// 0 means no limit. Reindexer never passes it, and it stays a valid ABI value.
	rx_embed_load_options unlimited = loadOptions(0);
	rx_embed_model* noLimit = nullptr;
	ASSERT_EQ(api_->model_load(path.c_str(), &unlimited, &noLimit, err_, sizeof(err_)), RX_EMBED_OK) << err_;
	api_->model_destroy(noLimit);

	rx_embed_load_options negative = loadOptions(-1);
	rx_embed_model* untouched = kHandleSentinel;
	EXPECT_EQ(api_->model_load(path.c_str(), &negative, &untouched, err_, sizeof(err_)), RX_EMBED_ERR_INVALID_ARG);
	EXPECT_EQ(untouched, kHandleSentinel);
	EXPECT_NE(std::string(err_).find("max_concurrency"), std::string::npos) << err_;
}

TEST_F(EmbeddingAbi, EnvOptionRangesAreChecked) {
	rx_embed_init_options options = envOptions();
	options.intra_op_num_threads = -1;
	EXPECT_EQ(api_->init(&options, err_, sizeof(err_)), RX_EMBED_ERR_INVALID_ARG);
	// The text has to name the option. Without that the test would also pass for a module that checks "already started" first, which the
	// contract forbids.
	EXPECT_NE(std::string(err_).find("intra_op_num_threads"), std::string::npos) << err_;

	// The refusal above did not start anything, so the module can still be started afterwards.
	ASSERT_NO_FATAL_FAILURE(initialize());
}

// ---------------------------------------------------------------- encode

TEST_F(EmbeddingAbi, EncodeIsDeterministicAndDependsOnThePrefix) {
	ASSERT_NO_FATAL_FAILURE(loadModel());
	const auto first = encodeOne("hello", "query: ");
	const auto again = encodeOne("hello", "query: ");
	const auto other = encodeOne("hello", "passage: ");
	EXPECT_EQ(first, again) << "the same input must give the same vector";
	EXPECT_NE(first, other) << "different prefixes must give different vectors, otherwise the cache key test is meaningless";

	// Moving one character between the prefix and the text must change the result: the prefix length is part of what is hashed.
	EXPECT_NE(encodeOne("bc", "a"), encodeOne("c", "ab"));
}

TEST_F(EmbeddingAbi, EncodeRejectsNullPrefix) {
	ASSERT_NO_FATAL_FAILURE(loadModel());
	const char* const inputs[1] = {"hello"};
	const size_t sizes[1] = {5};
	std::vector<float> out(kFixtureDimension);
	size_t written = 4242;
	EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, nullptr, out.data(), out.size(), &written, err_, sizeof(err_)),
			  RX_EMBED_ERR_INVALID_ARG);
	EXPECT_EQ(written, 4242u) << "out_floats_written must not change on error";
}

TEST_F(EmbeddingAbi, EncodeRejectsUnknownInputTypeAndInvalidUtf8) {
	ASSERT_NO_FATAL_FAILURE(loadModel());
	std::vector<float> out(kFixtureDimension);
	size_t written = 0;

	const char* const inputs[1] = {"hello"};
	const size_t sizes[1] = {5};
	EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT + 1, inputs, sizes, 1, "", out.data(), out.size(), &written, err_, sizeof(err_)),
			  RX_EMBED_ERR_INVALID_ARG);

	const char* const broken[1] = {"\xff\xfe"};
	const size_t brokenSizes[1] = {2};
	EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, broken, brokenSizes, 1, "", out.data(), out.size(), &written, err_, sizeof(err_)),
			  RX_EMBED_ERR_INVALID_ARG);

	// An overlong encoding of the null character is invalid UTF-8 as well.
	const char* const overlong[1] = {"\xc0\x80"};
	EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, overlong, brokenSizes, 1, "", out.data(), out.size(), &written, err_, sizeof(err_)),
			  RX_EMBED_ERR_INVALID_ARG);

	// A prefix longer than the published limit is rejected, and the limit itself is accepted.
	const std::string tooLong(RX_EMBED_MAX_PREFIX_BYTES + 1, 'x');
	EXPECT_EQ(
		api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, tooLong.c_str(), out.data(), out.size(), &written, err_, sizeof(err_)),
		RX_EMBED_ERR_INVALID_ARG);
	const std::string atLimit(RX_EMBED_MAX_PREFIX_BYTES, 'x');
	EXPECT_EQ(
		api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, atLimit.c_str(), out.data(), out.size(), &written, err_, sizeof(err_)),
		RX_EMBED_OK)
		<< err_;

	// A zero byte inside an input is ordinary data, because the length is passed separately.
	const char* const embedded[1] = {"a\0b"};
	const size_t embeddedSizes[1] = {3};
	EXPECT_EQ(
		api_->encode(model_, RX_EMBED_INPUT_TEXT, embedded, embeddedSizes, 1, "", out.data(), out.size(), &written, err_, sizeof(err_)),
		RX_EMBED_OK)
		<< err_;
}

TEST_F(EmbeddingAbi, EncodeEmptyBatch) {
	ASSERT_NO_FATAL_FAILURE(loadModel());
	size_t written = 999;
	EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, nullptr, nullptr, 0, "", nullptr, 0, &written, err_, sizeof(err_)), RX_EMBED_OK)
		<< err_;
	EXPECT_EQ(written, 0u);

	written = 999;
	EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, nullptr, nullptr, 0, "", nullptr, 8, &written, err_, sizeof(err_)),
			  RX_EMBED_ERR_INVALID_ARG)
		<< "out_capacity_floats must be 0 when count is 0";
	EXPECT_EQ(written, 999u);
}

TEST_F(EmbeddingAbi, EncodeLeavesTheOutputUntouchedOnEveryErrorClass) {
	ASSERT_NO_FATAL_FAILURE(loadModel());
	const char* const inputs[2] = {"hello", "world"};
	const size_t sizes[2] = {5, 5};

	// Too small a buffer: the status says so and nothing at all is written.
	std::vector<float> out(kFixtureDimension, -1.0f);
	const std::vector<float> untouched = out;
	size_t written = 555;
	EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 2, "", out.data(), out.size(), &written, err_, sizeof(err_)),
			  RX_EMBED_ERR_BUFFER_TOO_SMALL);
	EXPECT_EQ(out, untouched);
	EXPECT_EQ(written, 555u);

	// An argument error leaves both alone as well.
	written = 556;
	EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 2, nullptr, out.data(), out.size(), &written, err_, sizeof(err_)),
			  RX_EMBED_ERR_INVALID_ARG);
	EXPECT_EQ(out, untouched);
	EXPECT_EQ(written, 556u);

	// A count that cannot be multiplied by the dimension is refused before the inputs are walked at all.
	written = 557;
	EXPECT_EQ(
		api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, SIZE_MAX / 2, "", out.data(), out.size(), &written, err_, sizeof(err_)),
		RX_EMBED_ERR_INVALID_ARG);
	EXPECT_EQ(out, untouched);
	EXPECT_EQ(written, 557u);
}

TEST_F(EmbeddingAbi, ErrorTextIsTerminatedAndOptional) {
	ASSERT_NO_FATAL_FAILURE(loadModel());
	const char* const inputs[1] = {"hello"};
	const size_t sizes[1] = {5};
	std::vector<float> out(kFixtureDimension);
	size_t written = 0;

	char small[8];
	std::memset(small, 'Z', sizeof(small));
	EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, nullptr, out.data(), out.size(), &written, small, sizeof(small)),
			  RX_EMBED_ERR_INVALID_ARG);
	EXPECT_EQ(small[sizeof(small) - 1], '\0') << "a truncated message is still terminated";
	EXPECT_LT(std::strlen(small), sizeof(small));

	// No error buffer at all is allowed, in both spellings.
	EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, nullptr, out.data(), out.size(), &written, nullptr, 0),
			  RX_EMBED_ERR_INVALID_ARG);
	EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, nullptr, out.data(), out.size(), &written, err_, 0),
			  RX_EMBED_ERR_INVALID_ARG);

	// On success the module does not touch the buffer.
	std::memset(err_, 'Q', sizeof(err_));
	EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, "", out.data(), out.size(), &written, err_, sizeof(err_)),
			  RX_EMBED_OK);
	EXPECT_EQ(err_[0], 'Q');
}

// ---------------------------------------------------------------- controlled failures

TEST_F(EmbeddingAbi, LoadFailuresFromTheFixture) {
	ASSERT_NO_FATAL_FAILURE(initialize());
	const rx_embed_load_options options = loadOptions();
	const struct {
		const char* fixture;
		rx_embed_status expected;
	} cases[] = {
		{"fail_load_not_found", RX_EMBED_ERR_MODEL_NOT_FOUND},
		{"fail_load_invalid", RX_EMBED_ERR_MODEL_INVALID},
		{"fail_load_unsupported", RX_EMBED_ERR_MODEL_UNSUPPORTED},
		{"fail_load_oom", RX_EMBED_ERR_OUT_OF_MEMORY},
		{"fail_load_internal", RX_EMBED_ERR_INTERNAL},
	};
	for (const auto& item : cases) {
		rx_embed_model* model = kHandleSentinel;
		std::memset(err_, 0, sizeof(err_));
		const std::string path = modelPath(item.fixture);
		EXPECT_EQ(api_->model_load(path.c_str(), &options, &model, err_, sizeof(err_)), item.expected) << item.fixture;
		EXPECT_EQ(model, kHandleSentinel) << item.fixture;
		EXPECT_GT(std::strlen(err_), 0u) << item.fixture << " must explain itself";
	}

	// A directory with no metadata at all is reported the same way as a missing model.
	rx_embed_model* model = nullptr;
	const std::string missing = modelPath("no_such_model_directory");
	EXPECT_EQ(api_->model_load(missing.c_str(), &options, &model, err_, sizeof(err_)), RX_EMBED_ERR_MODEL_NOT_FOUND);
}

TEST_F(EmbeddingAbi, EncodeFailuresFromTheFixture) {
	for (const auto& item :
		 {std::pair{"fail_encode_inference", RX_EMBED_ERR_INFERENCE}, std::pair{"fail_encode_internal", RX_EMBED_ERR_INTERNAL}}) {
		rx_embed_model* model = nullptr;
		ASSERT_NO_FATAL_FAILURE(initialize());
		const rx_embed_load_options options = loadOptions();
		const std::string path = modelPath(item.first);
		ASSERT_EQ(api_->model_load(path.c_str(), &options, &model, err_, sizeof(err_)), RX_EMBED_OK) << err_;

		const char* const inputs[1] = {"hello"};
		const size_t sizes[1] = {5};
		std::vector<float> out(kFixtureDimension);
		size_t written = 77;
		EXPECT_EQ(api_->encode(model, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, "", out.data(), out.size(), &written, err_, sizeof(err_)),
				  item.second)
			<< item.first;
		EXPECT_EQ(written, 77u) << "out_floats_written must survive an inference failure";
		api_->model_destroy(model);
	}
}

TEST_F(EmbeddingAbi, MetadataTheModuleMustRefuse) {
	// The stub has to refuse exactly what the module refuses, otherwise a test can pass here and fail against the real thing.
	ASSERT_NO_FATAL_FAILURE(initialize());
	const rx_embed_load_options options = loadOptions();
	const struct {
		const char* fixture;
		rx_embed_status expected;
	} cases[] = {
		{"invalid_pooling", RX_EMBED_ERR_MODEL_INVALID},
		{"invalid_truncation", RX_EMBED_ERR_MODEL_INVALID},
		{"invalid_padding", RX_EMBED_ERR_MODEL_INVALID},
		{"missing_graph_file", RX_EMBED_ERR_MODEL_INVALID},
		{"missing_tokenizer_file", RX_EMBED_ERR_MODEL_INVALID},
		{"missing_graph_output", RX_EMBED_ERR_MODEL_INVALID},
		{"missing_normalize", RX_EMBED_ERR_MODEL_INVALID},
		{"missing_input_role", RX_EMBED_ERR_MODEL_INVALID},
		{"duplicate_input_roles", RX_EMBED_ERR_MODEL_INVALID},
		// Each half is legal on its own; only the combination is refused, and with a different status.
		{"cls_with_left_padding", RX_EMBED_ERR_MODEL_UNSUPPORTED},
	};
	for (const auto& item : cases) {
		rx_embed_model* model = kHandleSentinel;
		std::memset(err_, 0, sizeof(err_));
		const std::string path = modelPath(item.fixture);
		EXPECT_EQ(api_->model_load(path.c_str(), &options, &model, err_, sizeof(err_)), item.expected) << item.fixture;
		EXPECT_EQ(model, kHandleSentinel) << item.fixture;
		EXPECT_GT(std::strlen(err_), 0u) << item.fixture << " must name the field it refused";
	}
}

TEST_F(EmbeddingAbi, BatchOfSeveralInputs) {
	ASSERT_NO_FATAL_FAILURE(loadModel());
	constexpr size_t kCount = 3;
	const char* const inputs[kCount] = {"first", "second", "third"};
	const size_t sizes[kCount] = {5, 6, 5};
	std::vector<float> out(kCount * kFixtureDimension, -7.0f);
	size_t written = 0;
	ASSERT_EQ(
		api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, kCount, "query: ", out.data(), out.size(), &written, err_, sizeof(err_)),
		RX_EMBED_OK)
		<< err_;
	EXPECT_EQ(written, kCount * kFixtureDimension);

	// Vector i occupies out_vectors[i * dimension .. (i + 1) * dimension), and each equals what a batch of one produces for that text.
	for (size_t i = 0; i < kCount; ++i) {
		const auto single = encodeOne(std::string_view(inputs[i], sizes[i]), "query: ");
		const std::vector<float> slice(out.begin() + long(i * kFixtureDimension), out.begin() + long((i + 1) * kFixtureDimension));
		EXPECT_EQ(slice, single) << "vector " << i << " of the batch differs from the same text encoded alone";
	}
	// Different texts must not collapse onto the same vector.
	EXPECT_NE(std::vector<float>(out.begin(), out.begin() + kFixtureDimension),
			  std::vector<float>(out.begin() + kFixtureDimension, out.begin() + 2 * kFixtureDimension));
}

TEST_F(EmbeddingAbi, WrongDimensionIsObservable) {
	ASSERT_NO_FATAL_FAILURE(loadModel("wrong_dimension"));
	rx_embed_model_info info{};
	info.struct_size = uint32_t(RX_EMBED_MODEL_INFO_MIN_SIZE);
	ASSERT_EQ(api_->model_info(model_, &info), RX_EMBED_OK);
	EXPECT_EQ(info.dimension, kFixtureDimension) << "the declared dimension is still the one from the metadata";

	// The fixture makes encode produce a different length. A caller sized by model_info sees BUFFER_TOO_SMALL.
	const char* const inputs[1] = {"hello"};
	const size_t sizes[1] = {5};
	std::vector<float> sized(info.dimension);
	size_t written = 0;
	EXPECT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, "", sized.data(), sized.size(), &written, err_, sizeof(err_)),
			  RX_EMBED_ERR_BUFFER_TOO_SMALL);

	// With a larger buffer the mismatch shows up through out_floats_written, which is what the trial inference relies on.
	std::vector<float> roomy(64);
	ASSERT_EQ(api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, "", roomy.data(), roomy.size(), &written, err_, sizeof(err_)),
			  RX_EMBED_OK)
		<< err_;
	EXPECT_NE(written, size_t(info.dimension));
	EXPECT_EQ(written, 7u);
}

TEST_F(EmbeddingAbi, ModelDirectoryWithNonAsciiCharacters) {
	ASSERT_NO_FATAL_FAILURE(initialize());
	// The fixture directory name is not ASCII. On Windows this is what tells an ANSI open apart from a UTF-8 one: the same bytes are a
	// valid UTF-8 name and an unusable ANSI one.
	const std::string path = modelPath("\u043c\u043e\u0434\u0435\u043b\u044c-unicode");
	const rx_embed_load_options options = loadOptions();
	rx_embed_model* model = nullptr;
	ASSERT_EQ(api_->model_load(path.c_str(), &options, &model, err_, sizeof(err_)), RX_EMBED_OK) << err_;
	api_->model_destroy(model);
}

// ---------------------------------------------------------------- concurrency

TEST_F(EmbeddingAbi, ConcurrentEncodeOnOneHandle) {
	ASSERT_NO_FATAL_FAILURE(loadModel("good", 4));
	const auto expected = encodeOne("hello", "query: ");

	std::atomic<int> mismatches{0};
	std::vector<std::thread> threads;
	threads.reserve(8);
	for (int i = 0; i < 8; ++i) {
		threads.emplace_back([&] {
			for (int j = 0; j < 50; ++j) {
				const char* const inputs[1] = {"hello"};
				const size_t sizes[1] = {5};
				std::vector<float> out(kFixtureDimension);
				size_t written = 0;
				char err[kErrSize] = {};
				rx_embed_model_info info{};
				info.struct_size = uint32_t(RX_EMBED_MODEL_INFO_MIN_SIZE);
				if (api_->model_info(model_, &info) != RX_EMBED_OK ||
					api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, "query: ", out.data(), out.size(), &written, err,
								 sizeof(err)) != RX_EMBED_OK ||
					written != kFixtureDimension || out != expected) {
					++mismatches;
				}
			}
		});
	}
	for (auto& thread : threads) {
		thread.join();
	}
	EXPECT_EQ(mismatches.load(), 0);
}

TEST_F(EmbeddingAbi, ConcurrentModelLoadOnOneEnvironment) {
	ASSERT_NO_FATAL_FAILURE(initialize());
	constexpr const char* kFixtures[] = {"good", "no_prefixes", "wrong_dimension", "slow_encode"};
	std::atomic<int> failures{0};
	std::vector<std::thread> threads;
	for (const char* fixture : kFixtures) {
		threads.emplace_back([&, fixture] {
			const rx_embed_load_options options = loadOptions();
			const std::string path = modelPath(fixture);
			rx_embed_model* model = nullptr;
			char err[kErrSize] = {};
			if (api_->model_load(path.c_str(), &options, &model, err, sizeof(err)) != RX_EMBED_OK || !model) {
				++failures;
				return;
			}
			// Each handle must work on its own, in parallel with the others being created.
			const char* const inputs[1] = {"hello"};
			const size_t sizes[1] = {5};
			std::vector<float> out(64);
			size_t written = 0;
			if (api_->encode(model, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, "", out.data(), out.size(), &written, err, sizeof(err)) !=
				RX_EMBED_OK) {
				++failures;
			}
			api_->model_destroy(model);
		});
	}
	for (auto& thread : threads) {
		thread.join();
	}
	EXPECT_EQ(failures.load(), 0);
}

TEST_F(EmbeddingAbi, ConcurrencyLimitBlocksInsteadOfFailing) {
	// The fixture makes every encode take 200 ms. With one slot the four calls have to queue, so the whole thing cannot take less than the
	// sum. This is a lower bound: an upper bound would be a timing assertion and would flake.
	constexpr int kThreads = 4;
	constexpr auto kDelay = std::chrono::milliseconds(200);
	ASSERT_NO_FATAL_FAILURE(loadModel("slow_encode", 1));

	std::atomic<int> failures{0};
	const auto started = std::chrono::steady_clock::now();
	std::vector<std::thread> threads;
	threads.reserve(kThreads);
	for (int i = 0; i < kThreads; ++i) {
		threads.emplace_back([&] {
			const char* const inputs[1] = {"hello"};
			const size_t sizes[1] = {5};
			std::vector<float> out(kFixtureDimension);
			size_t written = 0;
			char err[kErrSize] = {};
			// No call may be refused: the ABI has no busy status, the extra call waits for a slot and then runs.
			if (api_->encode(model_, RX_EMBED_INPUT_TEXT, inputs, sizes, 1, "", out.data(), out.size(), &written, err, sizeof(err)) !=
				RX_EMBED_OK) {
				++failures;
			}
		});
	}
	for (auto& thread : threads) {
		thread.join();
	}
	const auto elapsed = std::chrono::steady_clock::now() - started;
	EXPECT_EQ(failures.load(), 0);
	EXPECT_GE(elapsed, kThreads * kDelay) << "with one slot the calls must run one after another";
}

}  // namespace
