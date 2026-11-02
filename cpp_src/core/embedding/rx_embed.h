/*
 * rx_embed.h - the C ABI between Reindexer and the embedding module (libreindexer-embedding).
 *
 * This file belongs to Reindexer. The module keeps a copy and its CI compares the two, so a change needs a coordinated release of both.
 *
 * The caller allocates every output buffer, the error buffer included; there is no global "last error". Extensible structures start with
 * struct_size and grow only at the end. The module gets a path to an unpacked model directory and returns vectors; it keeps no state about
 * model names, databases or storage.
 *
 * Reindexer opens the library with dlopen(RTLD_NOW | RTLD_LOCAL); RTLD_GLOBAL is forbidden, because ONNX Runtime links protobuf into
 * itself statically. It then calls GetApi(RX_EMBED_API_VERSION); NULL means the module was built from another version of this contract,
 * and Reindexer logs one line and works without embedders. The library is never unloaded.
 *
 * Rules for every function below. The compiler does not check them.
 *
 *   1. Exceptions must not cross the boundary: an implementation catches everything and returns a status. model_destroy has no status and
 *      must not throw at all.
 *   2. RX_EMBED_CALL fixes the calling convention. The compiler default is not enough: MSVC changes it with /Gd /Gr /Gz, and a mismatch
 *      corrupts the stack instead of failing to link.
 *   3. Pointers from rx_embed_get_api_base, GetApi and GetVersionString point to static data and are never freed.
 *   4. On any status other than RX_EMBED_OK the output handle stays unchanged, and the other output buffers are undefined unless the call
 *      says otherwise.
 *   5. The error buffer is filled only when the status is not RX_EMBED_OK, and is then NUL terminated even when the text is cut short.
 *      There is no truncation flag. err == NULL or err_size == 0 is always allowed.
 *   6. struct_size fails only when it is too small. A larger value is never an error.
 *   7. Every function pointer of rx_embed_api_base, and of the RX_EMBED_API_MIN_SIZE prefix of rx_embed_api, must be non-NULL; a NULL
 *      there makes the library unusable.
 */

#ifndef RX_EMBED_H_INCLUDED
#define RX_EMBED_H_INCLUDED

/* The header compiles as C and as C++ with no include on the user side. */
#include <stddef.h>
#include <stdint.h>

/* Version of this contract. GetApi accepts this number and refuses any other. Versions are not negotiated.
 *
 * The number moves rarely, because struct_size absorbs most growth: a member added to a table, or a field added to a structure, keeps it
 * as it is. It moves when a layout shifts, or when a field keeps its place and changes its meaning. */
#define RX_EMBED_API_VERSION 1

/* Marks the single exported entry point and nothing else. On ELF and Mach-O the module and the test stub define RX_EMBED_BUILDING_LIBRARY
 * while they build themselves and a consumer never does; the export list is then cut down by a version script on ELF and an exported
 * symbols list on Mach-O.
 *
 * On Windows the name must reach GetProcAddress undecorated, so an MSVC build names the export in a module definition file and does not
 * define this macro: two mechanisms would put two names into the table. WINDOWS_EXPORT_ALL_SYMBOLS stays off in every case. */
#if defined(_WIN32)
#if defined(RX_EMBED_BUILDING_LIBRARY)
#define RX_EMBED_EXPORT __declspec(dllexport)
#else
#define RX_EMBED_EXPORT
#endif
#elif defined(__GNUC__) || defined(__clang__)
#define RX_EMBED_EXPORT __attribute__((visibility("default")))
#else
#define RX_EMBED_EXPORT
#endif

/* The calling convention of the entry point and of every function pointer below. See rule 2. */
#if defined(_WIN32)
#define RX_EMBED_CALL __cdecl
#else
#define RX_EMBED_CALL
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* The constants live in an unnamed enum and the type itself is a fixed width integer. A typedef enum has no guaranteed width, and C and
 * C++ may choose different underlying types for it. */
typedef int32_t rx_embed_status;
enum {
	RX_EMBED_OK = 0,
	RX_EMBED_ERR_INVALID_ARG = 1,
	RX_EMBED_ERR_MODEL_NOT_FOUND = 2,	/* The model directory or a required file is missing. */
	RX_EMBED_ERR_MODEL_INVALID = 3,		/* The metadata or the graph is invalid. */
	RX_EMBED_ERR_MODEL_UNSUPPORTED = 4, /* The runtime does not support this model. */
	RX_EMBED_ERR_OUT_OF_MEMORY = 5,
	RX_EMBED_ERR_BUFFER_TOO_SMALL = 6,
	RX_EMBED_ERR_INFERENCE = 7,
	RX_EMBED_ERR_INTERNAL = 8
};

typedef int32_t rx_embed_input_type;
enum {
	RX_EMBED_INPUT_TEXT = 0
	/* RX_EMBED_INPUT_IMAGE = 1 is reserved and is not implemented. */
};

/* Longest prefix encode accepts, in bytes, without the terminating NUL. Reindexer applies the same limit to the prefixes in the model
 * metadata and in the index configuration. */
#define RX_EMBED_MAX_PREFIX_BYTES 1024

/* Opaque handle. Its layout belongs to the module. */
typedef struct rx_embed_model rx_embed_model;

/* Options of init, filled by the caller. struct_size must be at least RX_EMBED_INIT_OPTIONS_MIN_SIZE. A smaller value is
 * RX_EMBED_ERR_INVALID_ARG, a larger one is not an error. A negative thread count is RX_EMBED_ERR_INVALID_ARG.
 *
 * One thread pool serves the whole process, and operators of one model never run in parallel. Neither can be chosen, so this structure
 * carries a single number. */
typedef struct {
	uint32_t struct_size;
	int32_t intra_op_num_threads; /* Threads used inside one operator. 0 selects automatically. */
} rx_embed_init_options;

/* Options of model_load, filled by the caller. The struct_size rule is the same as for rx_embed_init_options. max_concurrency limits how
 * many encode calls run inference on one model handle at the same time. 0 means no limit, a negative value is RX_EMBED_ERR_INVALID_ARG. */
typedef struct {
	uint32_t struct_size;
	int32_t max_concurrency; /* 0 disables the limit. */
} rx_embed_load_options;

/* Model information. The caller fills struct_size before the call. The module writes only the fields that fit into that size and does not
 * touch struct_size. A size below RX_EMBED_MODEL_INFO_MIN_SIZE is RX_EMBED_ERR_INVALID_ARG. */
typedef struct {
	uint32_t struct_size;
	uint32_t dimension;			 /* Length of one output vector, in floats. */
	uint64_t memory_usage_bytes; /* ORT allocator bytes of this model, see below. */
	uint32_t max_input_tokens;	 /* Token limit of one input, taken from the model metadata. */
} rx_embed_model_info;

/* memory_usage_bytes is TotalAllocated of the private CPU arena of the inference session behind this handle: the regions in use and the
 * free ones the arena holds in reserve, and nothing else - not the thread pools, the library, the tokenizer or the buffers of the caller.
 * Sessions do not share allocators, so the values of several handles can be added up. It is a snapshot and can go down as well as up. */

/* Starts the module with the settings it uses for the whole process. Called once, before the first model_load, and there is no matching
 * shutdown: whatever it starts stays until the process ends. opts == NULL means intra_op_num_threads equal to the number of logical
 * processors, at least 1. A second successful init is RX_EMBED_ERR_INVALID_ARG; a rejected call does not count, so options are checked
 * first.
 *
 * Thread safety: the caller calls this once per process and serializes it against everything else. */
typedef rx_embed_status(RX_EMBED_CALL* rx_embed_init_fn)(const rx_embed_init_options* opts, char* err, size_t err_size);

/* Loads one model from an unpacked directory. init must have run before. model_dir and out are required, model_dir is a non-empty NUL
 * terminated UTF-8 path, and on error the value out points to stays unchanged. The rest of the settings come from the metadata file in that
 * directory, whose format is specified separately. If the runtime reports no allocator statistics for the new session, model_load fails
 * with RX_EMBED_ERR_INTERNAL. opts == NULL means max_concurrency equal to the number of logical processors, at least 1, and the model uses
 * the thread pool that init started.
 *
 * Thread safety: parallel calls are safe, each produces an independent handle, and a call may overlap with encode and model_info on other
 * handles. */
typedef rx_embed_status(RX_EMBED_CALL* rx_embed_model_load_fn)(const char* model_dir, const rx_embed_load_options* opts,
															   rx_embed_model** out, char* err, size_t err_size);

/* Destroys one model handle. model == NULL is a no-op. This function must not throw.
 *
 * Thread safety: the caller never calls it at the same time as encode or model_info on the same handle. */
typedef void(RX_EMBED_CALL* rx_embed_model_destroy_fn)(rx_embed_model* model);

/* Reports the properties of a loaded model. model and out are required. On error not a single byte of the structure of the caller
 * changes. A valid handle allows only two failures: RX_EMBED_ERR_INVALID_ARG for a struct_size that is too small, and
 * RX_EMBED_ERR_INTERNAL when the allocator statistics cannot be read. That is why this call has no error buffer.
 *
 * Thread safety: any number of parallel calls on one handle is safe, in any combination with encode on it. */
typedef rx_embed_status(RX_EMBED_CALL* rx_embed_model_info_fn)(const rx_embed_model* model, rx_embed_model_info* out);

/* Turns a batch of texts into vectors. model, prefix and out_floats_written are required, and input_type must be RX_EMBED_INPUT_TEXT.
 *
 * inputs holds count texts and input_sizes their lengths in bytes; each text is exactly input_sizes[i] bytes of valid UTF-8, and a U+0000
 * inside it is ordinary data. When count > 0, inputs, input_sizes, out_vectors and each inputs[i] are required. Invalid UTF-8, or NULL
 * where a pointer is required, is RX_EMBED_ERR_INVALID_ARG.
 *
 * prefix goes in front of every text, is read up to the first NUL, must be valid UTF-8 of at most RX_EMBED_MAX_PREFIX_BYTES bytes and must
 * not contain U+0000. An empty string means no prefix; NULL is RX_EMBED_ERR_INVALID_ARG.
 *
 * out_vectors belongs to the caller and holds at least count * dimension floats, with dimension from model_info; vector i occupies
 * out_vectors[i * dimension .. (i + 1) * dimension). A result that does not fit into out_capacity_floats is
 * RX_EMBED_ERR_BUFFER_TOO_SMALL. On RX_EMBED_OK the number of floats goes into out_floats_written, and any number other than
 * count * dimension means a broken model: this contract has no partial results.
 *
 * count == 0 returns RX_EMBED_OK and writes 0; inputs, input_sizes and out_vectors may then be NULL, and out_capacity_floats must be 0.
 * Size arithmetic that does not fit into size_t is RX_EMBED_ERR_INVALID_ARG, a failed allocation of a valid size is
 * RX_EMBED_ERR_OUT_OF_MEMORY, and for an argument error, an overflow or a buffer that is too small nothing is written to out_vectors.
 * When max_concurrency is reached the call waits inside the module for a free slot: no BUSY status, no queue order, no cancellation.
 *
 * Thread safety: any number of parallel calls on one handle is safe, in any combination with model_info on it, and a call may overlap with
 * model_load. */
typedef rx_embed_status(RX_EMBED_CALL* rx_embed_encode_fn)(const rx_embed_model* model, rx_embed_input_type input_type,
														   const char* const* inputs, const size_t* input_sizes, size_t count,
														   const char* prefix, float* out_vectors, size_t out_capacity_floats,
														   size_t* out_floats_written, char* err, size_t err_size);

/* The function table. The module fills struct_size with the size of the table it carries, and the caller checks that this size covers
 * every member it calls. The table only grows at the end and never changes what is already there, so the two sides may differ by a few
 * added members without a new version. */
typedef struct {
	uint32_t struct_size;
	rx_embed_init_fn init;
	rx_embed_model_load_fn model_load;
	rx_embed_model_destroy_fn model_destroy;
	rx_embed_model_info_fn model_info;
	rx_embed_encode_fn encode;
} rx_embed_api;

/* The part of each structure that must always be there. An incoming struct_size is compared against these macros, not against sizeof: the
 * caller writes the matching macro into struct_size, the receiver refuses anything smaller, and a field added at the end leaves these
 * values as they are. Each macro is a size_t constant expression that fits into uint32_t and carries no cast, which Reindexer would reject
 * under -Wold-style-cast. RX_EMBED_MODEL_INFO_MIN_SIZE covers the fields, not the padding after them. */
#define RX_EMBED_INIT_OPTIONS_MIN_SIZE (offsetof(rx_embed_init_options, intra_op_num_threads) + sizeof(int32_t))
#define RX_EMBED_LOAD_OPTIONS_MIN_SIZE (offsetof(rx_embed_load_options, max_concurrency) + sizeof(int32_t))
#define RX_EMBED_MODEL_INFO_MIN_SIZE (offsetof(rx_embed_model_info, max_input_tokens) + sizeof(uint32_t))
#define RX_EMBED_API_MIN_SIZE (offsetof(rx_embed_api, encode) + sizeof(rx_embed_encode_fn))

/* Returns the function table if version is the version the module was built with, and NULL otherwise.
 *
 * Thread safety: safe for parallel calls and needs no initialization. */
typedef const rx_embed_api*(RX_EMBED_CALL* rx_embed_get_api_fn)(uint32_t version);

/* Returns a human readable version string of the module. The result is always non-NULL, NUL terminated and UTF-8.
 *
 * Thread safety: safe for parallel calls and needs no initialization. */
typedef const char*(RX_EMBED_CALL* rx_embed_get_version_string_fn)(void);

/* The base table, reached through the single exported symbol. */
typedef struct {
	rx_embed_get_api_fn GetApi;
	rx_embed_get_version_string_fn GetVersionString;
} rx_embed_api_base;

/* static_assert is spelled differently in the two languages this header must compile in. */
#if defined(__cplusplus)
#define RX_EMBED_STATIC_ASSERT(cond, msg) static_assert(cond, msg)
#else
#define RX_EMBED_STATIC_ASSERT(cond, msg) _Static_assert(cond, msg)
#endif

/* The frozen layout, in numbers. The _MIN_SIZE macros are built from offsetof, so comparing them with offsetof again would hold after a
 * field moved; these assertions stop the build instead. The tables are expressed in multiples of one member, so that they hold on any
 * pointer width. sizeof(rx_embed_model_info) is left out: its trailing padding differs between ABIs. */
RX_EMBED_STATIC_ASSERT(sizeof(rx_embed_status) == 4, "a status is a fixed width 32-bit integer");
RX_EMBED_STATIC_ASSERT(sizeof(rx_embed_input_type) == 4, "an input type is a fixed width 32-bit integer");

RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_init_options, struct_size) == 0, "struct_size comes first");
RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_init_options, intra_op_num_threads) == 4, "frozen layout");
RX_EMBED_STATIC_ASSERT(sizeof(rx_embed_init_options) == 8, "frozen layout");
RX_EMBED_STATIC_ASSERT(RX_EMBED_INIT_OPTIONS_MIN_SIZE == 8, "frozen boundary");

RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_load_options, struct_size) == 0, "struct_size comes first");
RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_load_options, max_concurrency) == 4, "frozen layout");
RX_EMBED_STATIC_ASSERT(sizeof(rx_embed_load_options) == 8, "frozen layout");
RX_EMBED_STATIC_ASSERT(RX_EMBED_LOAD_OPTIONS_MIN_SIZE == 8, "frozen boundary");

RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_model_info, struct_size) == 0, "struct_size comes first");
RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_model_info, dimension) == 4, "frozen layout");
RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_model_info, memory_usage_bytes) == 8, "frozen layout");
RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_model_info, max_input_tokens) == 16, "frozen layout");
RX_EMBED_STATIC_ASSERT(RX_EMBED_MODEL_INFO_MIN_SIZE == 20, "frozen boundary");
RX_EMBED_STATIC_ASSERT(RX_EMBED_MODEL_INFO_MIN_SIZE <= sizeof(rx_embed_model_info), "the boundary fits into the structure");

/* One member of the table, used as the unit of the offsets below. Every member is a function pointer, so they all have this size. */
#define RX_EMBED_API_SLOT sizeof(rx_embed_encode_fn)

RX_EMBED_STATIC_ASSERT(sizeof(rx_embed_init_fn) == RX_EMBED_API_SLOT, "every member of the table is a function pointer");
RX_EMBED_STATIC_ASSERT(sizeof(rx_embed_get_api_fn) == RX_EMBED_API_SLOT, "and so is every member of the base table");
RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_api, struct_size) == 0, "struct_size comes first");
RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_api, init) == RX_EMBED_API_SLOT, "frozen layout: struct_size, then the members, with no gaps");
RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_api, model_load) == 2 * RX_EMBED_API_SLOT, "frozen layout");
RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_api, model_destroy) == 3 * RX_EMBED_API_SLOT, "frozen layout");
RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_api, model_info) == 4 * RX_EMBED_API_SLOT, "frozen layout");
RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_api, encode) == 5 * RX_EMBED_API_SLOT, "frozen layout");
RX_EMBED_STATIC_ASSERT(sizeof(rx_embed_api) == 6 * RX_EMBED_API_SLOT, "frozen layout");
RX_EMBED_STATIC_ASSERT(RX_EMBED_API_MIN_SIZE == 6 * RX_EMBED_API_SLOT, "frozen boundary");

RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_api_base, GetApi) == 0, "frozen layout");
RX_EMBED_STATIC_ASSERT(offsetof(rx_embed_api_base, GetVersionString) == RX_EMBED_API_SLOT, "frozen layout");
RX_EMBED_STATIC_ASSERT(sizeof(rx_embed_api_base) == 2 * RX_EMBED_API_SLOT, "frozen layout");

/* The only symbol the module exports. It always returns a non-NULL pointer to a static table.
 *
 * Thread safety: safe for parallel calls and needs no initialization. */
RX_EMBED_EXPORT const rx_embed_api_base* RX_EMBED_CALL rx_embed_get_api_base(void);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* RX_EMBED_H_INCLUDED */
