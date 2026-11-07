// The contract compiled as plain C11. This file has no other purpose: C++ syntax slipping into the header has to break the build here.
#include "core/embedding/rx_embed.h"

// Named once each, so that the declarations are instantiated as C and not only parsed.
rx_embed_status rx_embed_c_compat_use_types(const rx_embed_api_base* base);

rx_embed_status rx_embed_c_compat_use_types(const rx_embed_api_base* base) {
	rx_embed_init_options init;
	rx_embed_load_options load;
	rx_embed_model_info info;
	const rx_embed_api* api;

	init.struct_size = (uint32_t)RX_EMBED_INIT_OPTIONS_MIN_SIZE;
	init.intra_op_num_threads = 1;
	load.struct_size = (uint32_t)RX_EMBED_LOAD_OPTIONS_MIN_SIZE;
	load.max_concurrency = 1;
	info.struct_size = (uint32_t)RX_EMBED_MODEL_INFO_MIN_SIZE;

	if (!base || !base->GetApi || !base->GetVersionString) {
		return RX_EMBED_ERR_INVALID_ARG;
	}
	api = base->GetApi(RX_EMBED_API_VERSION);
	if (!api || api->struct_size < RX_EMBED_API_MIN_SIZE) {
		return RX_EMBED_ERR_MODEL_UNSUPPORTED;
	}
	return (init.intra_op_num_threads > 0 && load.max_concurrency > 0 && info.struct_size > 0) ? RX_EMBED_OK : RX_EMBED_ERR_INTERNAL;
}
