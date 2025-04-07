#pragma once

#include "estl/h_vector.h"

namespace reindexer {

struct PoolConfig {
	size_t connections{10};
	std::string endpointUrl;
	size_t connect_timeout_ms{300};
	size_t read_timeout_ms{5'000};
	size_t write_timeout_ms{5'000};
};

struct EmbedderConfig {
	std::string cacheTag;
	reindexer::h_vector<std::string, 1> fields;
	enum class Strategy { Always, EmptyOnly, Strict } strategy{Strategy::Always};
};

}  // namespace reindexer
