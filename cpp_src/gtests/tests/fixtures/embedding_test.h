#pragma once

#include "reindexer_api.h"

class [[nodiscard]] EmbeddingTest : public ReindexerApi {
protected:
	ReindexerTestApi<reindexer::Reindexer> rt;
};
