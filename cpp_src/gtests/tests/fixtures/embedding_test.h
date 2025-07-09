#pragma once

#include "reindexer_api.h"

class EmbeddingTest : public ReindexerApi {
protected:
	ReindexerTestApi<reindexer::Reindexer> rt;
};
