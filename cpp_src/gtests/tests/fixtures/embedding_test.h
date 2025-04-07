#pragma once

#include "reindexer_api.h"

class EmbeddingTest : public ReindexerApi {
protected:
	void SetUp() override;

	ReindexerTestApi<reindexer::Reindexer> rt;
};
