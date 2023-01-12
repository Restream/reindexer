#pragma once

#include <gtest/gtest.h>

#include "core/reindexer.h"
#include "queries_verifier.h"

class Fuzzing : public virtual ::testing::Test, public QueriesVerifier {
protected:
	reindexer::Reindexer rx_;
};
