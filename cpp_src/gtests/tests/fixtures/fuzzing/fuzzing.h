#pragma once

#include <gtest/gtest.h>

#include "core/reindexer.h"
#include "queries_verifier.h"

class [[nodiscard]] Fuzzing : public QueriesVerifier {
public:
	void SetUp() override {
		const auto err = rx_.Connect(dsn());
		ASSERT_TRUE(err.ok()) << err.what();
	}
	static void SetDsn(std::string d) noexcept { dsn() = std::move(d); }

private:
	[[nodiscard]] static std::string& dsn() noexcept {
		static std::string d /*{"builtin:///tmp/fuzzing"}*/;
		return d;
	}

protected:
	reindexer::Reindexer rx_;
};
