#pragma once

#include "base_fixture.h"

namespace reindexer_benchmarks {

class [[nodiscard]] JoinItems : private BaseFixture {
public:
	virtual ~JoinItems() {}

	JoinItems(Reindexer* db, size_t maxItems, size_t idStart = 7'000) : BaseFixture(db, "JoinItems", maxItems, idStart) {
		using reindexer::IndexOpts;

		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK())
			.AddIndex("name", "tree", "string", IndexOpts())
			.AddIndex("location", "hash", "string", IndexOpts())
			.AddIndex("device", "hash", "string", IndexOpts());
	}

	reindexer::Error Initialize() override;
	void RegisterAllCases();

private:
	reindexer::Item MakeItem(benchmark::State&) override;

	std::string randomString(const std::string& prefix);

	std::vector<std::string> adjectives_;
	std::vector<std::string> devices_;
	std::vector<std::string> locations_;
	std::vector<std::string> names_;
};

}  // namespace reindexer_benchmarks
