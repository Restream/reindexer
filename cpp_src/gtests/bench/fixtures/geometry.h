#pragma once

#include <string>
#include "base_fixture.h"

namespace reindexer_benchmarks {

class [[nodiscard]] Geometry : private BaseFixture {
public:
	~Geometry() override = default;
	Geometry(Reindexer* db, std::string_view name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		using reindexer::IndexOpts;

		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK());
	}

	void RegisterAllCases();
	reindexer::Error Initialize() override;

private:
	reindexer::Item MakeItem(benchmark::State&) override;

	template <size_t N>
	void Insert(State& state);
	template <size_t N>
	void GetDWithin(State& state);
	template <reindexer::IndexOpts::RTreeIndexType rtreeType>
	void Reset(State& state);

	reindexer::WrSerializer wrSer_;
	int id_ = 0;
};

}  // namespace reindexer_benchmarks
