#pragma once

#include <string>
#include "base_fixture.h"

class [[nodiscard]] Aggregation : protected BaseFixture {
public:
	~Aggregation() override = default;
	Aggregation(Reindexer* db, std::string_view name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK());
		nsdef_.AddIndex("int_data", "hash", "int", IndexOpts());
		nsdef_.AddIndex("int_array_data", "hash", "int", IndexOpts().Array());
		nsdef_.AddIndex("str_data", "hash", "string", IndexOpts());
	}

	void RegisterAllCases();
	reindexer::Error Initialize() override;

private:
	class FacetNotEmptyChecker;

	reindexer::Item MakeItem(benchmark::State&) override;

	template <size_t N>
	void Insert(State& state);
	void Facet(State&);
	void ArrayFacet(State&);
	void MultiFacet(State&);

	reindexer::WrSerializer wrSer_;
	int id_ = 0;
};
