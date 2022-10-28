#pragma once

#include <string>
#include <vector>

#include "base_fixture.h"

class Aggregation : protected BaseFixture {
public:
	~Aggregation() override = default;
	Aggregation(Reindexer* db, const string& name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK());
		nsdef_.AddIndex("int_data", "hash", "int", IndexOpts());
		nsdef_.AddIndex("int_array_data", "hash", "int", IndexOpts().Array());
		nsdef_.AddIndex("str_data", "hash", "string", IndexOpts());
	}

	void RegisterAllCases() override;
	reindexer::Error Initialize() override;

protected:
	reindexer::Item MakeItem() override;

	template <size_t N>
	void Insert(State& state);
	void Facet(State&);
	void ArrayFacet(State&);
	void MultiFacet(State&);

private:
	reindexer::WrSerializer wrSer_;
	int id_ = 0;
};
