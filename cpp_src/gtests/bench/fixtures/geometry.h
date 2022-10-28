#pragma once

#include <string>
#include <vector>

#include "base_fixture.h"

class Geometry : protected BaseFixture {
public:
	~Geometry() override = default;
	Geometry(Reindexer* db, const string& name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK());
	}

	void RegisterAllCases() override;
	reindexer::Error Initialize() override;

protected:
	reindexer::Item MakeItem() override;

	template <size_t N>
	void Insert(State& state);
	template <size_t N>
	void GetDWithin(State& state);
	template <IndexOpts::RTreeIndexType rtreeType>
	void Reset(State& state);

private:
	reindexer::WrSerializer wrSer_;
	int id_ = 0;
};
