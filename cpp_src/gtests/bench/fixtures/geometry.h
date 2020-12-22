#pragma once

#include <string>
#include <vector>

#include "base_fixture.h"

class Geometry : protected BaseFixture {
public:
	virtual ~Geometry() {}
	Geometry(Reindexer* db, const string& name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK());
	}

	virtual void RegisterAllCases();
	virtual Error Initialize();

protected:
	virtual Item MakeItem();

protected:
	void WarmUpIndexes(State& state);

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
