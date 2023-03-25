#pragma once

#include "base_fixture.h"

class JoinItems : protected BaseFixture {
public:
	virtual ~JoinItems() {}

	JoinItems(Reindexer* db, size_t maxItems, size_t idStart = 7000) : BaseFixture(db, "JoinItems", maxItems, idStart) {
		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK())
			.AddIndex("name", "tree", "string", IndexOpts())
			.AddIndex("location", "hash", "string", IndexOpts())
			.AddIndex("device", "hash", "string", IndexOpts());
	}

	virtual reindexer::Error Initialize();
	virtual void RegisterAllCases();

protected:
	virtual reindexer::Item MakeItem();

	std::string randomString(const std::string& prefix);

private:
	std::vector<std::string> adjectives_;
	std::vector<std::string> devices_;
	std::vector<std::string> locations_;
	std::vector<std::string> names_;
};
