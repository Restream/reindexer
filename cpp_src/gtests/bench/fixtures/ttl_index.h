#pragma once

#include "base_fixture.h"

class TtlIndexFixture : protected BaseFixture {
public:
	TtlIndexFixture(Reindexer* db, const string& name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK())
			.AddIndex("data", "tree", "int", IndexOpts())
			.AddIndex(IndexDef("date", {"date"}, "ttl", "int64", IndexOpts(), 2));
	}
	virtual ~TtlIndexFixture() {}

	void RegisterAllCases() override;
	Error Initialize() override;
	Item MakeItem() override;

	void ItemsSimpleVanishing(State& state);
	void ItemsVanishingAfterInsertRemove(State& state);

protected:
	void addDataToNs(size_t count);
	void removeAll();
	void removeItems(int idFirst, int idLast);
	void removeItemsSlowly();
	void insertItemsSlowly();
	void selectData();
    int waitForVanishing();
	size_t getItemsCount();
};
