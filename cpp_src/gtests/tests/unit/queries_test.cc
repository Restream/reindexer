#include "queries_api.h"

TEST_F(QueriesApi, QueriesStandardTestSet) {
	FillDefaultNamespace(0, 2500, 20);
	FillDefaultNamespace(2500, 2500, 0);
	FillTestSimpleNamespace();

	CheckStandartQueries();
	CheckAggregationQueries();
	CheckSqlQueries();

	int itemsCount = 0;
	InsertedItemsByPk& items = insertedItems[default_namespace];
	for (auto it = items.begin(); it != items.end();) {
		Error err = reindexer->Delete(default_namespace, it->second.get());
		EXPECT_TRUE(err.ok()) << err.what();
		it = items.erase(it);
		if (++itemsCount == 4000) break;
	}

	FillDefaultNamespace(0, 500, 0);
	FillDefaultNamespace(0, 1000, 5);

	itemsCount = 0;
	items = insertedItems[default_namespace];
	for (auto it = items.begin(); it != items.end();) {
		Error err = reindexer->Delete(default_namespace, it->second.get());
		EXPECT_TRUE(err.ok()) << err.what();
		it = items.erase(it);
		if (++itemsCount == 5000) break;
	}

	for (size_t i = 0; i < 5000; ++i) {
		auto itToRemove = items.begin();
		if (itToRemove != items.end()) {
			Error err = reindexer->Delete(default_namespace, itToRemove->second.get());
			EXPECT_TRUE(err.ok()) << err.what();
			items.erase(itToRemove);
		}
		FillDefaultNamespace(rand() % 100, 1, 0);

		itToRemove = items.begin();
		std::advance(itToRemove, rand() % 100);
		if (itToRemove != items.end()) {
			Error err = reindexer->Delete(default_namespace, itToRemove->second.get());
			EXPECT_TRUE(err.ok()) << err.what();
			items.erase(itToRemove);
		}
	}

	for (auto it = items.begin(); it != items.end();) {
		Error err = reindexer->Delete(default_namespace, it->second.get());
		EXPECT_TRUE(err.ok()) << err.what();
		it = items.erase(it);
	}

	FillDefaultNamespace(3000, 1000, 20);
	FillDefaultNamespace(1000, 500, 00);

	CheckStandartQueries();
	CheckAggregationQueries();
	CheckSqlQueries();
}
