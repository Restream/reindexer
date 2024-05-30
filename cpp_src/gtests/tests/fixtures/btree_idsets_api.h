#pragma once

#include "reindexer_api.h"
#include "tools/logger.h"

class BtreeIdsetsApi : public ReindexerApi {
public:
	void SetUp() override {
		Error err = rt.reindexer->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->OpenNamespace(joinedNsName);
		ASSERT_TRUE(err.ok()) << err.what();

		DefineNamespaceDataset(default_namespace, {IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0},
												   IndexDeclaration{kFieldOne, "hash", "string", IndexOpts(), 0},
												   IndexDeclaration{kFieldTwo, "hash", "int", IndexOpts(), 0}});

		DefineNamespaceDataset(joinedNsName, {IndexDeclaration{kFieldIdFk, "hash", "int", IndexOpts().PK(), 0},
											  IndexDeclaration{kFieldThree, "hash", "int", IndexOpts(), 0}});

		FillDefaultNs();
		FillJoinedNs();

		//		reindexer::logInstallWriter([](int level, char* buf) {
		//			if (level <= LogTrace) {
		//				std::cout << buf << std::endl;
		//			}
		//		});
	}

protected:
	void FillDefaultNs() {
		int currIntValue = rand() % 100000;
		std::string currStrValue = RandString();
		for (int i = 0; i < 10000; ++i) {
			Item item(rt.reindexer->NewItem(default_namespace));
			EXPECT_TRUE(!!item);
			EXPECT_TRUE(item.Status().ok()) << item.Status().what();

			item[kFieldId] = i;
			item[kFieldOne] = currStrValue;
			item[kFieldTwo] = currIntValue;

			Upsert(default_namespace, item);
			EXPECT_TRUE(item.Status().ok()) << item.Status().what();

			if (i % 100 == 0) currStrValue = RandString();
			if (i % 200 == 0) currIntValue = rand() % 100000;
		}

		lastStrValue = currStrValue;

		Commit(default_namespace);
	}

	void FillJoinedNs() {
		int currValue = rand() % 10000;
		for (int i = 0; i < 5000; ++i) {
			Item item(rt.reindexer->NewItem(joinedNsName));
			EXPECT_TRUE(!!item);
			EXPECT_TRUE(item.Status().ok()) << item.Status().what();

			item[kFieldIdFk] = rand() % 10000;
			item[kFieldThree] = currValue;

			Upsert(joinedNsName, item);
			EXPECT_TRUE(item.Status().ok()) << item.Status().what();

			if (i % 300) currValue = rand() % 10000;
		}
		Commit(joinedNsName);
	}

	const char* kFieldId = "id";
	const char* kFieldOne = "f1";
	const char* kFieldTwo = "f2";
	const char* kFieldIdFk = "id_fk";
	const char* kFieldThree = "f3";

	std::string lastStrValue;

	const std::string joinedNsName = "joined_ns";
};
