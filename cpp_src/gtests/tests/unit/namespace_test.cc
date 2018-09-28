#include "ns_api.h"

TEST_F(NsApi, UpsertWithPrecepts) {
	Error err = reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK()},
											   IndexDeclaration{updatedTimeSecFieldName.c_str(), "", "int64", IndexOpts()},
											   IndexDeclaration{updatedTimeMSecFieldName.c_str(), "", "int64", IndexOpts()},
											   IndexDeclaration{updatedTimeUSecFieldName.c_str(), "", "int64", IndexOpts()},
											   IndexDeclaration{updatedTimeNSecFieldName.c_str(), "", "int64", IndexOpts()},
											   IndexDeclaration{serialFieldName.c_str(), "", "int64", IndexOpts()}});

	Item item = NewItem(default_namespace);
	item["id"] = idNum;

	// Set precepts
	vector<string> precepts = {updatedTimeSecFieldName + "=NOW()", updatedTimeMSecFieldName + "=NOW(msec)",
							   updatedTimeUSecFieldName + "=NOW(usec)", updatedTimeNSecFieldName + "=NOW(nsec)",
							   serialFieldName + "=SERIAL()"};
	item.SetPrecepts(precepts);

	// Upsert item a few times
	for (int i = 0; i < upsertTimes; i++) {
		auto err = reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	// Get item
	reindexer::QueryResults res;
	err = reindexer->Select("SELECT * FROM " + default_namespace + " WHERE id=" + to_string(idNum), res);
	ASSERT_TRUE(err.ok()) << err.what();

	for (auto it : res) {
		Item item = it.GetItem();
		for (auto idx = 1; idx < item.NumFields(); idx++) {
			auto field = item[idx].Name();

			if (field == updatedTimeSecFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_TRUE(reindexer::getTimeNow("sec") - value < 1) << "Precept function `now()/now(sec)` doesn't work properly";
			} else if (field == updatedTimeMSecFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_TRUE(reindexer::getTimeNow("msec") - value < 1000) << "Precept function `now(msec)` doesn't work properly";
			} else if (field == updatedTimeUSecFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_TRUE(reindexer::getTimeNow("usec") - value < 1000000) << "Precept function `now(usec)` doesn't work properly";
			} else if (field == updatedTimeNSecFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_TRUE(reindexer::getTimeNow("nsec") - value < 1000000000) << "Precept function `now(nsec)` doesn't work properly";
			} else if (field == serialFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_TRUE(value == upsertTimes) << "Precept function `serial()` didn't increment a value to " << upsertTimes << " after "
												  << upsertTimes << " upsert times";
			}
		}
	}
}

TEST_F(NsApi, UpdateIndex) {
	Error err = reindexer->InitSystemNamespaces();
	ASSERT_TRUE(err.ok()) << err.what();
	err = reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK()}});

	auto newIdx = reindexer::IndexDef(idIdxName.c_str(), idIdxName.c_str(), "-", "int64", IndexOpts().PK().Dense());
	err = reindexer->UpdateIndex(default_namespace, newIdx);
	ASSERT_TRUE(err.ok()) << err.what();

	vector<reindexer::NamespaceDef> nsDefs;
	err = reindexer->EnumNamespaces(nsDefs, false);
	ASSERT_TRUE(err.ok()) << err.what();

	auto nsDefIt =
		std::find_if(nsDefs.begin(), nsDefs.end(), [&](const reindexer::NamespaceDef &nsDef) { return nsDef.name == default_namespace; });

	ASSERT_TRUE(nsDefIt != nsDefs.end()) << "Namespace " + default_namespace + " is not found";

	auto &indexes = nsDefIt->indexes;
	auto receivedIdx = std::find_if(indexes.begin(), indexes.end(), [&](const reindexer::IndexDef &idx) { return idx.name_ == idIdxName; });
	ASSERT_TRUE(receivedIdx != indexes.end()) << "Expect index was created, but it wasn't";

	reindexer::WrSerializer newIdxSer(true);
	newIdx.GetJSON(newIdxSer);

	reindexer::WrSerializer receivedIdxSer(true);
	receivedIdx->GetJSON(receivedIdxSer);

	string newIdxJson = newIdxSer.Slice().ToString();
	string receivedIdxJson = receivedIdxSer.Slice().ToString();

	ASSERT_TRUE(newIdxJson == receivedIdxJson);
}
