#include "ns_api.h"

TEST_F(NsApi, UpsertWithPrecepts) {
	CreateNamespace(default_namespace);

	IndexOpts idOpts{false, true, false};
	DefineNamespaceDataset(
		default_namespace,
		{IndexDeclaration{"id", "hash", "int", idOpts}, IndexDeclaration{updatedTimeSecFieldName.c_str(), "", "int64", IndexOpts()},
		 IndexDeclaration{updatedTimeMSecFieldName.c_str(), "", "int64", IndexOpts()},
		 IndexDeclaration{updatedTimeUSecFieldName.c_str(), "", "int64", IndexOpts()},
		 IndexDeclaration{updatedTimeNSecFieldName.c_str(), "", "int64", IndexOpts()},
		 IndexDeclaration{serialFieldName.c_str(), "", "int64", IndexOpts()}});

	auto item = AddData(default_namespace, "id", idNum);

	// Set precepts
	vector<string> precepts = {updatedTimeSecFieldName + "=NOW()", updatedTimeMSecFieldName + "=NOW(msec)",
							   updatedTimeUSecFieldName + "=NOW(usec)", updatedTimeNSecFieldName + "=NOW(nsec)",
							   serialFieldName + "=SERIAL()"};
	item->SetPrecepts(precepts);

	// Upsert item a few times
	for (int i = 0; i < upsertTimes; i++) {
		auto err = reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	// Get item
	reindexer::QueryResults res;
	auto err = reindexer->Select("SELECT * FROM " + default_namespace + " WHERE id=" + to_string(idNum), res);
	ASSERT_TRUE(err.ok()) << err.what();

	for (size_t i = 0; i < res.size(); ++i) {
		std::unique_ptr<reindexer::Item> item(res.GetItem(static_cast<int>(i)));
		auto ritem = reinterpret_cast<reindexer::ItemImpl *>(item.get());
		for (auto idx = 1; idx < ritem->NumFields(); idx++) {
			auto field = ritem->Type().Field(idx).Name();

			if (field == updatedTimeSecFieldName) {
				int64_t value = reindexer::KeyValue(ritem->GetField(field)).toInt64();
				ASSERT_TRUE(reindexer::getTimeNow("sec") - value < 1) << "Precept function `now()/now(sec)` doesn't work properly";
			} else if (field == updatedTimeMSecFieldName) {
				int64_t value = reindexer::KeyValue(ritem->GetField(field)).toInt64();
				ASSERT_TRUE(reindexer::getTimeNow("msec") - value < 1000) << "Precept function `now(msec)` doesn't work properly";
			} else if (field == updatedTimeUSecFieldName) {
				int64_t value = reindexer::KeyValue(ritem->GetField(field)).toInt64();
				ASSERT_TRUE(reindexer::getTimeNow("usec") - value < 1000000) << "Precept function `now(usec)` doesn't work properly";
			} else if (field == updatedTimeNSecFieldName) {
				int64_t value = reindexer::KeyValue(ritem->GetField(field)).toInt64();
				ASSERT_TRUE(reindexer::getTimeNow("nsec") - value < 1000000000) << "Precept function `now(nsec)` doesn't work properly";
			} else if (field == serialFieldName) {
				int64_t value = reindexer::KeyValue(ritem->GetField(field)).toInt64();
				ASSERT_TRUE(value == upsertTimes) << "Precept function `serial()` didn't increment a value to " << upsertTimes << " after "
												  << upsertTimes << " upsert times";
			}
		}
	}
}
