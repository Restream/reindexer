#pragma once

#include "reindexer_api.h"
#include "tools/fsops.h"

class DISABLED_StorageLazyLoadApi : public ReindexerApi {
public:
	DISABLED_StorageLazyLoadApi() : pk_(0), inserted_(0) { rt.reindexer.reset(new Reindexer); }
	~DISABLED_StorageLazyLoadApi() { dropNs(); }

	void SetUp() override {
		connectToDb();
		openNs();
		defineNs();
		fillNs(100);
		closeNs();

		ChangeConfigSettings(default_namespace, true, 1);
		openNs();
		fillNs(100);

		openMemstatNs();
	}

	void ChangeConfigSettings(const std::string& ns, bool lazyLoad, int noQueryIdleThresholdSec) {
		Item item = NewItem(kConfigNamespace);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		char json[1024];
		snprintf(json, sizeof(json) - 1, jsonConfigTemplate, ns.c_str(), lazyLoad ? "true" : "false", noQueryIdleThresholdSec);

		Error err = item.FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what();

		Upsert(kConfigNamespace, item);

		err = Commit(kConfigNamespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void SelectAll() {
		QueryResults qr;
		Error err = rt.reindexer->Select(Query(default_namespace), qr);
		ASSERT_TRUE(err.ok()) << err.what();
	}

protected:
	void connectToDb() {
		reindexer::fs::RmDirAll(kStoragePath);
		Error err = rt.reindexer->Connect("builtin://" + kStoragePath);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void openNs() {
		Error err = rt.reindexer->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void openMemstatNs() {
		Error err = rt.reindexer->OpenNamespace(kMemstatsNamespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void defineNs() {
		DefineNamespaceDataset(default_namespace, {IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0},
												   IndexDeclaration{kFieldRandomName, "tree", "string", IndexOpts(), 0}});
	}

	void fillNs(int count) {
		for (int i = 0; i < count; ++i) {
			Item item = NewItem(default_namespace);
			EXPECT_TRUE(item.Status().ok()) << item.Status().what();

			item[kFieldId] = pk_++;
			item[kFieldRandomName] = RandString();

			Upsert(default_namespace, item);
			++inserted_;
		}
		auto err = Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	void closeNs() {
		Error err = rt.reindexer->CloseNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void dropNs() { rt.reindexer->DropNamespace(default_namespace); }

	int64_t getItemsCount(bool& storageLoaded) {
		QueryResults qr;
		Error err = rt.reindexer->Select(Query(kMemstatsNamespace).Where(kMemstatsFieldName, CondEq, Variant(default_namespace)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_TRUE(qr.Count() > 0);
		Item firstItem = qr[0].GetItem(false);
		Variant itemsCountKv = firstItem[kMemstatsFieldItemsCount];
		Variant storageLoadedKv = firstItem[kMemstatsFieldStorageLoaded];
		storageLoaded = storageLoadedKv.As<bool>();
		return itemsCountKv.As<int64_t>();
	}

	const char* kFieldId = "id";
	const char* kFieldRandomName = "random_name";
	const char* kConfigNamespace = "#config";
	const std::string kStoragePath = "/tmp/reindex/lazy_load_test";
	const char* jsonConfigTemplate = R"json({
                                            "type":"namespaces",
                                            "namespaces":[
                                            {"namespace":"%s","log_level":"none","lazyload":%s,"unload_idle_threshold":%d}
                                            ]})json";

	const char* kMemstatsNamespace = "#memstats";
	const char* kMemstatsFieldName = "name";
	const char* kMemstatsFieldStorageLoaded = "storage_loaded";
	const char* kMemstatsFieldItemsCount = "items_count";

	std::atomic<int> pk_;
	std::atomic<int> inserted_;

	Reindexer db;
};
