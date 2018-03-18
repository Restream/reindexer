#pragma once

#include "reindexer_api.h"

class RuntimeIndexesApi : public ReindexerApi {
public:
	void SetUp() override {
		CreateNamespace(default_namespace);
		DefineNamespaceDataset(
			default_namespace,
			{IndexDeclaration{bookid, "hash", "int", IndexOpts().PK()}, IndexDeclaration{bookid2, "hash", "int", IndexOpts().PK()},
			 IndexDeclaration{title, "text", "string", IndexOpts()}, IndexDeclaration{pages, "hash", "int", IndexOpts()},
			 IndexDeclaration{price, "hash", "int", IndexOpts()}, IndexDeclaration{name, "text", "string", IndexOpts()},
			 IndexDeclaration{string(title + string("+") + price).c_str(), "hash", "composite", IndexOpts()}});
	}

protected:
	void FillNamespace(size_t since, size_t till) {
		for (size_t i = since; i < till; ++i) {
			int id = static_cast<int>(i);
			Item item = NewItem(default_namespace);
			item[bookid] = id + 500;
			item[title] = title + RandString();
			item[pages] = rand() % 10000;
			item[price] = rand() % 1000;
			item[name] = name + RandString();
			Upsert(default_namespace, item);
			Commit(default_namespace);
		}
	}

	void AddRuntimeIntArrayIndex(int indexNumber) {
		string indexName = getRuntimeIntIndexName(indexNumber);
		Error err = reindexer->AddIndex(default_namespace, {indexName, indexName, "hash", "int", IndexOpts().Array()});
		EXPECT_TRUE(err.ok()) << err.what();
		err = reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	void DropRuntimeIntArrayIndex(int indexNumber) {
		string indexName = getRuntimeIntIndexName(indexNumber);
		Error err = reindexer->DropIndex(default_namespace, indexName);
		EXPECT_TRUE(err.ok()) << err.what();
		err = reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	void AddDataForRuntimeIntIndex(int indexNumber) {
		string indexName = getRuntimeIntIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(default_namespace);
			item[indexName] = rand() % 100;
			Upsert(default_namespace, item);
		}
		Commit(default_namespace);
	}

	void AddRuntimeStringIndex(int indexNumber, bool pk = false) {
		IndexOpts opts;
		if (pk) opts.PK();
		string indexName = getRuntimeStringIndexName(indexNumber);
		Error err = reindexer->AddIndex(default_namespace, {indexName, indexName, "hash", "string", opts});
		EXPECT_TRUE(err.ok()) << err.what();
		err = reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	void DropRuntimeStringIndex(int indexNumber) {
		string indexName = getRuntimeStringIndexName(indexNumber);
		Error err = reindexer->DropIndex(default_namespace, indexName);
		EXPECT_TRUE(err.ok()) << err.what();
		err = reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	void AddDataForRuntimeStringIndex(int indexNumber) {
		string indexName = getRuntimeStringIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(default_namespace);
			item[indexName] = RandString();
			Upsert(default_namespace, item);
		}
		Commit(default_namespace);
	}

	void AddRuntimeCompositeIndex(bool pk = false) {
		string indexName(getRuntimeCompositeIndexName(pk));
		Error err = reindexer->AddIndex(default_namespace, {indexName, indexName, "tree", "composite", IndexOpts().PK()});
		EXPECT_TRUE(err.ok()) << err.what();
		err = reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	void DropRuntimeCompositeIndex(bool pk = false) {
		string indexName(getRuntimeCompositeIndexName(pk));
		Error err = reindexer->DropIndex(default_namespace, indexName);
		EXPECT_TRUE(err.ok()) << err.what();
		err = reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	void CheckSelectValidity(const Query& query) {
		QueryResults qr;
		Error err = reindexer->Select(query, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	string getRuntimeCompositeIndexName(bool pk) {
		string indexName;
		if (pk) {
			indexName = bookid + string("+") + bookid2;
		} else {
			indexName = bookid + string("+") + title;
		}
		return indexName;
	}

	string getRuntimeIntIndexName(int indexNumber) { return runtime_int + to_string(indexNumber); }
	string getRuntimeStringIndexName(int indexNumber) { return runtime_string + to_string(indexNumber); }

private:
	const char* bookid = "bookid";
	const char* bookid2 = "bookid2";
	const char* title = "title";
	const char* pages = "pages";
	const char* price = "price";
	const char* name = "name";
	const char* runtime_int = "runtime_int_";
	const char* runtime_string = "runtime_string_";

	static const int max_runtime_indexes = 10;
};
