#pragma once

#include "reindexer_api.h"

class CompositeIndexesApi : public ReindexerApi {
public:
	enum CompositeIndexType { CompositeIndexHash, CompositeIndexBTree };

public:
	void SetUp() override {
		Error err = rt.reindexer->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();

		DefineNamespaceDataset(default_namespace, {IndexDeclaration{kFieldNameBookid, "hash", "int", IndexOpts()},
												   IndexDeclaration{kFieldNameBookid2, "hash", "int", IndexOpts()},
												   IndexDeclaration{kFieldNameTitle, "text", "string", IndexOpts()},
												   IndexDeclaration{kFieldNamePages, "hash", "int", IndexOpts()},
												   IndexDeclaration{kFieldNamePrice, "hash", "int", IndexOpts()},
												   IndexDeclaration{kFieldNameName, "text", "string", IndexOpts()}});
	}

	void fillNamespace(size_t since, size_t till) {
		for (size_t i = since; i < till; ++i) {
			int idValue(static_cast<int>(i));
			addOneRow(idValue, idValue + 77777, kFieldNameTitle + RandString(), rand() % 1000 + 10, rand() % 1000 + 100,
					  kFieldNameName + RandString());
		}
	}

	void addOneRow(int bookid, int bookid2, const string& title, int pages, int price, const string& name) {
		Item item = NewItem(default_namespace);
		item[this->kFieldNameBookid] = bookid;
		item[this->kFieldNameBookid2] = bookid2;
		item[this->kFieldNameTitle] = title;
		item[this->kFieldNamePages] = pages;
		item[this->kFieldNamePrice] = price;
		item[this->kFieldNameName] = name;
		Upsert(default_namespace, item);
		Commit(default_namespace);
	}

	void addCompositeIndex(initializer_list<string> indexes, CompositeIndexType type, const IndexOpts& opts) {
		reindexer::IndexDef indexDeclr;
		indexDeclr.name_ = getCompositeIndexName(indexes);
		indexDeclr.indexType_ = indexTypeToName(type);
		indexDeclr.fieldType_ = "composite";
		indexDeclr.opts_ = opts;
		indexDeclr.jsonPaths_ = reindexer::JsonPaths(indexes);
		Error err = rt.reindexer->AddIndex(default_namespace, indexDeclr);
		EXPECT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	void dropIndex(const string& name) {
		reindexer::IndexDef idef(name);
		Error err = rt.reindexer->DropIndex(default_namespace, idef);
		EXPECT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	string getCompositeIndexName(initializer_list<string> indexes) {
		size_t i = 0;
		string indexName;
		for (const string& subIdx : indexes) {
			indexName += subIdx;
			if (i++ != indexes.size() - 1) {
				indexName += compositePlus;
			}
		}
		return indexName;
	}

	string indexTypeToName(CompositeIndexType indexType) const {
		switch (indexType) {
			case CompositeIndexHash:
				return "hash";
			case CompositeIndexBTree:
				return "tree";
			default:
				throw runtime_error("No such type of composite indexes!");
				break;
		}
	}

	const char* kFieldNameBookid = "bookid";
	const char* kFieldNameBookid2 = "bookid2";
	const char* kFieldNameTitle = "title";
	const char* kFieldNamePages = "pages";
	const char* kFieldNamePrice = "price";
	const char* kFieldNameName = "name";

	const string compositePlus = "+";
};
