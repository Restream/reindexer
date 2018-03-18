#pragma once

#include "reindexer_api.h"

class CompositeIndexesApi : public ReindexerApi {
public:
	enum CompositeIndexType { CompositeIndexHash, CompositeIndexBTree };

public:
	void SetUp() override {
		CreateNamespace(default_namespace);
		DefineNamespaceDataset(default_namespace, {IndexDeclaration{kFieldNameBookid, "hash", "int", IndexOpts().PK()},
												   IndexDeclaration{kFieldNameBookid2, "hash", "int", IndexOpts().PK()},
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

	void addIndex(const string& name, CompositeIndexType type, const IndexOpts& opts) {
		reindexer::IndexDef indexDeclr;
		indexDeclr.name = name;
		indexDeclr.jsonPath = name;
		indexDeclr.indexType = indexTypeToName(type);
		indexDeclr.fieldType = "composite";
		indexDeclr.opts = opts;
		Error err = reindexer->AddIndex(default_namespace, indexDeclr);
		EXPECT_TRUE(err.ok()) << err.what();
		err = reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	void dropIndex(const string& name) {
		Error err = reindexer->DropIndex(default_namespace, name);
		EXPECT_TRUE(err.ok()) << err.what();
		err = reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	string getCompositeIndexName(initializer_list<const char*> indexes) {
		size_t i = 0;
		string indexName;
		for (const char* subIdx : indexes) {
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
