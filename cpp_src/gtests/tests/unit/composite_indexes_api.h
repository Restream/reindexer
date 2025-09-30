#pragma once

#include "reindexer_api.h"

class [[nodiscard]] CompositeIndexesApi : public ReindexerApi {
public:
	enum CompositeIndexType { CompositeIndexHash, CompositeIndexBTree };

public:
	void SetUp() override {
		ReindexerApi::SetUp();
		rt.OpenNamespace(default_namespace);
		DefineNamespaceDataset(default_namespace, {IndexDeclaration{kFieldNameBookid, "hash", "int", IndexOpts(), 0},
												   IndexDeclaration{kFieldNameBookid2, "hash", "int", IndexOpts(), 0},
												   IndexDeclaration{kFieldNameTitle, "text", "string", IndexOpts(), 0},
												   IndexDeclaration{kFieldNamePages, "hash", "int", IndexOpts(), 0},
												   IndexDeclaration{kFieldNamePrice, "hash", "int", IndexOpts(), 0},
												   IndexDeclaration{kFieldNameName, "text", "string", IndexOpts(), 0}});
	}

	void fillNamespace(size_t since, size_t till) {
		for (size_t i = since; i < till; ++i) {
			int idValue(static_cast<int>(i));
			addOneRow(idValue, idValue + 77777, kFieldNameTitle + RandString(), rand() % 1000 + 10, rand() % 1000 + 100,
					  kFieldNameName + RandString());
		}
	}

	void addOneRow(int bookid, int bookid2, const std::string& title, int pages, int price, const std::string& name) {
		Item item = NewItem(default_namespace);
		item[this->kFieldNameBookid] = bookid;
		item[this->kFieldNameBookid2] = bookid2;
		item[this->kFieldNameTitle] = title;
		item[this->kFieldNamePages] = pages;
		item[this->kFieldNamePrice] = price;
		item[this->kFieldNameName] = name;
		Upsert(default_namespace, item);
	}

	Error tryAddCompositeIndex(std::initializer_list<std::string> indexes, CompositeIndexType type, const IndexOpts& opts) {
		reindexer::IndexDef indexDeclr{getCompositeIndexName(indexes), reindexer::JsonPaths(indexes), indexTypeToName(type), "composite",
									   opts};
		return rt.reindexer->AddIndex(default_namespace, indexDeclr);
	}

	void addCompositeIndex(std::initializer_list<std::string> indexes, CompositeIndexType type, const IndexOpts& opts) {
		Error err = tryAddCompositeIndex(std::move(indexes), type, opts);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	void dropIndex(const std::string& name) { rt.DropIndex(default_namespace, name); }

	static std::string getCompositeIndexName(std::initializer_list<std::string> indexes) {
		size_t i = 0;
		std::string indexName;
		for (const std::string& subIdx : indexes) {
			indexName += subIdx;
			if (i++ != indexes.size() - 1) {
				indexName += compositePlus;
			}
		}
		return indexName;
	}

	std::string indexTypeToName(CompositeIndexType indexType) const {
		switch (indexType) {
			case CompositeIndexHash:
				return "hash";
			case CompositeIndexBTree:
				return "tree";
			default:
				throw std::runtime_error("No such type of composite indexes!");
				break;
		}
	}

	QueryResults execAndCompareQuery(const Query& query) {
		auto qr = rt.Select(query);
		const auto sqlQuery = query.GetSQL();
		auto qrSql = rt.ExecSQL(sqlQuery);
		EXPECT_EQ(qr.Count(), qrSql.Count()) << "SQL: " << sqlQuery;
		for (auto it = qr.begin(), itSql = qrSql.begin(); it != qr.end() && itSql != qrSql.end(); ++it, ++itSql) {
			EXPECT_TRUE(it.Status().ok()) << it.Status().what();
			assert(it.Status().ok());
			reindexer::WrSerializer ser, serSql;
			auto err = it.GetCJSON(ser);
			EXPECT_TRUE(err.ok()) << err.what();
			assert(err.ok());
			err = itSql.GetCJSON(serSql);
			EXPECT_TRUE(err.ok()) << err.what();
			assert(err.ok());
			EXPECT_EQ(ser.Slice(), serSql.Slice()) << "SQL: " << sqlQuery;
		}
		return qr;
	}

	static constexpr char kFieldNameBookid[] = "bookid";
	static constexpr char kFieldNameBookid2[] = "bookid2";
	static constexpr char kFieldNameTitle[] = "title";
	static constexpr char kFieldNamePages[] = "pages";
	static constexpr char kFieldNamePrice[] = "price";
	static constexpr char kFieldNameName[] = "name";

	static constexpr char compositePlus = '+';
	static constexpr std::string_view kSubindexesNamespace = "subindexes_namespace";
};
