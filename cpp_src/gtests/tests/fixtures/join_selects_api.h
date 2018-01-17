#pragma once

#include <gtest/gtest.h>
#include "core/payload/payloadtype.h"
#include "reindexer_api.h"

class JoinSelectsApi : public ReindexerApi {
protected:
	using BookId = int;
	using FieldName = std::string;
	using QueryResultRow = std::map<FieldName, reindexer::KeyRefs>;
	using QueryResultRows = std::map<BookId, QueryResultRow>;

	void SetUp() override {
		CreateNamespace(authors_namespace);
		CreateNamespace(books_namespace);
		CreateNamespace(genres_namespace);

		IndexOpts opts(false, true, false);

		DefineNamespaceDataset(genres_namespace,
							   {tuple<const char*, const char*, const char*, IndexOpts>{genreid, "hash", "int", opts},
								tuple<const char*, const char*, const char*, IndexOpts>{genrename, "text", "string", IndexOpts()}});

		DefineNamespaceDataset(authors_namespace,
							   {tuple<const char*, const char*, const char*, IndexOpts>{authorid, "hash", "int", opts},
								tuple<const char*, const char*, const char*, IndexOpts>{name, "text", "string", IndexOpts()},
								tuple<const char*, const char*, const char*, IndexOpts>{age, "hash", "int", IndexOpts()}});

		DefineNamespaceDataset(books_namespace,
							   {tuple<const char*, const char*, const char*, IndexOpts>{bookid, "hash", "int", opts},
								tuple<const char*, const char*, const char*, IndexOpts>{title, "text", "string", IndexOpts()},
								tuple<const char*, const char*, const char*, IndexOpts>{pages, "hash", "int", IndexOpts()},
								tuple<const char*, const char*, const char*, IndexOpts>{price, "hash", "int", IndexOpts()},
								tuple<const char*, const char*, const char*, IndexOpts>{genreId_fk, "hash", "int", IndexOpts()},
								tuple<const char*, const char*, const char*, IndexOpts>{authorid_fk, "hash", "int", IndexOpts()}});

		FillGenresNamespace();
		FillAuthorsNamespace(500);
		FillBooksNamespace(10000);
		FillAuthorsNamespace(100);
	}

	void FillAuthorsNamespace(int32_t count) {
		int authorIdValue = 0;
		auto itMaxIt = std::max_element(authorsIds.begin(), authorsIds.end());
		if (itMaxIt != authorsIds.end()) {
			authorIdValue = *itMaxIt;
		}
		for (int32_t i = 0; i < count; ++i) {
			auto item = AddData(authors_namespace, authorid, ++authorIdValue);
			AddData(authors_namespace, name, name + underscore + RandString(), item);
			AddData(authors_namespace, age, rand() % 80 + 20, item);

			Upsert(authors_namespace, item);
			Commit(authors_namespace);

			authorsIds.push_back(i);
		}
	}

	void FillBooksNamespace(int32_t count) {
		for (int32_t i = 0; i < count; ++i) {
			auto item = AddData(books_namespace, bookid, i);
			AddData(books_namespace, title, title + underscore + RandString(), item);
			AddData(books_namespace, pages, rand() % 10000, item);
			AddData(books_namespace, price, rand() % 1000, item);
			AddData(books_namespace, authorid_fk, authorsIds[rand() % (authorsIds.size() - 1)], item);
			AddData(books_namespace, genreId_fk, genresIds[rand() % (genresIds.size() - 1)], item);

			Upsert(books_namespace, item);
			Commit(books_namespace);
		}
	}

	void FillGenresNamespace() {
		AddGenre(1, "science fiction");
		AddGenre(2, "poetry");
		AddGenre(3, "detective story");
		AddGenre(4, "documentary");
	}

	void AddGenre(int id, const std::string& name) {
		auto item = AddData(genres_namespace, genreid, id);
		AddData(genres_namespace, genrename, name, item);
		Upsert(genres_namespace, item);
		Commit(genres_namespace);
		genresIds.push_back(id);
	}

	void FillQueryResultFromItem(reindexer::Item* item, QueryResultRow& resultRow) {
		auto* ritem = reinterpret_cast<reindexer::ItemImpl*>(item);
		for (int idx = 1; idx < ritem->NumFields(); idx++) {
			std::string fieldName = ritem->Type().Field(idx).Name();
			reindexer::KeyRefs& fieldValue = resultRow[fieldName];
			ritem->Get(idx, fieldValue);
		}
	}

	int ParseItemJsonWithJoins(const reindexer::QueryResults& queryRes) {
		reindexer::WrSerializer wrSer;
		queryRes.GetJSON(0, wrSer, false);
		string json = reindexer::Slice(reinterpret_cast<const char*>(wrSer.Buf()), wrSer.Len()).ToString();
		//		puts(json.c_str());
		//		fflush(stdout);

		char* endptr = nullptr;
		JsonValue value;
		JsonAllocator jsonAllocator;
		return jsonParse(const_cast<char*>(json.c_str()), &endptr, &value, jsonAllocator);
	}

	void FillQueryResultRows(reindexer::QueryResults& reindexerRes, QueryResultRows& testRes) {
		for (size_t i = 0; i < reindexerRes.size(); ++i) {
			unique_ptr<reindexer::Item> item(reindexerRes.GetItem(i));
			auto* ritem = reinterpret_cast<reindexer::ItemImpl*>(item.get());

			KeyRef bookIdKeyRef = ritem->GetField(bookid);
			BookId bookId = static_cast<int>(bookIdKeyRef);
			QueryResultRow& resultRow = testRes[bookId];

			FillQueryResultFromItem(item.get(), resultRow);

			const reindexer::ItemRef& rowid = reindexerRes[i];
			vector<QueryResults>& joinQueryRes(reindexerRes.joined_[rowid.id]);
			const QueryResults& joinResult(joinQueryRes[0]);
			for (size_t i = 0; i < joinResult.size(); ++i) {
				unique_ptr<reindexer::Item> joinItem(joinResult.GetItem(i));
				FillQueryResultFromItem(joinItem.get(), resultRow);
			}
		}
	}

	bool CompareQueriesResults(QueryResultRows& lhs, QueryResultRows& rhs) {
		EXPECT_EQ(lhs.size(), rhs.size()) << "Two queries results have different size!";
		if (lhs.size() != rhs.size()) return false;

		for (auto it = lhs.begin(); it != lhs.end(); ++it) {
			const BookId& bookId(it->first);
			const QueryResultRow& queryResultRow1(it->second);

			auto itBookId(rhs.find(bookId));
			EXPECT_TRUE(itBookId != rhs.end()) << "Two queries results contain different keys!";
			if (itBookId != rhs.end()) {
				const QueryResultRow& queryResultRow2(itBookId->second);
				for (auto it2 = queryResultRow1.begin(); it2 != queryResultRow1.end(); ++it2) {
					const FieldName& fieldName(it2->first);
					auto itFieldValue(queryResultRow2.find(fieldName));
					EXPECT_TRUE(itFieldValue != queryResultRow2.end()) << "No such field!";
					if (itFieldValue != queryResultRow2.end()) {
						const reindexer::KeyRefs& fieldVal1(it2->second);
						const reindexer::KeyRefs& fieldVal2(itFieldValue->second);
						EXPECT_TRUE(fieldVal1 == fieldVal2) << "Fields " << fieldName << " have different values!";
					} else
						return false;
				}
			} else
				return false;
		}

		return true;
	}

	const char* authorid = "authorid";
	const char* authorid_fk = "authorid_fk";
	const char* bookid = "bookid";
	const char* title = "title";
	const char* pages = "pages";
	const char* price = "price";
	const char* name = "name";
	const char* age = "age";
	const char* genreid = "genreid";
	const char* genreId_fk = "genreid_fk";
	const char* genrename = "genre_name";

	const std::string underscore = "_";
	const std::string books_namespace = "books_namespace";
	const std::string authors_namespace = "authors_namespace";
	const std::string genres_namespace = "genres_namespace";

	std::vector<int> authorsIds;
	std::vector<int> genresIds;
};
