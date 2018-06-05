#pragma once

#include <gtest/gtest.h>
#include <map>
#include "gason/gason.h"
#include "reindexer_api.h"
#include "tools/serializer.h"

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

		DefineNamespaceDataset(genres_namespace, {IndexDeclaration{genreid, "hash", "int", IndexOpts().PK()},
												  IndexDeclaration{genrename, "text", "string", IndexOpts()}});

		DefineNamespaceDataset(authors_namespace,
							   {IndexDeclaration{authorid, "hash", "int", IndexOpts().PK()},
								IndexDeclaration{name, "text", "string", IndexOpts()}, IndexDeclaration{age, "hash", "int", IndexOpts()}});

		DefineNamespaceDataset(
			books_namespace,
			{IndexDeclaration{bookid, "hash", "int", IndexOpts().PK()}, IndexDeclaration{title, "text", "string", IndexOpts()},
			 IndexDeclaration{pages, "hash", "int", IndexOpts()}, IndexDeclaration{price, "hash", "int", IndexOpts()},
			 IndexDeclaration{genreId_fk, "hash", "int", IndexOpts()}, IndexDeclaration{authorid_fk, "hash", "int", IndexOpts()},
			 IndexDeclaration{string(pages + string("+") + bookid).c_str(), "hash", "composite", IndexOpts()}});

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
			Item item = NewItem(authors_namespace);
			item[authorid] = ++authorIdValue;
			item[name] = name + underscore + RandString();
			item[age] = rand() % 80 + 20;

			Upsert(authors_namespace, item);
			Commit(authors_namespace);

			authorsIds.push_back(authorIdValue);
		}
	}

	void FillBooksNamespace(int32_t count) {
		for (int32_t i = 0; i < count; ++i) {
			Item item = NewItem(books_namespace);
			item[bookid] = i;
			item[title] = title + underscore + RandString();
			item[pages] = rand() % 10000;
			item[price] = rand() % 1000;
			item[authorid_fk] = authorsIds[rand() % (authorsIds.size() - 1)];
			item[genreId_fk] = genresIds[rand() % (genresIds.size() - 1)];
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
		Item item = NewItem(genres_namespace);
		item[genreid] = id;
		item[genrename] = name;
		Upsert(genres_namespace, item);
		Commit(genres_namespace);
		genresIds.push_back(id);
	}

	void FillQueryResultFromItem(Item& item, QueryResultRow& resultRow) {
		for (int idx = 1; idx < item.NumFields(); idx++) {
			std::string fieldName = item[idx].Name();
			resultRow[fieldName] = item[idx];
		}
	}

	int ParseItemJsonWithJoins(const QueryResults& queryRes) {
		if (queryRes.empty()) return JSON_OK;
		reindexer::WrSerializer wrSer;
		queryRes.GetJSON(0, wrSer, false);
		string json = reindexer::string_view(reinterpret_cast<const char*>(wrSer.Buf()), wrSer.Len()).ToString();

		char* endptr = nullptr;
		JsonValue value;
		JsonAllocator jsonAllocator;
		return jsonParse(const_cast<char*>(json.c_str()), &endptr, &value, jsonAllocator);
	}

	void FillQueryResultRows(reindexer::QueryResults& reindexerRes, QueryResultRows& testRes) {
		for (size_t i = 0; i < reindexerRes.size(); ++i) {
			Item item(reindexerRes.GetItem(i));

			BookId bookId = item[bookid].Get<int>();
			QueryResultRow& resultRow = testRes[bookId];

			FillQueryResultFromItem(item, resultRow);

			const reindexer::ItemRef& rowid = reindexerRes[i];
			auto it = reindexerRes.joined_->find(rowid.id);
			if (it == reindexerRes.joined_->end()) {
				continue;
			}

			reindexer::QRVector& joinQueryRes(it->second);
			const QueryResults& joinResult(joinQueryRes[0]);
			for (size_t i = 0; i < joinResult.size(); ++i) {
				Item joinItem(joinResult.GetItem(i));
				FillQueryResultFromItem(joinItem, resultRow);
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
