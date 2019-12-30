#pragma once

#include <gtest/gtest.h>
#include <map>
#include <sstream>
#include "core/cjson/jsonbuilder.h"
#include "core/dbconfig.h"
#include "core/queryresults/joinresults.h"
#include "gason/gason.h"
#include "reindexer_api.h"
#include "tools/fsops.h"
#include "tools/serializer.h"

class JoinSelectsApi : public ReindexerApi {
protected:
	using BookId = int;
	using FieldName = std::string;
	using QueryResultRow = std::map<FieldName, reindexer::VariantArray>;
	using QueryResultRows = std::map<BookId, QueryResultRow>;

	void SetUp() override {
		Error err;

		reindexer::fs::RmDirAll("/tmp/join_test/");
		err = rt.reindexer->Connect("builtin:///tmp/join_test/");
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->OpenNamespace(authors_namespace);
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->OpenNamespace(books_namespace);
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->OpenNamespace(genres_namespace);
		ASSERT_TRUE(err.ok()) << err.what();

		DefineNamespaceDataset(genres_namespace, {IndexDeclaration{genreid, "hash", "int", IndexOpts().PK(), 0},
												  IndexDeclaration{genrename, "hash", "string", IndexOpts(), 0}});

		DefineNamespaceDataset(authors_namespace, {IndexDeclaration{authorid, "hash", "int", IndexOpts().PK(), 0},
												   IndexDeclaration{name, "hash", "string", IndexOpts(), 0},
												   IndexDeclaration{age, "tree", "int", IndexOpts(), 0}});

		DefineNamespaceDataset(
			books_namespace,
			{IndexDeclaration{bookid, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{title, "hash", "string", IndexOpts(), 0},
			 IndexDeclaration{pages, "tree", "int", IndexOpts(), 0}, IndexDeclaration{price, "tree", "int", IndexOpts(), 0},
			 IndexDeclaration{genreId_fk, "hash", "int", IndexOpts(), 0}, IndexDeclaration{authorid_fk, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{string(pages + string("+") + bookid).c_str(), "hash", "composite", IndexOpts(), 0}});

		FillGenresNamespace();
		FillAuthorsNamespace(500);
		FillBooksNamespace(10000);
		FillAuthorsNamespace(10);
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

		Item bestItem = NewItem(authors_namespace);
		bestItem[authorid] = DostoevskyAuthorId;
		bestItem[name] = "Fedor Dostoevsky";
		bestItem[age] = 60;
		Upsert(authors_namespace, bestItem);
		Commit(authors_namespace);

		authorsIds.push_back(DostoevskyAuthorId);
	}

	void FillBooksNamespace(int32_t count) {
		int authorIdIdx = rand() % authorsIds.size();
		for (int32_t i = 0; i < count; ++i) {
			Item item = NewItem(books_namespace);
			item[bookid] = i;
			item[title] = title + underscore + RandString();
			item[pages] = rand() % 10000;
			item[price] = rand() % 10000;
			item[authorid_fk] = authorsIds[authorIdIdx];
			item[genreId_fk] = genresIds[rand() % genresIds.size()];
			Upsert(books_namespace, item);
			Commit(books_namespace);

			if (i % 4 == 0) authorIdIdx = rand() % authorsIds.size();
		}

		std::stringstream json;
		json << "{" << addQuotes(bookid) << ":" << ++count << "," << addQuotes(title) << ":" << addQuotes("Crime and Punishment") << ","
			 << addQuotes(pages) << ":" << 100500 << "," << addQuotes(price) << ":" << 5000 << "," << addQuotes(authorid_fk) << ":"
			 << DostoevskyAuthorId << "," << addQuotes(genreId_fk) << ":" << 4 << "," << addQuotes(rating) << ":" << 100 << "}";
		Item bestItem = NewItem(books_namespace);
		ASSERT_TRUE(bestItem.Status().ok()) << bestItem.Status().what();

		Error err = bestItem.FromJSON(json.str());
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Upsert(books_namespace, bestItem);
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Commit(books_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void FillGenresNamespace() {
		AddGenre(1, "science fiction");
		AddGenre(2, "poetry");
		AddGenre(3, "detective story");
		AddGenre(4, "documentary");
		AddGenre(5, "non fiction");
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
			std::string fieldName(item[idx].Name());
			resultRow[fieldName] = item[idx];
		}
	}

	Error VerifyResJSON(const QueryResults& queryRes) {
		Error err;
		try {
			reindexer::WrSerializer wrSer;
			for (auto& qr : queryRes) {
				wrSer.Reset();
				err = qr.GetJSON(wrSer, false);
				if (!err.ok()) break;
				gason::JsonParser().Parse(reindexer::giftStr(wrSer.Slice()));
			}
		} catch (const gason::Exception& ex) {
			return Error(errParseJson, "VerifyResJSON: %s", ex.what());
		}
		return err;
	}

	void PrintResultRows(QueryResults& qr) {
		for (auto rowIt : qr) {
			Item item(rowIt.GetItem());
			std::cout << "ROW: " << item.GetJSON() << std::endl;

			int idx = 1;
			auto itemIt = reindexer::joins::ItemIterator::FromQRIterator(rowIt);
			for (auto joinedFieldIt = itemIt.begin(); joinedFieldIt != itemIt.end(); ++joinedFieldIt) {
				std::cout << "JOINED: " << idx << std::endl;
				for (int i = 0; i < joinedFieldIt.ItemsCount(); ++i) {
					reindexer::ItemImpl joinItem(joinedFieldIt.GetItem(i, qr.getPayloadType(1), qr.getTagsMatcher(1)));
					std::cout << joinItem.GetJSON() << std::endl;
				}
				std::cout << std::endl;
				++idx;
				if (itemIt.getJoinedFieldsCount() > 1) std::cout << std::endl;
			}
		}
	}

	void FillQueryResultRows(reindexer::QueryResults& qr, QueryResultRows& testRes) {
		for (auto rowIt : qr) {
			Item item(rowIt.GetItem());

			BookId bookId = item[bookid].Get<int>();
			QueryResultRow& resultRow = testRes[bookId];

			FillQueryResultFromItem(item, resultRow);
			auto itemIt = reindexer::joins::ItemIterator::FromQRIterator(rowIt);
			auto joinedFieldIt = itemIt.begin();
			QueryResults jres = joinedFieldIt.ToQueryResults();
			jres.addNSContext(qr.getPayloadType(1), qr.getTagsMatcher(1), qr.getFieldsFilter(1));
			for (auto it : jres) {
				Item joinedItem = it.GetItem();
				FillQueryResultFromItem(joinedItem, resultRow);
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
						const reindexer::VariantArray& fieldVal1(it2->second);
						const reindexer::VariantArray& fieldVal2(itFieldValue->second);
						EXPECT_TRUE(fieldVal1 == fieldVal2) << "Fields " << fieldName << " have different values!";
					} else
						return false;
				}
			} else
				return false;
		}

		return true;
	}

	void ChangeNsOptimizationTimeout(const string& nsName, int optimizationTimeout) {
		reindexer::WrSerializer ser;
		reindexer::JsonBuilder jb(ser);

		jb.Put("type", "namespaces");
		auto nsArray = jb.Array("namespaces");
		auto ns = nsArray.Object();
		ns.Put("namespace", nsName.c_str());
		ns.Put("log_level", "none");
		ns.Put("lazyload", false);
		ns.Put("unload_idle_threshold", 0);
		ns.Put("join_cache_mode", "off");
		ns.Put("start_copy_politics_count", 10000);
		ns.Put("merge_limit_count", 20000);
		ns.Put("optimization_timeout_ms", optimizationTimeout);
		ns.End();
		nsArray.End();
		jb.End();

		auto item = rt.NewItem(config_namespace);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		auto err = item.FromJSON(ser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();

		rt.Upsert(config_namespace, item);
		err = rt.Commit(config_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void TurnOnJoinCache(const string& nsName) {
		reindexer::WrSerializer ser;
		reindexer::JsonBuilder jb(ser);

		jb.Put("type", "namespaces");
		auto nsArray = jb.Array("namespaces");
		auto ns = nsArray.Object();
		ns.Put("namespace", nsName.c_str());
		ns.Put("log_level", "none");
		ns.Put("lazyload", false);
		ns.Put("unload_idle_threshold", 0);
		ns.Put("join_cache_mode", "on");
		ns.Put("start_copy_politics_count", 10000);
		ns.Put("merge_limit_count", 20000);
		ns.End();
		nsArray.End();
		jb.End();

		auto item = rt.NewItem(config_namespace);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		auto err = item.FromJSON(ser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();

		rt.Upsert(config_namespace, item);
		err = rt.Commit(config_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void CheckJoinsInComplexWhereCondition(const QueryResults& qr) {
		for (auto it : qr) {
			Item item = it.GetItem();

			Variant priceFieldValue = item[price];
			const bool priceConditionResult = ((static_cast<int>(priceFieldValue) >= 9540) && (static_cast<int>(priceFieldValue) <= 9550));

			bool joinsBracketConditionsResult = false;
			if ((static_cast<int>(priceFieldValue) >= 1000) && (static_cast<int>(priceFieldValue) <= 2000)) {
				auto jitemIt = reindexer::joins::ItemIterator::FromQRIterator(it);
				auto authorNsFieldIt = jitemIt.at(0);
				auto genreNsFieldIt = jitemIt.at(1);
				if (authorNsFieldIt != jitemIt.end() && genreNsFieldIt != jitemIt.end() &&
					(authorNsFieldIt.ItemsCount() > 0 || genreNsFieldIt.ItemsCount() > 0)) {
					if (authorNsFieldIt.ItemsCount() > 0) {
						Variant authorIdFieldValue = item[authorid_fk];
						EXPECT_TRUE((static_cast<int>(authorIdFieldValue) >= 10) && (static_cast<int>(authorIdFieldValue) <= 25));
						for (int i = 0; i < authorNsFieldIt.ItemsCount(); ++i) {
							reindexer::ItemImpl itemimpl = authorNsFieldIt.GetItem(i, qr.getPayloadType(1), qr.getTagsMatcher(1));
							Variant authorIdFkFieldValue = itemimpl.GetField(qr.getPayloadType(1).FieldByName(authorid));
							EXPECT_TRUE(authorIdFieldValue == authorIdFkFieldValue);
						}
					}
					if (genreNsFieldIt.ItemsCount() > 0) {
						Variant genreIdFieldValue = item[genreId_fk];
						EXPECT_TRUE(static_cast<int>(genreIdFieldValue) != 1);
						for (int i = 0; i < genreNsFieldIt.ItemsCount(); ++i) {
							reindexer::ItemImpl itemimpl = genreNsFieldIt.GetItem(i, qr.getPayloadType(2), qr.getTagsMatcher(2));
							Variant genreIdFkFieldValue = itemimpl.GetField(qr.getPayloadType(2).FieldByName(genreid));
							EXPECT_TRUE(genreIdFieldValue == genreIdFkFieldValue);
						}
					}
					joinsBracketConditionsResult = true;
				}
			}

			Variant pagesFieldValue = item[pages];
			const bool pagesConditionResult = (static_cast<int>(pagesFieldValue) == 0);

			bool joinsNoBracketConditionsResult = false;
			auto jitemIt = reindexer::joins::ItemIterator::FromQRIterator(it);
			auto authorNsFieldIt = jitemIt.at(2);
			if ((authorNsFieldIt != jitemIt.end() ||
				 ((authorNsFieldIt = jitemIt.at(0)) != jitemIt.end() && jitemIt.at(1) == jitemIt.end())) &&
				authorNsFieldIt.ItemsCount() > 0) {
				Variant authorIdFieldValue = item[authorid_fk];
				EXPECT_TRUE((static_cast<int>(authorIdFieldValue) >= 300) && (static_cast<int>(authorIdFieldValue) <= 400));
				for (int i = 0; i < authorNsFieldIt.ItemsCount(); ++i) {
					reindexer::ItemImpl itemimpl = authorNsFieldIt.GetItem(i, qr.getPayloadType(3), qr.getTagsMatcher(3));
					Variant authorIdFkFieldValue = itemimpl.GetField(qr.getPayloadType(3).FieldByName(authorid));
					EXPECT_TRUE(authorIdFieldValue == authorIdFkFieldValue);
				}
				joinsNoBracketConditionsResult = true;
			}

			EXPECT_TRUE(pagesConditionResult || joinsBracketConditionsResult || priceConditionResult || joinsNoBracketConditionsResult);
		}
	}

	static string addQuotes(const string& str) {
		string output;
		output += "\"";
		output += str;
		output += "\"";
		return output;
	}

	const char* id = "id";
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
	const char* rating = "rating";

	const int DostoevskyAuthorId = 111777;

	const std::string underscore = "_";
	const std::string books_namespace = "books_namespace";
	const std::string authors_namespace = "authors_namespace";
	const std::string genres_namespace = "genres_namespace";
	const std::string config_namespace = "#config";

	std::vector<int> authorsIds;
	std::vector<int> genresIds;
};
