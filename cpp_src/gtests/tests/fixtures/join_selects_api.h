#pragma once

#include <gmock/gmock.h>
#include <map>
#include <sstream>
#include "core/cjson/jsonbuilder.h"
#include "core/dbconfig.h"
#include "core/nsselecter/joins/item_context.h"
#include "core/nsselecter/joins/iterators.h"
#include "core/system_ns_names.h"
#include "estl/gift_str.h"
#include "estl/lock.h"
#include "estl/shared_mutex.h"
#include "gason/gason.h"
#include "reindexer_api.h"
#include "tools/fsops.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer_tests {

class [[nodiscard]] JoinSelectsApi : public ReindexerApi {
protected:
	using BookId = int;
	using FieldName = std::string;
	using QueryResultRow = std::map<FieldName, reindexer::VariantArray>;
	using QueryResultRows = std::map<BookId, QueryResultRow>;

	void Init(const std::string& dbName = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "join_test")) RX_REQUIRES(!authorsMutex) {
		using reindexer::IndexOpts;

		std::ignore = reindexer::fs::RmDirAll(dbName);
		rt.Connect("builtin://" + dbName);

		rt.OpenNamespace(authors_namespace);
		rt.OpenNamespace(books_namespace);
		rt.OpenNamespace(genres_namespace);
		rt.OpenNamespace(location_namespace);
		rt.OpenNamespace(countries_namespace);

		DefineNamespaceDataset(countries_namespace, {IndexDeclaration{countryid, "hash", "int", IndexOpts().PK(), 0},
													 IndexDeclaration{countryName, "hash", "string", IndexOpts(), 0},
													 IndexDeclaration{countryCode, "hash", "int", IndexOpts(), 0}});

		DefineNamespaceDataset(location_namespace, {IndexDeclaration{locationid, "hash", "int", IndexOpts().PK(), 0},
													IndexDeclaration{countryid_fk, "hash", "int", IndexOpts(), 0},
													IndexDeclaration{code, "hash", "int", IndexOpts(), 0},
													IndexDeclaration{city, "hash", "string", IndexOpts(), 0}});

		DefineNamespaceDataset(genres_namespace, {IndexDeclaration{genreid, "hash", "int", IndexOpts().PK(), 0},
												  IndexDeclaration{genrename, "hash", "string", IndexOpts(), 0}});

		DefineNamespaceDataset(
			authors_namespace,
			{IndexDeclaration{authorid, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{name, "hash", "string", IndexOpts(), 0},
			 IndexDeclaration{age, "tree", "int", IndexOpts(), 0}, IndexDeclaration{locationid_fk, "tree", "int", IndexOpts(), 0}});

		DefineNamespaceDataset(
			books_namespace,
			{IndexDeclaration{bookid, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{title, "hash", "string", IndexOpts(), 0},
			 IndexDeclaration{pages, "tree", "int", IndexOpts(), 0}, IndexDeclaration{price, "tree", "int", IndexOpts(), 0},
			 IndexDeclaration{genreId_fk, "hash", "int", IndexOpts(), 0}, IndexDeclaration{authorid_fk, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{(pages + std::string("+") + bookid).c_str(), "hash", "composite", IndexOpts(), 0}});

		FillCountriesNamespace();
		FillLocationsNamespace();
		FillGenresNamespace();
		FillAuthorsNamespace(500);
		FillBooksNamespace(0, 10000);
		FillAuthorsNamespace(10);
	}

	void SetUp() override RX_REQUIRES(!authorsMutex) {
		ReindexerApi::SetUp();
		Init();
	}

	void FillLocationsNamespace() {
		for (size_t i = 0; i < locations.size(); ++i) {
			Item item = NewItem(location_namespace);
			item[locationid] = int(i);
			item[code] = rand() % 65536;
			item[city] = locations[i];
			item[countryid_fk] = int(rand() % countries.size());
			Upsert(location_namespace, item);
		}
	}

	void FillCountriesNamespace() {
		for (size_t i = 0; i < countries.size(); ++i) {
			Item item = NewItem(countries_namespace);
			item[countryid] = int(i);
			item[countryCode] = rand() % 65536;
			item[countryName] = countries[i];
			Upsert(countries_namespace, item);
		}
	}

	void FillAuthorsNamespace(int32_t count) RX_REQUIRES(!authorsMutex) {
		int authorIdValue = 0;
		{
			reindexer::shared_lock<reindexer::shared_timed_mutex> lck(authorsMutex);
			auto itMaxIt = std::max_element(authorsIds.begin(), authorsIds.end());
			if (itMaxIt != authorsIds.end()) {
				authorIdValue = *itMaxIt;
			}
		}
		for (int32_t i = 0; i < count; ++i) {
			Item item = NewItem(authors_namespace);
			item[authorid] = ++authorIdValue;
			item[name] = name + underscore + RandString();
			item[age] = rand() % 80 + 20;
			item[locationid_fk] = int(rand() % locations.size());

			Upsert(authors_namespace, item);

			{
				reindexer::unique_lock lck(authorsMutex);
				authorsIds.push_back(authorIdValue);
			}
		}

		Item bestItem = NewItem(authors_namespace);
		bestItem[authorid] = DostoevskyAuthorId;
		bestItem[name] = "Fedor Dostoevsky";
		bestItem[age] = 60;
		Upsert(authors_namespace, bestItem);

		{
			reindexer::unique_lock lck(authorsMutex);
			if (std::find_if(authorsIds.begin(), authorsIds.end(), [this](int id) { return DostoevskyAuthorId == id; }) ==
				authorsIds.end()) {
				authorsIds.push_back(DostoevskyAuthorId);
			}
		}
	}

	void RemoveLastAuthors(int32_t count) {
		VariantArray idsToRemove;
		idsToRemove.reserve(std::min(size_t(count), authorsIds.size()));
		auto rend = authorsIds.rbegin() + std::min(size_t(count), authorsIds.size());
		for (auto ait = authorsIds.rbegin(); ait != rend; ++ait) {
			idsToRemove.emplace_back(*ait);
		}
		const auto removed = Delete(Query(authors_namespace).Where(authorid, CondSet, idsToRemove));
		ASSERT_EQ(removed, count);
	}

	void FillBooksNamespace(int32_t since, int32_t count) RX_REQUIRES(!authorsMutex) {
		int authorIdIdx = 0;
		{
			reindexer::shared_lock<reindexer::shared_timed_mutex> lck(authorsMutex);
			authorIdIdx = rand() % authorsIds.size();
		}

		for (int32_t i = since; i < count; ++i) {
			Item item = NewItem(books_namespace);
			item[bookid] = i;
			item[title] = title + underscore + RandString();
			item[pages] = rand() % 10000;
			item[price] = rand() % 10000;

			{
				reindexer::shared_lock<reindexer::shared_timed_mutex> lck(authorsMutex);
				item[authorid_fk] = authorsIds[authorIdIdx];
			}

			item[genreId_fk] = genres[rand() % genres.size()].id;
			Upsert(books_namespace, item);

			{
				reindexer::shared_lock<reindexer::shared_timed_mutex> lck(authorsMutex);
				if (i % 4 == 0) {
					authorIdIdx = rand() % authorsIds.size();
				}
			}
		}

		std::stringstream json;
		json << "{" << addQuotes(bookid) << ":" << ++count << "," << addQuotes(title) << ":" << addQuotes("Crime and Punishment") << ","
			 << addQuotes(pages) << ":" << 100500 << "," << addQuotes(price) << ":" << 5000 << "," << addQuotes(authorid_fk) << ":"
			 << DostoevskyAuthorId << "," << addQuotes(genreId_fk) << ":" << 4 << "," << addQuotes(rating) << ":" << 100 << "}";
		rt.UpsertJSON(books_namespace, json.str());
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
		auto found = std::find_if(genres.begin(), genres.end(), [id](const Genre& g) { return g.id == id; });
		ASSERT_EQ(found, genres.end());
		genres.push_back(Genre{id, name});
	}
	void RemoveGenre(int id) {
		Item item = NewItem(genres_namespace);
		item[genreid] = id;
		Delete(genres_namespace, item);
		genres.erase(std::remove_if(genres.begin(), genres.end(), [id](const Genre& g) { return g.id == id; }), genres.end());
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
				if (!err.ok()) {
					break;
				}
				gason::JsonParser parser;
				parser.Parse(reindexer::giftStr(wrSer.Slice()));
			}
		} catch (const gason::Exception& ex) {
			return Error(errParseJson, "VerifyResJSON: {}", ex.what());
		}
		return err;
	}

	void PrintResultRows(QueryResults& qr) {
		for (auto rowIt : qr.ToLocalQr()) {
			Item item(rowIt.GetItem(false));
			std::cout << "ROW: " << item.GetJSON() << std::endl;

			int idx = 1;
			auto itemItCtx = rowIt.GetJoinedContext();
			auto& itemIt = itemItCtx.iterator;
			for (auto joinedFieldIt = itemIt.Begin(); joinedFieldIt != itemIt.End(); ++joinedFieldIt) {
				std::cout << "JOINED: " << idx << std::endl;
				for (auto it : joinedFieldIt.ToQueryResults(itemItCtx)) {
					auto joinItem(it.GetItem());
					std::cout << joinItem.GetJSON() << std::endl;
				}
				std::cout << std::endl;
				++idx;
				if (itemIt.GetFieldsCount() > 1) {
					std::cout << std::endl;
				}
			}
		}
	}

	void FillQueryResultRows(reindexer::QueryResults& qr, QueryResultRows& testRes) {
		for (auto rowIt : qr) {
			Item item(rowIt.GetItem(false));

			BookId bookId = item[bookid].Get<int>();
			QueryResultRow& resultRow = testRes[bookId];

			FillQueryResultFromItem(item, resultRow);
			auto joinedItemCtx{rowIt.GetJoinedContext()};
			auto joinedFieldIt{joinedItemCtx.iterator.Begin()};
			for (auto it : joinedFieldIt.ToQueryResults(joinedItemCtx)) {
				Item joinedItem{it.GetItem(false)};
				FillQueryResultFromItem(joinedItem, resultRow);
			}
		}
	}

	bool CompareQueriesResults(QueryResultRows& lhs, QueryResultRows& rhs) {
		EXPECT_EQ(lhs.size(), rhs.size()) << "Two queries results have different size!";
		if (lhs.size() != rhs.size()) {
			return false;
		}

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
					} else {
						return false;
					}
				}
			} else {
				return false;
			}
		}

		return true;
	}

	void ChangeNsOptimizationTimeout(const std::string& nsName, int optimizationTimeout) {
		reindexer::WrSerializer ser;
		reindexer::JsonBuilder jb(ser);

		jb.Put("type", "namespaces");
		auto nsArray = jb.Array("namespaces");
		auto ns = nsArray.Object();
		ns.Put("namespace", nsName.c_str());
		ns.Put("log_level", "none");
		ns.Put("join_cache_mode", "off");
		ns.Put("start_copy_policy_tx_size", 10000);
		ns.Put("optimization_timeout_ms", optimizationTimeout);
		ns.End();
		nsArray.End();
		jb.End();

		rt.UpsertJSON(reindexer::kConfigNamespace, ser.Slice());
	}

	void TurnOnJoinCache(const std::string& nsName) {
		reindexer::WrSerializer ser;
		reindexer::JsonBuilder jb(ser);

		jb.Put("type", "namespaces");
		auto nsArray = jb.Array("namespaces");
		auto ns = nsArray.Object();
		ns.Put("namespace", nsName.c_str());
		ns.Put("log_level", "none");
		ns.Put("join_cache_mode", "on");
		ns.Put("start_copy_policy_tx_size", 10000);
		ns.End();
		nsArray.End();
		jb.End();

		rt.UpsertJSON(reindexer::kConfigNamespace, ser.Slice());
	}

	void CheckJoinsInComplexWhereCondition(const QueryResults& qr) {
		for (auto it : qr) {
			Item item = it.GetItem(false);

			Variant priceFieldValue = item[price];
			const bool priceConditionResult = ((static_cast<int>(priceFieldValue) >= 9540) && (static_cast<int>(priceFieldValue) <= 9550));

			bool joinsBracketConditionsResult = false;
			if ((static_cast<int>(priceFieldValue) >= 1000) && (static_cast<int>(priceFieldValue) <= 2000)) {
				auto jitemItCtx = it.GetJoinedContext();
				auto& jitemIt = jitemItCtx.iterator;
				auto authorNsFieldIt = jitemIt.At(0);
				auto genreNsFieldIt = jitemIt.At(1);
				if (authorNsFieldIt != jitemIt.End() && genreNsFieldIt != jitemIt.End() &&
					(authorNsFieldIt.ItemsCount() > 0 || genreNsFieldIt.ItemsCount() > 0)) {
					if (authorNsFieldIt.ItemsCount() > 0) {
						Variant authorIdFieldValue = item[authorid_fk];
						EXPECT_TRUE((static_cast<int>(authorIdFieldValue) >= 10) && (static_cast<int>(authorIdFieldValue) <= 25));
						for (auto it : authorNsFieldIt.ToQueryResults(jitemItCtx)) {
							auto item{it.GetItem()};
							EXPECT_TRUE(authorIdFieldValue == Variant{item[authorid]});
						}
					}
					if (genreNsFieldIt.ItemsCount() > 0) {
						Variant genreIdFieldValue = item[genreId_fk];
						EXPECT_TRUE(static_cast<int>(genreIdFieldValue) != 1);
						for (int i = 0; i < genreNsFieldIt.ItemsCount(); ++i) {
							reindexer::ItemImpl itemimpl = genreNsFieldIt.GetItem(i, qr.GetPayloadType(2), qr.GetTagsMatcher(2));
							Variant genreIdFkFieldValue = itemimpl.GetField(qr.GetPayloadType(2).FieldByName(genreid));
							EXPECT_TRUE(genreIdFieldValue == genreIdFkFieldValue);
						}
					}
					joinsBracketConditionsResult = true;
				}
			}

			Variant pagesFieldValue = item[pages];
			const bool pagesConditionResult = (static_cast<int>(pagesFieldValue) == 0);

			bool joinsNoBracketConditionsResult = false;
			auto jitemItCtx = it.GetJoinedContext();
			auto& jitemIt = jitemItCtx.iterator;
			auto authorNsFieldIt = jitemIt.At(2);
			if ((authorNsFieldIt != jitemIt.End() ||
				 ((authorNsFieldIt = jitemIt.At(0)) != jitemIt.End() && jitemIt.At(1) == jitemIt.End())) &&
				authorNsFieldIt.ItemsCount() > 0) {
				Variant authorIdFieldValue = item[authorid_fk];
				EXPECT_TRUE((static_cast<int>(authorIdFieldValue) >= 300) && (static_cast<int>(authorIdFieldValue) <= 400));
				for (auto it : authorNsFieldIt.ToQueryResults(jitemItCtx)) {
					auto item{it.GetItem()};
					EXPECT_TRUE(authorIdFieldValue == Variant{item[authorid]});
				}
				joinsNoBracketConditionsResult = true;
			}

			EXPECT_TRUE(pagesConditionResult || joinsBracketConditionsResult || priceConditionResult || joinsNoBracketConditionsResult);
		}
	}
	void ValidateQueryError(std::string_view sql, ErrorCode expectedCode, std::string_view expectedText) {
		{
			QueryResults qr;
			auto err = rt.reindexer->ExecSQL(sql, qr);
			EXPECT_EQ(err.code(), expectedCode) << sql;
			EXPECT_EQ(err.what(), expectedText) << sql;
		}
		{
			QueryResults qr;
			const Query q = Query::FromSQL(sql);
			auto err = rt.reindexer->Select(q, qr);
			EXPECT_EQ(err.code(), expectedCode) << sql;
			EXPECT_EQ(err.what(), expectedText) << sql;
		}
	}
	void ValidateQueryThrow(std::string_view sql, ErrorCode expectedCode, std::string_view expectedRegex) {
		QueryResults qr;
		{
			auto err = rt.reindexer->ExecSQL(sql, qr);
			EXPECT_EQ(err.code(), expectedCode) << sql;
			EXPECT_THAT(err.what(), testing::ContainsRegex(expectedRegex)) << sql;
		}
		EXPECT_THROW(const Query q = Query::FromSQL(sql), Error) << sql;
	}

	static std::string addQuotes(const std::string& str) {
		std::string output;
		output += "\"";
		output += str;
		output += "\"";
		return output;
	}

	void SetQueriesCacheHitsCount(unsigned hitsCount) {
		auto q = reindexer::Query(reindexer::kConfigNamespace)
					 .Set("namespaces.cache.query_count_hit_to_cache", int64_t(hitsCount))
					 .Where("type", CondEq, "namespaces");
		auto updated = Update(q);
		ASSERT_EQ(updated, 1);
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
	const char* city = "city";
	const char* code = "code";
	const char* locationid = "locationid";
	const char* locationid_fk = "locationid_fk";
	const char* countryid_fk = "countryid_fk";
	const char* countryid = "countryid";
	const char* countryName = "country_name";
	const char* countryCode = "country_code";

	const int DostoevskyAuthorId = 111777;

	const std::string underscore = "_";
	const std::string books_namespace = "books_namespace";
	const std::string authors_namespace = "authors_namespace";
	const std::string genres_namespace = "genres_namespace";
	const std::string location_namespace = "location_namespace";
	const std::string countries_namespace = "countries_namespace";

	struct [[nodiscard]] Genre {
		int id;
		std::string name;
	};

	std::vector<int> authorsIds;
	std::vector<Genre> genres;

	// clang-format off
	const std::vector<std::string_view> locations = {
		"Москва",
		"Тамбов",
		"Казань",
		"Ульяновск",
		"Краснодар",
		"Урюпинск",
		"Минск",
		"Киев",
		"Бердянск",
		"Новочеркасск",
		"Грозный",
		"Amsterdam",
		"Paris",
		"Berlin",
		"New York",
		"Rotterdam",
		"Калининград",
		"Томск",
		"Новосибирск",
		"Londonderry",
		"Belfast",
		"Dublin",
	};

	const std::vector<std::string_view> countries = {
		"Страна Северная",
		"Страна Южная",
		"Страна Западная",
		"Страна Восточная",
		"Страна Жаркая",
		"Страна Холодная",
		"Страна Развивающаяся",
		"Страна Процветающая",
	};
	// clang-format on

	reindexer::shared_timed_mutex authorsMutex;
};

}  // namespace reindexer_tests
