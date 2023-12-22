#include "equalpositionapi.h"

using QueryResults = ReindexerApi::QueryResults;
using Item = ReindexerApi::Item;
using Reindexer = ReindexerApi::Reindexer;

bool Compare(const Variant& key1, const Variant& key2, CondType condType) {
	int res = key1.Compare(key2);
	switch (condType) {
		case CondEq:
			return res == 0;
		case CondGe:
			return res >= 0;
		case CondGt:
			return res > 0;
		case CondLe:
			return res <= 0;
		case CondLt:
			return res < 0;
		case CondAny:
		case CondRange:
		case CondSet:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
			throw std::runtime_error("Do not support this operation yet!");
	}
	return false;
}

void VerifyQueryResult(const QueryResults& qr, const std::vector<std::string>& fields, const std::vector<Variant>& keys,
					   const std::vector<CondType>& condTypes) {
	EXPECT_TRUE(fields.size() == keys.size());
	EXPECT_TRUE(keys.size() == condTypes.size());
	size_t totalFound = 0;
	for (auto& iter : qr) {
		size_t len = INT_MAX;
		Item it = iter.GetItem(false);

		std::vector<VariantArray> vals(keys.size());
		for (size_t j = 0; j < fields.size(); ++j) {
			VariantArray v = it[fields[j]];
			vals[j] = v;
			len = std::min(static_cast<size_t>(vals[j].size()), len);
		}
		size_t j = 0;
		auto eof = [&j, &len]() { return j >= len; };
		bool equal = true;
		for (;;) {
			size_t key = 0;
			while ((j < len) && !Compare(vals[key][j], keys[key], condTypes[key])) ++j;
			if (eof()) break;
			equal = true;
			while (++key < keys.size()) {
				equal &= Compare(vals[key][j], keys[key], condTypes[key]);
				if (!equal) {
					break;
				}
			}
			if (equal) {
				++totalFound;
				break;
			}
			++j;
		}
		if (!equal) TEST_COUT << it.GetJSON() << std::endl;
	}
	EXPECT_TRUE(totalFound == qr.Count()) << " totalFound=" << totalFound << ", qr.Count()=" << qr.Count();
}

TEST_F(EqualPositionApi, SelectGt) {
	QueryResults qr;
	const Variant key1(static_cast<int>(1050));
	const Variant key2(static_cast<int>(2100));
	Query q{Query(default_namespace).Debug(LogTrace).Where(kFieldA1, CondGt, key1).Where(kFieldA2, CondGt, key2)};
	q.AddEqualPosition({kFieldA1, kFieldA2});
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {kFieldA1, kFieldA2}, {key1, key2}, {CondGt, CondGt});
}

TEST_F(EqualPositionApi, SelectGt2) {
	QueryResults qr;
	const Variant key1(static_cast<int>(1120));
	const Variant key2(static_cast<int>(2240));
	Query q{Query(default_namespace).Debug(LogTrace).Where(kFieldA1, CondGt, key1).Where(kFieldA2, CondGt, key2)};
	q.AddEqualPosition({kFieldA1, kFieldA2});
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {kFieldA1, kFieldA2}, {key1, key2}, {CondGt, CondGt});
}

TEST_F(EqualPositionApi, SelectGe) {
	QueryResults qr;
	const Variant key1(static_cast<int>(1120));
	const Variant key2(static_cast<int>(2240));
	Query q{Query(default_namespace).Debug(LogTrace).Where(kFieldA1, CondGe, key1).Where(kFieldA2, CondGe, key2)};
	q.AddEqualPosition({kFieldA1, kFieldA2});
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {kFieldA1, kFieldA2}, {key1, key2}, {CondGe, CondGe});
}

TEST_F(EqualPositionApi, SelectGe2) {
	QueryResults qr;
	const Variant key1(static_cast<int>(0));
	const Variant key2(static_cast<int>(0));
	Query q{Query(default_namespace).Debug(LogTrace).Where(kFieldA1, CondGe, key1).Where(kFieldA2, CondGe, key2)};
	q.AddEqualPosition({kFieldA1, kFieldA2});
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {kFieldA1, kFieldA2}, {key1, key2}, {CondGe, CondGe});
}

TEST_F(EqualPositionApi, SelectLt) {
	QueryResults qr;
	const Variant key1(static_cast<int>(400));
	const Variant key2(static_cast<int>(800));
	Query q{Query(default_namespace).Debug(LogTrace).Where(kFieldA1, CondLt, key1).Where(kFieldA2, CondLt, key2)};
	q.AddEqualPosition({kFieldA1, kFieldA2});
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {kFieldA1, kFieldA2}, {key1, key2}, {CondLt, CondLt});
}

TEST_F(EqualPositionApi, SelectEq) {
	QueryResults qr;
	const Variant key1(static_cast<int>(900));
	const Variant key2(static_cast<int>(1800));
	const Variant key3(static_cast<int>(2700));
	Query q{
		Query(default_namespace).Debug(LogTrace).Where(kFieldA1, CondEq, key1).Where(kFieldA2, CondEq, key2).Where(kFieldA3, CondEq, key3)};
	q.AddEqualPosition({kFieldA1, kFieldA2, kFieldA3});
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {kFieldA1, kFieldA2, kFieldA3}, {key1, key2, key3}, {CondEq, CondEq, CondEq});
}

TEST_F(EqualPositionApi, SelectNonIndexedArrays) {
	const char* ns = "ns2";
	Error err = rt.reindexer->OpenNamespace(ns, StorageOpts().Enabled(false));
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(ns, {"id", "hash", "string", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Commit(ns);
	EXPECT_TRUE(err.ok()) << err.what();

	const char jsonPattern[] = R"xxx({"id": "%s", "nested": {"a1": [%d, %d, %d], "a2": [%d, %d, %d], "a3": [%d, %d, %d]}})xxx";

	for (int i = 0; i < 100; ++i) {
		Item item = rt.reindexer->NewItem(ns);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		char json[1024];
		std::string pk("pk" + std::to_string(i));
		snprintf(json, sizeof(json) - 1, jsonPattern, pk.c_str(), rand() % 10, rand() % 10, rand() % 10, rand() % 10, rand() % 10,
				 rand() % 10, rand() % 10, rand() % 10, rand() % 10);

		err = item.FromJSON(json);
		EXPECT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Upsert(ns, item);
		EXPECT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Commit(ns);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	QueryResults qr;
	const Variant key1(static_cast<int64_t>(3));
	const Variant key2(static_cast<int64_t>(4));
	Query q{Query(ns).Debug(LogTrace).Where("nested.a2", CondGe, key1).Where("nested.a3", CondGe, key2)};
	q.AddEqualPosition({"nested.a2", "nested.a3"});
	err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {"nested.a2", "nested.a3"}, {key1, key2}, {CondGe, CondGe});
}

TEST_F(EqualPositionApi, SelectMixedArrays) {
	const char* ns = "ns2";
	Error err = rt.reindexer->OpenNamespace(ns, StorageOpts().Enabled(false));
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(ns, {"id", "hash", "string", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(ns, {"a1", "hash", "int64", IndexOpts().Array()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Commit(ns);
	EXPECT_TRUE(err.ok()) << err.what();

	const char jsonPattern[] = R"xxx({"id": "%s", "a1": [%d, %d, %d], "nested": {"a2": [%d, %d, %d], "a3": [%d, %d, %d]}})xxx";

	for (int i = 0; i < 100; ++i) {
		Item item = rt.reindexer->NewItem(ns);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		char json[1024];
		std::string pk("pk" + std::to_string(i));
		snprintf(json, sizeof(json) - 1, jsonPattern, pk.c_str(), rand() % 10, rand() % 10, rand() % 10, rand() % 10, rand() % 10,
				 rand() % 10, rand() % 10, rand() % 10, rand() % 10);

		err = item.FromJSON(json);
		EXPECT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Upsert(ns, item);
		EXPECT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Commit(ns);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	QueryResults qr;
	const Variant key1(static_cast<int64_t>(4));
	const Variant key2(static_cast<int64_t>(5));
	Query q{Query(ns).Debug(LogTrace).Where("a1", CondGe, key1).Where("nested.a2", CondGe, key2)};
	q.AddEqualPosition({"a1", "nested.a2"});
	err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {"a1", "nested.a2"}, {key1, key2}, {CondGe, CondGe});
}

TEST_F(EqualPositionApi, EmptyCompOpErr) {
	const char* ns = "ns2";
	Error err = rt.reindexer->OpenNamespace(ns, StorageOpts().Enabled(false));
	EXPECT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(ns, {"id", "hash", "int", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();
	const char jsonPattern[] = R"xxx({"id": %d, "a1": [10, 20, 30], "a2": [20, 30, 40]}})xxx";
	for (int i = 0; i < 10; ++i) {
		Item item = rt.reindexer->NewItem(ns);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		char json[1024];
		std::string pk("pk" + std::to_string(i));

		snprintf(json, sizeof(json) - 1, jsonPattern, i);

		err = item.FromJSON(json);
		EXPECT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Upsert(ns, item);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		QueryResults qr;
		Query q = Query::FromSQL("SELECT * FROM ns2 WHERE a1=10 AND a2=20 equal_position(a1, a2)");
		err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		QueryResults qr;
		Query q = Query::FromSQL("SELECT * FROM ns2 WHERE a1 IS NULL AND a2=20 equal_position(a1, a2)");
		err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.what() == "Condition IN(with empty parameter list), IS NULL, IS EMPTY not allowed for equal position!")
			<< err.what();
		EXPECT_FALSE(err.ok());
	}
	{
		QueryResults qr;
		Query q = Query::FromSQL("SELECT * FROM ns2 WHERE a1 =10 AND a2 IS EMPTY equal_position(a1, a2)");
		err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.what() == "Condition IN(with empty parameter list), IS NULL, IS EMPTY not allowed for equal position!")
			<< err.what();
		EXPECT_FALSE(err.ok());
	}
	{
		QueryResults qr;
		Query q = Query::FromSQL("SELECT * FROM ns2 WHERE a1 IN () AND a2 IS EMPTY equal_position(a1, a2)");
		err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.what() == "Condition IN(with empty parameter list), IS NULL, IS EMPTY not allowed for equal position!")
			<< err.what();
		EXPECT_FALSE(err.ok());
	}
}

// Make sure equal_position() works only with unique fields
TEST_F(EqualPositionApi, SamePosition) {
	QueryResults qr;
	const Variant key(static_cast<int>(1050));
	// Build query that contains conditions for field 'a1'
	Query q{Query(default_namespace).Debug(LogTrace).Where(kFieldA1, CondGt, key).Where(kFieldA1, CondGt, key)};
	// query contains equal_position() for field 'a1' twice
	q.AddEqualPosition({kFieldA1, kFieldA1});
	// Make sure processing this query leads to error
	const Error err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "equal positions fields should be unique: [a1, a1]");
}

// Make sure equal_position() works only with unique fields
// when it set by SQL query
TEST_F(EqualPositionApi, SamePositionFromSql) {
	QueryResults qr;
	// SQL query contains equal_position() for field 'a1' twice
	const std::string_view sql = "select * from test_namespace where a1 > 0 and a1 < 10 equal_position(a1, a1)";
	Query q = Query::FromSQL(sql);
	// Make sure processing this query leads to error
	const Error err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "equal positions fields should be unique: [a1, a1]");
}

TEST_F(EqualPositionApi, SelectBrackets) {
	QueryResults qr;
	const Variant key1(static_cast<int>(900));
	const Variant key2(static_cast<int>(1800));
	const Variant key3(static_cast<int>(2700));
	Query q = Query(default_namespace)
				  .Debug(LogTrace)
				  .OpenBracket()
				  .Where(kFieldA1, CondEq, key1)
				  .Where(kFieldA2, CondEq, key2)
				  .Where(kFieldA3, CondEq, key3)
				  .AddEqualPosition({kFieldA1, kFieldA2, kFieldA3})
				  .CloseBracket();
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {kFieldA1, kFieldA2, kFieldA3}, {key1, key2, key3}, {CondEq, CondEq, CondEq});
}
