#include "equalpositionapi.h"

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
		default:
			throw std::runtime_error("Do not support this operation yet!");
	}
	return false;
}

void VerifyQueryResult(const QueryResults& qr, const std::vector<string>& fields, const std::vector<Variant>& keys,
					   const std::vector<CondType>& condTypes) {
	EXPECT_TRUE(fields.size() == keys.size());
	EXPECT_TRUE(keys.size() == condTypes.size());
	size_t totalFound = 0;
	for (size_t i = 0; i < qr.Count(); ++i) {
		size_t len = INT_MAX;
		Item it = qr[i].GetItem();

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
	Query q = std::move(Query(default_namespace).Debug(LogTrace).Where(kFieldA1, CondGt, key1).Where(kFieldA2, CondGt, key2));
	q.AddEqualPosition({kFieldA1, kFieldA2});
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {kFieldA1, kFieldA2}, {key1, key2}, {CondGt, CondGt});
}

TEST_F(EqualPositionApi, SelectGt2) {
	QueryResults qr;
	const Variant key1(static_cast<int>(1120));
	const Variant key2(static_cast<int>(2240));
	Query q = std::move(Query(default_namespace).Debug(LogTrace).Where(kFieldA1, CondGt, key1).Where(kFieldA2, CondGt, key2));
	q.AddEqualPosition({kFieldA1, kFieldA2});
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {kFieldA1, kFieldA2}, {key1, key2}, {CondGt, CondGt});
}

TEST_F(EqualPositionApi, SelectGe) {
	QueryResults qr;
	const Variant key1(static_cast<int>(1120));
	const Variant key2(static_cast<int>(2240));
	Query q = std::move(Query(default_namespace).Debug(LogTrace).Where(kFieldA1, CondGe, key1).Where(kFieldA2, CondGe, key2));
	q.AddEqualPosition({kFieldA1, kFieldA2});
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {kFieldA1, kFieldA2}, {key1, key2}, {CondGe, CondGe});
}

TEST_F(EqualPositionApi, SelectGe2) {
	QueryResults qr;
	const Variant key1(static_cast<int>(0));
	const Variant key2(static_cast<int>(0));
	Query q = std::move(Query(default_namespace).Debug(LogTrace).Where(kFieldA1, CondGe, key1).Where(kFieldA2, CondGe, key2));
	q.AddEqualPosition({kFieldA1, kFieldA2});
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {kFieldA1, kFieldA2}, {key1, key2}, {CondGe, CondGe});
}

TEST_F(EqualPositionApi, SelectLt) {
	QueryResults qr;
	const Variant key1(static_cast<int>(400));
	const Variant key2(static_cast<int>(800));
	Query q = std::move(Query(default_namespace).Debug(LogTrace).Where(kFieldA1, CondLt, key1).Where(kFieldA2, CondLt, key2));
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
	Query q = std::move(
		Query(default_namespace).Debug(LogTrace).Where(kFieldA1, CondEq, key1).Where(kFieldA2, CondEq, key2).Where(kFieldA3, CondEq, key3));
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
		string pk("pk" + std::to_string(i));
		sprintf(json, jsonPattern, pk.c_str(), rand() % 10, rand() % 10, rand() % 10, rand() % 10, rand() % 10, rand() % 10, rand() % 10,
				rand() % 10, rand() % 10);

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
	Query q = std::move(Query(ns).Debug(LogTrace).Where("nested.a2", CondGe, key1).Where("nested.a3", CondGe, key2));
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
		string pk("pk" + std::to_string(i));
		sprintf(json, jsonPattern, pk.c_str(), rand() % 10, rand() % 10, rand() % 10, rand() % 10, rand() % 10, rand() % 10, rand() % 10,
				rand() % 10, rand() % 10);

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
	Query q = std::move(Query(ns).Debug(LogTrace).Where("a1", CondGe, key1).Where("nested.a2", CondGe, key2));
	q.AddEqualPosition({"a1", "nested.a2"});
	err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	VerifyQueryResult(qr, {"a1", "nested.a2"}, {key1, key2}, {CondGe, CondGe});
}
