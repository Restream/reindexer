#pragma once

#include "gtests/tools.h"
#include "reindexer_api.h"
#include "tools/random.h"

class RuntimeIndexesApi : public ReindexerApi {
public:
	void SetUp() override {
		using namespace std::string_literals;
		Error err = rt.reindexer->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();

		DefineNamespaceDataset(
			default_namespace,
			{IndexDeclaration{bookid, "hash", "int", IndexOpts(), 0}, IndexDeclaration{bookid2, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{title, "text", "string", IndexOpts(), 0}, IndexDeclaration{pages, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{price, "hash", "int", IndexOpts(), 0}, IndexDeclaration{name, "text", "string", IndexOpts(), 0},
			 IndexDeclaration{uuid, "hash", "uuid", IndexOpts(), 0},
			 IndexDeclaration{(title + "+"s + price).c_str(), "hash", "composite", IndexOpts(), 0},
			 IndexDeclaration{"bookid+bookid2", "hash", "composite", IndexOpts().PK(), 0}});

		err = rt.reindexer->OpenNamespace(geom_namespace);
		ASSERT_TRUE(err.ok()) << err.what();

		DefineNamespaceDataset(geom_namespace, {IndexDeclaration{id, "hash", "int", IndexOpts().PK(), 0},
												IndexDeclaration{qpoints, "rtree", "point", IndexOpts().RTreeType(IndexOpts::Quadratic), 0},
												IndexDeclaration{lpoints, "rtree", "point", IndexOpts().RTreeType(IndexOpts::Linear), 0},
												IndexDeclaration{gpoints, "rtree", "point", IndexOpts().RTreeType(IndexOpts::Greene), 0},
												IndexDeclaration{spoints, "rtree", "point", IndexOpts().RTreeType(IndexOpts::RStar), 0}});
	}

protected:
	void FillNamespaces(size_t since, size_t till) {
		for (size_t i = since; i < till; ++i) {
			int id = static_cast<int>(i);

			Item defItem = NewItem(default_namespace);
			defItem[bookid] = id + 500;
			defItem[title] = title + RandString();
			defItem[pages] = rand() % 10000;
			defItem[price] = rand() % 1000;
			defItem[name] = name + RandString();
			if (rand() % 2) {
				defItem[uuid] = randStrUuid();
			} else {
				defItem[uuid] = randUuid();
			}
			Upsert(default_namespace, defItem);
			auto err = Commit(default_namespace);
			ASSERT_TRUE(err.ok()) << err.what();

			Item geoItem = NewItem(geom_namespace);
			geoItem[this->id] = id + 500;
			geoItem[qpoints] = randPoint(10);
			geoItem[lpoints] = randPoint(10);
			geoItem[gpoints] = randPoint(10);
			geoItem[spoints] = randPoint(10);
			Upsert(geom_namespace, geoItem);
			err = Commit(geom_namespace);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	void AddRuntimeIntArrayIndex(int indexNumber) {
		std::string indexName = getRuntimeIntIndexName(indexNumber);
		Error err = rt.reindexer->AddIndex(default_namespace, {indexName, "hash", "int", IndexOpts().Array()});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void DropRuntimeIntArrayIndex(int indexNumber) {
		reindexer::IndexDef idef(getRuntimeIntIndexName(indexNumber));
		Error err = rt.reindexer->DropIndex(default_namespace, idef);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddDataForRuntimeIntIndex(int indexNumber) {
		std::string indexName = getRuntimeIntIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(default_namespace);
			item[indexName] = rand() % 100;
			Upsert(default_namespace, item);
		}
		auto err = Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddRuntimeUuidIndex(int indexNumber) {
		std::string indexName = getRuntimeUuidIndexName(indexNumber);
		Error err = rt.reindexer->AddIndex(default_namespace, {indexName, "hash", "uuid", IndexOpts()});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddRuntimeUuidArrayIndex(int indexNumber) {
		std::string indexName = getRuntimeUuidArrayIndexName(indexNumber);
		Error err = rt.reindexer->AddIndex(default_namespace, {indexName, "hash", "uuid", IndexOpts().Array()});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddDataForRuntimeUuidIndex(int indexNumber) {
		std::string indexName = getRuntimeUuidIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(default_namespace);
			if (rand() % 2 == 0) {
				item[indexName] = randStrUuid();
			} else {
				item[indexName] = randUuid();
			}
			Upsert(default_namespace, item);
		}
		auto err = Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddDataForRuntimeUuidArrayIndex(int indexNumber) {
		std::string indexName = getRuntimeUuidArrayIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(default_namespace);
			if (rand() % 2 == 0) {
				std::vector<std::string> uuids;
				const size_t s = rand() % 20;
				uuids.reserve(s);
				for (size_t i = 0; i < s; ++i) {
					uuids.emplace_back(randStrUuid());
				}
				item[indexName] = std::move(uuids);
			} else {
				std::vector<reindexer::Uuid> uuids;
				const size_t s = rand() % 20;
				uuids.reserve(s);
				for (size_t i = 0; i < s; ++i) {
					uuids.emplace_back(randUuid());
				}
				item[indexName] = std::move(uuids);
			}
			Upsert(default_namespace, item);
		}
		auto err = Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void DropRuntimeUuidIndex(int indexNumber) {
		reindexer::IndexDef idef(getRuntimeUuidIndexName(indexNumber));
		Error err = rt.reindexer->DropIndex(default_namespace, idef);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void DropRuntimeUuidArrayIndex(int indexNumber) {
		reindexer::IndexDef idef(getRuntimeUuidArrayIndexName(indexNumber));
		Error err = rt.reindexer->DropIndex(default_namespace, idef);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddRuntimeStringIndex(int indexNumber, bool pk = false) {
		IndexOpts opts;
		if (pk) opts.PK();
		std::string indexName = getRuntimeStringIndexName(indexNumber);
		Error err = rt.reindexer->AddIndex(default_namespace, {indexName, "hash", "string", opts});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void DropRuntimeStringIndex(int indexNumber) {
		reindexer::IndexDef idef(getRuntimeStringIndexName(indexNumber));
		Error err = rt.reindexer->DropIndex(default_namespace, idef);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddDataForRuntimeStringIndex(int indexNumber) {
		std::string indexName = getRuntimeStringIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(default_namespace);
			item[indexName] = RandString();
			Upsert(default_namespace, item);
		}
		auto err = Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddRuntimeCompositeIndex(bool pk = false) {
		std::string indexName(getRuntimeCompositeIndexName(pk));
		Error err = rt.reindexer->AddIndex(default_namespace,
										   {indexName, getRuntimeCompositeIndexParts(pk), "tree", "composite", IndexOpts().PK(pk)});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void DropRuntimeCompositeIndex(bool pk = false) {
		reindexer::IndexDef idef(getRuntimeCompositeIndexName(pk));
		Error err = rt.reindexer->DropIndex(default_namespace, idef);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddRuntimeQPointIndex(int indexNumber) {
		std::string indexName = getRuntimeQPointIndexName(indexNumber);
		Error err = rt.reindexer->AddIndex(geom_namespace, {indexName, "rtree", "point", IndexOpts().RTreeType(IndexOpts::Quadratic)});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(geom_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddRuntimeLPointIndex(int indexNumber) {
		std::string indexName = getRuntimeLPointIndexName(indexNumber);
		Error err = rt.reindexer->AddIndex(geom_namespace, {indexName, "rtree", "point", IndexOpts().RTreeType(IndexOpts::Linear)});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(geom_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddRuntimeGPointIndex(int indexNumber) {
		std::string indexName = getRuntimeGPointIndexName(indexNumber);
		Error err = rt.reindexer->AddIndex(geom_namespace, {indexName, "rtree", "point", IndexOpts().RTreeType(IndexOpts::Greene)});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(geom_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddRuntimeSPointIndex(int indexNumber) {
		std::string indexName = getRuntimeSPointIndexName(indexNumber);
		Error err = rt.reindexer->AddIndex(geom_namespace, {indexName, "rtree", "point", IndexOpts().RTreeType(IndexOpts::RStar)});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(geom_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddDataForRuntimeQPointIndex(int indexNumber) {
		std::string indexName = getRuntimeQPointIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(geom_namespace);
			item[indexName] = randPoint(10);
			Upsert(geom_namespace, item);
		}
		auto err = Commit(geom_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddDataForRuntimeLPointIndex(int indexNumber) {
		std::string indexName = getRuntimeLPointIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(geom_namespace);
			item[indexName] = randPoint(10);
			Upsert(geom_namespace, item);
		}
		auto err = Commit(geom_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddDataForRuntimeGPointIndex(int indexNumber) {
		std::string indexName = getRuntimeGPointIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(geom_namespace);
			item[indexName] = randPoint(10);
			Upsert(geom_namespace, item);
		}
		auto err = Commit(geom_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddDataForRuntimeSPointIndex(int indexNumber) {
		std::string indexName = getRuntimeSPointIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(geom_namespace);
			item[indexName] = randPoint(10);
			Upsert(geom_namespace, item);
		}
		auto err = Commit(geom_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void DropRuntimeQPointIndex(int indexNumber) {
		reindexer::IndexDef idef(getRuntimeQPointIndexName(indexNumber));
		Error err = rt.reindexer->DropIndex(geom_namespace, idef);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(geom_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void DropRuntimeLPointIndex(int indexNumber) {
		reindexer::IndexDef idef(getRuntimeLPointIndexName(indexNumber));
		Error err = rt.reindexer->DropIndex(geom_namespace, idef);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(geom_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void DropRuntimeGPointIndex(int indexNumber) {
		reindexer::IndexDef idef(getRuntimeGPointIndexName(indexNumber));
		Error err = rt.reindexer->DropIndex(geom_namespace, idef);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(geom_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void DropRuntimeSPointIndex(int indexNumber) {
		reindexer::IndexDef idef(getRuntimeSPointIndexName(indexNumber));
		Error err = rt.reindexer->DropIndex(geom_namespace, idef);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Commit(geom_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void CheckSelectValidity(const Query& query) {
		QueryResults qr;
		Error err = rt.reindexer->Select(query, qr);
		ASSERT_TRUE(err.ok()) << query.GetSQL() << '\n' << err.what();
	}

	std::string getRuntimeCompositeIndexName(bool pk) {
		using namespace std::string_literals;
		std::string indexName;
		if (pk) {
			indexName = bookid + "+"s + bookid2;
		} else {
			indexName = bookid + "+"s + title;
		}
		return indexName;
	}

	reindexer::JsonPaths getRuntimeCompositeIndexParts(bool pk) {
		if (pk) {
			return {bookid, bookid2};
		} else {
			return {bookid, title};
		}
	}

	std::string getRuntimeIntIndexName(int indexNumber) { return runtime_int + std::to_string(indexNumber); }
	std::string getRuntimeUuidIndexName(int indexNumber) { return runtime_uuid + std::to_string(indexNumber); }
	std::string getRuntimeUuidArrayIndexName(int indexNumber) { return runtime_uuid_array + std::to_string(indexNumber); }
	std::string getRuntimeStringIndexName(int indexNumber) { return runtime_string + std::to_string(indexNumber); }
	std::string getRuntimeQPointIndexName(int indexNumber) { return runtime_qpoint + std::to_string(indexNumber); }
	std::string getRuntimeLPointIndexName(int indexNumber) { return runtime_lpoint + std::to_string(indexNumber); }
	std::string getRuntimeGPointIndexName(int indexNumber) { return runtime_gpoint + std::to_string(indexNumber); }
	std::string getRuntimeSPointIndexName(int indexNumber) { return runtime_spoint + std::to_string(indexNumber); }

	const char* geom_namespace = "geom_ns";

private:
	const char* id = "id";
	const char* bookid = "bookid";
	const char* bookid2 = "bookid2";
	const char* title = "title";
	const char* pages = "pages";
	const char* price = "price";
	const char* qpoints = "qpoints";
	const char* lpoints = "lpoints";
	const char* gpoints = "gpoints";
	const char* spoints = "spoints";
	const char* name = "name";
	const char* runtime_int = "runtime_int_";
	const char* runtime_string = "runtime_string_";
	const char* runtime_qpoint = "runtime_qpoint_";
	const char* runtime_lpoint = "runtime_lpoint_";
	const char* runtime_gpoint = "runtime_gpoint_";
	const char* runtime_spoint = "runtime_spoint_";
	const char* uuid = "uuid";
	const char* runtime_uuid = "runtime_uuid_";
	const char* runtime_uuid_array = "runtime_uuid_array_";

	static const int max_runtime_indexes = 10;
};
