#pragma once

#include "gtests/tools.h"
#include "reindexer_api.h"

class [[nodiscard]] RuntimeIndexesApi : public ReindexerApi {
public:
	void SetUp() override {
		using namespace std::string_literals;
		ReindexerApi::SetUp();

		rt.OpenNamespace(default_namespace);

		DefineNamespaceDataset(
			default_namespace,
			{IndexDeclaration{bookid, "hash", "int", IndexOpts(), 0}, IndexDeclaration{bookid2, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{title, "text", "string", IndexOpts(), 0}, IndexDeclaration{pages, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{price, "hash", "int", IndexOpts(), 0}, IndexDeclaration{name, "text", "string", IndexOpts(), 0},
			 IndexDeclaration{uuid, "hash", "uuid", IndexOpts(), 0},
			 IndexDeclaration{(title + "+"s + price).c_str(), "hash", "composite", IndexOpts(), 0},
			 IndexDeclaration{"bookid+bookid2", "hash", "composite", IndexOpts().PK(), 0}});

		rt.OpenNamespace(geom_namespace);
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

			Item geoItem = NewItem(geom_namespace);
			geoItem[this->id] = id + 500;
			geoItem[qpoints] = randPoint(10);
			geoItem[lpoints] = randPoint(10);
			geoItem[gpoints] = randPoint(10);
			geoItem[spoints] = randPoint(10);
			Upsert(geom_namespace, geoItem);
		}
	}

	void AddRuntimeIntArrayIndex(int indexNumber) {
		std::string indexName = getRuntimeIntIndexName(indexNumber);
		rt.AddIndex(default_namespace, {indexName, "hash", "int", IndexOpts().Array()});
	}

	void DropRuntimeIntArrayIndex(int indexNumber) { rt.DropIndex(default_namespace, getRuntimeIntIndexName(indexNumber)); }

	void AddDataForRuntimeIntIndex(int indexNumber) {
		std::string indexName = getRuntimeIntIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(default_namespace);
			item[indexName] = rand() % 100;
			Upsert(default_namespace, item);
		}
	}

	void AddRuntimeUuidIndex(int indexNumber) {
		std::string indexName = getRuntimeUuidIndexName(indexNumber);
		rt.AddIndex(default_namespace, {indexName, "hash", "uuid", IndexOpts()});
	}

	void AddRuntimeUuidArrayIndex(int indexNumber) {
		std::string indexName = getRuntimeUuidArrayIndexName(indexNumber);
		rt.AddIndex(default_namespace, {indexName, "hash", "uuid", IndexOpts().Array()});
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
	}

	void DropRuntimeUuidIndex(int indexNumber) { rt.DropIndex(default_namespace, getRuntimeUuidIndexName(indexNumber)); }
	void DropRuntimeUuidArrayIndex(int indexNumber) { rt.DropIndex(default_namespace, getRuntimeUuidArrayIndexName(indexNumber)); }
	void DropRuntimeStringIndex(int indexNumber) { rt.DropIndex(default_namespace, getRuntimeStringIndexName(indexNumber)); }

	void AddRuntimeStringIndex(int indexNumber, bool pk = false) {
		IndexOpts opts;
		if (pk) {
			opts.PK();
		}
		std::string indexName = getRuntimeStringIndexName(indexNumber);
		rt.AddIndex(default_namespace, {indexName, "hash", "string", opts});
	}

	void AddDataForRuntimeStringIndex(int indexNumber) {
		std::string indexName = getRuntimeStringIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(default_namespace);
			item[indexName] = RandString();
			Upsert(default_namespace, item);
		}
	}

	void AddRuntimeCompositeIndex(bool pk = false) {
		std::string indexName(getRuntimeCompositeIndexName(pk));
		rt.AddIndex(default_namespace, {indexName, getRuntimeCompositeIndexParts(pk), "tree", "composite", IndexOpts().PK(pk)});
	}

	void DropRuntimeCompositeIndex(bool pk = false) { rt.DropIndex(default_namespace, getRuntimeCompositeIndexName(pk)); }

	void AddRuntimeQPointIndex(int indexNumber) {
		std::string indexName = getRuntimeQPointIndexName(indexNumber);
		rt.AddIndex(geom_namespace, {indexName, "rtree", "point", IndexOpts().RTreeType(IndexOpts::Quadratic)});
	}

	void AddRuntimeLPointIndex(int indexNumber) {
		std::string indexName = getRuntimeLPointIndexName(indexNumber);
		rt.AddIndex(geom_namespace, {indexName, "rtree", "point", IndexOpts().RTreeType(IndexOpts::Linear)});
	}

	void AddRuntimeGPointIndex(int indexNumber) {
		std::string indexName = getRuntimeGPointIndexName(indexNumber);
		rt.AddIndex(geom_namespace, {indexName, "rtree", "point", IndexOpts().RTreeType(IndexOpts::Greene)});
	}

	void AddRuntimeSPointIndex(int indexNumber) {
		std::string indexName = getRuntimeSPointIndexName(indexNumber);
		rt.AddIndex(geom_namespace, {indexName, "rtree", "point", IndexOpts().RTreeType(IndexOpts::RStar)});
	}

	void AddDataForRuntimeQPointIndex(int indexNumber) {
		std::string indexName = getRuntimeQPointIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(geom_namespace);
			item[indexName] = randPoint(10);
			Upsert(geom_namespace, item);
		}
	}

	void AddDataForRuntimeLPointIndex(int indexNumber) {
		std::string indexName = getRuntimeLPointIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(geom_namespace);
			item[indexName] = randPoint(10);
			Upsert(geom_namespace, item);
		}
	}

	void AddDataForRuntimeGPointIndex(int indexNumber) {
		std::string indexName = getRuntimeGPointIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(geom_namespace);
			item[indexName] = randPoint(10);
			Upsert(geom_namespace, item);
		}
	}

	void AddDataForRuntimeSPointIndex(int indexNumber) {
		std::string indexName = getRuntimeSPointIndexName(indexNumber);
		for (size_t i = 0; i < 10; ++i) {
			Item item = NewItem(geom_namespace);
			item[indexName] = randPoint(10);
			Upsert(geom_namespace, item);
		}
	}

	void DropRuntimeQPointIndex(int indexNumber) { rt.DropIndex(geom_namespace, getRuntimeQPointIndexName(indexNumber)); }
	void DropRuntimeLPointIndex(int indexNumber) { rt.DropIndex(geom_namespace, getRuntimeLPointIndexName(indexNumber)); }
	void DropRuntimeGPointIndex(int indexNumber) { rt.DropIndex(geom_namespace, getRuntimeGPointIndexName(indexNumber)); }
	void DropRuntimeSPointIndex(int indexNumber) { rt.DropIndex(geom_namespace, getRuntimeSPointIndexName(indexNumber)); }

	void CheckSelectValidity(const Query& query) { std::ignore = rt.Select(query); }

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
