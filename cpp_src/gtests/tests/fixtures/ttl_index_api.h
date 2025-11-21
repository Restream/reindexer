#pragma once

#include <thread>
#include "reindexer_api.h"

class [[nodiscard]] TtlIndexApi : public ReindexerApi {
public:
	void SetUp() override {
		ReindexerApi::SetUp();
		rt.OpenNamespace(default_namespace);
		DefineNamespaceDataset(default_namespace, {IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0},
												   IndexDeclaration{kFieldData, "tree", "int", IndexOpts().Array(), 0},
												   IndexDeclaration{kFieldData, "tree", "int", IndexOpts().Array(), 0}});
		rt.AddIndex(default_namespace, reindexer::IndexDef(kFieldDate, {kFieldDate}, "ttl", "int64", IndexOpts(), 1));

		AddDataToNs(3000);
	}

	Item MakeItem() {
		Item item(rt.NewItem(default_namespace));
		if (item.Status().ok()) {
			item["id"] = id++;
			item["data"] = int(random() % 5000);
			item["date"] = static_cast<int64_t>(time(nullptr));
		}
		return item;
	}

	void AddDataToNs(size_t count) {
		for (size_t i = 0; i < count; ++i) {
			Item item(MakeItem());
			rt.Upsert(default_namespace, item);
		}
	}

	void RemoveAll() { rt.Delete(Query(default_namespace)); }

	void RemoveItems(int idFirst, int idLast) {
		if (idFirst < idLast) {
			rt.Delete(Query(default_namespace).Where("id", CondGe, Variant(idFirst)).Where("id", CondLe, Variant(idLast)));
		}
	}

	size_t GetItemsCount() { return rt.Select(Query(default_namespace)).Count(); }

	int WaitForVanishing() {
#if !defined(REINDEX_WITH_TSAN) && !defined(REINDEX_WITH_ASAN) && !defined(RX_WITH_STDLIB_DEBUG)
		constexpr auto kStep = std::chrono::milliseconds(100);
#else	// defined (REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN) || defined(RX_WITH_STDLIB_DEBUG)
		constexpr auto kStep = std::chrono::milliseconds(500);
#endif	// defined (REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN) || defined(RX_WITH_STDLIB_DEBUG)
		constexpr size_t kStepsCount = 20;
		size_t count = GetItemsCount();
		if (count > 0) {
			for (size_t i = 0; i < kStepsCount; ++i) {
				std::this_thread::sleep_for(kStep);
				count = GetItemsCount();
				if (count == 0) {
					break;
				}
			}
		}
		return count;
	}

	void SelectData() {
		for (size_t i = 0; i < 10; ++i) {
			int from = random() % 500 + 1;
			int till = random() % 5000 + (from + 1);
			std::ignore = rt.Select(Query(default_namespace).Where("id", CondGe, from).Where("id", CondLe, till));

			std::this_thread::sleep_for(std::chrono::milliseconds(200));
		}
	}

	void RemoveItemsSlowly() {
		size_t idFirst = 0, idLast = 100;
		for (size_t i = 0; i < 10; ++i) {
			RemoveItems(idFirst, idLast);

			std::this_thread::sleep_for(std::chrono::milliseconds(100));

			size_t offset = random() % 300 + 1;
			idFirst += offset;
			idLast += offset;
		}
	}

	void InsertItemsSlowly() {
		for (size_t i = 0; i < 15; ++i) {
			AddDataToNs(random() % 300 + 10);
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
	}

protected:
	const char* kFieldId = "id";
	const char* kFieldData = "data";
	const char* kFieldDate = "date";
	int id = 0;
};
