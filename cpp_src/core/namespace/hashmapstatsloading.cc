#include "hashmapstatsloading.h"
#include <chrono>
#include <thread>
#include "asyncstorage.h"
#include "core/index/index.h"
#include "tools/logger.h"
#include "tools/serilize/serializer.h"

namespace reindexer {

Error LoadIndexesHashMapStats(AsyncStorage& storage_, const std::string& storageKey, std::vector<IndexHashMapStats>& indexesStats) {
	indexesStats.resize(0);
	indexesStats.reserve(100);
	std::string content;

	Error status = storage_.Read(StorageOpts().FillCache(), storageKey, content);
	if (!status.ok()) [[unlikely]] {
		return status;
	}

	Serializer ser(content.data(), content.size());
	auto version = ser.GetUInt64();
	if (version != kHashMapStatsVersion) {
		return Error(errVersion, "Error on loading hash map stats, incorrect version " + std::to_string(version) + ", current version is " +
									 std::to_string(kHashMapStatsVersion));
	}
	while (!ser.Eof()) {
		std::string_view indexName = ser.GetSlice();
		std::string_view stats = ser.GetSlice();

		indexesStats.emplace_back(indexName, stats);
	}

	return errOK;
}

void UpdateNamespaceHashMapsStats(size_t numThreads, std::string_view namespaceName, const std::vector<std::unique_ptr<Index>>& indexes,
								  AsyncStorage& storage, const std::string& storageKey) noexcept {
	struct [[nodiscard]] Task {
		std::vector<Index*> indexes;
		std::vector<std::vector<char>> indexesStats;
	};

	try {
		const auto t0 = system_clock_w::now_coarse();

		std::vector<Task> tasks;
		tasks.resize(numThreads);

		size_t taskIdx = 0;
		for (const auto& index : indexes) {
			tasks[(taskIdx++) % numThreads].indexes.emplace_back(index.get());
		}

		if (numThreads > 1) {
			std::vector<std::thread> threadPool;
			threadPool.reserve(numThreads);
			for (size_t taskId = 0; taskId < numThreads; ++taskId) {
				threadPool.emplace_back([&tasks, taskId]() {
					Task& task = tasks[taskId];
					task.indexesStats.resize(task.indexes.size());
					for (size_t idx = 0; idx < task.indexes.size(); ++idx) {
						task.indexes[idx]->HashTablesStats(task.indexesStats[idx]);
					}
				});
			}

			for (auto& th : threadPool) {
				th.join();
			}
		} else {
			assertrx_dbg(numThreads == 1);
			tasks[0].indexesStats.resize(tasks[0].indexes.size());
			for (size_t idx = 0; idx < tasks[0].indexes.size(); ++idx) {
				tasks[0].indexes[idx]->HashTablesStats(tasks[0].indexesStats[idx]);
			}
		}

		WrSerializer ser;
		ser.PutUInt64(kHashMapStatsVersion);
		for (const auto& task : tasks) {
			for (size_t idx = 0; idx < task.indexes.size(); ++idx) {
				ser.PutSlice(task.indexes[idx]->Name());
				ser.PutSlice(std::string_view(task.indexesStats[idx].data(), task.indexesStats[idx].size()));
			}
		}

		const auto t1 = system_clock_w::now_coarse();
		storage.WriteSync(StorageOpts(), storageKey, ser.Slice());
		const auto t2 = system_clock_w::now_coarse();

		logFmt(LogInfo, "[{}] Hash tables stats were updated: size: {} KB; creation time: {} ms; writing time: {} ms", namespaceName,
			   ser.Len() / 1024, std::chrono::duration_cast<std::chrono::milliseconds>((t1 - t0)).count(),
			   std::chrono::duration_cast<std::chrono::milliseconds>((t2 - t1)).count());
	} catch (const std::exception& exc) {
		logFmt(LogError, "[{}] Error while updating hash tables stats: {}", namespaceName, exc.what());
	} catch (...) {
		logFmt(LogError, "[{}] Unknown error while updating hash tables stats", namespaceName);
	}
}

}  // namespace reindexer