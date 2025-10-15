#pragma once

#include <optional>
#include "core/indexdef.h"
#include "core/payload/fieldsset.h"
#include "core/rdxcontext.h"
#include "estl/contexted_locks.h"
#include "estl/shared_mutex.h"
#include "tools/stringstools.h"
#include "vendor/sparse-map/sparse_map.h"

namespace reindexer {

class Index;
class AsyncStorage;
class PayloadType;
class NamespaceImpl;
class FloatVectorIndex;

namespace ann_storage_cache {

std::string GetStorageKey(std::string_view name) noexcept;
bool IsANNCacheEnabledByEnv() noexcept;

class [[nodiscard]] UpdateInfo {
public:
	using nanoseconds = std::chrono::nanoseconds;

	void Update(const std::string& idx, nanoseconds lastUpdateTime) noexcept;
	void Remove(std::string_view idx) noexcept;
	nanoseconds LastUpdateTime(std::string_view idx) const noexcept;
	void Clear() noexcept;

private:
	struct [[nodiscard]] SingleUpdateInfo {
		nanoseconds lastUpdateTime{0};
	};

	mutable mutex mtx_;
	tsl::sparse_map<std::string, SingleUpdateInfo, nocase_hash_str, nocase_equal_str> infoMap_;
};

class [[nodiscard]] Writer {
public:
	using milliseconds = std::chrono::milliseconds;
	using nanoseconds = std::chrono::nanoseconds;
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::Namespace>;
	using RLockT = contexted_shared_lock<Mutex, const RdxContext>;

	Writer(const NamespaceImpl& ns) noexcept;

	bool TryUpdateNextPart(RLockT&& lock, AsyncStorage& storage, UpdateInfo& updateInfo, const std::atomic_int32_t& cancel);

private:
	struct [[nodiscard]] StorageCacheWriteResult {
		Error err;
		bool isCacheable = false;
	};

	std::pair<int, const Index*> getPKField() const noexcept;
	StorageCacheWriteResult writeSingleIndexCache(FloatVectorIndex& ann, WrSerializer& ser, const std::atomic_int32_t& cancel);

	const NamespaceImpl& ns_;
	const nanoseconds lastUpdateTime_;
	const int64_t lsnCounter_;
	unsigned curIndex_{0};
};

class [[nodiscard]] Reader {
public:
	using nanoseconds = std::chrono::nanoseconds;

	Reader(std::string_view nsName, nanoseconds lastUpdate, unsigned pkField, AsyncStorage&,
		   std::function<int(std::string_view)>&& nameToField, const std::function<IndexDef(size_t)>& getIndexDefinitionF);

	struct [[nodiscard]] CachedIndex {
		size_t field;
		std::chrono::nanoseconds lastUpdate;
		std::string name;
		std::string data;
	};

	std::optional<CachedIndex> GetNextCachedIndex();
	bool HasCacheFor(size_t field) const noexcept { return field < kMaxIndexes && cachedIndexes_.test(field); }

private:
	bool checkIndexDef(Serializer& ser, unsigned field, std::string_view keySlice, std::string_view indexType,
					   const std::function<IndexDef(size_t)>& getIndexDefinitionF);

	struct [[nodiscard]] CachedIndexMeta {
		size_t field;
		std::string name;
	};

	std::bitset<kMaxIndexes> cachedIndexes_;
	std::vector<CachedIndexMeta> cachedMeta_;
	mutex mtx_;
	AsyncStorage& storage_;
	std::string_view nsName_;
};

}  // namespace ann_storage_cache
}  // namespace reindexer
