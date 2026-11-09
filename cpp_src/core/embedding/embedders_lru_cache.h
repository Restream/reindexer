#pragma once

#include <optional>
#include "core/enums.h"
#include "core/storage/idatastorage.h"
#include "embedderscache.h"
#include "embeddingconfig.h"
#include "estl/elist.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "vendor/sparse-map/sparse_map.h"

namespace reindexer {

class [[nodiscard]] EmbeddersLRUCache final {
public:
	static constexpr std::string_view kStorageStatusOK{"OK"};

	EmbeddersLRUCache(CacheTag tag, size_t capacity, uint32_t hitToCache);

	EmbeddersLRUCache() noexcept = delete;
	EmbeddersLRUCache(EmbeddersLRUCache&&) noexcept = delete;
	EmbeddersLRUCache(const EmbeddersLRUCache&) noexcept = delete;
	EmbeddersLRUCache& operator=(const EmbeddersLRUCache&) noexcept = delete;
	EmbeddersLRUCache& operator=(EmbeddersLRUCache&&) noexcept = delete;
	~EmbeddersLRUCache() = default;

	Error EnableStorage(const std::string& storagePathRoot, datastorage::StorageType type) noexcept;

	std::optional<embedding::ValueT> Get(const embedding::Adapter& srcAdapter, bool enablePerfStat);
	void Put(const embedding::Adapter& srcAdapter, const embedding::ValueT& values);

	void Clear(NeedCreate recreateStorage) noexcept;

	EmbeddersCacheMemStat GetMemStat() const;
	LRUCachePerfStat GetPerfStat() const;
	bool IsActive() const noexcept;

	void ResetPerfStat() noexcept;

	template <typename T>
	void Dump(T& os, std::string_view step, std::string_view offset) const;

private:
	uint32_t initBaseSize() const noexcept;
	uint32_t calculateItemSize(const embedding::StrorageKeyT& key) const noexcept;
	uint32_t calculateStorageItemSize(std::string_view key, uint64_t valueSize) const noexcept;
	uint32_t calculateStorageItemSize(std::string_view key, std::string_view value) noexcept;
	void loadStorage();
	Error activateStorage(datastorage::StorageType type, const std::string& storageFile, unsigned recursionLevel = 0);

	const CacheTag tag_;
	const size_t capacity_{0};
	const uint32_t hitToCache_{0};

	using OrderQueue = elist<std::pair<embedding::StrorageKeyT, uint32_t>>;
	OrderQueue queue_;
	using SearchMap = tsl::sparse_map<embedding::StrorageKeyT, OrderQueue::iterator>;
	SearchMap map_;

	std::unique_ptr<datastorage::IDataStorage> storage_;
	std::string storagePath_;
	std::string storageStatus_;

	uint64_t totalCacheSize_{0};
	std::atomic_uint64_t valueSize_{0};
	std::atomic_uint64_t totalStorageSize_{0};
	std::atomic_uint64_t hits_{0};
	std::atomic_uint64_t misses_{0};

	mutable mutex cacheMtx_;
	mutable shared_mutex storageMtx_;
};

}  // namespace reindexer
