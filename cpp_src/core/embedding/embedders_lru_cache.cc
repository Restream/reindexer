#include "embedders_lru_cache.h"
#include "core/cjson/jsonbuilder.h"
#include "core/enums.h"
#include "core/storage/storagefactory.h"
#include "core/type_consts.h"
#include "embedderscache.h"
#include "embeddingconfig.h"
#include "estl/elist.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

namespace {

const std::string kStorageBaseName{"cache"};
const std::string_view kStorageVersionKey{"embedder_storage_version"};
const std::string_view kCurrentStorageVersion{"1"};
constexpr std::string_view kStorageStatusDisabled{"DISABLED"};

};	// namespace

namespace reindexer {

EmbeddersLRUCache::EmbeddersLRUCache(CacheTag tag, size_t capacity, uint32_t hitToCache)
	: tag_{std::move(tag)}, capacity_{capacity}, hitToCache_{hitToCache} {
	assertrx_dbg(!tag_.Tag().empty());
	totalCacheSize_ = initBaseSize();
	storageStatus_ = kStorageStatusDisabled;
}

bool EmbeddersLRUCache::IsActive() const noexcept {
	shared_lock lck(storageMtx_);
	return ((capacity_ > 0) && storage_);
}

Error EmbeddersLRUCache::EnableStorage(const std::string& storagePathRoot, datastorage::StorageType type) noexcept {
	if (capacity_ == 0) {
		return {};
	}

	if (storagePathRoot.empty()) {
		logFmt(LogWarning, "Can't update embedder cache storage path with empty value ('{}')", tag_);
		return {};
	}

	std::string storageFile;
	unique_lock lck(storageMtx_);
	if (!storagePath_.empty()) {
		return {errParams, "Embedders cache storage is already enabled"};
	}

	try {
		storageFile = fs::JoinPath(storagePathRoot, tag_.Tag() + "_" + kStorageBaseName + "." + datastorage::StorageTypeToString(type));
		{
			auto err = activateStorage(type, storageFile);
			if (!err.ok()) {
				storageStatus_ = err.what();
				return err;
			}

			storagePath_ = storageFile;
			storageStatus_ = kStorageStatusOK;
		}
	} catch (const Error& err) {
		storageStatus_ = err.what();
		return err;
	} catch (const std::exception& ex) {
		storageStatus_ = ex.what();
		return {errLogic, "Can't load embedder cache from file '{}': {}", storageFile, ex.what()};
	} catch (...) {
		storageStatus_ = "FAILED";
		return {errLogic, "Can't load embedder cache from file '{}'", storageFile};
	}

	return {};
}

std::optional<embedding::ValueT> EmbeddersLRUCache::Get(const embedding::Adapter& srcAdapter, bool enablePerfStat) {
	if (!IsActive()) {
		return std::nullopt;
	}

	auto addCounterIfEnable = [enablePerfStat](std::atomic_uint64_t& counter) {
		if (enablePerfStat) {
			counter.fetch_add(1u, std::memory_order_relaxed);
		}
	};

	{
		lock_guard lck(cacheMtx_);
		const auto it = map_.find(srcAdapter.View());
		if (it == map_.end()) {
			addCounterIfEnable(misses_);
			return std::nullopt;
		}
		queue_.splice(queue_.begin(), queue_, it->second);
		if (it->second->second < hitToCache_) {
			addCounterIfEnable(misses_);
			return std::nullopt;
		}
	}

	std::string value;
	{
		static constexpr StorageOpts opts;
		shared_lock lck(storageMtx_);
		const auto err = storage_->Read(opts, srcAdapter.View(), value);
		if (!err.ok()) {
			addCounterIfEnable(misses_);
			return std::nullopt;
		}
	}

	Serializer rdser(value);
	const auto count = rdser.GetUInt32();
	embedding::ValueT res;
	res.reserve(count);
	for (unsigned i = 0; i < count; ++i) {
		res.emplace_back(rdser.GetFloatVectorView());
	}
	addCounterIfEnable(hits_);
	return res;
}

void EmbeddersLRUCache::Put(const embedding::Adapter& srcAdapter, const embedding::ValueT& values) {
	if (!IsActive()) {
		return;
	}

	static constexpr StorageOpts opts;
	static constexpr uint32_t hitInitVal = std::numeric_limits<uint32_t>::max();
	uint32_t hitToCache = hitInitVal;

	{
		lock_guard lck(cacheMtx_);
		const auto& key = srcAdapter.View();
		const auto it = map_.find(key);
		if (it != map_.end()) {
			++it->second->second;
			queue_.splice(queue_.begin(), queue_, it->second);
			hitToCache = it->second->second;
		} else {
			if (queue_.size() == capacity_) {
				const auto keyToRemove = queue_.back().first;
				map_.erase(keyToRemove);
				queue_.pop_back();

				shared_lock lckStorage(storageMtx_);
				auto err = storage_->Delete(opts, keyToRemove);
				lckStorage.unlock();
				if (!err.ok()) {
					throw err;
				}
				totalStorageSize_.fetch_sub(calculateStorageItemSize(keyToRemove, valueSize_.load(std::memory_order_relaxed)),
											std::memory_order_relaxed);
			}

			static constexpr uint32_t kInitCounterValue{1};
			std::ignore = queue_.emplace_front(key, kInitCounterValue);
			map_.emplace(key, queue_.begin());

			totalCacheSize_ += calculateItemSize(key);
			hitToCache = kInitCounterValue;
		}
	}

	if (hitToCache >= hitToCache_) {
		WrSerializer wrser;
		wrser.PutUInt32(values.size());
		for (const auto& v : values) {
			wrser.PutFloatVectorView(v.View());
		}
		auto val = wrser.Slice();

		shared_lock lck(storageMtx_);
		const auto err = storage_->Write(opts, srcAdapter.View(), val);
		lck.unlock();
		if (!err.ok()) {
			logFmt(LogWarning, "Can't write value to embedder cache storage ('{}:{}'): {}", srcAdapter.View(), val, err.what());
		}
		totalStorageSize_.fetch_add(calculateStorageItemSize(srcAdapter.View(), val), std::memory_order_relaxed);
	}
}

void EmbeddersLRUCache::Clear(NeedCreate createStorage) noexcept {
	if (!IsActive()) {
		return;
	}

	try {
		totalCacheSize_ = initBaseSize();
		SearchMap emptySearchMap;
		OrderQueue emptyOrderQueue;
		{
			scoped_lock lck{cacheMtx_, storageMtx_};
			emptySearchMap.swap(map_);
			emptyOrderQueue.swap(queue_);
			if (storage_) {
				totalStorageSize_.store(0, std::memory_order_relaxed);
				storage_->Destroy(storagePath_);
				if (createStorage == NeedCreate_True) {
					auto err = activateStorage(storage_->Type(), storagePath_);
					if (!err.ok()) {
						throw err;
					}
				}
			}
		}
		hits_.store(0u, std::memory_order_relaxed);
		misses_.store(0u, std::memory_order_relaxed);
	} catch (const std::exception& ex) {
		logFmt(LogError, "Can't clear embedder cache: {}", ex.what());
		return;
	} catch (...) {
		logFmt(LogError, "Can't clear embedder cache");
		return;
	}
	logFmt(LogInfo, "Cache with cache_tag '{}' was cleared", tag_);
}

EmbeddersCacheMemStat EmbeddersLRUCache::GetMemStat() const {
	if (!IsActive()) {
		return {};
	}

	EmbeddersCacheMemStat stats;
	stats.tag = tag_.Tag();
	stats.capacity = capacity_;
	auto& cache = stats.cache;
	cache.emptyCount = 0;
	cache.hitCountLimit = hitToCache_;
	{
		shared_lock lck(storageMtx_);
		stats.storageSize = totalStorageSize_.load(std::memory_order_relaxed);
		stats.storagePath = storagePath_;
		stats.storageEnabled = !storagePath_.empty();
		stats.storageStatus = storageStatus_;
		stats.storageOK = (storageStatus_ == kStorageStatusOK || storageStatus_ == kStorageStatusDisabled) && !stats.storagePath.empty();
	}
	{
		lock_guard lck(cacheMtx_);
		cache.totalSize = totalCacheSize_ + storagePath_.capacity() * sizeof(std::string::value_type) +
						  storageStatus_.capacity() * sizeof(std::string::value_type);
		cache.itemsCount = map_.size();
	}

	return stats;
}

LRUCachePerfStat EmbeddersLRUCache::GetPerfStat() const {
	LRUCachePerfStat stats{
		.state = IsActive() ? LRUCachePerfStat::State::Active : LRUCachePerfStat::State::Inactive, .hits = hits_, .misses = misses_};
	return stats;
}

void EmbeddersLRUCache::ResetPerfStat() noexcept {
	hits_.store(0u, std::memory_order_relaxed);
	misses_.store(0u, std::memory_order_relaxed);
}

template <typename T>
void EmbeddersLRUCache::Dump(T& os, std::string_view step, std::string_view offset) const {
	std::string curOffset{offset};
	curOffset += step;

	os << offset << "{\n" << curOffset << "tag: ";
	os << tag_ << ",\n"
	   << curOffset << "totalCacheSize: " << totalCacheSize_ << ",\n"
	   << curOffset << "totalStorageSize: " << totalStorageSize_.load(std::memory_order_relaxed) << ",\n"
	   << curOffset << "active: " << IsActive() << ",\n"
	   << curOffset << "storage path: " << storagePath_ << ",\n"
	   << curOffset << "storage status: " << storageStatus_ << ",\n"
	   << curOffset << "cacheSizeLimit: " << capacity_ << ",\n"
	   << curOffset << "hitCountToCache: " << hitToCache_ << ",\n"
	   << curOffset << "hit: " << hits_.load(std::memory_order_relaxed) << ",\n"
	   << curOffset << "misses: " << misses_.load(std::memory_order_relaxed) << ",\n"
	   << curOffset << "itemsCount: " << map_.size() << ",\n"
	   << offset << "}";
	// NOTE: no items. When there are a lot of them, it only complicates everything
}

uint32_t EmbeddersLRUCache::initBaseSize() const noexcept {
	assertrx_dbg(!tag_.Tag().empty());
	return sizeof(EmbeddersLRUCache) + sizeof(CacheTag) + tag_.Tag().capacity() * sizeof(tag_.Tag().front());
}

uint32_t EmbeddersLRUCache::calculateItemSize(const embedding::StrorageKeyT& key) const noexcept {
	static constexpr uint32_t kListDataConstSize = sizeof(std::pair<embedding::StrorageKeyT, uint32_t>) + 2 * sizeof(void*);
	size_t keySize = sizeof(embedding::StrorageKeyT) + key.capacity() * sizeof(embedding::StrorageKeyT::value_type);
	return 2 * keySize + kListDataConstSize + sizeof(OrderQueue::iterator);
}

uint32_t EmbeddersLRUCache::calculateStorageItemSize(std::string_view key, std::string_view value) noexcept {
	auto valSize = value.size();
	valueSize_.store(valSize, std::memory_order_relaxed);  // NOTE: it is expected that size is always same
	return calculateStorageItemSize(key, valSize);
}

uint32_t EmbeddersLRUCache::calculateStorageItemSize(std::string_view key, uint64_t valueSize) const noexcept {
	return sizeof(std::string::size_type) * 2 + (key.size() + valueSize) * sizeof(std::string::value_type);
}

void EmbeddersLRUCache::loadStorage() {
	StorageOpts opts;
	std::vector<std::string> keysToRemove;

	// read all keys from storage
	std::unique_ptr<datastorage::Cursor> dbIter(storage_->GetCursor(opts));
	for (dbIter->SeekToFirst(); dbIter->Valid(); dbIter->Next()) {
		const auto storageKey = std::string(dbIter->Key());
		if (storageKey == kStorageVersionKey) {
			continue;
		}
		if (queue_.size() >= capacity_) {
			keysToRemove.emplace_back(storageKey);
			continue;
		}

		queue_.emplace_back(storageKey, hitToCache_);
		auto iter = queue_.end();
		--iter;
		map_[storageKey] = iter;
		totalCacheSize_ += calculateItemSize(storageKey);
		totalStorageSize_.fetch_add(calculateStorageItemSize(storageKey, dbIter->Value()), std::memory_order_relaxed);
	}

	for (const auto& key : keysToRemove) {
		auto err = storage_->Delete(opts, key);
		if (!err.ok()) {
			throw err;
		}
	}
}

Error EmbeddersLRUCache::activateStorage(datastorage::StorageType type, const std::string& storageFile, unsigned recursionLevel) {
	storage_.reset(datastorage::StorageFactory::create(type));

	const bool storageFound = fs::DirectoryExists(storageFile);

	StorageOpts opts;
	opts.DropOnFileFormatError(true);
	opts.CreateIfMissing(!storageFound);
	auto err = storage_->Open(storageFile, opts);
	if (!err.ok()) {
		return err;
	}

	if (storageFound) {
		try {
			std::string version;
			err = storage_->Read(opts, kStorageVersionKey, version);
			if (!err.ok()) {
				if (err.code() == errNotFound) {
					if (recursionLevel > 0) {
						return {errLogic, "Unsupported version of embedder cache storage '{}'. Cannot recreate it", storageFile};
					}
					logFmt(LogWarning, "Unsupported version of embedder cache storage '{}'. Recreating it", storageFile);
					storage_->Destroy(storageFile);
					return activateStorage(type, storageFile, recursionLevel + 1);
				} else {
					return err;
				}
			}

			if (version == kCurrentStorageVersion) {
				loadStorage();
			} else if (recursionLevel > 0) {
				return {errLogic, "Unsupported version of embedder cache storage '{}'. Cannot recreate it", storageFile};
			} else {
				logFmt(LogWarning, "Unsupported version of embedder cache storage '{}'. Recreating it", storageFile);
				storage_->Destroy(storageFile);
				return activateStorage(type, storageFile, recursionLevel + 1);
			}
		} catch (const Error& err) {
			return err;
		} catch (const std::exception& ex) {
			return {errLogic, "Can't load embedders from file '{}': {}", storageFile, ex.what()};
		} catch (...) {
			return {errLogic, "Can't load embedders from file '{}'", storageFile};
		}
	} else {
		err = storage_->Write(opts, kStorageVersionKey, kCurrentStorageVersion);
		if (!err.ok()) {
			return err;
		}
	}

	return {};
}

}  // namespace reindexer
