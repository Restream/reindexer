#include "core/embedding/embedderscache.h"

#include <optional>
#include "core/cjson/jsonbuilder.h"
#include "core/enums.h"
#include "core/storage/storagefactory.h"
#include "core/system_ns_names.h"
#include "estl/chunk.h"
#include "estl/elist.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/stringstools.h"
#include "vendor/gason/gason.h"
#include "vendor/sparse-map/sparse_map.h"

namespace reindexer {

namespace {
const std::string kStorageBaseName{"cache"};
constexpr std::string_view kDataFieldName{"data"};
constexpr std::string_view kResultDataName{"products"};
constexpr std::string_view kWildcard{"*"};
constexpr std::string_view kStorageStatusOK{"OK"};
constexpr std::string_view kStorageStatusDisabled{"DISABLED"};
constexpr size_t kProductDimension{1024};
};	// namespace

namespace embedding {

Error Adapter::VectorFromJSON(const StrorageKeyT& json, ValuesT& result) noexcept {
	try {
		assertrx_dbg(!json.empty());

		result.resize(0);

		gason::JsonParser parser;
		auto root = parser.Parse(json);
		vectorFromJSON(root, result);
	} catch (const std::exception& e) {
		return {errParseJson, "Embed source adapter can't parse vector '{}': {}", json, e.what()};
	} catch (...) {
		return {errParseJson, "Embed source adapter can't parse vector '{}'", json};
	}
	return {};
}

Adapter::Adapter(const BaseKeyT& source) {
	WrSerializer ser;
	{  // [text0]
		JsonBuilder json{ser, ObjType::TypePlain};
		json.Put(TagName::Empty(), source);
	}
	view_ = std::string{ser.Slice()};
}

Adapter::Adapter(std::span<const std::vector<std::pair<std::string, VariantArray>>> sources) {
	WrSerializer ser;
	{  // {'fld0':text,'fld1':[Val0,Val1,...],...}
		JsonBuilder json{ser, ObjType::TypePlain};
		for (const auto& docSource : sources) {
			auto arrNodeItem = json.Object(TagName::Empty());
			for (const auto& itemSource : docSource) {
				if (!itemSource.second.IsArrayValue() && itemSource.second.size() == 1) {
					arrNodeItem.Put(itemSource.first, itemSource.second.front());
				} else {
					auto arrNode = arrNodeItem.Array(itemSource.first);
					for (const auto& item : itemSource.second) {
						arrNode.Put(TagName::Empty(), item);
					}
				}
			}
			arrNodeItem.End();
		}
	}
	view_ = std::string{ser.Slice()};
}

chunk Adapter::Content() const {
	WrSerializer ser;
	{  // {'data':[*view_*]}
		JsonBuilder json{ser};
		auto arrNodeDoc = json.Array(kDataFieldName);
		arrNodeDoc.Raw(view_);
	}
	return ser.DetachChunk();
}

void Adapter::vectorFromJSON(const gason::JsonNode& root, ValuesT& result) {
	using namespace std::string_view_literals;
	static thread_local std::vector<float> values(kProductDimension);
	for (auto products : root[kResultDataName]) {
		values.resize(0);
		for (auto product : products) {
			// auto chunk = product["chunk"sv].As<std::string>();
			for (auto val : product["embedding"sv]) {
				values.emplace_back(val.As<double>());
			}
			if (!result.empty()) {
				throw Error(errLogic, "Chunks are not supported: {}", product["embedding"sv].As<std::string>());
			}
			result.emplace_back(values);
		}
	}
}

}  // namespace embedding

class [[nodiscard]] EmbeddersLRUCache final {
public:
	EmbeddersLRUCache(CacheTag tag, size_t capacity, uint32_t hitToCache);

	EmbeddersLRUCache() noexcept = delete;
	EmbeddersLRUCache(EmbeddersLRUCache&&) noexcept = delete;
	EmbeddersLRUCache(const EmbeddersLRUCache&) noexcept = delete;
	EmbeddersLRUCache& operator=(const EmbeddersLRUCache&) noexcept = delete;
	EmbeddersLRUCache& operator=(EmbeddersLRUCache&&) noexcept = delete;
	~EmbeddersLRUCache() = default;

	Error EnableStorage(const std::string& storagePathRoot, datastorage::StorageType type) noexcept;

	std::optional<embedding::ValueT> Get(const embedding::Adapter& srcAdapter);
	void Put(const embedding::Adapter& srcAdapter, const embedding::ValuesT& values);

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
	Error activateStorage(datastorage::StorageType type, const std::string& storageFile);

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

std::optional<embedding::ValueT> EmbeddersLRUCache::Get(const embedding::Adapter& srcAdapter) {
	if (!IsActive()) {
		return std::nullopt;
	}

	{
		lock_guard lck(cacheMtx_);
		const auto it = map_.find(srcAdapter.View());
		if (it == map_.end()) {
			misses_.fetch_add(1u, std::memory_order_relaxed);
			return std::nullopt;
		}
		queue_.splice(queue_.begin(), queue_, it->second);
		if (it->second->second < hitToCache_) {
			misses_.fetch_add(1u, std::memory_order_relaxed);
			return std::nullopt;
		}
	}

	std::string value;
	{
		static constexpr StorageOpts opts;
		shared_lock lck(storageMtx_);
		const auto err = storage_->Read(opts, srcAdapter.View(), value);
		if (!err.ok()) {
			misses_.fetch_add(1u, std::memory_order_relaxed);
			return std::nullopt;
		}
	}

	Serializer rdser(value);
	embedding::ValueT res{rdser.GetFloatVectorView()};

	hits_.fetch_add(1u, std::memory_order_relaxed);
	return res;
}

void EmbeddersLRUCache::Put(const embedding::Adapter& srcAdapter, const embedding::ValuesT& values) {
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
		// NOTE: now store only one, first value
		assertrx_throw(values.size() == 1);
		WrSerializer wrser;
		wrser.PutFloatVectorView(values.front().View());
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
		{
			scoped_lock lck{cacheMtx_, storageMtx_};
			SearchMap().swap(map_);
			OrderQueue().swap(queue_);
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

		logFmt(LogInfo, "Cache with cache_tag '{}' cleared", tag_);
	} catch (const std::exception& ex) {
		logFmt(LogError, "Can't clear embedder cache: {}", ex.what());
	} catch (...) {
		logFmt(LogError, "Can't clear embedder cache");
	}
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

Error EmbeddersLRUCache::activateStorage(datastorage::StorageType type, const std::string& storageFile) {
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
			loadStorage();
		} catch (const Error& err) {
			return err;
		} catch (const std::exception& ex) {
			return {errLogic, "Can't load embedders from file '{}': {}", storageFile, ex.what()};
		} catch (...) {
			return {errLogic, "Can't load embedders from file '{}'", storageFile};
		}
	}

	return {};
}

bool EmbeddersCache::IsEmbedderSystemName(std::string_view nsName) noexcept {
	return !nsName.empty() && iequals(nsName, kEmbeddersPseudoNamespace);
}

EmbeddersCache::~EmbeddersCache() = default;

Error EmbeddersCache::UpdateConfig(fast_hash_map<std::string, EmbedderConfigData, hash_str, equal_str, less_str> config) {
	try {
		unique_lock lk(mtx_);
		if (!config_.has_value() || *config_ != config) {
			h_vector<CacheTag, 1> tags;
			if (config_.has_value()) {
				tags.reserve(caches_.size());
				for (const auto& cache : caches_) {
					cache.second->Clear(NeedCreate_False);
					tags.emplace_back(cache.first);
				}
				caches_.clear();
			}
			config_ = std::move(config);
			// recreate caches
			for (const auto& tag : tags) {
				includeTag(tag);
			}
		}
	} catch (const Error& err) {
		return err;
	} catch (const std::exception& ex) {
		return {errLogic, "Can't update configuration for embedders cache: {}", ex.what()};
	} catch (...) {
		return {errLogic, "Can't update configuration for embedders cache"};
	}
	return {};
}

Error EmbeddersCache::EnableStorage(const std::string& storagePathRoot, datastorage::StorageType type) {
	if (storagePathRoot.empty()) {
		logFmt(LogWarning, "Can't update embedders cache storage path with empty value");
		return errOK;
	}

	unique_lock lk(mtx_);
	if (!storagePath_.empty()) {
		return {errParams, "Embedders cache storage is already enabled"};
	}
	auto storagePath = fs::JoinPath(storagePathRoot, kEmbeddersPseudoNamespace);
	if (fs::MkDirAll(storagePath) < 0) {
		return {errParams, "Can't create directory '{}' for embedders cache storage: {}", storagePath, strerror(errno)};
	}
	for (const auto& cache : caches_) {
		auto err = cache.second->EnableStorage(storagePath, type);
		if (!err.ok()) {
			return err;
		}
	}

	type_ = type;
	storagePath_ = storagePath;
	return {};
}

void EmbeddersCache::IncludeTag(std::string_view tag) {
	unique_lock lk(mtx_);
	includeTag(CacheTag{tag});
}

std::optional<embedding::ValueT> EmbeddersCache::Get(const CacheTag& tag, const embedding::Adapter& srcAdapter) {
	if (tag.Tag().empty()) {
		return std::nullopt;  // NOTE: do nothing - valid situation
	}

	{
		shared_lock lk(mtx_);
		const auto it = caches_.find(tag, tag.Hash());
		if (it != caches_.end()) {
			return it->second->Get(srcAdapter);
		}
	}

	logFmt(LogWarning, "Get. Embedder cache tag '{}' not found", tag.Tag());
	return std::nullopt;
}

void EmbeddersCache::Put(const CacheTag& tag, const embedding::Adapter& srcAdapter, const embedding::ValuesT& values) {
	if (tag.Tag().empty()) {
		return;	 // NOTE: do nothing - valid situation
	}

	{
		shared_lock lk(mtx_);
		const auto it = caches_.find(tag, tag.Hash());
		if (it != caches_.end()) {
			it->second->Put(srcAdapter, values);
		}
	}

	logFmt(LogWarning, "Put. Embedder cache tag '{}' not found", tag);
}

bool EmbeddersCache::IsActive() const noexcept {
	shared_lock lk(mtx_);
	for (const auto& cache : caches_) {
		if (cache.second->IsActive()) {
			return true;
		}
	}
	return false;
}

NamespaceMemStat EmbeddersCache::GetMemStat() const {
	shared_lock lk(mtx_);
	NamespaceMemStat stats;
	stats.name = NamespaceName{kEmbeddersPseudoNamespace};
	stats.type = NamespaceMemStat::kEmbeddersStatType;
	stats.storageOK = !caches_.empty();
	stats.storageEnabled = !caches_.empty();
	stats.storagePath = storagePath_;
	stats.storageStatus = kStorageStatusOK;
	stats.embedders.reserve(caches_.size());
	for (const auto& cache : caches_) {
		auto stat = cache.second->GetMemStat();
		if (!stat.storageOK) {
			stats.storageOK = false;
			stats.storageStatus = stat.storageStatus;
		}
		stats.storageEnabled &= stat.storageEnabled;
		stats.itemsCount += stat.cache.itemsCount;
		stats.Total.cacheSize += stat.cache.totalSize;

		stats.embedders.emplace_back(std::move(stat));
	}
	return stats;
}

EmbedderCachePerfStat EmbeddersCache::GetPerfStat(std::string_view tag) const {
	shared_lock lk(mtx_);
	CacheTag tg(tag);
	auto it = caches_.find(tg, tg.Hash());
	if (it == caches_.end()) {
		return {};
	}

	auto st = it->second->GetPerfStat();
	EmbedderCachePerfStat stat;
	stat.tag = tag;
	stat.state = st.state;
	stat.hits = st.hits;
	stat.misses = st.misses;
	return stat;
}

void EmbeddersCache::ResetPerfStat() noexcept {
	shared_lock lk(mtx_);
	for (const auto& cache : caches_) {
		cache.second->ResetPerfStat();
	}
}

void EmbeddersCache::Clear(std::string_view tag) {
	if (tag.empty()) {
		throw Error(errParams, "Attempt to clear cache with unspecified cache_tag");
	}

	shared_lock lk(mtx_);
	const bool forAny = (kWildcard == tag);
	for (const auto& cache : caches_) {
		if (forAny || (cache.first.Tag() == tag)) {
			cache.second->Clear(NeedCreate_True);
		}
	}
}

template <typename T>
void EmbeddersCache::Dump(T& os, std::string_view step, std::string_view offset) const {
	std::string newOffset{offset};
	newOffset += step;

	os << offset << "{\n" << newOffset << "EmbeddersCache: [\n";
	shared_lock lk(mtx_);
	bool first = true;
	for (const auto& cache : caches_) {
		os << newOffset << step << cache.first << ":";
		cache.second->Dump(os, step, newOffset);
		if (first) {
			first = false;
		} else {
			os << ",\n";
		}
	}
	os << newOffset << "]\n" << offset << '}';
}

bool EmbeddersCache::getEmbeddersConfig(std::string_view tag, EmbedderConfigData& data) {
	// NOLINTBEGIN (bugprone-unchecked-optional-access) Optional parameters were checked one level higher
	assertrx_dbg(config_.has_value());
	auto it = config_->find(tag);
	if (it == config_->end()) {
		it = config_->find(kWildcard);
	}
	const bool found = (it != config_->end());
	// NOLINTEND (bugprone-unchecked-optional-access) Optional parameters were checked one level higher
	data = found ? it->second : EmbedderConfigData{};
	return found;
}

void EmbeddersCache::includeTag(const CacheTag& tag) {
	if (tag.Tag().empty()) {
		return;	 // NOTE: do nothing - valid situation
	}

	if (!config_.has_value()) {
		logFmt(LogError, "Configuration not found, cache_tag '{}' ignored", tag);
		return;
	}
	if (caches_.find(tag, tag.Hash()) != caches_.end()) {
		return;	 // NOTE: only once added (unique by tag), in configuration possible multi definitions
	}
	EmbedderConfigData data;
	if (!getEmbeddersConfig(tag.Tag(), data)) {
		logFmt(LogError, "Unexpected cache_tag '{}' in Embedder config", tag);
	} else if (data.maxCacheItems == 0) {
		logFmt(LogWarning, "cache_tag '{}' is skipped, max_cache_items is 0", tag);
	}

	auto cache = std::make_shared<EmbeddersLRUCache>(tag, data.maxCacheItems, data.hitToCache);
	if (storagePath_.empty()) {
		logFmt(LogError, "cache_tag '{}' storage not enabled", tag);
	} else {
		auto err = cache->EnableStorage(storagePath_, type_);
		if (!err.ok()) {
			throw err;
		}
	}
	caches_.emplace(tag, cache);
}

}  // namespace reindexer
