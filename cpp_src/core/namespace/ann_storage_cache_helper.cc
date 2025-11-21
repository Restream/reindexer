#include "ann_storage_cache_helper.h"
#include "core/formatters/namespacesname_fmt.h"
#include "core/index/float_vector/float_vector_index.h"
#include "namespaceimpl.h"
#include "tools/logger.h"
#include "tools/scope_guard.h"

namespace reindexer::ann_storage_cache {

#define kStorageANNCachePrefix "ann_cache"
constexpr static uint8_t kANNCacheFormatVersion = 1;

std::string GetStorageKey(std::string_view name) noexcept {
	std::string key(kStorageANNCachePrefix);
	key.append(".").append(toLower(name));
	return key;
}

bool IsANNCacheEnabledByEnv() noexcept {
	const auto kEnvPtr = std::getenv("RX_DISABLE_ANN_CACHE");
	if (!kEnvPtr) {
		return true;
	}
	const auto len = strlen(kEnvPtr);
	return !len || (len == 1 && kEnvPtr[0] == '0');
}

void UpdateInfo::Update(const std::string& idx, nanoseconds lastUpdateTime) noexcept {
	lock_guard lck(mtx_);
	infoMap_[idx].lastUpdateTime = lastUpdateTime;
}

void UpdateInfo::Remove(std::string_view idx) noexcept {
	lock_guard lck(mtx_);
	infoMap_.erase(idx);
}

UpdateInfo::nanoseconds UpdateInfo::LastUpdateTime(std::string_view idx) const noexcept {
	lock_guard lck(mtx_);
	const auto found = infoMap_.find(idx);
	return (found != infoMap_.end()) ? found->second.lastUpdateTime : UpdateInfo::nanoseconds(0);
}

void UpdateInfo::Clear() noexcept {
	lock_guard lck(mtx_);
	infoMap_.clear();
}

Writer::Writer(const NamespaceImpl& ns) noexcept : ns_{ns}, lastUpdateTime_{ns_.lastUpdateTimeNano()}, lsnCounter_{ns_.wal_.LSNCounter()} {}

bool Writer::TryUpdateNextPart(RLockT&& lock, AsyncStorage& storage, UpdateInfo& updateInfo, const std::atomic_int32_t& cancel) {
	assertrx(lock.owns_lock());
	assertrx(ns_.locker_.IsMutexCorrect(lock.mutex()));

	if (lastUpdateTime_ != nanoseconds(ns_.lastUpdateTimeNano()) || lsnCounter_ != ns_.wal_.LSNCounter()) {
		// Namespace has been modified between writer's calls. New writer object is required
		return false;
	}
	if (!IsANNCacheEnabledByEnv()) {
		return false;
	}
	if (cancel.load(std::memory_order_relaxed)) {
		return false;
	}
	if (!ns_.storage_.IsValid()) {
		return false;
	}
	const unsigned regularIndexes = ns_.indexes_.firstSparsePos();
	while (curIndex_ < regularIndexes) {
		auto counterGuard = MakeScopeGuard([this]() noexcept { ++curIndex_; });

		auto idx = ns_.indexes_[curIndex_].get();
		if (auto ann = dynamic_cast<FloatVectorIndex*>(idx); ann) {
			if (updateInfo.LastUpdateTime(ann->Name()) == lastUpdateTime_) {
				continue;
			}
			if (cancel.load(std::memory_order_relaxed)) {
				counterGuard.Disable();
				return false;
			}
			const auto t0 = system_clock_w::now_coarse();
			WrSerializer ser;
			auto res = writeSingleIndexCache(*ann, ser, cancel);
			if (!res.isCacheable) {
				logFmt(LogInfo, "[{}] ANN index '{}' does not requires caching. Skipping this index", ns_.name_, ann->Name());
				updateInfo.Update(ann->Name(), lastUpdateTime_);  // Still performing time update
				continue;
			}
			if (cancel.load(std::memory_order_relaxed)) {
				counterGuard.Disable();
				return false;
			}
			if (!res.err.ok()) {
				logFmt(LogWarning, "[{}] Unable to create storage cache for ANN index '{}': {}. Skipping this index", ns_.name_,
					   ann->Name(), res.err.what());
				updateInfo.Update(ann->Name(), lastUpdateTime_);  // Still performing time update
				continue;
			}

			const std::string indexName = ann->Name();
			const auto nsName = ns_.name_;

			// !!! Unlockig namespace mutex to perform WriteSync without any locks
			lock.unlock();

			const auto t1 = system_clock_w::now_coarse();
			storage.WriteSync(StorageOpts(), GetStorageKey(ann->Name()), ser.Slice());
			const auto t2 = system_clock_w::now_coarse();
			logFmt(LogInfo, "[{}] Storage cache for ANN index was updated: size: {} KB; creation time: {} ms; writing time: {} ms", nsName,
				   ser.Len() / 1024, std::chrono::duration_cast<milliseconds>((t1 - t0)).count(),
				   std::chrono::duration_cast<milliseconds>((t2 - t1)).count());

			updateInfo.Update(indexName, lastUpdateTime_);
			return true;
		}
	}
	return false;
}

std::pair<int, const Index*> Writer::getPKField() const noexcept {
	for (unsigned i = 0, size = ns_.indexes_.size(); i < size; ++i) {
		auto idx = ns_.indexes_[i].get();
		const auto& opts = idx->Opts();
		if (opts.IsPK() && !opts.IsSparse() && !opts.IsArray()) {
			// Array or Sparse PK is not expected in this writer (currently rx does not support such PKs either)
			const Index* pkIndex = ns_.indexes_[i].get();
			const FieldsSet& pkFields = pkIndex->Fields();
			for (const int f : pkFields) {
				if (f < 0) {
					logFmt(
						LogWarning,
						"[{}] Unable to create storage cache for ANN indexes: PK index has unexpected structure (this is probably a bug)",
						ns_.name_);
					return std::make_pair(-1, nullptr);
				}
			}
			return std::make_pair(int(i), pkIndex);
		}
	}
	return std::make_pair(-1, nullptr);
}

Writer::StorageCacheWriteResult Writer::writeSingleIndexCache(FloatVectorIndex& ann, WrSerializer& ser, const std::atomic_int32_t& cancel) {
	auto res = StorageCacheWriteResult{.err = {}, .isCacheable = true};
	const auto pk = getPKField();
	const auto pkField = pk.first;	// Unable to capture structure binding in C++17, so using explicit pair
	const auto pkIndex = pk.second;
	if (pkField < 0 || !pkIndex) {
		res.err = Error(errLogic, "PK index does not exist");
		return res;
	}
	const auto& pkFields = pkIndex->Fields();

	auto getPKSingle = [this, pkField](IdType id) {
		VariantArray ret;
		if (size_t(id) >= ns_.items_.size() || ns_.items_[id].IsFree()) [[unlikely]] {
			throw Error(errLogic, "Item ID {} does not exist", id);
		}
		ConstPayload pl(ns_.payloadType_, ns_.items_[id]);
		pl.Get(pkField, ret);
		return ret;
	};
	auto getPKComposite = [this, &pkFields](IdType id) {
		VariantArray ret, tmp;
		if (size_t(id) >= ns_.items_.size() || ns_.items_[id].IsFree()) [[unlikely]] {
			throw Error(errLogic, "Item ID {} does not exist", id);
		}
		ConstPayload pl(ns_.payloadType_, ns_.items_[id]);
		for (const int f : pkFields) {
			pl.Get(f, tmp);
			ret.emplace_back(tmp[0]);
		}
		return ret;
	};

	WrSerializer indexDefJson;
	ser.PutUInt8(kANNCacheFormatVersion);
	ser.PutUInt64(lastUpdateTime_.count());
	IndexDef indexDef = ns_.getIndexDefinition(pkField);
	indexDef.GetJSON(indexDefJson);
	ser.PutSlice(indexDefJson.Slice());
	indexDefJson.Reset();
	indexDef = ns_.getIndexDefinition(curIndex_);
	indexDef.GetJSON(indexDefJson);
	ser.PutSlice(indexDefJson.Slice());

	const auto hdrLen = ser.Len();
	const bool isCompositePK = IsComposite(pkIndex->Type());
	auto idxRes = isCompositePK ? ann.WriteIndexCache(ser, getPKComposite, isCompositePK, cancel)
								: ann.WriteIndexCache(ser, getPKSingle, isCompositePK, cancel);
	res.isCacheable = idxRes.isCacheable;
	res.err = std::move(idxRes.err);
	if (res.err.ok() && res.isCacheable && ser.Len() == hdrLen) {
		res.err = Error(errLogic, "Empty ANN cache");
	}
	return res;
}

constexpr size_t kHdrSize = sizeof(uint64_t) + sizeof(uint8_t);

Reader::Reader(std::string_view nsName, nanoseconds lastUpdate, unsigned int pkField, AsyncStorage& storage,
			   std::function<int(std::string_view)>&& nameToField, const std::function<IndexDef(size_t)>& getIndexDefinitionF)
	: storage_{storage}, nsName_{nsName} {
	assertrx(nameToField);
	if (!storage_.IsValid()) {
		assertrx_dbg(false);  // Do not expecting this error in test scenarios
		throw Error(errParams, "Storage for '{}' is not valid. Unable to read ANN cache");
	}

	if (!IsANNCacheEnabledByEnv()) {
		logFmt(LogInfo, "[{}] Skipping ANN cache check: disabled by env", nsName_);
		return;
	}

	std::vector<std::string> keysToRemove;
	StorageOpts opts;
	{  // dbIter must be destroyed after this scope
		auto dbIter = storage_.GetCursor(opts);
		for (dbIter->Seek(kStorageANNCachePrefix);
			 dbIter->Valid() &&
			 dbIter->GetComparator().Compare(dbIter->Key(), std::string_view(kStorageANNCachePrefix "\xFF\xFF\xFF\xFF")) < 0;
			 dbIter->Next()) {
			std::string_view keySlice = dbIter->Key();
			auto dotPos = keySlice.find_first_of('.');
			if (dotPos == std::string_view::npos) {
				assertrx_dbg(false);  // Do not expecting this error in test scenarios
				logFmt(LogInfo, "[{}] Skipping ANN cache entry ({}): incorrect key format", nsName_, keySlice);
				keysToRemove.emplace_back(keySlice);
				continue;
			}
			std::string_view indexName = keySlice.substr(dotPos + 1);
			if (indexName.empty()) {
				assertrx_dbg(false);  // Do not expecting this error in test scenarios
				logFmt(LogInfo, "[{}] Skipping ANN cache entry ({}): empty index name", nsName_, keySlice);
				keysToRemove.emplace_back(keySlice);
				continue;
			}
			std::string_view dataSlice = dbIter->Value();
			if (dataSlice.size() < kHdrSize) {
				assertrx_dbg(false);  // Do not expecting this error in test scenarios
				logFmt(LogInfo, "[{}] Skipping ANN cache entry ({}): no version or last update time data", nsName_, keySlice);
				keysToRemove.emplace_back(keySlice);
				continue;
			}
			Serializer ser(dataSlice);
			const auto version = ser.GetUInt8();
			if (version != kANNCacheFormatVersion) {
				assertrx_dbg(false);  // Do not expecting this error in test scenarios
				logFmt(LogInfo, "[{}] Skipping ANN cache entry ({}): unsupported format version {}", nsName_, keySlice, version);
				keysToRemove.emplace_back(keySlice);
				continue;
			}
			const auto cachedLastUpdate = nanoseconds(ser.GetUInt64());
			if (cachedLastUpdate != lastUpdate) {
				assertrx_dbg(false);  // Do not expecting this error in test scenarios
				logFmt(LogInfo, "[{}] Skipping ANN cache entry ({}): outdated: {{required_last_update: {}, found_last_update: {}}}",
					   nsName_, keySlice, lastUpdate.count(), cachedLastUpdate.count());
				keysToRemove.emplace_back(keySlice);
				continue;
			}
			assertrx(kHdrSize == ser.Pos());

			const int field = nameToField(indexName);
			if (field >= kMaxIndexes || field < 0) {
				assertrx_dbg(false);  // Do not expecting this error in test scenarios
				logFmt(LogInfo, "[{}] Skipping ANN cache entry ({}): unable to map index name to index field number (got {})", nsName_,
					   keySlice, field);
				keysToRemove.emplace_back(keySlice);
				continue;
			}

			// Check PK
			if (!checkIndexDef(ser, pkField, keySlice, "PK", getIndexDefinitionF)) {
				assertrx_dbg(false);  // Do not expecting this error in test scenarios
				keysToRemove.emplace_back(keySlice);
				continue;
			}
			// Check ANN
			if (!checkIndexDef(ser, unsigned(field), keySlice, "ANN", getIndexDefinitionF)) {
				assertrx_dbg(false);  // Do not expecting this error in test scenarios
				keysToRemove.emplace_back(keySlice);
				continue;
			}

			cachedMeta_.emplace_back(CachedIndexMeta{.field = size_t(field), .name = std::string(indexName)});
			cachedIndexes_.set(field);
		}
	}

	for (auto& key : keysToRemove) {
		storage_.RemoveSync(opts, key);
	}
}

std::optional<Reader::CachedIndex> Reader::GetNextCachedIndex() {
	std::optional<CachedIndex> ret;
	CachedIndexMeta meta;

	{
		lock_guard lck(mtx_);
		if (cachedMeta_.empty()) {
			return ret;
		}
		meta = std::move(cachedMeta_.back());
		cachedMeta_.pop_back();
	}

	StorageOpts opts;
	auto dbIter = storage_.GetCursor(opts);
	const auto key = GetStorageKey(meta.name);
	dbIter->Seek(key);
	if (!dbIter->Valid()) {
		logFmt(LogWarning, "[{}] Unable to load ANN cache for key '{}'. Expected this cache entry being valid", nsName_,
			   GetStorageKey(meta.name));
		storage_.RemoveSync(opts, key);
		return ret;
	}
	std::string_view dataSlice = dbIter->Value();
	if (dataSlice.size() < kHdrSize) {
		logFmt(LogWarning, "[{}] Unable to load ANN cache for key '{}': data underflow. Expected this cache entry being valid", nsName_,
			   GetStorageKey(meta.name));
		storage_.RemoveSync(opts, key);
		return ret;
	}

	Serializer ser(dataSlice);
	[[maybe_unused]] uint8_t version = ser.GetUInt8();
	const std::chrono::nanoseconds lastUpdate{ser.GetUInt64()};
	[[maybe_unused]] std::string_view pkIndexDef = ser.GetSlice();
	[[maybe_unused]] std::string_view annIndexDef = ser.GetSlice();

	ret.emplace();
	ret->name = std::move(meta.name);
	ret->field = meta.field;
	ret->lastUpdate = lastUpdate;
	ret->data = std::string(dataSlice.substr(ser.Pos()));
	return ret;
}

bool Reader::checkIndexDef(Serializer& ser, unsigned int field, std::string_view keySlice, std::string_view indexType,
						   const std::function<IndexDef(size_t)>& getIndexDefinitionF) {
	assertrx(getIndexDefinitionF);
	std::string_view indexDefJsonCached = ser.GetSlice();
	const auto cachedIndexDef = IndexDef::FromJSON(indexDefJsonCached);
	if (!cachedIndexDef) {
		logFmt(LogInfo, "[{}] Skipping ANN cache entry ({}): unable to parse cached {} index definition: {}", nsName_, keySlice, indexType,
			   cachedIndexDef.error().what());
		return false;
	}
	const IndexDef indexDef = getIndexDefinitionF(field);
	if (!indexDef.Compare(*cachedIndexDef).Equal()) {
		WrSerializer indexDefJson;
		indexDef.GetJSON(indexDefJson);
		logFmt(LogInfo, "[{}] Skipping ANN cache entry ({}): different {} index definitions:\ncached:{}\nvs\ncurrent:{}", nsName_, keySlice,
			   indexType, indexDefJsonCached, indexDefJson.Slice());
		return false;
	}
	return true;
}

}  // namespace reindexer::ann_storage_cache
