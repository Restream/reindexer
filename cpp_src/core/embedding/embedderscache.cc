#include "embedderscache.h"

#include <optional>
#include "core/embedding/protocol/iembed_protocol.h"
#include "core/system_ns_names.h"
#include "core/type_consts.h"
#include "embedders_lru_cache.h"
#include "estl/chunk.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/stringstools.h"
#include "vendor/gason/gason.h"

namespace {

constexpr std::string_view kWildcard{"*"};

}  // namespace

namespace reindexer {
namespace embedding {

Error Adapter::VectorsFromJSON(const StrorageKeyT& json, EmbedderConfig::Protocol protocol, ValueT& result) noexcept {
	try {
		assertrx_dbg(!json.empty());

		result.resize(0);

		gason::JsonParser parser;
		auto root = parser.Parse(json);
		GetEmbedProtocol(protocol).ParseResponse(root, result);
	} catch (const std::exception& e) {
		return {errParseJson, "Embed source adapter can't parse vector '{}': {}", json, e.what()};
	} catch (...) {
		return {errParseJson, "Embed source adapter can't parse vector '{}'", json};
	}
	return {};
}

Adapter::Adapter(const BaseKeyT& source, EmbedderConfig::Protocol protocol, std::string_view model) : protocol_{protocol} {
	GetEmbedProtocol(protocol_).PrepareQuery(source, model, request_);
}

Adapter::Adapter(std::span<const DocSource> sources, EmbedderConfig::Protocol protocol, EmbedderConfig::FieldsFormat fieldsFormat,
				 std::string_view model)
	: protocol_{protocol} {
	GetEmbedProtocol(protocol_).PrepareUpsert(sources, fieldsFormat, model, request_);
}

chunk Adapter::Content() const { return GetEmbedProtocol(protocol_).BuildRequest(request_); }

}  // namespace embedding

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

std::optional<embedding::ValueT> EmbeddersCache::Get(const CacheTag& tag, const embedding::Adapter& srcAdapter, bool enablePerfStat) {
	if (tag.Tag().empty()) {
		return std::nullopt;  // NOTE: do nothing - valid situation
	}

	{
		shared_lock lk(mtx_);
		const auto it = caches_.find(tag, tag.Hash());
		if (it != caches_.end()) {
			return it->second->Get(srcAdapter, enablePerfStat);
		}
	}

	logFmt(LogWarning, "Get. Embedder cache tag '{}' not found", tag.Tag());
	return std::nullopt;
}

void EmbeddersCache::Put(const CacheTag& tag, const embedding::Adapter& srcAdapter, const embedding::ValueT& values) {
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
	stats.storageStatus = EmbeddersLRUCache::kStorageStatusOK;
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
