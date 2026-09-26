#pragma once

#include <span>
#include <vector>
#include "core/dbconfig.h"
#include "core/embedding/embeddingconfig.h"
#include "core/embedding/protocol/iembed_protocol.h"
#include "core/keyvalue/float_vector.h"
#include "core/namespace/namespacestat.h"
#include "core/storage/storagetype.h"
#include "estl/fast_hash_map.h"
#include "estl/shared_mutex.h"
#include "tools/errors.h"

namespace reindexer {

class chunk;
class EmbeddersLRUCache;

namespace embedding {

using StrorageKeyT = std::string;
using BaseKeyT = std::string;

class [[nodiscard]] Adapter final {
public:
	static Error VectorsFromJSON(const StrorageKeyT& json, EmbedderConfig::Protocol protocol, ValueT& result) noexcept;

	explicit Adapter(const BaseKeyT& source, EmbedderConfig::Protocol protocol, std::string_view model);
	explicit Adapter(std::span<const DocSource> sources, EmbedderConfig::Protocol protocol, EmbedderConfig::FieldsFormat fieldsFormat,
					 std::string_view model);
	const StrorageKeyT& View() const& noexcept { return request_.view; }
	auto View() const&& = delete;
	const StrorageKeyT& CacheKey() const& noexcept { return request_.cacheKey.empty() ? request_.view : request_.cacheKey; }
	auto CacheKey() const&& = delete;
	chunk Content() const;

private:
	PreparedEmbedderRequest request_;
	EmbedderConfig::Protocol protocol_;
};

}  // namespace embedding

class [[nodiscard]] EmbeddersCache final {
public:
	static bool IsEmbedderSystemName(std::string_view nsName) noexcept;

	EmbeddersCache() = default;
	EmbeddersCache(EmbeddersCache&&) noexcept = delete;
	EmbeddersCache(const EmbeddersCache&) noexcept = delete;
	EmbeddersCache& operator=(const EmbeddersCache&) noexcept = delete;
	EmbeddersCache& operator=(EmbeddersCache&&) noexcept = delete;
	~EmbeddersCache();

	Error UpdateConfig(fast_hash_map<std::string, EmbedderConfigData, hash_str, equal_str, less_str> config);
	Error EnableStorage(const std::string& storagePathRoot, datastorage::StorageType type);

	void IncludeTag(std::string_view tag);
	std::optional<embedding::ValueT> Get(const CacheTag& tag, const embedding::Adapter& srcAdapter, bool enablePerfStat);
	void Put(const CacheTag& tag, const embedding::Adapter& srcAdapter, const embedding::ValueT& values);

	bool IsActive() const noexcept;
	NamespaceMemStat GetMemStat() const;
	EmbedderCachePerfStat GetPerfStat(std::string_view tag) const;

	void ResetPerfStat() noexcept;

	void Clear(std::string_view tag);

	template <typename T>
	void Dump(T& os, std::string_view step, std::string_view offset) const;

private:
	bool getEmbeddersConfig(std::string_view tag, EmbedderConfigData& data);
	void includeTag(const CacheTag& tag);

	std::optional<fast_hash_map<std::string, EmbedderConfigData, hash_str, equal_str, less_str>> config_;
	std::string storagePath_;
	datastorage::StorageType type_{datastorage::StorageType::LevelDB};
	fast_hash_map<CacheTag, std::shared_ptr<EmbeddersLRUCache>, CacheTagHash, CacheTagEqual, CacheTagLess> caches_;
	mutable shared_mutex mtx_;
};

}  // namespace reindexer
