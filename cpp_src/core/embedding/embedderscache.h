#pragma once

#include <shared_mutex>
#include <vector>
#include "core/dbconfig.h"
#include "core/embedding/embeddingconfig.h"
#include "core/keyvalue/float_vector.h"
#include "core/namespace/namespacestat.h"
#include "core/storage/idatastorage.h"
#include "core/storage/storagetype.h"
#include "estl/fast_hash_map.h"
#include "estl/shared_mutex.h"
#include "tools/errors.h"

namespace reindexer {

class EmbeddersLRUCache;

namespace embedding {
using ValueT = ConstFloatVector;
using ValuesT = h_vector<ValueT, 1>;
using StrorageKeyT = std::string;
using BaseKeyT = std::string;
using KeyT = std::vector<std::vector<BaseKeyT>>;

class [[nodiscard]] Adapter final {
public:
	static Error VectorFromJSON(const StrorageKeyT& json, ValuesT& result) noexcept;
	static Error KeyFromJSON(const StrorageKeyT& json, KeyT& result) noexcept;
	static StrorageKeyT ConvertKeyToView(const KeyT& key);

	Adapter(std::span<const std::vector<VariantArray>> sources);
	[[nodiscard]] const KeyT& Key() const& noexcept;
	auto Key() const&& = delete;
	[[nodiscard]] const StrorageKeyT& View() const& noexcept { return view_; }
	auto View() const&& = delete;

private:
	static void vectorFromJSON(const gason::JsonNode& root, ValuesT& result);
	static void keyFromJSON(const gason::JsonNode& root, KeyT& result);

	std::vector<KeyT> sources_;
	StrorageKeyT view_;
};
}  // namespace embedding

class [[nodiscard]] EmbeddersCache final {
public:
	[[nodiscard]] static bool IsEmbedderSystemName(std::string_view nsName) noexcept;

	EmbeddersCache() noexcept = default;
	EmbeddersCache(EmbeddersCache&&) noexcept = delete;
	EmbeddersCache(const EmbeddersCache&) noexcept = delete;
	EmbeddersCache& operator=(const EmbeddersCache&) noexcept = delete;
	EmbeddersCache& operator=(EmbeddersCache&&) noexcept = delete;
	~EmbeddersCache();

	Error UpdateConfig(fast_hash_map<std::string, EmbedderConfigData, hash_str, equal_str, less_str> config);
	Error EnableStorage(const std::string& storagePathRoot, datastorage::StorageType type);

	void IncludeTag(std::string_view tag);
	[[nodiscard]] std::optional<embedding::ValueT> Get(const CacheTag& tag, const embedding::Adapter& srcAdapter);
	void Put(const CacheTag& tag, const embedding::Adapter& srcAdapter, const embedding::ValuesT& values);

	[[nodiscard]] bool IsActive() const noexcept;
	[[nodiscard]] NamespaceMemStat GetMemStat() const;
	[[nodiscard]] EmbedderCachePerfStat GetPerfStat(std::string_view tag) const;

	void ResetPerfStat() noexcept;

	void Clear(std::string_view tag);

	template <typename T>
	void Dump(T& os, std::string_view step, std::string_view offset) const;

private:
	[[nodiscard]] bool getEmbeddersConfig(std::string_view tag, EmbedderConfigData& data);
	void includeTag(const CacheTag& tag);

	std::optional<fast_hash_map<std::string, EmbedderConfigData, hash_str, equal_str, less_str>> config_;
	std::string storagePath_;
	datastorage::StorageType type_{datastorage::StorageType::LevelDB};
	fast_hash_map<CacheTag, std::shared_ptr<EmbeddersLRUCache>, CacheTagHash, CacheTagEqual, CacheTagLess> caches_;
	mutable std::shared_mutex mtx_;
};

}  // namespace reindexer
