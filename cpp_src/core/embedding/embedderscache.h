#pragma once

#include <vector>
#include "core/dbconfig.h"
#include "core/embedding/embeddingconfig.h"
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
using ValueT = ConstFloatVector;
using ValuesT = h_vector<ValueT, 1>;
using StrorageKeyT = std::string;
using BaseKeyT = std::string;

class [[nodiscard]] Adapter final {
public:
	static Error VectorFromJSON(const StrorageKeyT& json, ValuesT& result) noexcept;

	explicit Adapter(const BaseKeyT& source);
	explicit Adapter(std::span<const std::vector<std::pair<std::string, VariantArray>>> sources);
	const StrorageKeyT& View() const& noexcept { return view_; }
	auto View() const&& = delete;
	chunk Content() const;

private:
	static void vectorFromJSON(const gason::JsonNode& root, ValuesT& result);

	StrorageKeyT view_;
};
}  // namespace embedding

class [[nodiscard]] EmbeddersCache final {
public:
	static bool IsEmbedderSystemName(std::string_view nsName) noexcept;

	EmbeddersCache() noexcept = default;
	EmbeddersCache(EmbeddersCache&&) noexcept = delete;
	EmbeddersCache(const EmbeddersCache&) noexcept = delete;
	EmbeddersCache& operator=(const EmbeddersCache&) noexcept = delete;
	EmbeddersCache& operator=(EmbeddersCache&&) noexcept = delete;
	~EmbeddersCache();

	Error UpdateConfig(fast_hash_map<std::string, EmbedderConfigData, hash_str, equal_str, less_str> config);
	Error EnableStorage(const std::string& storagePathRoot, datastorage::StorageType type);

	void IncludeTag(std::string_view tag);
	std::optional<embedding::ValueT> Get(const CacheTag& tag, const embedding::Adapter& srcAdapter);
	void Put(const CacheTag& tag, const embedding::Adapter& srcAdapter, const embedding::ValuesT& values);

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
