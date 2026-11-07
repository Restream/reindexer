#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include "core/enums.h"
#include "estl/h_vector.h"

namespace reindexer {
class EmbeddersCache;
class Index;
class IndexDef;
class NamespaceName;
class TagsMatcher;
struct NamespaceCacheConfigData;
}  // namespace reindexer

namespace reindexer::ns_indexes {

class Registry;

/**
 * @brief Short-lived validator using the registry's public metadata and index access.
 *
 * Borrows the registry; the caller must keep it stable for the duration of validation, normally under the
 * namespace lock. Dependencies needed only to probe an index update are supplied through UpdateContext.
 */
class [[nodiscard]] Validator {
public:
	/**
	 * @brief Borrowed settings and current item count for probing index construction and field conversion.
	 *
	 * Created by NamespaceImpl::indexUpdateContext(). The namespace must remain alive and locked while the
	 * context is in use.
	 */
	struct [[nodiscard]] UpdateContext {
		size_t itemsCount;
		const NamespaceCacheConfigData& cacheConfig;
		const std::shared_ptr<EmbeddersCache>& embeddersCache;
		const NamespaceName& nsName;
		bool enablePerfCounters;
	};

	explicit Validator(const Registry& registry) noexcept : registry_{registry} {}

	/**
	 * @brief Checks whether an existing index can be reused for the requested definition.
	 * @param indexDef - requested index definition.
	 * @param requireTtlUpdate - set to RequireTtlUpdate_True if both indexes are TTL indexes with different
	 * expiration values; otherwise left unchanged. Expiration differences are ignored when checking compatibility.
	 * @return true if a compatible index exists, false if the index name is not found.
	 * @throws Error if the existing definition cannot be retrieved or its settings are incompatible.
	 */
	bool CheckIfSameIndexExists(const IndexDef& indexDef, RequireTtlUpdate& requireTtlUpdate) const;
	/**
	 * @brief Checks whether an index change affects the primary key.
	 * @param indexDef - requested index definition.
	 * @return true if the requested definition or the existing index with this name is a PK index.
	 */
	bool IsPkAffectingIndexChange(const IndexDef& indexDef) const noexcept;

	/** @brief Verifies that the index can be dropped and returns its position in the registry. */
	int VerifyDropIndex(const IndexDef& indexDef, size_t itemsCount) const;
	void VerifyCompositeIndex(const IndexDef& indexDef) const;
	void VerifyUpsertIndex(std::string_view action, const IndexDef& indexDef, size_t itemsCount) const;
	void VerifyEmbeddingFields(const h_vector<std::string, 1>& fields, std::string_view fieldName, std::string_view action) const;
	void VerifyUpsertEmbedder(std::string_view action, const IndexDef& indexDef) const;
	void VerifyUpsertQuantizationConfigHNSWIndex(std::string_view action, const IndexDef& indexDef) const;
	void VerifyUpdateQuantizationConfigHNSWIndex(const Index& curIndex, const IndexDef& newIndexDef, size_t itemsCount) const;

	/**
	 * @brief Probes index construction and field conversion using temporary metadata.
	 * @param indexDef - requested index definition.
	 * @param tm - a copy of the tags matcher, never the live namespace tags matcher: registering JSON-paths is
	 * irreversible, so failed verification must not leave tags in the live metadata.
	 * @param ctx - current item count and settings for constructing temporary indexes and payload fields.
	 *
	 * The registry stays unchanged. Constructing embedding fields may register JSON-paths in EmbeddersCache;
	 * these cache registrations are not rolled back on failure.
	 */
	void VerifyUpdateIndex(const IndexDef& indexDef, TagsMatcher& tm, const UpdateContext& ctx) const;

private:
	std::string_view nsName() const noexcept;

	const Registry& registry_;
};

}  // namespace reindexer::ns_indexes
