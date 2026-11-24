#pragma once

#include "core/namespace/namespace_metadata_view.h"

namespace reindexer {

class QueryField;

/** @brief Resolves query fields and verifies JOIN fields using the locked namespace metadata. */
class [[nodiscard]] QueryFieldResolver {
public:
	explicit QueryFieldResolver(NamespaceMetadataView metadata) noexcept : metadata_{metadata} {}

	/** @brief Resolves an index name or '#pk' before trying the field's JSON-path. */
	void Resolve(QueryField&) const;
	/** @brief Binds an already resolved index position. @pre pos is a valid index position. */
	void BindIndex(QueryField&, int pos) const;
	/** @brief Checks index kind and strict-mode restrictions for a resolved JOIN ON field. */
	void VerifyJoinField(const QueryField&, StrictMode strictMode) const;

private:
	NamespaceMetadataView metadata_;
};

}  // namespace reindexer
