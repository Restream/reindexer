#pragma once

#include <string>
#include <string_view>
#include "core/cjson/tagspath.h"
#include "core/key_value_type.h"

namespace reindexer {

class FieldsSet;
class Index;
class NamespaceImpl;
class PayloadType;
namespace ns_indexes {
class Registry;
}

/**
 * @brief Borrows the namespace field metadata without exposing index mutation or namespace operations.
 * @pre The namespace must outlive the view. The caller must hold the namespace lock while using the view and
 * references obtained from it; those references must not be retained across changes to the field layout.
 */
class [[nodiscard]] NamespaceMetadataView {
public:
	/** @brief Index properties borrowed from the same locked namespace as the metadata view. */
	class [[nodiscard]] IndexMetadata {
	public:
		const std::string& Name() const& noexcept { return name_; }
		const FieldsSet& Fields() const& noexcept { return fields_; }
		IndexType Type() const noexcept { return type_; }
		KeyValueType KeyType() const noexcept { return keyType_; }
		KeyValueType SelectKeyType() const noexcept { return selectKeyType_; }

		auto Name() const&& = delete;
		auto Fields() const&& = delete;

	private:
		friend class NamespaceMetadataView;

		explicit IndexMetadata(const Index&) noexcept;

		const std::string& name_;
		const FieldsSet& fields_;
		IndexType type_;
		KeyValueType keyType_;
		KeyValueType selectKeyType_;
	};

	/** @brief Half-open range of composite index positions. */
	struct [[nodiscard]] CompositeIndexesRange {
		int begin;
		int end;
	};

	/**
	 * @brief Looks up a case-insensitive index name or '#pk', then falls back to a JSON-path lookup.
	 * @param pos - set to the index position if found; unchanged otherwise.
	 * @return false if neither lookup matches; the JSON-path lookup excludes indexes with multiple paths.
	 */
	bool TryGetIndexPosByNameOrJsonPath(std::string_view name, int& pos) const;
	/**
	 * @brief Looks up a regular or sparse index by JSON-path, excluding indexes with multiple JSON-paths.
	 * @param pos - set to the index position if found; unchanged otherwise.
	 * @return false if no index matches. Index names and the '#pk' alias are not resolved.
	 */
	bool TryGetIndexPosByJsonPath(std::string_view jsonPath, int& pos) const noexcept;
	/** @brief Reports whether the index at pos is a float vector index. @pre pos is a valid index position. */
	bool IsFloatVectorIndex(int pos) const noexcept;
	/** @brief Reports whether the index at pos is a fulltext index. @pre pos is a valid index position. */
	bool IsFulltextIndex(int pos) const noexcept;
	/** @brief Borrows the properties of the index at pos. @pre pos is a valid index position. */
	IndexMetadata GetIndexMetadata(int pos) const& noexcept;
	/** @brief Returns the positions occupied by composite indexes in the current field layout. */
	CompositeIndexesRange GetCompositeIndexesRange() const noexcept;
	/** @brief Returns the fields of the index at pos. @pre pos is a valid index position. */
	const FieldsSet& GetIndexFields(int pos) const& noexcept;
	/** @brief Returns the current payload layout. */
	const PayloadType& GetPayloadType() const& noexcept;
	/** @brief Resolves a JSON-path using existing tags only; returns an empty path if a tag is unknown. */
	TagsPath GetTagsPath(std::string_view jsonPath) const;

	auto GetIndexMetadata(int pos) const&& = delete;
	auto GetIndexFields(int pos) const&& = delete;
	auto GetPayloadType() const&& = delete;

private:
	friend class NamespaceImpl;

	explicit NamespaceMetadataView(const ns_indexes::Registry& registry) noexcept : registry_{&registry} {}

	const ns_indexes::Registry* registry_;
};

}  // namespace reindexer
