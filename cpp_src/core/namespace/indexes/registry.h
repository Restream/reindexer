#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "estl/fast_hash_map.h"
#include "index_names.h"
#include "indexes_storage.h"
#include "tools/stringstools.h"

namespace reindexer {
class Index;
class PayloadFieldType;
}  // namespace reindexer

namespace reindexer::ns_indexes {

/**
 * @brief Owns the indexes, their names, sparse/float-vector positions and the field->composite mapping - together
 * with the payload type and tags matcher (Metadata) this fully describes the namespace fields.
 *
 * Only constant access is exposed here. TransactionDDL handles adding, dropping and replacing fields, updating
 * everything at once; the methods below only cover changes that keep the fields layout intact.
 */
class [[nodiscard]] Registry {
public:
	using NamesMap = fast_hash_map<std::string, int, nocase_hash_str, nocase_equal_str, nocase_less_str>;
	using CompositesMap = fast_hash_map<int, std::vector<int>>;

	Registry(const std::string& nsName, int32_t tmStateToken);
	/** @brief Clones src, including its metadata. */
	Registry(const Registry& src, size_t newItemsCapacity);
	~Registry();

	Registry(Registry&&) = delete;
	Registry& operator=(const Registry&) = delete;
	Registry& operator=(Registry&&) = delete;

	const IndexesStorage& Indexes() const& noexcept { return indexes_; }
	const CompositesMap& Composites() const& noexcept { return composites_; }
	/**
	 * @brief Looks up an index by name, or by the '#pk' PK alias.
	 * @param name - index name.
	 * @param pos - set to the index position in Indexes() if found.
	 * @return false if there is no such name.
	 */
	bool TryGetIndexPos(std::string_view name, int& pos) const noexcept {
		const auto it = names_.find(name);
		if (it == names_.end()) {
			return false;
		}
		pos = it->second;
		return true;
	}
	/** @brief Ascending positions of the float vector indexes in Indexes(). */
	const std::vector<size_t>& FloatVectorsPositions() const& noexcept { return floatVectorsPositions_; }
	bool HasFloatVectorIndexes() const noexcept { return !floatVectorsPositions_.empty(); }
	const PayloadType& GetPayloadType() const& noexcept { return md_.GetPayloadType(); }
	const TagsMatcher& GetTagsMatcher() const& noexcept { return md_.GetTagsMatcher(); }
	/**
	 * @brief Non-constant tags matcher access, for registering JSON-paths of incoming items and for deserializing on
	 * storage load. Cannot change the fields layout - those TagsMatcher methods are only available inside this
	 * module.
	 */
	TagsMatcher& GetTagsMatcher() & noexcept { return md_.tagsMatcher_; }

	auto Indexes() const&& = delete;
	auto Composites() const&& = delete;
	auto FloatVectorsPositions() const&& = delete;
	auto GetPayloadType() const&& = delete;
	auto GetTagsMatcher() const&& = delete;

	/**
	 * @brief Replaces the index at pos with an equivalent one - recreated with a different internal structure,
	 * reloaded from storage, or of another concrete type after a fast index update (see IndexFastUpdate::Try). The
	 * new index must keep the same composite/sparse/float-vector kind, so the layout and metadata stay intact; the
	 * concrete index type itself is free to change.
	 * @param pos - position of the index to replace.
	 * @param newIndex - the replacement.
	 * @return the previous index object.
	 */
	std::unique_ptr<Index> ReplaceIndex(int pos, std::unique_ptr<Index>&& newIndex);
	/**
	 * @brief Replaces the payload field type of the index at pos. The new type must describe the same field at the
	 * same offset - only auxiliary options (e.g. the embedders config) may differ.
	 * @param pos - index position.
	 * @param fieldType - the replacement field type.
	 */
	void ReplaceFieldType(int pos, PayloadFieldType&& fieldType);
	/** @brief Propagates the current payload type into all of the indexes. */
	void PropagatePayloadType();
	/**
	 * @brief Renames the payload type after the namespace rename.
	 * @param name - the new namespace name.
	 */
	void RenamePayloadType(std::string_view name);
	/**
	 * @brief Replaces the whole tags matcher (replication only), keeping its payload type in sync.
	 * @param tm - the new tags matcher.
	 */
	void ReplaceTagsMatcher(TagsMatcher&& tm);
	/** @brief Recalculates the 'index field -> composite indexes' mapping. Must stay noexcept to keep the indexes
	 * consistent. */
	void RebuildCompositesMapping() noexcept;
	void ResetCompositesMapping() noexcept { composites_.clear(); }

	/** @brief Releases the indexes without destroying them (namespace leak mode). The registry must not be used
	 * afterwards. */
	void LeakIndexes() noexcept;
	/**
	 * @brief Destroys a single index (multithreaded namespace destruction). The registry must not be used
	 * afterwards.
	 * @param pos - index position.
	 */
	void DestroyIndex(size_t pos) noexcept;

	enum class [[nodiscard]] ConsistencyState {
		BeforeModification,
		AfterModification,
	};

	/**
	 * @brief Validates mutual consistency of the registry content and the metadata. An empty registry (before the
	 * very first AddIndex(), from the NamespaceImpl constructor) is trivially consistent.
	 */
	template <ConsistencyState consistencyState>
	void CheckConsistency() const noexcept(consistencyState == ConsistencyState::AfterModification);

private:
	friend class TransactionDDL;
	friend class TargetState;

	std::string_view nsName() const noexcept { return md_.GetPayloadType().Name(); }

	Metadata md_;
	IndexesStorage indexes_;
	NamesMap names_;
	/// Maps index fields to the corresponding composite indexes
	CompositesMap composites_;
	std::vector<size_t> floatVectorsPositions_;
};

}  // namespace reindexer::ns_indexes
