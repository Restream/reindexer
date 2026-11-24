#pragma once

#include "core/enums.h"
#include "tools/errors.h"

namespace reindexer {

class FieldsSet;
class NamespaceImpl;
class WrSerializer;
struct FloatVectorsIndexes;

namespace migrations {

/**
 * @brief Migrating NS items while altering PKs.
 *
 * Provides its own crash-recovery mechanism:
 *  - the incomplete marker is written with Sync before any item rewrite; a failure aborts the migration;
 *  - rewrites and removals are flushed before the completed marker is synchronously committed. Errors leave
 *    the migration incomplete, so NamespaceImpl::LoadFromStorage() can retry the recovery;
 *  - the cleanup only removes a stale key once its replacement under the new PK is confirmed present in the storage -
 *    a failed write of the new canonical key never takes the matching old key down with it too;
 *  - while the migration is marked incomplete, every further PK-affecting Add/Update/Drop is rejected (see
 *    HasIncompleteMigration(), which checks storage and pending write failures) until recovery completes.
 */
class [[nodiscard]] PKMigrationService {
public:
	explicit PKMigrationService(NamespaceImpl& nsImpl) : nsImpl_{nsImpl} {}
	~PKMigrationService() = default;

	/**
	 * @param pk - new PK fields.
	 * @return true, if the migration completed (status ended up 'True'); false otherwise - see the class comment.
	 */
	bool MigrateToNewPK(const FieldsSet& pk) noexcept;

	/**
	 * @brief Finishes an incomplete MigrateToNewPK() from a previous run. Safe to call unconditionally, including when
	 * no PK migration ever ran for this namespace.
	 */
	void RemoveItemsWithObsoletePK() noexcept;

	/**
	 * @brief True if the migration is incomplete or its status cannot be read. A failed pending durable write is
	 * treated as an error, never as a successfully persisted status.
	 */
	bool HasIncompleteMigration() noexcept;

private:
	/**
	 * (Re)writes one item into the storage under its new PK-derived key. Does not touch the old key - see
	 * removeObsoletePkRecords(). A free item (a deleted row's slot) is a trivial success.
	 * @return true, if no errors occurred.
	 */
	bool migrateItem(size_t rowId, const FieldsSet& newPk, WrSerializer& pkBuf, WrSerializer& itemBuf) noexcept;

	void writeStatus(MigrationStatus status);

	/**
	 * Iterates over every storage item, decoding only its 'pk' fields (see ItemImpl::FromCJSON()'s pkOnly).
	 * @param onReadItem - callback for every iterated item.
	 * @return true, if the whole storage was scanned successfully; false, if the scan was aborted by an exception, in
	 * which case the caller must not treat the scan as having completed.
	 */
	template <typename Fn>
	bool iterateOverStorageItems(const FieldsSet& pk, Fn&& onReadItem) noexcept;

	/**
	 * Removes every storage record whose key doesn't match the one its own payload would produce under 'pk' - but only
	 * once a direct storage lookup confirms the expected replacement actually exists.
	 * @return true, if the storage was fully scanned and every confirmed-obsolete record was removed.
	 */
	bool removeObsoletePkRecords(const FieldsSet& pk);

	/**
	 * @return reading error status. errNotFound means no PK migration has ever run for this namespace - that is not an
	 * error condition by itself.
	 */
	reindexer::Error readStatus(MigrationStatus& status) noexcept;

	NamespaceImpl& nsImpl_;
};

}  // namespace migrations
}  // namespace reindexer
