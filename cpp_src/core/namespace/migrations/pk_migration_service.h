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
 *  - new canonical records are written synchronously, so a crash right after the call returns cannot lose the item
 *    outright - except for the known residual case, where the storage already had a pending flush error and the
 *    synchronous write silently degraded into an async one;
 *  - the migration status only turns into 'completed' once every write and the following cleanup scan have actually
 *    succeeded; a crash or a failure anywhere along the way leaves it marked as incomplete, so the next
 *    NamespaceImpl::LoadFromStorage() retries the recovery;
 *  - the cleanup only removes a stale key once its replacement under the new PK is confirmed present in the storage -
 *    a failed write of the new canonical key never takes the matching old key down with it too;
 *  - while the migration is marked incomplete, every further PK-affecting Add/Update/Drop is rejected (see
 *    HasIncompleteMigration() - a direct storage read every time, nothing is cached) until a restart completes the
 *    recovery.
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
	 * @brief True, if the persisted migration status for this namespace is not 'completed'. Always a direct storage
	 * read - see the class comment for why nothing is cached.
	 */
	bool HasIncompleteMigration() noexcept;

private:
	/**
	 * (Re)writes one item into the storage under its new PK-derived key. Does not touch the old key - see
	 * removeObsoletePkRecords(). A free item (a deleted row's slot) is a trivial success.
	 * @return true, if no errors occurred.
	 */
	bool migrateItem(size_t rowId, const FieldsSet& newPk, WrSerializer& pkBuf, WrSerializer& itemBuf) noexcept;

	void writeStatus(MigrationStatus status) noexcept;

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
	uint64_t version_ = 0;
};

}  // namespace migrations
}  // namespace reindexer
