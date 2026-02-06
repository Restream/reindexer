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
 */
class [[nodiscard]] PKMigrationService {
public:
	explicit PKMigrationService(NamespaceImpl& nsImpl) : nsImpl_{nsImpl} {}
	~PKMigrationService() = default;

	/**
	 * @brief Migrating NS items from old to new PK.
	 * @param from - old PK fields.
	 * @param to - new PK fields.
	 */
	void MigrateFromOldToNewPK(const FieldsSet& from, const FieldsSet& to) noexcept;

	/**
	 * @brief Removing NS items with PK that is different from the actual one
	 * in case if the last call of 'MigrateFromOldToNewPK' failed.
	 */
	void RemoveItemsWithObsoletePK();

private:
	/**
	 * Migrate item with certain rowId.
	 * @param rowId - rowID of an item.
	 * @param oldPk - PK to migrate from.
	 * @param newPk - PK to migrate to.
	 * @param pkBuf - buffer for PK.
	 * @param itemBuf - buffer for item.
	 * @param vectorIndexes - const reference float vector indexes.
	 * @return true, if no errors occurred.
	 */
	bool migrateItem(size_t rowId, const FieldsSet& oldPk, const FieldsSet& newPk, WrSerializer& pkBuf, WrSerializer& itemBuf,
					 const FloatVectorsIndexes& vectorIndexes) noexcept;

	/**
	 * Save migration status to storage.
	 * @param status - migration status value.
	 */
	void writeStatus(MigrationStatus status) noexcept;

	/**
	 * Read migration status from storage.
	 * @param status - migration status.
	 * @return reading error status.
	 */
	reindexer::Error readStatus(MigrationStatus& status) noexcept;

	NamespaceImpl& nsImpl_;
	uint64_t version_ = 0;
};

}  // namespace migrations
}  // namespace reindexer
