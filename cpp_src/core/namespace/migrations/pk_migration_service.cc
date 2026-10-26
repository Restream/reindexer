#include "pk_migration_service.h"
#include "core/id_type.h"
#include "core/namespace/namespaceimpl.h"
#include "core/storage/storage_prefixes.h"
#include "tools/logger.h"
#include "tools/unaligned.h"

namespace reindexer {
namespace migrations {

namespace {
constexpr std::string_view kStoragePkMigrationStatusPrefix = "pk_migration_status";

void serializeItemPk(const ConstPayload& pl, const FieldsSet& pk, WrSerializer& buf) {
	buf.Reset();
	buf << kRxStorageItemPrefix;
	pl.SerializeFields(buf, pk);
}
}  // namespace

bool PKMigrationService::migrateItem(size_t id, const FieldsSet& newPk, WrSerializer& pkBuf, WrSerializer& itemBuf) noexcept {
	const auto rowId = IdType::FromNumber(id);
	if (nsImpl_.items_[rowId].IsFree()) {
		return true;
	}
	try {
		ItemImpl item{nsImpl_.payloadType(), nsImpl_.items_[rowId], nsImpl_.tagsMatcher()};
		item.Unsafe(true);
		Error err{nsImpl_.tryWriteItemIntoStorage(newPk, item, rowId, pkBuf, itemBuf)};
		if (!err.ok()) {
			logFmt(LogError, "Failed to migrate '{}' item with row_id={}: {}", nsImpl_.name_, id, err.what());
			return false;
		}
	} catch (const std::exception& ex) {
		logFmt(LogError, "Failed to migrate '{}' item with row_id={}: {}", nsImpl_.name_, id, ex.what());
		return false;
	}
	return true;
}

bool PKMigrationService::MigrateToNewPK(const FieldsSet& pk) noexcept {
	if (!nsImpl_.storage_.IsValid() || nsImpl_.items_.empty()) {
		return true;
	}
	MigrationStatus status{MigrationStatus_True};

	try {
		logFmt(LogInfo, "Migrating '{}' items to new PK.", nsImpl_.name_);

		writeStatus(MigrationStatus_False);

		// Flushing all the items waiting in queue.
		nsImpl_.storage_.Flush(StorageFlushOpts{});

		WrSerializer pkBuf, itemBuf;
		for (size_t id = 0; id < nsImpl_.items_.size(); ++id) {
			if (!migrateItem(id, pk, pkBuf, itemBuf)) {
				status = MigrationStatus_False;
			}
		}

		// No-op in the healthy case (WriteSync() never queues on success) - only matters if a write degraded to
		// async (AsyncStorage::modifySync()), which must be on disk before the cleanup below reads storage back
		nsImpl_.storage_.Flush(StorageFlushOpts{});

		logFmt(LogInfo, "Removing '{}' items with old PK.", nsImpl_.name_);

		if (!removeObsoletePkRecords(pk)) {
			status = MigrationStatus_False;
		}

		// Same reasoning as the flush above, but for RemoveSync(): it can degrade to async too
		nsImpl_.storage_.Flush(StorageFlushOpts{});
	} catch (const std::exception& ex) {
		logFmt(LogError, "Migrating '{}' to new PK failed: {}", nsImpl_.name_, ex.what());
		status = MigrationStatus_False;
	}

	writeStatus(status);

	logFmt(((status == MigrationStatus_False) ? LogError : LogInfo), "Migrating '{}' to new PK finished with status: {}", nsImpl_.name_,
		   static_cast<bool>(status));
	return status == MigrationStatus_True;
}

void PKMigrationService::RemoveItemsWithObsoletePK() noexcept {
	if (!nsImpl_.storage_.IsValid()) {
		return;
	}
	try {
		auto* pk{nsImpl_.pkFields()};
		if (!pk) {
			auto dbIter{nsImpl_.storage_.GetCursor(StorageOpts().FillCache(false))};
			dbIter->Seek(kRxStorageItemPrefix);
			if (const bool hasItems = dbIter->Valid() && checkIfStartsWith(kRxStorageItemPrefix, dbIter->Key()); hasItems) {
				logFmt(LogError, "Error removing items with obsolete PK for NS='{}': namespace contains items, but doesn't contain PK",
					   nsImpl_.name_);
			}
			return;
		}

		MigrationStatus status{MigrationStatus_False};
		reindexer::Error error{readStatus(status)};
		if (!error.ok()) {
			if (error.code() == errNotFound) {
				return;
			}
			// A genuine read error: we don't know whether the migration completed, so it stays marked incomplete
			// rather than being assumed done
			logFmt(LogError, "Failed to read PK migration status for '{}', leaving it marked as incomplete: {}", nsImpl_.name_,
				   error.what());
			return;
		}

		if (status == MigrationStatus_True) {
			return;
		}

		logFmt(LogTrace, "Removing '{}' items with obsolete PK.", nsImpl_.name_);
		if (removeObsoletePkRecords(*pk)) {
			// RemoveSync() can degrade to async on a storage error - confirm the removals are on disk before writing 'completed'.
			nsImpl_.storage_.Flush(StorageFlushOpts{});
			writeStatus(MigrationStatus_True);
			logFmt(LogInfo, "PK migration recovery for '{}' completed successfully", nsImpl_.name_);
		} else {
			// Incomplete scan or a removal failure: status stays 'incomplete' (already MigrationStatus_False on the
			// storage - nothing to write), so the next LoadFromStorage() retries
			logFmt(LogError, "PK migration recovery for '{}' did not complete successfully; will retry on the next load", nsImpl_.name_);
		}
	} catch (const std::exception& ex) {
		logFmt(LogError, "Unexpected error during PK migration recovery for '{}': {}", nsImpl_.name_, ex.what());
	}
}

bool PKMigrationService::HasIncompleteMigration() noexcept {
	if (!nsImpl_.storage_.IsValid()) {
		return false;
	}
	MigrationStatus status{MigrationStatus_False};
	reindexer::Error error{readStatus(status)};
	if (!error.ok()) {
		// errNotFound means no migration has ever run - that's not the same as incomplete. Any other error is
		// unknown, so it is conservatively treated as incomplete, same as in RemoveItemsWithObsoletePK()
		return error.code() != errNotFound;
	}
	return status == MigrationStatus_False;
}

bool PKMigrationService::removeObsoletePkRecords(const FieldsSet& pk) {
	struct [[nodiscard]] Candidate {
		std::string actualKey;
		std::string expectedKey;
	};

	std::vector<Candidate> candidates;
	WrSerializer buf;

	const bool fullyIterated = iterateOverStorageItems(pk, [&](const ItemImpl& item, AsyncStorage::Cursor& cursor, const StorageOpts&) {
		serializeItemPk(item.GetConstPayload(), pk, buf);
		if (buf.Slice() != cursor->Key()) {
			candidates.emplace_back(std::string(cursor->Key()), std::string(buf.Slice()));
		}
	});

	bool allRemoved = true;
	std::string throwaway;
	const StorageOpts opts = StorageOpts().FillCache(false);
	for (const auto& c : candidates) {
		if (!nsImpl_.storage_.Read(opts, c.expectedKey, throwaway).ok()) {
			logFmt(LogTrace,
				   "Keeping item ('{}') with an obsolete-looking key in '{}' storage as a recovery copy: its expected "
				   "replacement ('{}') was not found",
				   c.actualKey, nsImpl_.name_, c.expectedKey);
			continue;
		}
		try {
			nsImpl_.storage_.RemoveSync(opts, c.actualKey);
			logFmt(LogTrace, "Removing item ('{}') with obsolete key from '{}' storage", c.actualKey, nsImpl_.name_);
		} catch (const std::exception& err) {
			logFmt(LogError, "Error removing item = '{}' for '{}': {}", c.actualKey, nsImpl_.name_, err.what());
			allRemoved = false;
		}
	}
	return fullyIterated && allRemoved;
}

template <typename Fn>
bool PKMigrationService::iterateOverStorageItems(const FieldsSet& pk, Fn&& onReadItem) noexcept {
	if (!nsImpl_.storage_.IsValid()) {
		return false;
	}
	try {
		ItemImpl item{nsImpl_.payloadType(), nsImpl_.tagsMatcher(), pk};
		item.Unsafe(true);

		StorageOpts opts;
		opts.FillCache(false);

		auto dbIter{nsImpl_.storage_.GetCursor(opts)};
		for (dbIter->Seek(kRxStorageItemPrefix);
			 dbIter->Valid() &&
			 dbIter->GetComparator().Compare(dbIter->Key(), std::string_view(kRxStorageItemPrefix "\xFF\xFF\xFF\xFF")) < 0;
			 dbIter->Next()) {
			std::string_view dataSlice{dbIter->Value()};
			if (dataSlice.empty()) {
				continue;
			}
			if (dataSlice.size() < sizeof(int64_t)) {
				continue;
			}

			const int64_t lsn{unaligned::read<int64_t>(dataSlice.data())};
			if (lsn < 0) {
				continue;
			}

			try {
				dataSlice = dataSlice.substr(sizeof(lsn));
				item.FromCJSON(dataSlice, /*pkOnly*/ true);
			} catch (const Error&) {
				continue;
			}

			onReadItem(item, dbIter, opts);
		}
		return true;
	} catch (const std::exception& ex) {
		logFmt(LogError, "Error reading items from storage for '{}': {}", nsImpl_.name_, ex.what());
		return false;
	}
}

reindexer::Error PKMigrationService::readStatus(MigrationStatus& status) noexcept {
	try {
		std::string content;
		Error err{nsImpl_.loadLatestSysRecord(kStoragePkMigrationStatusPrefix, version_, content)};
		if (err.ok()) {
			Serializer ser{content.data(), content.size()};
			status = static_cast<MigrationStatus>(ser.GetUInt8());
		}
		return err;
	} catch (std::exception& ex) {
		return ex;
	}
}

void PKMigrationService::writeStatus(MigrationStatus status) noexcept {
	try {
		WrSerializer ser;
		ser.PutUInt64(version_);
		ser.PutUInt8(static_cast<bool>(status));
		nsImpl_.writeSysRecToStorage(ser.Slice(), kStoragePkMigrationStatusPrefix, version_, true);
	} catch (const std::exception& ex) {
		logFmt(LogError, "Failed to write PK migration status: {}", ex.what());
	}
}

}  // namespace migrations
}  // namespace reindexer
