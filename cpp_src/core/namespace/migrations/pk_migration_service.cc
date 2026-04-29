#include "pk_migration_service.h"
#include "core/id_type.h"
#include "core/namespace/namespaceimpl.h"
#include "core/storage/storage_prefixes.h"
#include "tools/logger.h"

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

bool PKMigrationService::migrateItem(size_t id, const FieldsSet& oldPk, const FieldsSet& newPk, WrSerializer& pkBuf,
									 WrSerializer& itemBuf) noexcept {
	const auto rowId = IdType::FromNumber(id);
	try {
		ItemImpl item{nsImpl_.payloadType_, nsImpl_.items_[rowId], nsImpl_.tagsMatcher_};
		item.Unsafe(true);
		// Serialize the new one.
		Error err{nsImpl_.tryWriteItemIntoStorage(newPk, item, rowId, pkBuf, itemBuf)};
		if (err.ok()) {
			// Remove the old one if successfull.
			serializeItemPk(item.GetConstPayload(), oldPk, pkBuf);
			nsImpl_.storage_.Remove(pkBuf.Slice());
		} else {
			logFmt(LogError, "Failed to migrate '{}' item with row_id={}: {}", nsImpl_.name_, id, err.what());
			return false;
		}
	} catch (const std::exception& ex) {
		logFmt(LogError, "Failed to migrate '{}' item with row_id={}: {}", nsImpl_.name_, id, ex.what());
		return false;
	}
	return true;
}

void PKMigrationService::MigrateFromOldToNewPK(const FieldsSet& oldPk, const FieldsSet& newPk) noexcept {
	if (nsImpl_.storage_.IsValid()) {
		writeStatus(MigrationStatus_False);

		MigrationStatus status{MigrationStatus_True};

		try {
			logFmt(LogTrace, "Migrating '{}' items from old to new PK.", nsImpl_.name_);

			WrSerializer pkBuf, itemBuf;

			for (size_t rowId = 0; rowId < nsImpl_.items_.size(); ++rowId) {
				if (nsImpl_.items_[IdType::FromNumber(rowId)].IsFree()) {
					continue;
				}
				if (!migrateItem(rowId, oldPk, newPk, pkBuf, itemBuf)) {
					status = MigrationStatus_False;
				}
			}

			logFmt(LogTrace, "Migrating '{}' to new PK finished with status: {}", nsImpl_.name_, static_cast<bool>(status));
		} catch (const std::exception& ex) {
			logFmt(LogError, "Migrating '{}' to new PK failed: {}", nsImpl_.name_, ex.what());
			status = MigrationStatus_False;
		}

		writeStatus(status);
	}
}

void PKMigrationService::MigrateToNewPK(const FieldsSet& pk) noexcept {
	if (nsImpl_.storage_.IsValid() && !nsImpl_.items_.empty()) {
		MigrationStatus status{MigrationStatus_True};

		try {
			logFmt(LogInfo, "Migrating '{}' items to new PK.", nsImpl_.name_);

			writeStatus(MigrationStatus_False);

			// Flushing all the items waiting in queue.
			nsImpl_.storage_.Flush(StorageFlushOpts{});

			WrSerializer pkBuf, itemBuf;
			for (size_t id = 0; id < nsImpl_.items_.size(); ++id) {
				const auto rowId = IdType::FromNumber(id);
				auto& pv = nsImpl_.items_[rowId];
				if (pv.IsFree()) {
					continue;
				}
				ItemImpl item{nsImpl_.payloadType_, pv, nsImpl_.tagsMatcher_};
				item.Unsafe(true);

				Error err{nsImpl_.tryWriteItemIntoStorage(pk, item, rowId, pkBuf, itemBuf)};
				if (!err.ok()) {
					logFmt(LogError, "Failed to migrate '{}' item with row_id={}: {}", nsImpl_.name_, id, err.what());
					status = MigrationStatus_False;
				}
			}

			logFmt(LogInfo, "Removing '{}' items with old PK.", nsImpl_.name_);

			iterateOverStorageItems([&, this](const ItemImpl& item, AsyncStorage::Cursor& cursor, const StorageOpts& opts) {
				try {
					serializeItemPk(item.GetConstPayload(), pk, pkBuf);
					if (pkBuf.Slice() != cursor->Key()) {
						cursor.RemoveThisKey(opts);
						logFmt(LogTrace, "Removing item ('{}') with obsolete key from '{}' storage", cursor->Key(), nsImpl_.name_);
					}
				} catch (const std::exception& err) {
					logFmt(LogError, "Error removing item = '{}' for '{}': {}", cursor->Key(), nsImpl_.name_, err.what());
					status = MigrationStatus_False;
				}
			});
		} catch (const std::exception& ex) {
			logFmt(LogError, "Migrating '{}' to new PK failed: {}", nsImpl_.name_, ex.what());
			status = MigrationStatus_False;
		}

		writeStatus(status);

		logFmt(((status == MigrationStatus_False) ? LogError : LogInfo), "Migrating '{}' to new PK finished with status: {}", nsImpl_.name_,
			   static_cast<bool>(status));
	}
}

void PKMigrationService::RemoveItemsWithObsoletePK() {
	if (nsImpl_.storage_.IsValid()) {
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
		if (error.ok() && status == MigrationStatus_False) {
			logFmt(LogTrace, "Removing '{}' items with obsolete PK.", nsImpl_.name_);

			WrSerializer buf;
			iterateOverStorageItems([&](const ItemImpl& item, AsyncStorage::Cursor& cursor, const StorageOpts& opts) {
				serializeItemPk(item.GetConstPayload(), *pk, buf);
				if (buf.Slice() != cursor->Key()) {
					try {
						cursor.RemoveThisKey(opts);
						logFmt(LogTrace, "Removing item ('{}') from '{}' storage: 'PK is obsolete.'", cursor->Key(), nsImpl_.name_);
					} catch (const std::exception& err) {
						throw Error{errParseBin, "Error recovering item = '{}' for '{}': {}", cursor->Key(), nsImpl_.name_, err.what()};
					}
				}
			});
		}
		writeStatus(MigrationStatus_True);
	}
}

template <typename Fn>
void PKMigrationService::iterateOverStorageItems(Fn&& onReadItem) noexcept {
	if (!nsImpl_.storage_.IsValid()) {
		return;
	}
	try {
		ItemImpl item{nsImpl_.payloadType_, nsImpl_.tagsMatcher_};
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

			const int64_t lsn{*reinterpret_cast<const int64_t*>(dataSlice.data())};
			if (lsn < 0) {
				continue;
			}

			try {
				dataSlice = dataSlice.substr(sizeof(lsn));
				item.FromCJSON(dataSlice);
			} catch (const Error&) {
				continue;
			}

			onReadItem(item, dbIter, opts);
		}
	} catch (const std::exception& ex) {
		logFmt(LogError, "Error reading items from storage for '{}': {}", nsImpl_.name_, ex.what());
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
