#include "pk_migration_service.h"
#include "core/namespace/namespaceimpl.h"
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

bool PKMigrationService::migrateItem(size_t rowId, const FieldsSet& oldPk, const FieldsSet& newPk, WrSerializer& pkBuf,
									 WrSerializer& itemBuf, const FloatVectorsIndexes& vectorIndexes) noexcept {
	try {
		ItemImpl item{nsImpl_.payloadType_, nsImpl_.items_[rowId], nsImpl_.tagsMatcher_};
		item.Unsafe(true);
		// Serialize the new one.
		Error err{nsImpl_.tryWriteItemIntoStorage(newPk, item, IdType::FromNumber(rowId), vectorIndexes, pkBuf, itemBuf)};
		if (err.ok()) {
			// Remove the old one if successfull.
			serializeItemPk(item.GetConstPayload(), oldPk, pkBuf);
			nsImpl_.storage_.Remove(pkBuf.Slice());
		} else {
			logFmt(LogError, "Failed to migrate '{}' item with row_id={}: {}", nsImpl_.name_, rowId, err.what());
			return false;
		}
	} catch (const std::exception& ex) {
		logFmt(LogError, "Failed to migrate '{}' item with row_id={}: {}", nsImpl_.name_, rowId, ex.what());
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
			const FloatVectorsIndexes vectorIndexes{nsImpl_.getVectorIndexes()};

			for (size_t rowId = 0; rowId < nsImpl_.items_.size(); ++rowId) {
				if (!migrateItem(rowId, oldPk, newPk, pkBuf, itemBuf, vectorIndexes)) {
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

void PKMigrationService::RemoveItemsWithObsoletePK() {
	if (nsImpl_.storage_.IsValid()) {
		MigrationStatus status{MigrationStatus_False};
		reindexer::Error error{readStatus(status)};
		if (error.ok() && status == MigrationStatus_False) {
			logFmt(LogTrace, "Removing '{}' items with obsolete PK.", nsImpl_.name_);

			WrSerializer buf;

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

				try {
					serializeItemPk(item.GetConstPayload(), nsImpl_.pkFields(), buf);
					if (buf.Slice() != dbIter->Key()) {
						// Item's PK is obsolete, removing this item from storage.
						nsImpl_.storage_.Remove(buf.Slice());
						logFmt(LogTrace, "Removing item ('{}') from '{}' storage: 'PK is obsolete.'", dbIter->Key(), nsImpl_.name_);
					}
				} catch (const Error& err) {
					throw Error{errParseBin, "Error recovering item = '{}' for '{}': {}", dbIter->Key(), nsImpl_.name_, err.what()};
				}
			}
		}
		writeStatus(MigrationStatus_True);
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
