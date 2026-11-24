#include "sys_records_storage.h"
#include <array>
#include <optional>
#include <utility>
#include "asyncstorage.h"
#include "core/formatters/namespacesname_fmt.h"
#include "core/storage/sys_record_update.h"
#include "estl/h_vector.h"
#include "tools/logger.h"
#include "tools/serilize/serializer.h"

namespace reindexer {
namespace {
constexpr size_t kLegacyCopies = 8;

std::string_view storageKey(SysRecordsStorage::Key key) {
	switch (key) {
		case SysRecordsStorage::Key::Indexes:
			return "indexes";
		case SysRecordsStorage::Key::Schema:
			return "schema";
		case SysRecordsStorage::Key::ReplState:
			return "repl";
		case SysRecordsStorage::Key::TagsMatcher:
			return "tags";
		case SysRecordsStorage::Key::PkMigrationStatus:
			return "pk_migration_status";
	}
	throw Error(errParams, "Unknown system record key: {}", int(key));
}
}  // namespace

SysRecordsStorage::ReadResult SysRecordsStorage::Read(Key key) const noexcept try {
	const auto keyName = storageKey(key);
	std::array<std::string, kLegacyCopies> legacyKeys;
	std::optional<uint64_t> latestVersion;
	std::string latestBody;
	for (size_t i = 0; i < legacyKeys.size(); ++i) {
		legacyKeys[i] = std::string{keyName} + "." + std::to_string(i);
		std::string content;
		auto status = storage_.ReadSysRecord(legacyKeys[i], content);
		if (status.code() == errNotFound) {
			continue;
		}
		if (!status.ok()) {
			return {.status = Error(errNotValid, "Error loading namespace '{}' record '{}': {}", nsName_, legacyKeys[i], status.what()),
					.content = {}};
		}
		Serializer ser{content};
		const auto version = ser.GetUInt64();
		if (!latestVersion || version >= *latestVersion) {
			latestVersion = version;
			latestBody = content.substr(sizeof(uint64_t));
		}
	}
	if (latestVersion) {
		std::array<SysRecordUpdate, kLegacyCopies + 1> updates;
		updates[0] = {.key = keyName, .value = latestBody};
		for (size_t i = 0; i < legacyKeys.size(); ++i) {
			updates[i + 1] = {.key = legacyKeys[i], .value = std::nullopt};
		}
		storage_.WriteSysRecords(updates);
		logFmt(LogTrace, "Migrated service record '{}' for namespace '{}' from rotating copies", keyName, nsName_);
		return {.status = Error(), .content = std::move(latestBody)};
	}
	ReadResult result;
	result.status = storage_.ReadSysRecord(keyName, result.content);
	return result;
} catch (std::exception& err) {
	return {.status = Error{std::move(err)}, .content = {}};
} catch (...) {
	return {.status = Error{errAssert, "Unexpected exception"}, .content = {}};
}

void SysRecordsStorage::Write(std::string_view body, Key key) const {
	const SysRecordUpdate record{.key = storageKey(key), .value = body};
	write(std::span{&record, 1});
}

void SysRecordsStorage::Write(std::span<const Record> updates) const {
	h_vector<SysRecordUpdate, 5> records;
	records.reserve(updates.size());
	for (const auto& update : updates) {
		records.push_back(SysRecordUpdate{.key = storageKey(update.key), .value = update.value});
	}
	write(records);
}

void SysRecordsStorage::write(std::span<const SysRecordUpdate> records) const {
	if (deferredError_) {
		storage_.WriteSysRecords(records, *deferredError_);
	} else {
		storage_.WriteSysRecords(records);
	}
}

}  // namespace reindexer
