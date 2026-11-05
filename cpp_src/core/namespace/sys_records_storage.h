#pragma once

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include "tools/errors.h"

namespace reindexer {

class AsyncStorage;
class NamespaceName;
struct SysRecordUpdate;

/**
 * @brief Stores one body per service-record key and migrates the former rotating copies on read.
 * @pre The storage and namespace name must outlive this object. Call under the namespace lock or during
 * exclusive initialization. A namespace copy stages writes until its storage is committed.
 */
class [[nodiscard]] SysRecordsStorage {
public:
	enum class [[nodiscard]] Key { Indexes, Schema, ReplState, TagsMatcher, PkMigrationStatus };

	/** @brief A borrowed system-record update; a missing value removes the record, an empty value stores an empty body. */
	struct [[nodiscard]] Record {
		Key key;
		std::optional<std::string_view> value;
	};

	struct [[nodiscard]] ReadResult {
		Error status;
		std::string content;
	};

	/** @param deferredError Borrowed error slot for a mutation that must finish before reporting a durable-write failure. */
	SysRecordsStorage(AsyncStorage& storage, const NamespaceName& nsName, Error* deferredError = nullptr) noexcept
		: storage_{storage}, nsName_{nsName}, deferredError_{deferredError} {}
	SysRecordsStorage(AsyncStorage&, NamespaceName&&, Error* = nullptr) = delete;
	SysRecordsStorage(AsyncStorage&, const NamespaceName&&, Error* = nullptr) = delete;

	/**
	 * @brief Reads the body, atomically migrating the newest rotating copy if present.
	 * Legacy slots take precedence over the unsuffixed key, which may contain an older legacy body.
	 * @return Status and body; all failures, including malformed headers and migration errors, are returned in status.
	 * Absence is errNotFound.
	 */
	ReadResult Read(Key key) const noexcept;
	/**
	 * @brief Stores a body with Sync. Backend errors are thrown, or recorded in deferredError when supplied to the constructor.
	 * Queueing errors always throw. A recorded error does not confirm durability; the caller must report it after completing its work.
	 */
	void Write(std::string_view body, Key key) const;
	/** @brief Stores related metadata in one batch, using the same durability policy as Write(body, key). */
	void Write(std::span<const Record>) const;

private:
	void write(std::span<const SysRecordUpdate>) const;
	AsyncStorage& storage_;
	const NamespaceName& nsName_;
	Error* deferredError_;
};

}  // namespace reindexer
