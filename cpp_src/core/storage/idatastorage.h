#pragma once

#include <memory>
#include "storagetype.h"
#include "tools/errors.h"

struct StorageOpts;

namespace reindexer {

using std::shared_ptr;

namespace datastorage {

class Cursor;
class Comparator;
class UpdatesCollection;

/// Particular state of DB. Iterators created with
/// this handle will all observe a stable snapshot
/// of the current DB.
class [[nodiscard]] Snapshot {
public:
	virtual ~Snapshot() = default;
	using Ptr = shared_ptr<const Snapshot>;
};

/// Low-level data storage abstraction.
class [[nodiscard]] IDataStorage {
public:
	virtual ~IDataStorage() = default;

	/// Opens a storage.
	/// @param path - path to storage.
	/// @param opts - options.
	/// @return Error object with an appropriate error code.
	virtual Error Open(const std::string& path, const StorageOpts& opts) = 0;

	/// Reads data from a storage.
	/// @param opts - read options.
	/// @param key - key value.
	/// @param value - value data.
	/// @return Error code or ok.
	virtual Error Read(const StorageOpts& opts, std::string_view key, std::string& value) = 0;

	/// Writes data directly to a Storage.
	/// @param opts - write options.
	/// @param key - key value;
	/// @param value - value.
	/// @return Error code or ok.
	virtual Error Write(const StorageOpts& opts, std::string_view key, std::string_view value) = 0;

	/// Performs batch write of
	/// updates' collection.
	/// @param opts - write options.
	/// @param buffer - buffer to be written;
	/// @return Error code or ok.
	virtual Error Write(const StorageOpts& opts, UpdatesCollection& buffer) = 0;

	/// Removes an entry from a Storage
	/// with an appropriate key.
	/// @param opts - options.
	/// @param key - key value of the entry to be deleted.
	/// @return Error code or ok.
	virtual Error Delete(const StorageOpts& opts, std::string_view key) = 0;

	/// Makes a snapshot of a current Storage state.
	/// Allows to iterate over a particular state.
	/// Caller should call ReleaseSnapshot when
	/// the snapshot is no longer needed.
	/// @return Pointer to a newly created Snapshot.
	virtual Snapshot::Ptr MakeSnapshot() = 0;

	/// Releases a snapshot. Caller should not
	/// use the snapshot after this call.
	/// @param snapshot - pointer to a snapshot
	/// that was acquired previously.
	virtual void ReleaseSnapshot(Snapshot::Ptr snapshot) = 0;

	/// Flushes all updates to Storage.
	virtual Error Flush() = 0;

	/// Reopens files on disk.
	virtual Error Reopen() = 0;

	/// Allocates and returns Cursor object to a Storage.
	/// The client itself is responsible for it's deallocation.
	/// @param opts - options.
	/// @return newly created Cursor object.
	virtual Cursor* GetCursor(StorageOpts& opts) = 0;

	/// Allocates and returns UpdatesCollection object to a Storage.
	/// The client itself is responsible for it's deallocation.
	/// @return newly created UpdatesCollection object.
	virtual UpdatesCollection* GetUpdatesCollection() = 0;

	/// Destroy the storage.
	/// @param path - path to Storage.
	virtual void Destroy(const std::string& path) = 0;

	/// Repair the storage
	/// @param path - path to Storage.
	virtual Error Repair(const std::string& path) = 0;

	/// Get storage type
	virtual StorageType Type() const noexcept = 0;
};

/// Buffer for a Batch Write.
/// Holds a collection of updates
/// to apply to Storage.
class [[nodiscard]] UpdatesCollection {
public:
	virtual ~UpdatesCollection() = default;

	/// Puts data to collection.
	/// @param key - key value.
	/// @param value - value data.
	virtual void Put(std::string_view key, std::string_view value) = 0;

	/// Removes value from collection.
	/// @param key - key of data to be removed.
	virtual void Remove(std::string_view key) = 0;

	/// Clears collection.
	virtual void Clear() = 0;

	/// Set batch id
	virtual void Id(uint64_t) noexcept = 0;
	/// Get batch id
	virtual uint64_t Id() const noexcept = 0;

	using Ptr = shared_ptr<UpdatesCollection>;
};

/// Cursor to iterate
/// over a Storage.
class [[nodiscard]] Cursor {
public:
	virtual ~Cursor() = default;

	/// Checks if cursor is valid.
	/// @return True if the cursor is valid.
	virtual bool Valid() const = 0;

	/// Positions at the first key in the source.
	virtual void SeekToFirst() = 0;

	/// Positions at the last key in the source.
	virtual void SeekToLast() = 0;

	/// Positions at the first key in the source
	/// that is at or past target.
	/// @param target - target key to seek at.
	virtual void Seek(std::string_view target) = 0;

	/// Moves to the next entry in the source.
	virtual void Next() = 0;

	/// Moves to the previous entry in the source.
	virtual void Prev() = 0;

	/// Returns the key for the current entry.
	/// @return Key for the current entry.
	virtual std::string_view Key() const = 0;

	/// Return the value for the current entry.
	/// @return Value for the current entry.
	virtual std::string_view Value() const = 0;

	/// Returns default comparator object.
	/// @return reference to a Comparator object.
	virtual Comparator& GetComparator() = 0;
};

/// Comparator object. Used for iterating
/// over a Storage along with a Cursor object.
class [[nodiscard]] Comparator {
public:
	virtual ~Comparator() = default;

	/// Three-way comparison.
	/// @return 1. < 0 if "a" < "b"
	///         2. == 0 if "a" == "b"
	///         3. > 0 if "a" > "b"
	virtual int Compare(std::string_view a, std::string_view b) const = 0;
};
}  // namespace datastorage
}  // namespace reindexer
