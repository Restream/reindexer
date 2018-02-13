#pragma once

#include <memory>
#include "tools/errors.h"

struct StorageOpts;

namespace reindexer {

using std::shared_ptr;

struct Slice;

namespace datastorage {

class Cursor;
class Comparator;
class UpdatesCollection;

/// Particular state of DB. Iterators created with
/// this handle will all observe a stable snapshot
/// of the current DB.
class Snapshot {
public:
	virtual ~Snapshot() = default;
	using Ptr = shared_ptr<const Snapshot>;
};

/// Low-level data storage abstraction.
class IDataStorage {
public:
	virtual ~IDataStorage() = default;

	/// Opens a storage.
	/// @param path - path to storage.
	/// @param opts - options.
	/// @return Error object with an appropriate error code.
	virtual Error Open(const string& path, const StorageOpts& opts) = 0;

	/// Reads data from a storage.
	/// @param opts - read options.
	/// @param key - key value.
	/// @param value - value data.
	/// @return Error code or ok.
	virtual Error Read(const StorageOpts& opts, const Slice& key, string& value) = 0;

	/// Writes data directly to a Storage.
	/// @param opts - write options.
	/// @param key - key value;
	/// @param value - value.
	/// @return Error code or ok.
	virtual Error Write(const StorageOpts& opts, const Slice& key, const Slice& value) = 0;

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
	virtual Error Delete(const StorageOpts& opts, const Slice& key) = 0;

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
	/// @return true if operation went ok.
	virtual void ReleaseSnapshot(Snapshot::Ptr snapshot) = 0;

	/// Flushes all updates to Storage.
	virtual void Flush() = 0;

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
	virtual void Destroy(const string& path) = 0;
};

/// Buffer for a Batch Write.
/// Holds a collection of updates
/// to apply to Storage.
class UpdatesCollection {
public:
	virtual ~UpdatesCollection() = default;

	/// Puts data to collection.
	/// @param key - key value.
	/// @param value - value data.
	virtual void Put(const Slice& key, const Slice& value) = 0;

	/// Removes value from collection.
	/// @param key - key of data to be removed.
	virtual void Remove(const Slice& key) = 0;

	/// Clears collection.
	virtual void Clear() = 0;

	using Ptr = shared_ptr<UpdatesCollection>;
};

/// Cursor to iterate
/// over a Storage.
class Cursor {
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
	virtual void Seek(const Slice& target) = 0;

	/// Moves to the next entry in the source.
	virtual void Next() = 0;

	/// Moves to the previous entry in the source.
	virtual void Prev() = 0;

	/// Returns the key for the current entry.
	/// @return Key for the current entry.
	virtual Slice Key() const = 0;

	/// Return the value for the current entry.
	/// @return Value for the current entry.
	virtual Slice Value() const = 0;

	/// Returns default comparator object.
	/// @return reference to a Comparator object.
	virtual Comparator& GetComparator() = 0;
};

/// Comparator object. Used for iterating
/// over a Storage along with a Cursor object.
class Comparator {
public:
	virtual ~Comparator() = default;

	/// Three-way comparison.
	/// @return 1. < 0 if "a" < "b"
	///         2. == 0 if "a" == "b"
	///         3. > 0 if "a" > "b"
	virtual int Compare(const Slice& a, const Slice& b) const = 0;
};
}  // namespace datastorage
}  // namespace reindexer
