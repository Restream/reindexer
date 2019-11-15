#pragma once

#include <core/keyvalue/variant.h>
#include <vector>
#include "core/storage/idatastorage.h"
#include "tools/errors.h"
#include "walrecord.h"

namespace reindexer {

static const int kDefaultWALSize = 1000000;

/// WAL trakcer
class WALTracker {
public:
	/// Initialize WAL tracker.
	/// @param maxLSN - Current LSN counter value
	/// @param storage - Storage object for store WAL records
	void Init(int64_t maxLSN, shared_ptr<datastorage::IDataStorage> storage);
	/// Add new record to WAL tracker
	/// @param rec - Record to be added
	/// @param oldLsn - Optional, previous LSN value of changed object
	/// @return LSN value of record
	int64_t Add(const WALRecord &rec, int64_t oldLsn = -1);
	/// Set record in WAL tracker
	/// @param rec - Record to be added
	/// @param lsn - LSN value
	/// @return true if set successful, false, if lsn is outdated
	bool Set(const WALRecord &rec, int64_t lsn);
	/// Get current LSN counter value
	/// @return current LSN counter value
	int64_t LSNCounter() const { return lsnCounter_; }

	/// Iterator for WAL records
	class iterator {
	public:
		iterator &operator++() { return idx_++, *this; }
		bool operator!=(const iterator &other) const { return idx_ != other.idx_; }
		WALRecord operator*() const {
			assertf(idx_ % wt_->walSize_ < int(wt_->records_.size()), "idx=%d,wt_->records_.size()=%d,lsnCounter=%d", idx_,
					wt_->records_.size(), wt_->lsnCounter_);

			return WALRecord(span<uint8_t>(wt_->records_[idx_ % wt_->walSize_]));
		}
		span<uint8_t> GetRaw() const { return wt_->records_[idx_ % wt_->walSize_]; }
		int64_t GetLSN() const { return idx_; }
		int64_t idx_;
		const WALTracker *wt_;
	};

	/// Get begin iterator
	/// @return iterator pointing to begin of WAL
	iterator begin() const { return {lsnCounter_ > walSize_ ? lsnCounter_ - walSize_ : 0, this}; }
	/// Get end iterator
	/// @return iterator pointing to end of WAL
	iterator end() const { return {lsnCounter_, this}; }
	/// Get upper_bound
	/// @param lsn LSN of record
	/// @return iterator pointing LSN record greate than lsn
	iterator upper_bound(int64_t lsn) const { return !available(lsn + 1) ? end() : iterator{lsn + 1, this}; }
	/// Check is LSN outdated, and complete log is not available
	/// @param lsn LSN of record
	/// @return true if LSN is outdated
	bool is_outdated(int64_t lsn) const { return bool(lsnCounter_ - lsn >= walSize_); }

	/// Get WAL size
	/// @return count of actual records in WAL
	size_t size() const { return lsnCounter_ > walSize_ ? walSize_ : lsnCounter_; }
	/// Get WAL heap size
	/// @return WAL memory consumption
	size_t heap_size() const;

protected:
	/// put WAL record into lsn position, grow ring buffer, if neccessary
	/// @param lsn LSN value
	/// @param rec - Record to be added
	void put(int64_t lsn, const WALRecord &rec);
	/// check if lsn is available. e.g. in range of ring buffer
	bool available(int64_t lsn) const { return lsn < lsnCounter_ && lsnCounter_ - lsn < walSize_; }
	/// flushes lsn value to storage
	/// @param lsn - lsn value
	void writeToStorage(int64_t lsn);
	std::vector<std::pair<int64_t, std::string>> readFromStorage(int64_t &maxLsn);

	/// Ring buffer of WAL records
	std::vector<PackedWALRecord> records_;
	/// LSN counter value. Contains LSN of next record
	int64_t lsnCounter_ = 0;
	/// Size of ring buffer
	int64_t walSize_ = kDefaultWALSize;

	std::weak_ptr<datastorage::IDataStorage> storage_;
};

}  // namespace reindexer
