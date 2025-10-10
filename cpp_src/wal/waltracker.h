#pragma once

#include <core/keyvalue/variant.h>
#include <vector>
#include "core/formatters/lsn_fmt.h"
#include "tools/errors.h"
#include "tools/lsn.h"
#include "walrecord.h"

namespace reindexer {

class AsyncStorage;

/// WAL trakcer
class [[nodiscard]] WALTracker {
public:
	explicit WALTracker(int64_t sz);
	WALTracker(const WALTracker&) = delete;
	WALTracker(const WALTracker& wal, AsyncStorage& storage);
	/// Initialize WAL tracker.
	/// @param sz - Max WAL size
	/// @param minLSN - Min available LSN number
	/// @param maxLSN - Current LSN counter value
	/// @param storage - Storage object for store WAL records
	void Init(int64_t sz, int64_t minLSN, int64_t maxLSN, AsyncStorage& storage);
	/// Add new record to WAL tracker
	/// @param rec - Record to be added
	/// @param originLsn - Origin LSN value (from server, where this record were initialy created)
	/// @param oldLsn - Optional, previous LSN value of changed object
	/// @return LSN value of record
	lsn_t Add(const WALRecord& rec, lsn_t originLsn, lsn_t oldLsn = lsn_t());
	/// Add new packed record to WAL tracker
	/// @param rec - Record to be added
	/// @param originLsn - Origin LSN value (from server, where this record were initialy created)
	/// @return LSN value of record
	lsn_t Add(WALRecType type, const PackedWALRecord& rec, lsn_t originLsn);

	/// Set record in WAL tracker
	/// @param rec - Record to be added
	/// @param lsn - LSN value
	/// @param ignoreServer - defines if it's allowed to ignore server id during availability check
	/// @return true if set successful, false, if lsn is outdated
	bool Set(const WALRecord& rec, lsn_t lsn, bool ignoreServer);
	/// Get current LSN counter value
	/// @return current LSN counter value
	int64_t LSNCounter() const { return lsnCounter_.Counter(); }
	/// Get last LSN value in WAL
	/// @return last LSN value
	lsn_t LastLSN() const noexcept { return lastLsn_; }
	/// Get first available LSN value in WAL
	/// @return first available LSN value
	lsn_t FirstLSN() const noexcept;
	/// Get LSN value from WAL by it's offset from the tail
	/// @return LSN value
	lsn_t LSNByOffset(int64_t offset) const noexcept;
	/// Set max WAL size
	/// @param sz - New WAL size
	/// @return true - if WAL max size was changed
	bool Resize(int64_t sz);
	/// Get current WAL capacity
	/// @return Max WAL size
	int64_t Capacity() const { return walSize_; }
	/// Set server id for local LSN counter
	/// @param server - Server ID
	void SetServer(int16_t server) { lsnCounter_.SetServer(server); }
	/// Get server id from local LSN counter
	int16_t GetServer() const noexcept { return lsnCounter_.Server(); }
	/// Reset WAL state
	void Reset();

	/// Iterator for WAL records
	class [[nodiscard]] iterator {
	public:
		iterator& operator++() noexcept {
			++idx_;
			return *this;
		}
		bool operator!=(const iterator& other) const noexcept { return idx_ != other.idx_; }
		WALRecord operator*() const {
			assertf(idx_ % wt_->walSize_ < int(wt_->records_.size()), "idx={},wt_->records_.size()={},lsnCounter={}", idx_,
					wt_->records_.size(), wt_->lsnCounter_);

			return WALRecord(std::span<const uint8_t>(wt_->records_[idx_ % wt_->walSize_]));
		}
		std::span<const uint8_t> GetRaw() const noexcept { return wt_->records_[idx_ % wt_->walSize_]; }
		lsn_t GetLSN() const noexcept {
			auto server = wt_->records_[idx_ % wt_->walSize_].server;
			return lsn_t(idx_, server);
		}
		int64_t idx_;
		const WALTracker* wt_;
	};

	/// Get end iterator
	/// @return iterator pointing to end of WAL
	iterator end() const { return {lsnCounter_.Counter(), this}; }
	/// Get upper_bound
	/// @param lsn LSN counter of record
	/// @return iterator pointing LSN record greate than LSN
	iterator upper_bound(lsn_t lsn) const { return !available(lsn) ? end() : iterator{lsn.Counter() + 1, this}; }
	/// Get inclusive_upper_bound
	/// @param lsn LSN counter of record
	/// @return iterator pointing LSN record greate or equal to LSN
	iterator inclusive_upper_bound(lsn_t lsn) const { return !available(lsn) ? end() : iterator{lsn.Counter(), this}; }
	/// Check if LSN is outdated, and complete log is not available
	/// @param lsn LSN of record
	/// @return true if LSN is outdated
	bool is_outdated(lsn_t lsn) const { return !available(lsn); }

	/// Get WAL size
	/// @return count of actual records in WAL
	int64_t size() const {
		auto counter = lsnCounter_.Counter();
		if (!counter || !walSize_) {
			return 0;
		}
		auto walEnd = counter % walSize_;
		if (walOffset_ == walEnd) {
			return walSize_;
		} else if (walOffset_ < walEnd) {
			return walEnd - walOffset_;
		}
		return walEnd + (int64_t(records_.size()) - walOffset_);
	}
	/// Get WAL heap size
	/// @return WAL memory consumption
	size_t heap_size() const { return heapSize_ + records_.capacity() * sizeof(MarkedPackedWALRecord); }

protected:
	/// put WAL record into lsn position, grow ring buffer, if neccessary
	/// @param lsn LSN value
	/// @param rec - Record to be added
	void put(lsn_t lsn, const WALRecord& rec);
	/// put packed WAL record into lsn position, grow ring buffer, if neccessary
	/// @param lsn LSN value
	/// @param rec - Record to be added
	void put(lsn_t lsn, const PackedWALRecord& rec);
	/// check if lsn is available. e.g. in range of ring buffer
	bool available(lsn_t lsn, bool ignoreServer = false) const;
	/// flushes lsn value to storage
	/// @param lsn - lsn value
	void writeToStorage(lsn_t lsn);
	std::vector<std::pair<lsn_t, std::string> > readFromStorage(int64_t& maxLsn);
	void initPositions(int64_t sz, int64_t minLSN, int64_t maxLSN);
	template <typename RecordT>
	lsn_t add(RecordT&& rec, lsn_t originLsn, bool toStorage, lsn_t oldLsn = lsn_t());

	/// Ring buffer of WAL records
	std::vector<MarkedPackedWALRecord> records_;
	/// LSN counter value. Contains LSN of next record
	lsn_t lsnCounter_ = lsn_t(0, 0);
	/// Conatins LSN of the latest record
	lsn_t lastLsn_ = lsn_t();
	/// Size of ring buffer
	int64_t walSize_ = 0;
	/// Current start position in buffer
	int64_t walOffset_ = 0;
	/// Cached heap size of WAL object
	size_t heapSize_ = 0;
	/// Datastorage pointer
	AsyncStorage* storage_ = nullptr;
};

}  // namespace reindexer
