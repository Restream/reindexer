#pragma once

#ifdef RX_LOGACTIVITY

#include <chrono>
#include <deque>
#include <string>
#include <unordered_map>
#include <vector>

#include "activity.h"

namespace reindexer {

class RdxActivityContext;

class [[nodiscard]] ActivityContainerLog {
public:
	static const int kOperationSlotsCount = 25000;
	ActivityContainerLog() {
		for (int i = 0; i < kOperationSlotsCount; i++) {
			emptySlots_.push_back(i);
		}
		operation_.resize(kOperationSlotsCount);
	}

	void AddOperation(const RdxActivityContext* ctx, Activity::State st, bool start);

	void Register(const RdxActivityContext* context);
	void Unregister(const RdxActivityContext* context);
	void Reregister(const RdxActivityContext* oldCtx, const RdxActivityContext* newCtx);

	void Dump(int serverId);
	void Reset();

	struct [[nodiscard]] ActivityOperation {
		ActivityOperation(Activity::State st, bool start) {
			tp = system_clock_w::now();
			isStart = start;
			state = st;
		}
		system_clock_w::time_point tp;
		bool isStart;
		Activity::State state;
	};

	struct [[nodiscard]] ActivityOperationRecord {
		ActivityOperationRecord() = default;
		void reset() {
			operations.clear();
			description.clear();
			completed = true;
		}
		std::vector<ActivityOperation> operations;
		std::string description;
		bool completed = true;
	};

private:
	std::deque<int> emptySlots_;
	std::unordered_map<const RdxActivityContext*, int> pointerToSlot_;
	std::vector<ActivityOperationRecord> operation_;
};
}  // namespace reindexer
#endif
