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

class ActivityContainerLog {
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

	struct ActivityOperation {
		ActivityOperation(Activity::State st, bool start) {
			tp = std::chrono::system_clock::now();
			isStart = start;
			state = st;
		}
		std::chrono::time_point<std::chrono::system_clock> tp;
		bool isStart;
		Activity::State state;
	};

	struct ActivityOperationRecord {
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
