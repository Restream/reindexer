#include "activitylog.h"

#ifdef RX_LOGACTIVITY

#include <fstream>
#include <iomanip>
#include <sstream>
#include <stack>
#include "activity_context.h"
#include "core/cjson/jsonbuilder.h"
#include "tools/serializer.h"
#include "tools/timetools.h"

namespace reindexer {

void ActivityContainerLog::AddOperation(const RdxActivityContext* ctx, Activity::State st, bool start) {
	auto it = pointerToSlot_.find(ctx);
	if (it != pointerToSlot_.end() && it->second != -1) {
		operation_[it->second].operations.emplace_back(st, start);
	}
}

void ActivityContainerLog::Register(const RdxActivityContext* context) {
	if (emptySlots_.size()) {
		int k = emptySlots_.front();
		emptySlots_.pop_front();
		pointerToSlot_.emplace(context, k);
		operation_[k].reset();
		operation_[k].completed = false;
		operation_[k].description = context->Query();
		operation_[k].operations.emplace_back(ActivityOperation(static_cast<Activity>(*context).state, true));
	}
}

void ActivityContainerLog::Unregister(const RdxActivityContext* context) {
	const auto it = pointerToSlot_.find(context);
	if (it != pointerToSlot_.end()) {
		operation_[it->second].operations.emplace_back(ActivityOperation(static_cast<Activity>(*context).state, false));
		operation_[it->second].completed = true;
		emptySlots_.push_back(it->second);
		pointerToSlot_.erase(it);
	}
}

void ActivityContainerLog::Reregister(const RdxActivityContext* oldCtx, const RdxActivityContext* newCtx) {
	const auto it = pointerToSlot_.find(oldCtx);
	if (it != pointerToSlot_.end()) {
		operation_[it->second].operations.emplace_back(ActivityOperation(static_cast<Activity>(*oldCtx).state, false));
		operation_[it->second].completed = true;
		emptySlots_.push_back(it->second);
		pointerToSlot_.erase(it);

		if (emptySlots_.size()) {
			int k = emptySlots_.front();
			emptySlots_.pop_front();
			pointerToSlot_.emplace(newCtx, k);
			operation_[k].reset();
			operation_[k].completed = false;
			operation_[k].description = newCtx->Query();
			operation_[k].operations.emplace_back(ActivityOperation(static_cast<Activity>(*newCtx).state, true));
		}
	}
}

void ActivityContainerLog::Reset() {
	emptySlots_.clear();
	for (int i = 0; i < kOperationSlotsCount; i++) {
		emptySlots_.push_back(i);
	}
	operation_.clear();
	operation_.resize(kOperationSlotsCount);
	pointerToSlot_.clear();
}

void ActivityContainerLog::Dump(int serverId) {
	std::string fileName = "activity_" + std::to_string(serverId) + ".json";
	std::ofstream f(fileName);
	auto timeToString = [](const std::chrono::time_point<system_clock_w>& tp) {
		auto tm = ssystem_clock_w::to_time_t(tp);
		std::tm tmTime = localtime(tm);
		auto timeInUs = std::chrono::duration_cast<std::chrono::microseconds>(tp.time_since_epoch()).count();
		int us = timeInUs % 1000;
		int ms = (timeInUs / 1000) % 1000;
		std::stringstream ss;
		ss << std::setfill('0') << std::setw(2) << tmTime.tm_hour << ":" << tmTime.tm_min << ":" << tmTime.tm_sec << ":";
		ss << std::setfill('0') << std::setw(3) << ms << "'" << us;
		return ss.str();
	};
	WrSerializer ser;
	JsonBuilder jb(ser);

	auto jba = jb.Array("blocks");
	for (const auto& o : operation_) {
		if (o.operations.empty()) {
			continue;
		}
		auto jbb = jba.Object();
		jbb.Put("description", o.description);
		jbb.Put("Completed", o.completed);
		std::stack<JsonBuilder> objectStack;
		objectStack.push(std::move(jbb));
		int level = 0;
		// JsonBuilder curobj=
		for (const auto& k : o.operations) {
			if (k.isStart) {
				if (level % 2 == 0) {
					auto a = objectStack.top().Array("sub_block");
					objectStack.push(std::move(a));
					level++;
				}
				auto jbsb = objectStack.top().Object();
				jbsb.Put("State", Activity::DescribeState(k.state));
				jbsb.Put("Start", timeToString(k.tp));
				objectStack.push(std::move(jbsb));
				level++;

			} else {
				if (level % 2 == 1) {  // close prev array
					objectStack.pop();
					level--;
				}
				objectStack.top().Put("End", timeToString(k.tp));
				objectStack.pop();
				level--;
			}
		}
		while (!objectStack.empty()) {
			objectStack.pop();
		}
	}
	jba.End();
	jb.End();
	f << ser.Slice() << std::endl;

	std::string fileNameTxt = "activity_" + std::to_string(serverId) + ".txt";
	std::ofstream fTxt(fileNameTxt);

	for (const auto& o : operation_) {
		if (o.operations.empty()) {
			continue;
		}
		fTxt << "Completed = " << o.completed << " description = " << o.description << std::endl;
		for (const auto& k : o.operations) {
			fTxt << "    "
				 << "Start = " << k.isStart << " State = " << Activity::DescribeState(k.state)
				 << " time = " << k.tp.time_since_epoch().count() << " (" << timeToString(k.tp) << ")" << std::endl;
		}
	}
	fTxt << std::endl;
}
}  // namespace reindexer
#endif
