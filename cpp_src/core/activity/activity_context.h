#pragma once

#include <atomic>
#include <string>
#include <string_view>
#include "activity.h"
#include "estl/marked_mutex.h"

namespace reindexer {

class ActivityContainer;

/// Threadsafe operations of objects of this class are
///		cast to Activity
///		BeforeLock
///		BeforeIndexWork
///		BeforeSelectLoop
///		CheckConnectionId
class [[nodiscard]] RdxActivityContext {
	constexpr static unsigned kStateShift = 3u;
	constexpr static unsigned kStateMask = (1u << kStateShift) - 1u;
	friend class RdxContext;

	class [[nodiscard]] Ward {
	public:
		Ward(RdxActivityContext* cont, Activity::State state) noexcept;
		Ward(RdxActivityContext* cont, MutexMark mutexMark) noexcept;
		Ward(Ward&& other) noexcept : context_(other.context_), prevState_(other.prevState_) { other.context_ = nullptr; }
		~Ward();

		Ward(const Ward&) = delete;
		Ward& operator=(const Ward&) = delete;
		Ward& operator=(Ward&&) = delete;

	private:
		RdxActivityContext* context_;
		unsigned prevState_{serializeState(Activity::InProgress)};
	};

public:
	RdxActivityContext(std::string_view activityTracer, std::string_view user, std::string_view query, ActivityContainer&,
					   int ipConnectionId, bool clientState = false);
	// NOLINTNEXTLINE (performance-noexcept-move-constructor)
	RdxActivityContext(RdxActivityContext&&);
	~RdxActivityContext();
	operator Activity() const;

	RdxActivityContext(const RdxActivityContext&) = delete;
	RdxActivityContext& operator=(const RdxActivityContext&) = delete;
	RdxActivityContext& operator=(RdxActivityContext&&) = delete;
	const std::string& Query() const noexcept { return data_.query; }

	/// returning value of these functions should be assined to a local variable which will be destroyed after the waiting work complete
	/// lifetime of the local variable should not exceed of the activityContext's
	Ward BeforeLock(MutexMark mutexMark) noexcept { return Ward(this, mutexMark); }
	Ward BeforeState(Activity::State st) noexcept { return Ward(this, st); }
	Ward BeforeIndexWork() noexcept { return Ward(this, Activity::IndexesLookup); }
	Ward BeforeSelectLoop() noexcept { return Ward(this, Activity::SelectLoop); }
	Ward BeforeClusterProxy() noexcept { return Ward(this, Activity::ProxiedViaClusterProxy); }
	Ward BeforeShardingProxy() noexcept { return Ward(this, Activity::ProxiedViaShardingProxy); }

	bool CheckConnectionId(int connectionId) const noexcept { return data_.connectionId == connectionId; }

private:
	static unsigned serializeState(MutexMark mark) noexcept { return Activity::WaitLock | (static_cast<unsigned>(mark) << kStateShift); }
	static unsigned serializeState(Activity::State state) noexcept { return static_cast<unsigned>(state); }
	static std::pair<Activity::State, std::string_view> deserializeState(unsigned state);
	static unsigned nextId() noexcept;

	const Activity data_;
	std::atomic<unsigned> state_ = {serializeState(Activity::InProgress)};	// kStateShift lower bits for state, other for details
	std::atomic<unsigned> refCount_ = {0};
	ActivityContainer* parent_ = nullptr;
};

}  // namespace reindexer
