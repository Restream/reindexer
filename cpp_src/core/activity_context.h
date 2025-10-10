#pragma once

#include <atomic>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>
#include "activity.h"
#include "estl/marked_mutex.h"
#include "estl/mutex.h"

namespace reindexer {

class RdxActivityContext;

class [[nodiscard]] ActivityContainer {
public:
	void Register(const RdxActivityContext*);
	void Unregister(const RdxActivityContext*);
	void Reregister(const RdxActivityContext* oldCtx, const RdxActivityContext* newCtx);
	std::vector<Activity> List(int serverId);
	std::optional<std::string> QueryForIpConnection(int id);

	void Reset();

#ifdef RX_LOGACTIVITY
	void AddOperation(const RdxActivityContext* ctx, Activity::State st, bool start);

private:
	ActivityContainerLog log_;
#endif
private:
	mutex mtx_;
	std::unordered_set<const RdxActivityContext*> cont_;
};

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
		Ward(RdxActivityContext* cont, Activity::State state) noexcept : context_(cont) {
			if (context_) {
				prevState_ = context_->state_.exchange(serializeState(state), std::memory_order_relaxed);
#ifndef NDEBUG
				context_->refCount_.fetch_add(1u, std::memory_order_relaxed);
#endif
#ifdef RX_LOGACTIVITY
				if (context_->parent_) {
					context_->parent_->AddOperation(context_, state, true);
				}
#endif
			}
		}
		Ward(RdxActivityContext* cont, MutexMark mutexMark) noexcept : context_(cont) {
			if (context_) {
				prevState_ = context_->state_.exchange(serializeState(mutexMark), std::memory_order_relaxed);
#ifndef NDEBUG
				context_->refCount_.fetch_add(1u, std::memory_order_relaxed);
#endif
#ifdef RX_LOGACTIVITY
				if (context_->parent_) {
					context_->parent_->AddOperation(context_, Activity::WaitLock, true);
				}
#endif
			}
		}
		Ward(Ward&& other) noexcept : context_(other.context_), prevState_(other.prevState_) { other.context_ = nullptr; }
		~Ward() {
			if (context_) {
#ifdef RX_LOGACTIVITY
				auto [state, mark] = deserializeState(context_->state_);
				(void)mark;
#endif

				context_->state_.store(prevState_, std::memory_order_relaxed);
				[[maybe_unused]] const auto refs = context_->refCount_.fetch_sub(1u, std::memory_order_relaxed);
				assertrx(refs != 0u);
#ifdef RX_LOGACTIVITY
				if (context_->parent_) {
					context_->parent_->AddOperation(context_, state, false);
				}
#endif
			}
		}

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
