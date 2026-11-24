#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <utility>

#include "estl/thread_annotation_attributes.h"

namespace reindexer {

/// @brief Owns a task callback until its event loop operations complete.
class [[nodiscard]] LoopTask {
public:
	class [[nodiscard]] Completion {
	public:
		void wait() noexcept {
			std::unique_lock lck{m_};
			cv_.wait(lck, [this]() RX_REQUIRES(m_) { return completed_; });
		}

		void complete() noexcept {
			std::lock_guard lck{m_};
			completed_ = true;
			cv_.notify_one();
		}

	private:
		std::mutex m_;
		std::condition_variable cv_;
		bool completed_ RX_GUARDED_BY(m_) = false;
	};

	LoopTask() = default;
	explicit LoopTask(std::function<bool()> f, Completion* completion = nullptr) : f_{std::move(f)}, completion_{completion} {}
	LoopTask(LoopTask&&) noexcept = default;
	LoopTask& operator=(LoopTask&&) noexcept = default;
	LoopTask(const LoopTask&) = delete;
	LoopTask& operator=(const LoopTask&) = delete;

	bool invoke() { return f_(); }

	void complete() noexcept {
		if (completion_) {
			completion_->complete();
		}
	}

private:
	std::function<bool()> f_;
	Completion* completion_{nullptr};
};

}  // namespace reindexer
