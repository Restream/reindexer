#pragma once

namespace reindexer {

class LoopTask;

/// @brief Accepts tasks for execution on an event loop.
class [[nodiscard]] ILoopTaskExecutor {
public:
	virtual ~ILoopTaskExecutor() = default;

	/// @brief Enqueues a task without waiting for its completion.
	/// @param task - Task whose ownership is transferred to the executor.
	virtual void addTask(LoopTask&& task) = 0;
};

}  // namespace reindexer
