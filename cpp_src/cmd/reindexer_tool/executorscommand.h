#pragma once

#include "commandsexecutor.h"
#include "tools/errors.h"

namespace reindexer_tool {

using reindexer::Error;

struct IExecutorsCommand {
	virtual Error Status() const = 0;
	virtual void Execute() = 0;
	virtual bool IsExecuted() const = 0;

	virtual ~IExecutorsCommand() = default;
};

class GenericCommand : public IExecutorsCommand {
public:
	using CallableT = std::function<Error()>;

	GenericCommand(CallableT command) : command_(std::move(command)) {}

	Error Status() const override final { return err_; }
	void Execute() override final {
		err_ = command_();
		executed_.store(true, std::memory_order_release);
	}
	bool IsExecuted() const override final { return executed_.load(std::memory_order_acquire); }

private:
	CallableT command_;
	Error err_;
	std::atomic<bool> executed_ = {false};
};

template <typename T>
class OutParamCommand : public IExecutorsCommand {
public:
	using CallableT = std::function<Error(T&)>;

	OutParamCommand(CallableT command, T& out) : command_(std::move(command)), out_(out) {}

	Error Status() const override final { return err_; }
	void Execute() override final {
		err_ = command_(out_);
		executed_.store(true, std::memory_order_release);
	}
	bool IsExecuted() const override final { return executed_.load(std::memory_order_acquire); }

private:
	CallableT command_;
	Error err_;
	T& out_;
	std::atomic<bool> executed_ = {false};
};

}  // namespace reindexer_tool
