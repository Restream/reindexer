#pragma once

namespace reindexer {

enum class NeedRollBack : bool { No = false, Yes = true };

class RollBackBase {
protected:
	RollBackBase() noexcept = default;
	virtual ~RollBackBase() = default;
	RollBackBase(RollBackBase &&other) noexcept : disabled_{other.disabled_} { other.Disable(); }
	RollBackBase(const RollBackBase &) = delete;
	RollBackBase &operator=(const RollBackBase &) = delete;
	RollBackBase &operator=(RollBackBase &&) = delete;
	virtual void Disable() noexcept { disabled_ = true; }
	bool IsDisabled() const noexcept { return disabled_; }

private:
	bool disabled_{false};
};

}  // namespace reindexer
