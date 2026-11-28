#pragma once

#include <exception>

namespace reindexer {

template <typename BT, typename FT, FT BT::* field>
class [[nodiscard]] VerifyingUpdater {
	using BaseType = BT;
	using FieldType = FT;

public:
	VerifyingUpdater(BaseType& base) noexcept : base_{base} {}
	operator FieldType&() & noexcept { return Get(); }
	FieldType& Get() & noexcept {
		touched_ = true;
		return base_.*field;
	}
	// NOLINTNEXTLINE(bugprone-unsafe-to-allow-exceptions) Verification errors propagate only outside stack unwinding.
	~VerifyingUpdater() noexcept(false) {
		if (touched_ && std::uncaught_exceptions() == 0) {
			base_.Verify();
		}
	}

private:
	BaseType& base_;
	bool touched_{false};
};

}  // namespace reindexer
