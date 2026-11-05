#pragma once

#include <chrono>
#include <cstdint>
#include <string_view>
#include "core/embedding/circuitbreaker_defaults.h"
#include "estl/mutex.h"
#include "tools/clock.h"
#include "tools/errors.h"

namespace reindexer {

/// Circuit breaker for embedder HTTP calls (cache misses only).
///
/// Closed: counts consecutive network errors. Opens after Config::threshold errors
/// with less than Config::thresholdTimeout idle gap between requests. A successful
/// network request resets the error counter. Idle for Config::thresholdTimeout also
/// resets the counter (when timeout > 0). Disabled when Config::threshold or
/// Config::cooldown is 0.
///
/// Open: rejects requests for Config::cooldown after activation. After cooldown,
/// allows a single probe. Other requests stay rejected until the probe finishes.
/// Probe success → Closed. Probe failure → stay Open and restart cooldown.
///
/// Each Permit is tagged with a role (Closed request vs HalfOpen probe) and a
/// generation captured at Acquire. Generation increments on Closed→Open, so a
/// stale Closed permit cannot affect the breaker after a full Open→HalfOpen→Closed
/// recovery cycle. State is guarded by a mutex held only for Acquire/Report* (not
/// during the HTTP call itself).
class [[nodiscard]] EmbedderCircuitBreaker {
public:
	struct [[nodiscard]] Config {
		size_t threshold{EmbedderCircuitBreakerDefaults::kThreshold};
		std::chrono::milliseconds thresholdTimeout{EmbedderCircuitBreakerDefaults::kThresholdTimeout};
		std::chrono::milliseconds cooldown{EmbedderCircuitBreakerDefaults::kCooldown};

		bool Enabled() const noexcept { return threshold > 0 && cooldown.count() > 0; }
	};

	class [[nodiscard]] Permit {
	public:
		Permit() noexcept = delete;
		Permit(const Permit&) = delete;
		Permit& operator=(const Permit&) = delete;
		Permit(Permit&& other) noexcept = delete;
		Permit& operator=(Permit&& other) = delete;
		~Permit();

		void ReportSuccess() noexcept { ReportSuccess(steady_clock_w::now()); }
		void ReportSuccess(steady_clock_w::time_point now) noexcept;

		void ReportFailure() noexcept { ReportFailure(steady_clock_w::now()); }
		void ReportFailure(steady_clock_w::time_point now) noexcept;

	private:
		friend class EmbedderCircuitBreaker;
		enum class [[nodiscard]] Kind : uint8_t { Closed = 0, Probe = 1 };

		Permit(EmbedderCircuitBreaker& breaker, Kind kind, uint64_t generation) noexcept;

		EmbedderCircuitBreaker* breaker_{nullptr};
		Kind kind_{Kind::Closed};
		uint64_t generation_{0};
	};

	EmbedderCircuitBreaker() noexcept = default;
	explicit EmbedderCircuitBreaker(Config config) noexcept;

	Permit Acquire(std::string_view fieldName);
	Permit Acquire(std::string_view fieldName, steady_clock_w::time_point now);

	bool IsOpen() const noexcept;
	size_t ConsecutiveErrors() const noexcept;

private:
	enum class [[nodiscard]] State : uint8_t { Closed = 0, Open = 1, HalfOpen = 2 };

	void reportSuccess(steady_clock_w::time_point now, Permit::Kind kind, uint64_t generation) noexcept;
	void reportFailure(steady_clock_w::time_point now, Permit::Kind kind, uint64_t generation) noexcept;
	void applyIdleReset(steady_clock_w::time_point now) noexcept RX_REQUIRES(mtx_);
	void updateLastRequest(steady_clock_w::time_point now) noexcept RX_REQUIRES(mtx_);
	Error makeOpenError(std::string_view fieldName, steady_clock_w::time_point now) const RX_REQUIRES(mtx_);

	Config config_;
	mutable mutex mtx_;
	State state_ RX_GUARDED_BY(mtx_){State::Closed};
	size_t consecutiveErrors_ RX_GUARDED_BY(mtx_){0};
	bool hasLastRequest_ RX_GUARDED_BY(mtx_){false};
	steady_clock_w::time_point lastRequest_ RX_GUARDED_BY(mtx_){};
	steady_clock_w::time_point openedAt_ RX_GUARDED_BY(mtx_){};
	uint64_t generation_ RX_GUARDED_BY(mtx_){0};
};

}  // namespace reindexer
