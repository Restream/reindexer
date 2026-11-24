#include "core/embedding/circuitbreaker.h"
#include "estl/lock.h"

namespace reindexer {

EmbedderCircuitBreaker::Permit::Permit(EmbedderCircuitBreaker& breaker, Kind kind, uint64_t generation) noexcept
	: breaker_{&breaker}, kind_{kind}, generation_{generation} {}

EmbedderCircuitBreaker::Permit::~Permit() {
	if (breaker_) {
		breaker_->reportFailure(steady_clock_w::now(), kind_, generation_);
	}
}

void EmbedderCircuitBreaker::Permit::ReportSuccess(steady_clock_w::time_point now) noexcept {
	if (breaker_) {
		breaker_->reportSuccess(now, kind_, generation_);
		breaker_ = nullptr;
	}
}

void EmbedderCircuitBreaker::Permit::ReportFailure(steady_clock_w::time_point now) noexcept {
	if (breaker_) {
		breaker_->reportFailure(now, kind_, generation_);
		breaker_ = nullptr;
	}
}

EmbedderCircuitBreaker::EmbedderCircuitBreaker(Config config) noexcept : config_{config} {}

void EmbedderCircuitBreaker::applyIdleReset(steady_clock_w::time_point now) noexcept {
	if (!hasLastRequest_ || config_.thresholdTimeout.count() <= 0) {
		return;
	}
	if (now - lastRequest_ >= config_.thresholdTimeout) {
		consecutiveErrors_ = 0;
	}
}

void EmbedderCircuitBreaker::updateLastRequest(steady_clock_w::time_point now) noexcept {
	if (!hasLastRequest_ || now > lastRequest_) {
		lastRequest_ = now;
	}
	hasLastRequest_ = true;
}

Error EmbedderCircuitBreaker::makeOpenError(std::string_view fieldName, steady_clock_w::time_point now) const {
	if (state_ == State::HalfOpen) {
		return Error{errNetwork, "Failed to get embedding for '{}'. Circuit breaker is open: probe request is in progress", fieldName};
	}
	const auto elapsed = now - openedAt_;
	if (elapsed >= config_.cooldown) {
		return Error{errNetwork, "Failed to get embedding for '{}'. Circuit breaker is open: probe request is in progress", fieldName};
	}
	const double remainingSec = std::chrono::duration<double>(config_.cooldown - elapsed).count();
	return Error{errNetwork, "Failed to get embedding for '{}'. Circuit breaker is open: retry in {:.1f}s", fieldName, remainingSec};
}

EmbedderCircuitBreaker::Permit EmbedderCircuitBreaker::Acquire(std::string_view fieldName) {
	if (!config_.Enabled()) {
		return Permit{*this, Permit::Kind::Closed, 0};
	}
	return Acquire(fieldName, steady_clock_w::now());
}

EmbedderCircuitBreaker::Permit EmbedderCircuitBreaker::Acquire(std::string_view fieldName, steady_clock_w::time_point now) {
	if (!config_.Enabled()) {
		return Permit{*this, Permit::Kind::Closed, 0};
	}

	lock_guard lock(mtx_);
	switch (state_) {
		case State::Closed:
			applyIdleReset(now);
			return Permit{*this, Permit::Kind::Closed, generation_};
		case State::Open:
			if (now - openedAt_ < config_.cooldown) {
				throw makeOpenError(fieldName, now);
			}
			state_ = State::HalfOpen;
			return Permit{*this, Permit::Kind::Probe, generation_};
		case State::HalfOpen:
			throw makeOpenError(fieldName, now);
	}
	throw Error{errLogic, "Failed to get embedding for '{}'. Unexpected circuit breaker state", fieldName};
}

void EmbedderCircuitBreaker::reportSuccess(steady_clock_w::time_point now, Permit::Kind kind, uint64_t generation) noexcept {
	if (!config_.Enabled()) {
		return;
	}

	lock_guard lock(mtx_);
	if (generation_ != generation) {
		return;
	}

	if (kind == Permit::Kind::Closed) {
		if (state_ != State::Closed) {
			return;
		}
		consecutiveErrors_ = 0;
		updateLastRequest(now);
		return;
	}

	if (state_ != State::HalfOpen) {
		return;
	}
	state_ = State::Closed;
	consecutiveErrors_ = 0;
	updateLastRequest(now);
}

void EmbedderCircuitBreaker::reportFailure(steady_clock_w::time_point now, Permit::Kind kind, uint64_t generation) noexcept {
	if (!config_.Enabled()) {
		return;
	}

	lock_guard lock(mtx_);
	if (generation_ != generation) {
		return;
	}

	if (kind == Permit::Kind::Closed) {
		if (state_ != State::Closed) {
			return;
		}
		applyIdleReset(now);
		++consecutiveErrors_;
		updateLastRequest(now);
		if (consecutiveErrors_ >= config_.threshold) {
			state_ = State::Open;
			openedAt_ = lastRequest_;
			++generation_;
		}
		return;
	}

	if (state_ != State::HalfOpen) {
		return;
	}
	state_ = State::Open;
	updateLastRequest(now);
	openedAt_ = lastRequest_;
}

bool EmbedderCircuitBreaker::IsOpen() const noexcept {
	if (!config_.Enabled()) {
		return false;
	}
	lock_guard lock(mtx_);
	return state_ != State::Closed;
}

size_t EmbedderCircuitBreaker::ConsecutiveErrors() const noexcept {
	lock_guard lock(mtx_);
	return consecutiveErrors_;
}

}  // namespace reindexer
