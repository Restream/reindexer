#pragma once

#include <stdint.h>
#include "tools/errors.h"
#include "vendor/gason/gason.h"

namespace reindexer {

class JsonBuilder;

// lsn format
// server id  counter
// SSS        NNN NNN NNN NNN NNN (18 decimal digits)

struct lsn_t {
private:
	static constexpr int16_t kMinServerIDValue = 0;
	static constexpr int16_t kMaxServerIDValue = 999;

	static constexpr int64_t kMaxCounter = 1000000000000000ll;
	static constexpr int64_t kDefaultCounter = kMaxCounter - 1;

public:
	void GetJSON(JsonBuilder& builder) const;

	void FromJSON(const gason::JsonNode& root) {
		const int server = root["server_id"].As<int>(0);
		const int64_t counter = root["counter"].As<int64_t>(kDefaultCounter);
		payload_ = int64_t(lsn_t(counter, server));
	}

	lsn_t() noexcept = default;
	lsn_t(const lsn_t&) noexcept = default;
	lsn_t(lsn_t&&) noexcept = default;
	lsn_t& operator=(const lsn_t&) noexcept = default;
	lsn_t& operator=(lsn_t&&) noexcept = default;
	explicit lsn_t(int64_t v) : lsn_t(v % kMaxCounter, v / kMaxCounter) {}
	lsn_t(int64_t counter, int16_t server) {
		validateCounter(counter);
		validateServerId(server);
		payload_ = server * kMaxCounter + counter;
	}
	explicit operator int64_t() const { return payload_; }

	bool operator==(lsn_t o) const noexcept { return payload_ == o.payload_; }
	bool operator!=(lsn_t o) const noexcept { return payload_ != o.payload_; }

	int64_t SetServer(short server) {
		validateServerId(server);
		payload_ = server * kMaxCounter + Counter();
		return payload_;
	}
	int64_t SetCounter(int64_t counter) {
		validateCounter(counter);
		payload_ = Server() * kMaxCounter + counter;
		return payload_;
	}
	int64_t Counter() const noexcept { return payload_ % kMaxCounter; }
	int16_t Server() const noexcept { return payload_ / kMaxCounter; }
	bool isEmpty() const noexcept { return Counter() == kDefaultCounter; }

	int compare(lsn_t o) const {
		if (Server() != o.Server()) {
			throw Error(errLogic, "Compare lsn from different server");
		}
		if (Counter() < o.Counter()) {
			return -1;
		} else if (Counter() > o.Counter()) {
			return 1;
		}
		return 0;
	}

	bool operator<(lsn_t o) const { return compare(o) == -1; }
	bool operator<=(lsn_t o) const { return compare(o) <= 0; }
	bool operator>(lsn_t o) const { return compare(o) == 1; }
	bool operator>=(lsn_t o) const { return compare(o) >= 0; }

private:
	int64_t payload_ = kDefaultCounter;
	static void validateServerId(int16_t server) {
		if (server < kMinServerIDValue) {
			throwValidation(errLogic, "Server id < %d", kMinServerIDValue);
		}
		if (server > kMaxServerIDValue) {
			throwValidation(errLogic, "Server id > %d", kMaxServerIDValue);
		}
	}
	static void validateCounter(int64_t counter) {
		if (counter > kDefaultCounter) {
			throwValidation(errLogic, "LSN Counter > Default LSN (%d)", kMaxCounter);
		}
	}

	[[noreturn]] static void throwValidation(ErrorCode, const char*, int64_t);
};

struct LSNPair {
	LSNPair() {}
	LSNPair(lsn_t upstreamLSN, lsn_t originLSN) : upstreamLSN_(upstreamLSN), originLSN_(originLSN) {}
	lsn_t upstreamLSN_;
	lsn_t originLSN_;
};

}  // namespace reindexer
