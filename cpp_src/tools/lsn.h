#pragma once

#include "tools/errors.h"

namespace gason {

struct JsonNode;

}  // namespace gason

namespace reindexer {

class JsonBuilder;

// lsn format
// server id  counter
// SSS        NNN NNN NNN NNN NNN (18 decimal digits)

struct LSNUnpacked {
	int16_t server;
	int64_t counter;
};

class lsn_t {
private:
	static constexpr int64_t kMaxCounter = 1'000'000'000'000'000ll;

public:
	static constexpr int16_t kMinServerIDValue = 0;
	static constexpr int16_t kMaxServerIDValue = 999;
	static constexpr int64_t kDefaultCounter = kMaxCounter - 1;

	void GetJSON(JsonBuilder& builder) const;
	void FromJSON(const gason::JsonNode& root);

	lsn_t() noexcept = default;
	lsn_t(const lsn_t&) noexcept = default;
	lsn_t(lsn_t&&) noexcept = default;
	lsn_t& operator=(const lsn_t&) noexcept = default;
	lsn_t& operator=(lsn_t&&) noexcept = default;
	explicit lsn_t(int64_t v) : lsn_t(v % kMaxCounter, v / kMaxCounter) {}
	lsn_t(int64_t counter, int16_t server) {
		validateCounter(counter);
		validateServerId(server);
		if (counter < 0) {
			counter = kDefaultCounter;
		}
		payload_ = server * kMaxCounter + counter;
	}
	explicit operator int64_t() const noexcept { return payload_; }
	explicit operator uint64_t() const noexcept { return static_cast<uint64_t>(payload_); }
	lsn_t& operator++() noexcept {
		SetCounter(Counter() + 1);
		return *this;
	}
	lsn_t& operator--() noexcept {
		SetCounter(Counter() - 1);
		return *this;
	}
	lsn_t operator++(int) noexcept {
		const lsn_t lsn = *this;
		SetCounter(Counter() + 1);
		return lsn;
	}

	bool operator==(lsn_t o) const noexcept { return payload_ == o.payload_; }
	bool operator!=(lsn_t o) const noexcept { return payload_ != o.payload_; }

	int64_t SetServer(int16_t server) {
		validateServerId(server);
		payload_ = server * kMaxCounter + Counter();
		return payload_;
	}
	int64_t SetCounter(int64_t counter) {
		validateCounter(counter);
		if (counter < 0) {
			counter = kDefaultCounter;
		}
		payload_ = Server() * kMaxCounter + counter;
		return payload_;
	}
	int64_t Counter() const noexcept { return payload_ % kMaxCounter; }
	int16_t Server() const noexcept { return payload_ / kMaxCounter; }
	LSNUnpacked Unpack() const noexcept { return {.server = Server(), .counter = Counter()}; }
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
			throwValidation(errLogic, "Server id (%d) < %d", server, kMinServerIDValue);
		}
		if (server > kMaxServerIDValue) {
			throwValidation(errLogic, "Server id (%d) > %d", server, kMaxServerIDValue);
		}
	}
	static void validateCounter(int64_t counter) {
		if (counter > kDefaultCounter) {
			throwValidation(errLogic, "LSN Counter (%d) > Default LSN (%d)", counter, kMaxCounter);
		}
	}

	[[noreturn]] static void throwValidation(ErrorCode, const char*, int64_t, int64_t);
};

inline static std::ostream& operator<<(std::ostream& o, const reindexer::lsn_t& sv) {
	o << sv.Server() << ":" << sv.Counter();
	return o;
}

class ExtendedLsn {
public:
	ExtendedLsn() = default;
	ExtendedLsn(lsn_t nsVersion, lsn_t lsn) noexcept : nsVersion_(nsVersion), lsn_(lsn) {}
	lsn_t NsVersion() const noexcept { return nsVersion_; }
	lsn_t LSN() const noexcept { return lsn_; }
	bool IsCompatibleByNsVersion(ExtendedLsn r) const noexcept {
		return nsVersion_.Server() == r.nsVersion_.Server() && nsVersion_ == r.nsVersion_;
	}
	bool IsEmpty() const noexcept { return nsVersion_.isEmpty() && lsn_.isEmpty(); }
	bool HasNewerCounterThan(ExtendedLsn r) const noexcept {
		const auto rNsV = r.nsVersion_.Counter();
		const auto lNsV = nsVersion_.Counter();
		return r.lsn_.isEmpty() || r.nsVersion_.isEmpty() || (lNsV > rNsV) || (lNsV == rNsV && lsn_.Counter() > r.lsn_.Counter());
	}
	bool operator!=(ExtendedLsn r) const noexcept {
		return nsVersion_.Server() != r.nsVersion_.Server() || nsVersion_.Counter() != r.nsVersion_.Counter() ||
			   lsn_.Server() != r.lsn_.Server() || lsn_.Counter() != r.lsn_.Counter();
	}
	bool operator==(ExtendedLsn r) const noexcept { return !(*this != r); }

private:
	lsn_t nsVersion_;
	lsn_t lsn_;
};

#ifdef REINDEX_WITH_V3_FOLLOWERS
struct LSNPair {
	LSNPair() = default;
	LSNPair(lsn_t upstreamLSN, lsn_t originLSN) noexcept : upstreamLSN_(upstreamLSN), originLSN_(originLSN) {}
	lsn_t upstreamLSN_;
	lsn_t originLSN_;
};
#endif	// REINDEX_WITH_V3_FOLLOWERS

}  // namespace reindexer
