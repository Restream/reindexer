#pragma once

#include <stdint.h>
#include "tools/errors.h"
#include "vendor/gason/gason.h"

namespace reindexer {

class JsonBuilder;

// lsn format
// server id  counter
// SSS        NNN NNN NNN NNN NNN (18 decimal digits)

struct LSNUnpacked {
	int16_t server;
	int64_t counter;
};

struct lsn_t {
	static constexpr int64_t kDigitCountLSNMult = 1000000000000000ll;

	static constexpr int64_t kCounterbitCount = 48;
	static constexpr int64_t kCounterMask = (1ull << kCounterbitCount) - 1ull;

	static constexpr int16_t kMinServerIDValue = 0;
	static constexpr int16_t kMaxServerIDValue = 999;

	void GetJSON(JsonBuilder &builder) const;

	void FromJSON(const gason::JsonNode &root) {
		const int server = root["server_id"].As<int>(0);
		const int64_t counter = root["counter"].As<int64_t>(kDigitCountLSNMult - 1ll);
		payload_ = int64_t(lsn_t(counter, server));
	}

	lsn_t() noexcept = default;
	lsn_t(const lsn_t &) noexcept = default;
	lsn_t(lsn_t &&) noexcept = default;
	lsn_t &operator=(const lsn_t &) noexcept = default;
	lsn_t &operator=(lsn_t &&) noexcept = default;
	explicit lsn_t(int64_t v) noexcept {
		if ((v & kCounterMask) == kCounterMask)	 // init -1
			payload_ = kDigitCountLSNMult - 1ll;
		else {
			payload_ = v;
		}
	}
	lsn_t(int64_t counter, int16_t server) {
		validateServerId(server);
		if ((counter & kCounterMask) == kCounterMask) counter = kDigitCountLSNMult - 1ll;
		const int64_t s = server * kDigitCountLSNMult;
		payload_ = s + counter;
	}
	explicit operator int64_t() const noexcept { return payload_; }
	explicit operator uint64_t() const noexcept { return static_cast<uint64_t>(payload_); }
	lsn_t &operator++() noexcept {
		SetCounter(Counter() + 1);
		return *this;
	}
	lsn_t &operator--() noexcept {
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

	int64_t SetServer(int16_t s) {
		validateServerId(s);
		const int64_t server = s * kDigitCountLSNMult;
		const int64_t serverOld = payload_ / kDigitCountLSNMult;
		payload_ = payload_ - serverOld * kDigitCountLSNMult + server;
		return payload_;
	}
	int64_t SetCounter(int64_t c) {
		if (c >= kDigitCountLSNMult) {
			throw Error(errLogic, "LSN Counter > digitCountLSNMult");
		}
		const int64_t server = payload_ / kDigitCountLSNMult;
		payload_ = server * kDigitCountLSNMult + c;
		return payload_;
	}
	int64_t Counter() const noexcept {
		const int64_t server = payload_ / kDigitCountLSNMult;
		return payload_ - server * kDigitCountLSNMult;
	}
	int16_t Server() const noexcept { return payload_ / kDigitCountLSNMult; }
	LSNUnpacked Unpack() const noexcept {
		LSNUnpacked unpacked;
		unpacked.server = payload_ / kDigitCountLSNMult;
		unpacked.counter = payload_ - int64_t(unpacked.server) * kDigitCountLSNMult;
		return unpacked;
	}
	bool isEmpty() const noexcept { return Counter() == kDigitCountLSNMult - 1ll; }

	int compare(lsn_t o) {
		if (Server() != o.Server()) throw Error(errLogic, "Compare lsn from different server");
		if (Counter() < o.Counter())
			return -1;
		else if (Counter() > o.Counter())
			return 1;
		return 0;
	}

	bool operator<(lsn_t o) { return compare(o) == -1; }
	bool operator<=(lsn_t o) { return compare(o) <= 0; }
	bool operator>(lsn_t o) { return compare(o) == 1; }
	bool operator>=(lsn_t o) { return compare(o) >= 0; }

protected:
	int64_t payload_ = kDigitCountLSNMult - 1ll;
	void validateServerId(int16_t server);
};

inline static std::ostream &operator<<(std::ostream &o, const reindexer::lsn_t &sv) {
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

}  // namespace reindexer
