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
	static const int64_t digitCountLSNMult = 1000000000000000ll;

	static const int64_t kCounterbitCount = 48;
	static const int64_t kCounterMask = (1ull << kCounterbitCount) - 1ull;

	void GetJSON(JsonBuilder &builder) const;

	void FromJSON(const gason::JsonNode &root) {
		int server = root["server_id"].As<int>(0);
		int64_t counter = root["counter"].As<int64_t>(digitCountLSNMult - 1ll);
		payload_ = int64_t(lsn_t(counter, server));
	}

	lsn_t() {}
	explicit lsn_t(int64_t v) {
		if ((v & kCounterMask) == kCounterMask)	 // init -1
			payload_ = digitCountLSNMult - 1ll;
		else {
			payload_ = v;
		}
	}
	lsn_t(int64_t counter, uint8_t server) {
		if ((counter & kCounterMask) == kCounterMask) counter = digitCountLSNMult - 1ll;
		int64_t s = server * digitCountLSNMult;
		payload_ = s + counter;
	}
	explicit operator int64_t() const { return payload_; }

	bool operator==(lsn_t o) { return payload_ == o.payload_; }
	bool operator!=(lsn_t o) { return payload_ != o.payload_; }

	int64_t SetServer(short s) {
		if (s > 999) throw Error(errLogic, "Server id > 999");
		int64_t server = s * digitCountLSNMult;
		int64_t serverOld = payload_ / digitCountLSNMult;
		payload_ = payload_ - serverOld * digitCountLSNMult + server;
		return payload_;
	}
	int64_t SetCounter(int64_t c) {
		if (c >= digitCountLSNMult) throw Error(errLogic, "LSN Counter > digitCountLSNMult");
		int64_t server = payload_ / digitCountLSNMult;
		payload_ = server * digitCountLSNMult + c;
		return payload_;
	}
	int64_t Counter() const {
		int64_t server = payload_ / digitCountLSNMult;
		return payload_ - server * digitCountLSNMult;
	}
	short Server() const { return payload_ / digitCountLSNMult; }
	bool isEmpty() const { return Counter() == digitCountLSNMult - 1ll; }

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
	int64_t payload_ = digitCountLSNMult - 1ll;
};

struct LSNPair {
	LSNPair() {}
	LSNPair(lsn_t upstreamLSN, lsn_t originLSN) : upstreamLSN_(upstreamLSN), originLSN_(originLSN) {}
	lsn_t upstreamLSN_;
	lsn_t originLSN_;
};

inline static std::ostream &operator<<(std::ostream &o, const reindexer::lsn_t &sv) {
	o << sv.Server() << ":" << sv.Counter();
	return o;
}
}  // namespace reindexer
