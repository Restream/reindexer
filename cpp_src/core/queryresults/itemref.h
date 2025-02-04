#pragma once

#include "core/payload/payloadvalue.h"
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "tools/assertrx.h"

namespace reindexer {

static const int kDefaultQueryResultsSize = 32;
class ItemRef {
public:
	ItemRef() noexcept : id_(0), proc_(0), raw_(0), valueInitialized_(false), nsid_(0) {}
	ItemRef(IdType id, const PayloadValue& value, uint16_t proc = 0, uint16_t nsid = 0, bool raw = false) noexcept
		: id_(id), proc_(proc), raw_(raw), valueInitialized_(true), nsid_(nsid), value_(value) {}
	ItemRef(IdType id, unsigned sortExprResultsIdx, uint16_t proc = 0, uint16_t nsid = 0) noexcept
		: id_(id), proc_(proc), raw_(0), valueInitialized_(false), nsid_(nsid), sortExprResultsIdx_(sortExprResultsIdx) {}
	ItemRef(ItemRef&& other) noexcept
		: id_(other.id_),
		  proc_(other.proc_),
		  raw_(other.raw_),
		  valueInitialized_(other.valueInitialized_),
		  nsid_(other.nsid_),
		  sortExprResultsIdx_(other.sortExprResultsIdx_) {
		if (valueInitialized_) {
			new (&value_) PayloadValue(std::move(other.value_));
		}
	}
	ItemRef(const ItemRef& other) noexcept
		: id_(other.id_),
		  proc_(other.proc_),
		  raw_(other.raw_),
		  valueInitialized_(other.valueInitialized_),
		  nsid_(other.nsid_),
		  sortExprResultsIdx_(other.sortExprResultsIdx_) {
		if (valueInitialized_) {
			new (&value_) PayloadValue(other.value_);
		}
	}
	ItemRef& operator=(ItemRef&& other) noexcept {
		if (&other == this) {
			return *this;
		}
		id_ = other.id_;
		proc_ = other.proc_;
		raw_ = other.raw_;
		nsid_ = other.nsid_;
		if (valueInitialized_) {
			if (other.valueInitialized_) {
				value_ = std::move(other.value_);
			} else {
				value_.~PayloadValue();
				sortExprResultsIdx_ = other.sortExprResultsIdx_;
			}
		} else {
			if (other.valueInitialized_) {
				new (&value_) PayloadValue(std::move(other.value_));
			} else {
				sortExprResultsIdx_ = other.sortExprResultsIdx_;
			}
		}
		valueInitialized_ = other.valueInitialized_;
		return *this;
	}
	ItemRef& operator=(const ItemRef& other) noexcept {
		if (&other == this) {
			return *this;
		}
		id_ = other.id_;
		proc_ = other.proc_;
		raw_ = other.raw_;
		nsid_ = other.nsid_;
		if (valueInitialized_) {
			if (other.valueInitialized_) {
				value_ = other.value_;
			} else {
				value_.~PayloadValue();
				sortExprResultsIdx_ = other.sortExprResultsIdx_;
			}
		} else {
			if (other.valueInitialized_) {
				new (&value_) PayloadValue(other.value_);
			} else {
				sortExprResultsIdx_ = other.sortExprResultsIdx_;
			}
		}
		valueInitialized_ = other.valueInitialized_;
		return *this;
	}
	~ItemRef() {
		if (valueInitialized_) {
			value_.~PayloadValue();
		}
	}

	IdType Id() const noexcept { return id_; }
	uint16_t Nsid() const noexcept { return nsid_; }
	uint16_t Proc() const noexcept { return proc_; }
	bool Raw() const noexcept { return raw_; }
	const PayloadValue& Value() const noexcept {
		assertrx(valueInitialized_);
		return value_;
	}
	PayloadValue& Value() noexcept {
		assertrx(valueInitialized_);
		return value_;
	}
	unsigned SortExprResultsIdx() const noexcept {
		assertrx(!valueInitialized_);
		return sortExprResultsIdx_;
	}
	void SetValue(PayloadValue&& value) noexcept {
		assertrx(!valueInitialized_);
		new (&value_) PayloadValue(std::move(value));
		valueInitialized_ = true;
	}
	void SetValue(const PayloadValue& value) noexcept {
		assertrx(!valueInitialized_);
		new (&value_) PayloadValue(value);
		valueInitialized_ = true;
	}
	bool ValueInitialized() const noexcept { return valueInitialized_; }

private:
	IdType id_ = 0;
	uint16_t proc_ : 14;
	uint16_t raw_ : 1;
	uint16_t valueInitialized_ : 1;
	uint16_t nsid_ = 0;
	union {
		PayloadValue value_;
		unsigned sortExprResultsIdx_ = 0u;
	};
};

using ItemRefVector = h_vector<ItemRef, kDefaultQueryResultsSize>;
}  // namespace reindexer
