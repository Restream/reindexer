#pragma once

#include <memory.h>
#include <unordered_set>
#include "core/index/payload_map.h"
#include "core/index/string_map.h"
#include "core/keyvalue/geometry.h"
#include "core/keyvalue/p_string.h"
#include "core/payload/fieldsset.h"
#include "estl/intrusive_ptr.h"
#include "tools/string_regexp_functions.h"
#include "vendor/hopscotch/hopscotch_sc_set.h"

namespace reindexer {

class ComparatorVars {
public:
	ComparatorVars(CondType cond, KeyValueType type, bool isArray, PayloadType payloadType, const FieldsSet &fields, void *rawData,
				   const CollateOpts &collateOpts)
		: cond_(cond),
		  type_(type),
		  isArray_(isArray),
		  rawData_(reinterpret_cast<uint8_t *>(rawData)),
		  collateOpts_(collateOpts),
		  payloadType_(std::move(payloadType)),
		  fields_(fields) {}
	ComparatorVars() = delete;

	CondType cond_ = CondEq;
	KeyValueType type_;
	bool isArray_ = false;
	unsigned offset_ = 0;
	unsigned sizeof_ = 0;
	uint8_t *rawData_ = nullptr;
	CollateOpts collateOpts_;
	PayloadType payloadType_;
	FieldsSet fields_;
};

template <class T>
class ComparatorImpl {
	using ValuesSet = intrusive_atomic_rc_wrapper<std::unordered_set<T>>;
	using AllSetValuesSet = intrusive_atomic_rc_wrapper<std::unordered_set<const T *>>;

public:
	ComparatorImpl(bool distinct = false) : distS_(distinct ? new ValuesSet : nullptr) {}

	void SetValues(CondType cond, const VariantArray &values) {
		if (cond == CondSet) {
			valuesS_.reset(new ValuesSet{});
		} else if (cond == CondAllSet) {
			valuesS_.reset(new ValuesSet{});
			allSetValuesS_.reset(new AllSetValuesSet{});
		}

		for (Variant key : values) {
			key.Type().EvaluateOneOf([](OneOf<KeyValueType::String, KeyValueType::Uuid>) {},
									 [&](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
											   KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null>) {
										 key.convert(type());
										 addValue(cond, static_cast<T>(key));
									 });
		}
	}

	inline bool Compare2(CondType cond, T lhs) {
		switch (cond) {
			case CondEq:
				assertrx_throw(!values_.empty());
				return lhs == values_[0];
			case CondGe:
				assertrx_throw(!values_.empty());
				return lhs >= values_[0];
			case CondLe:
				assertrx_throw(!values_.empty());
				return lhs <= values_[0];
			case CondLt:
				assertrx_throw(!values_.empty());
				return lhs < values_[0];
			case CondGt:
				assertrx_throw(!values_.empty());
				return lhs > values_[0];
			case CondRange:
				assertrx_throw(values_.size() == 2);
				return lhs >= values_[0] && lhs <= values_[1];
			case CondSet:
				return valuesS_->find(lhs) != valuesS_->end();
			case CondAllSet: {
				const auto it = valuesS_->find(lhs);
				if (it == valuesS_->end()) return false;
				allSetValuesS_->insert(&*it);
				return allSetValuesS_->size() == valuesS_->size();
			}
			case CondAny:
				return true;
			case CondEmpty:
			case CondLike:
				return false;
			case CondDWithin:
				break;
		}
		std::abort();
	}
	bool Compare(CondType cond, T lhs) {
		bool ret = Compare2(cond, lhs);
		if (!ret || !distS_) return ret;
		return distS_->find(lhs) == distS_->end();
	}

	void ExcludeDistinct(T value) { distS_->emplace(value); }
	void ClearDistinct() {
		if (distS_) distS_->clear();
	}
	void ClearAllSetValues() {
		assertrx(allSetValuesS_);
		allSetValuesS_->clear();
	}

	h_vector<T, 1> values_;
	intrusive_ptr<ValuesSet> valuesS_, distS_;
	intrusive_ptr<AllSetValuesSet> allSetValuesS_;

private:
	KeyValueType type() {
		if constexpr (std::is_same_v<T, int>)
			return KeyValueType::Int{};
		else if constexpr (std::is_same_v<T, bool>)
			return KeyValueType::Bool{};
		else if constexpr (std::is_same_v<T, int64_t>)
			return KeyValueType::Int64{};
		else if constexpr (std::is_same_v<T, double>)
			return KeyValueType::Double{};
		else if constexpr (std::is_same_v<T, Uuid>)
			return KeyValueType::Uuid{};
		else {
			static_assert(std::is_same_v<T, int>, "Unknown KeyValueType");
		}
	}

	void addValue(CondType cond, T value) {
		if (cond == CondSet || cond == CondAllSet) {
			valuesS_->emplace(value);
		} else {
			values_.emplace_back(value);
		}
	}
};

template <>
class ComparatorImpl<Uuid> {
	using ValuesSet = intrusive_atomic_rc_wrapper<std::unordered_set<Uuid>>;
	using AllSetValuesSet = intrusive_atomic_rc_wrapper<std::unordered_set<const Uuid *>>;

public:
	ComparatorImpl(bool distinct = false) : distS_(distinct ? new ValuesSet : nullptr) {}

	void SetValues(CondType cond, const VariantArray &values) {
		if (cond == CondSet) {
			valuesS_.reset(new ValuesSet{});
		} else if (cond == CondAllSet) {
			valuesS_.reset(new ValuesSet{});
			allSetValuesS_.reset(new AllSetValuesSet{});
		}

		for (const Variant &key : values) {
			key.Type().EvaluateOneOf(
				overloaded{[&](KeyValueType::Uuid) { addValue(cond, key.As<Uuid>()); },
						   [&](KeyValueType::String) {
							   const auto uuid{Uuid::TryParse(key.As<p_string>())};
							   if (uuid) {
								   addValue(cond, *uuid);
							   }
						   },
						   [](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Bool,
									KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::Null, KeyValueType::Tuple>) {}});
		}
	}

	inline bool Compare2(CondType cond, Uuid lhs) {
		switch (cond) {
			case CondEq:
				assertrx_throw(!values_.empty());
				return lhs == values_[0];
			case CondGe:
				assertrx_throw(!values_.empty());
				return lhs >= values_[0];
			case CondLe:
				assertrx_throw(!values_.empty());
				return lhs <= values_[0];
			case CondLt:
				assertrx_throw(!values_.empty());
				return lhs < values_[0];
			case CondGt:
				assertrx_throw(!values_.empty());
				return lhs > values_[0];
			case CondRange:
				assertrx_throw(values_.size() >= 2);
				return lhs >= values_[0] && lhs <= values_[1];
			case CondSet:
				return valuesS_->find(lhs) != valuesS_->end();
			case CondAllSet: {
				const auto it = valuesS_->find(lhs);
				if (it == valuesS_->end()) return false;
				allSetValuesS_->insert(&*it);
				return allSetValuesS_->size() == valuesS_->size();
			}
			case CondAny:
				return true;
			case CondEmpty:
			case CondLike:
				return false;
			case CondDWithin:
				break;
		}
		std::abort();
	}
	bool Compare(CondType cond, Uuid lhs) {
		bool ret = Compare2(cond, lhs);
		if (!ret || !distS_) return ret;
		return distS_->find(lhs) == distS_->end();
	}

	void ExcludeDistinct(Uuid value) { distS_->emplace(value); }
	void ClearDistinct() {
		if (distS_) distS_->clear();
	}
	void ClearAllSetValues() {
		assertrx(allSetValuesS_);
		allSetValuesS_->clear();
	}

	h_vector<Uuid, 1> values_;
	intrusive_ptr<ValuesSet> valuesS_, distS_;
	intrusive_ptr<AllSetValuesSet> allSetValuesS_;

private:
	void addValue(CondType cond, Uuid value) {
		if (cond == CondSet || cond == CondAllSet) {
			valuesS_->emplace(value);
		} else {
			values_.emplace_back(value);
		}
	}
};

template <>
class ComparatorImpl<key_string> {
public:
	ComparatorImpl(bool distinct = false) : distS_(distinct ? new intrusive_atomic_rc_wrapper<std::unordered_set<key_string>> : nullptr) {}

	void SetValues(CondType cond, const VariantArray &values, const CollateOpts &collateOpts_) {
		if (cond == CondSet) {
			valuesS_.reset(new intrusive_atomic_rc_wrapper<key_string_set>(collateOpts_));
		} else if (cond == CondAllSet) {
			valuesS_.reset(new intrusive_atomic_rc_wrapper<key_string_set>(collateOpts_));
			allSetValuesS_.reset(new intrusive_atomic_rc_wrapper<std::unordered_set<const key_string *>>{});
		}

		for (Variant key : values) {
			key.convert(KeyValueType::String{});
			addValue(cond, static_cast<key_string>(key));
		}
	}

	bool inline Compare2(CondType cond, p_string lhs, const CollateOpts &collateOpts) {
		auto rhs = cachedValueSV_;
		switch (cond) {
			case CondEq:
				return collateCompare(std::string_view(lhs), rhs, collateOpts) == 0;
			case CondGe:
				return collateCompare(std::string_view(lhs), rhs, collateOpts) >= 0;
			case CondLe:
				return collateCompare(std::string_view(lhs), rhs, collateOpts) <= 0;
			case CondLt:
				return collateCompare(std::string_view(lhs), rhs, collateOpts) < 0;
			case CondGt:
				return collateCompare(std::string_view(lhs), rhs, collateOpts) > 0;
			case CondRange:
				return collateCompare(std::string_view(lhs), rhs, collateOpts) >= 0 &&
					   collateCompare(std::string_view(lhs), std::string_view(*values_[1]), collateOpts) <= 0;
			case CondSet:
				return valuesS_->find(std::string_view(lhs)) != valuesS_->end();
			case CondAllSet: {
				auto it = valuesS_->find(lhs);
				if (it == valuesS_->end()) return false;
				allSetValuesS_->insert(&*it);
				return allSetValuesS_->size() == valuesS_->size();
			}
			case CondAny:
				return true;
			case CondEmpty:
				return false;
			case CondLike: {
				return matchLikePattern(std::string_view(lhs), rhs);
			}
			case CondDWithin:
				break;
		}
		std::abort();
	}
	bool Compare(CondType cond, p_string lhs, const CollateOpts &collateOpts) {
		bool ret = Compare2(cond, lhs, collateOpts);
		if (!ret || !distS_) return ret;
		return distS_->find(lhs.getOrMakeKeyString()) == distS_->end();
	}

	void ExcludeDistinct(p_string value) { distS_->emplace(value.getOrMakeKeyString()); }
	void ClearDistinct() {
		if (distS_) distS_->clear();
	}
	void ClearAllSetValues() {
		assertrx(allSetValuesS_);
		allSetValuesS_->clear();
	}

	h_vector<key_string, 1> values_;
	std::string_view cachedValueSV_;

	class key_string_set : public tsl::hopscotch_sc_set<key_string, hash_key_string, equal_key_string, less_key_string> {
	public:
		key_string_set(const CollateOpts &opts)
			: tsl::hopscotch_sc_set<key_string, hash_key_string, equal_key_string, less_key_string>(
				  1000, hash_key_string(CollateMode(opts.mode)), equal_key_string(opts), std::allocator<key_string>(),
				  less_key_string(opts)) {}
	};

	intrusive_ptr<intrusive_atomic_rc_wrapper<key_string_set>> valuesS_;
	intrusive_ptr<intrusive_atomic_rc_wrapper<std::unordered_set<key_string>>> distS_;
	intrusive_ptr<intrusive_atomic_rc_wrapper<std::unordered_set<const key_string *>>> allSetValuesS_;

private:
	void addValue(CondType cond, const key_string &value) {
		if (cond == CondSet || cond == CondAllSet) {
			valuesS_->emplace(value);
		} else {
			values_.emplace_back(value);
			if (values_.size() == 1) {
				cachedValueSV_ = std::string_view(*values_[0]);
			}
		}
	}
};

template <>
class ComparatorImpl<PayloadValue> {
public:
	ComparatorImpl() = default;

	void SetValues(CondType cond, const VariantArray &values, const ComparatorVars &vars) {
		if (cond == CondSet) {
			valuesSet_.reset(new intrusive_atomic_rc_wrapper<unordered_payload_set>(
				values.size(), hash_composite(vars.payloadType_, vars.fields_), equal_composite(vars.payloadType_, vars.fields_)));
		} else if (cond == CondAllSet) {
			valuesSet_.reset(new intrusive_atomic_rc_wrapper<unordered_payload_set>(
				values.size(), hash_composite(vars.payloadType_, vars.fields_), equal_composite(vars.payloadType_, vars.fields_)));
			allSetValuesSet_.reset(new intrusive_atomic_rc_wrapper<std::unordered_set<const PayloadValue *>>{});
		}

		for (const Variant &kv : values) {
			addValue(cond, static_cast<const PayloadValue &>(kv));
		}
	}

	bool Compare(CondType cond, const PayloadValue &leftValue, const ComparatorVars &vars) {
		assertrx(vars.fields_.size() > 0);
		ConstPayload lhs(vars.payloadType_, leftValue);
		switch (cond) {
			case CondEq:
				assertrx_throw(!values_.empty());
				return (lhs.Compare<WithString::Yes>(values_[0], vars.fields_, vars.collateOpts_) == 0);
			case CondGe:
				assertrx_throw(!values_.empty());
				return (lhs.Compare<WithString::Yes>(values_[0], vars.fields_, vars.collateOpts_) >= 0);
			case CondGt:
				assertrx_throw(!values_.empty());
				return (lhs.Compare<WithString::Yes>(values_[0], vars.fields_, vars.collateOpts_) > 0);
			case CondLe:
				assertrx_throw(!values_.empty());
				return (lhs.Compare<WithString::Yes>(values_[0], vars.fields_, vars.collateOpts_) <= 0);
			case CondLt:
				assertrx_throw(!values_.empty());
				return (lhs.Compare<WithString::Yes>(values_[0], vars.fields_, vars.collateOpts_) < 0);
			case CondRange:
				assertrx_throw(values_.size() == 2);
				return (lhs.Compare<WithString::Yes>(values_[0], vars.fields_, vars.collateOpts_) >= 0) &&
					   (lhs.Compare<WithString::Yes>(values_[1], vars.fields_, vars.collateOpts_) <= 0);
			case CondSet:
				assertrx_throw(!valuesSet_->empty());
				return valuesSet_->find(leftValue) != valuesSet_->end();
			case CondAllSet: {
				assertrx_throw(!valuesSet_->empty());
				auto it = valuesSet_->find(leftValue);
				if (it == valuesSet_->end()) return false;
				allSetValuesSet_->insert(&*it);
				return allSetValuesSet_->size() == valuesSet_->size();
			}
			case CondAny:
				return true;
			case CondEmpty:
			case CondLike:
				return false;
			case CondDWithin:
				break;
		}
		std::abort();
	}
	void ClearAllSetValues() {
		assertrx(allSetValuesSet_);
		allSetValuesSet_->clear();
	}

	h_vector<PayloadValue, 1> values_;
	intrusive_ptr<intrusive_atomic_rc_wrapper<unordered_payload_set>> valuesSet_;
	intrusive_ptr<intrusive_atomic_rc_wrapper<std::unordered_set<const PayloadValue *>>> allSetValuesSet_;

private:
	void addValue(CondType cond, const PayloadValue &pv) {
		if (cond == CondSet || cond == CondAllSet) {
			valuesSet_->emplace(pv);
		} else {
			values_.emplace_back(pv);
		}
	}
};

template <>
class ComparatorImpl<Point> {
public:
	ComparatorImpl(bool distinct = false)
		: distS_(distinct ? new intrusive_atomic_rc_wrapper<std::unordered_set<Point>> : nullptr), rhs_{}, distance_{} {}

	void SetValues(const VariantArray &values) {
		if (values.size() != 2) throw Error(errQueryExec, "CondDWithin expects two arguments");
		if (values[0].Type().Is<KeyValueType::Tuple>()) {
			rhs_ = values[0].As<Point>();
			distance_ = values[1].As<double>();
		} else {
			rhs_ = values[1].As<Point>();
			distance_ = values[0].As<double>();
		}
	}

	bool inline Compare2(Point lhs) const noexcept { return DWithin(lhs, rhs_, distance_); }
	bool Compare(Point lhs) {
		bool ret = Compare2(lhs);
		if (!ret || !distS_) return ret;
		return distS_->find(lhs) == distS_->end();
	}

	void ExcludeDistinct(Point value) { distS_->emplace(value); }
	void ClearDistinct() {
		if (distS_) distS_->clear();
	}

	intrusive_ptr<intrusive_atomic_rc_wrapper<std::unordered_set<Point>>> distS_;

private:
	Point rhs_;
	double distance_;
};

}  // namespace reindexer
