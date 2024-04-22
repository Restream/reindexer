#pragma once

#include <memory.h>
#include <unordered_set>
#include "core/index/string_map.h"
#include "core/keyvalue/geometry.h"
#include "core/keyvalue/p_string.h"
#include "estl/intrusive_ptr.h"
#include "estl/one_of.h"
#include "tools/string_regexp_functions.h"
#include "vendor/hopscotch/hopscotch_sc_set.h"

namespace reindexer {

template <class T>
class EqualPositionComparatorImpl {
	using ValuesSet = intrusive_atomic_rc_wrapper<std::unordered_set<T>>;
	using AllSetValuesSet = intrusive_atomic_rc_wrapper<std::unordered_set<const T *>>;

public:
	EqualPositionComparatorImpl() noexcept = default;

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

	inline bool Compare(CondType cond, T lhs) {
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

	void ClearAllSetValues() {
		assertrx(allSetValuesS_);
		allSetValuesS_->clear();
	}

	h_vector<T, 2> values_;
	intrusive_ptr<ValuesSet> valuesS_;
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
class EqualPositionComparatorImpl<Uuid> {
	using ValuesSet = intrusive_atomic_rc_wrapper<std::unordered_set<Uuid>>;
	using AllSetValuesSet = intrusive_atomic_rc_wrapper<std::unordered_set<const Uuid *>>;

public:
	EqualPositionComparatorImpl() noexcept = default;

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

	inline bool Compare(CondType cond, Uuid lhs) {
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
	void ClearAllSetValues() {
		assertrx(allSetValuesS_);
		allSetValuesS_->clear();
	}

	h_vector<Uuid, 1> values_;
	intrusive_ptr<ValuesSet> valuesS_;
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
class EqualPositionComparatorImpl<key_string> {
public:
	EqualPositionComparatorImpl(const CollateOpts &collate) : collate_{collate} {}

	void SetValues(CondType cond, const VariantArray &values) {
		if (cond == CondSet) {
			valuesS_ = make_intrusive<intrusive_rc_wrapper<key_string_set>>(collate_);
		} else if (cond == CondAllSet) {
			valuesS_ = make_intrusive<intrusive_rc_wrapper<key_string_set>>(collate_);
			allSetValuesS_ = make_intrusive<intrusive_rc_wrapper<std::unordered_set<const key_string *>>>();
		}

		for (Variant key : values) {
			key.convert(KeyValueType::String{});
			addValue(cond, static_cast<key_string>(key));
		}
	}

	bool inline Compare(CondType cond, p_string lhs) {
		auto rhs = cachedValueSV_;
		switch (cond) {
			case CondEq:
				return collateCompare(std::string_view(lhs), rhs, collate_) == ComparationResult::Eq;
			case CondGe:
				return collateCompare(std::string_view(lhs), rhs, collate_) & ComparationResult::Ge;
			case CondLe:
				return collateCompare(std::string_view(lhs), rhs, collate_) & ComparationResult::Le;
			case CondLt:
				return collateCompare(std::string_view(lhs), rhs, collate_) == ComparationResult::Lt;
			case CondGt:
				return collateCompare(std::string_view(lhs), rhs, collate_) == ComparationResult::Gt;
			case CondRange:
				return (collateCompare(std::string_view(lhs), rhs, collate_) & ComparationResult::Ge) &&
					   (collateCompare(std::string_view(lhs), std::string_view(*values_[1]), collate_) & ComparationResult::Le);
			case CondSet:
				assertrx_dbg(valuesS_);
				return valuesS_->find(std::string_view(lhs)) != valuesS_->end();
			case CondAllSet: {
				assertrx_dbg(valuesS_);
				assertrx_dbg(allSetValuesS_);
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
	void ClearAllSetValues() {
		assertrx_dbg(allSetValuesS_);
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

	intrusive_ptr<intrusive_rc_wrapper<key_string_set>> valuesS_;
	intrusive_ptr<intrusive_rc_wrapper<std::unordered_set<const key_string *>>> allSetValuesS_;

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
	CollateOpts collate_;
};

template <>
class EqualPositionComparatorImpl<PayloadValue> {
public:
	EqualPositionComparatorImpl() = delete;
};

template <>
class EqualPositionComparatorImpl<Point> {
public:
	EqualPositionComparatorImpl() noexcept = default;

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

	bool inline Compare(Point lhs) const noexcept { return DWithin(lhs, rhs_, distance_); }

private:
	Point rhs_{};
	double distance_{};
};

}  // namespace reindexer
