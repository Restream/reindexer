#pragma once

#include "tools/errors.h"
#include "variant.h"

namespace reindexer {

template <NotComparable notComparable>
struct [[nodiscard]] RelaxedComparator {
	static bool equal(const Variant& lhs, const Variant& rhs) {
		return lhs.RelaxCompare<WithString::Yes, notComparable, kDefaultNullsHandling>(rhs) == ComparationResult::Eq;
	}
};

template <NotComparable notComparable>
struct [[nodiscard]] RelaxedHasher {
	constexpr static size_t indexesCount = notComparable == NotComparable::Return ? 8 : 7;
	static std::pair<size_t, size_t> hash(const Variant& v) noexcept(notComparable == NotComparable::Return) {
		return v.Type().EvaluateOneOf(
			[&v](KeyValueType::Bool) noexcept { return std::pair<size_t, size_t>{0, v.Hash()}; },
			[&v](KeyValueType::Int) noexcept { return std::pair<size_t, size_t>{1, v.Hash()}; },
			[&v](KeyValueType::Int64) noexcept { return std::pair<size_t, size_t>{2, v.Hash()}; },
			[&v](KeyValueType::Double) noexcept { return std::pair<size_t, size_t>{3, v.Hash()}; },
			[&v](KeyValueType::String) noexcept { return std::pair<size_t, size_t>{4, v.Hash()}; },
			[&v](KeyValueType::Uuid) noexcept { return std::pair<size_t, size_t>{5, v.Hash()}; },
			[&v](KeyValueType::Float) noexcept { return std::pair<size_t, size_t>{6, v.Hash()}; },
			[&v](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null,
								 KeyValueType::FloatVector> auto) noexcept(notComparable ==
																		   NotComparable::Return) -> std::pair<size_t, size_t> {
				if constexpr (notComparable == NotComparable::Return) {
					return {indexesCount - 1, v.Hash()};
				} else {
					throw Error{errQueryExec, "Cannot compare value of '{}' type with number, string or uuid", v.Type().Name()};
				}
			});
	}
	static size_t hash(size_t i, const Variant& v) {
		switch (i) {
			case 0:
				return v.Type().EvaluateOneOf(
					[&v](KeyValueType::Bool) noexcept { return v.Hash(); },
					[&v](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
										 KeyValueType::String> auto) {
						if constexpr (notComparable == NotComparable::Return) {
							const auto var = v.tryConvert(KeyValueType::Bool{});
							if (var) {
								return var->Hash();
							} else {
								return v.Hash();
							}
						} else {
							return v.convert(KeyValueType::Bool{}).Hash();
						}
					},
					[&v](concepts::OneOf<KeyValueType::Uuid, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite,
										 KeyValueType::Null, KeyValueType::FloatVector> auto) -> size_t {
						if constexpr (notComparable == NotComparable::Return) {
							return v.Hash();
						} else {
							throw Error{errQueryExec, "Cannot compare value of '{}' type with bool", v.Type().Name()};
						}
					});
			case 1:
				return v.Type().EvaluateOneOf(
					[&v](KeyValueType::Int) noexcept { return v.Hash(); },
					[&v](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
										 KeyValueType::String> auto) {
						if constexpr (notComparable == NotComparable::Return) {
							const auto var = v.tryConvert(KeyValueType::Int{});
							if (var) {
								return var->Hash();
							} else {
								return v.Hash();
							}
						} else {
							return v.convert(KeyValueType::Int{}).Hash();
						}
					},
					[&v](concepts::OneOf<KeyValueType::Uuid, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite,
										 KeyValueType::Null, KeyValueType::FloatVector> auto) -> size_t {
						if constexpr (notComparable == NotComparable::Return) {
							return v.Hash();
						} else {
							throw Error{errQueryExec, "Cannot compare value of '{}' type with number", v.Type().Name()};
						}
					});
			case 2:
				return v.Type().EvaluateOneOf(
					[&v](KeyValueType::Int64) noexcept { return v.Hash(); },
					[&v](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Double, KeyValueType::Float,
										 KeyValueType::String> auto) {
						if constexpr (notComparable == NotComparable::Return) {
							const auto var = v.tryConvert(KeyValueType::Int64{});
							if (var) {
								return var->Hash();
							} else {
								return v.Hash();
							}
						} else {
							return v.convert(KeyValueType::Int64{}).Hash();
						}
					},
					[&v](concepts::OneOf<KeyValueType::Uuid, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite,
										 KeyValueType::Null, KeyValueType::FloatVector> auto) -> size_t {
						if constexpr (notComparable == NotComparable::Return) {
							return v.Hash();
						} else {
							throw Error{errQueryExec, "Cannot compare value of '{}' type with number", v.Type().Name()};
						}
					});
			case 3:
				return v.Type().EvaluateOneOf(
					[&v](KeyValueType::Double) noexcept { return v.Hash(); },
					[&v](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Float,
										 KeyValueType::String> auto) {
						if constexpr (notComparable == NotComparable::Return) {
							const auto var = v.tryConvert(KeyValueType::Double{});
							if (var) {
								return var->Hash();
							} else {
								return v.Hash();
							}
						} else {
							return v.convert(KeyValueType::Double{}).Hash();
						}
					},
					[&v](concepts::OneOf<KeyValueType::Uuid, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite,
										 KeyValueType::Null, KeyValueType::FloatVector> auto) -> size_t {
						if constexpr (notComparable == NotComparable::Return) {
							return v.Hash();
						} else {
							throw Error{errQueryExec, "Cannot compare value of '{}' type with number", v.Type().Name()};
						}
					});
			case 4:
				return v.Type().EvaluateOneOf([&v](KeyValueType::String) noexcept { return v.Hash(); },
											  [&v](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64,
																   KeyValueType::Double, KeyValueType::Float, KeyValueType::Uuid> auto) {
												  if constexpr (notComparable == NotComparable::Return) {
													  const auto var = v.tryConvert(KeyValueType::String{});
													  if (var) {
														  return var->Hash();
													  } else {
														  return v.Hash();
													  }
												  } else {
													  return v.convert(KeyValueType::String{}).Hash();
												  }
											  },
											  [&v](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite,
																   KeyValueType::Null, KeyValueType::FloatVector> auto) -> size_t {
												  if constexpr (notComparable == NotComparable::Return) {
													  return v.Hash();
												  } else {
													  throw Error{errQueryExec, "Cannot compare value of '{}' type with string",
																  v.Type().Name()};
												  }
											  });
			case 5:
				return v.Type().EvaluateOneOf(
					[&v](KeyValueType::Uuid) noexcept { return v.Hash(); },
					[&v](KeyValueType::String) noexcept {
						if constexpr (notComparable == NotComparable::Return) {
							const auto var = v.tryConvert(KeyValueType::String{});
							if (var) {
								return var->Hash();
							} else {
								return v.Hash();
							}
						} else {
							return v.convert(KeyValueType::Uuid{}).Hash();
						}
					},
					[&v](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
										 KeyValueType::Float, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite,
										 KeyValueType::Null, KeyValueType::FloatVector> auto) -> size_t {
						if constexpr (notComparable == NotComparable::Return) {
							return v.Hash();
						} else {
							throw Error{errQueryExec, "Cannot compare value of '{}' type with uuid", v.Type().Name()};
						}
					});
			case 6:
				return v.Type().EvaluateOneOf(
					[&v](KeyValueType::Float) noexcept { return v.Hash(); },
					[&v](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
										 KeyValueType::String> auto) {
						if constexpr (notComparable == NotComparable::Return) {
							const auto var = v.tryConvert(KeyValueType::Float{});
							if (var) {
								return var->Hash();
							} else {
								return v.Hash();
							}
						} else {
							return v.convert(KeyValueType::Float{}).Hash();
						}
					},
					[&v](concepts::OneOf<KeyValueType::Uuid, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite,
										 KeyValueType::Null, KeyValueType::FloatVector> auto) -> size_t {
						if constexpr (notComparable == NotComparable::Return) {
							return v.Hash();
						} else {
							throw Error{errQueryExec, "Cannot compare value of '{}' type with number", v.Type().Name()};
						}
					});
			default:
				if constexpr (notComparable == NotComparable::Return) {
					return v.Hash();
				} else {
					assertrx_throw(i < indexesCount);
				}
		}
	}
};

}  // namespace reindexer
