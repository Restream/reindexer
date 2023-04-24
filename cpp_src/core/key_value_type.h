#pragma once

#include <cstdlib>
#include <string_view>
#include <utility>
#include "core/type_consts.h"
#include "estl/overloaded.h"
#include "tools/assertrx.h"
#include "tools/errors.h"

namespace reindexer {

class KeyValueType {
public:
	// Change name of function Is<> when add new type
	struct Int64 {};
	struct Double {};
	struct String {};
	struct Bool {};
	struct Null {};
	struct Int {};
	struct Undefined {};
	struct Composite {};
	struct Tuple {};

private:
	template <template <typename> typename T>
	static constexpr bool ForAllTypes = T<Int64>::value && T<Double>::value && T<String>::value && T<Bool>::value && T<Null>::value &&
										T<Int>::value && T<Undefined>::value && T<Composite>::value && T<Tuple>::value;
	template <template <typename> typename T>
	static constexpr bool ForAnyType = T<Int64>::value || T<Double>::value || T<String>::value || T<Bool>::value || T<Null>::value ||
									   T<Int>::value || T<Undefined>::value || T<Composite>::value || T<Tuple>::value;

	template <typename F, typename... Fs>
	struct IsNoexcept {
		template <typename T>
		struct Overloaded {
			static constexpr bool value = noexcept(std::declval<overloaded<F, Fs...>>()(std::declval<T>()));
		};
	};
	template <typename F>
	struct IsNoexcept<F> {
		template <typename T>
		struct Overloaded {
			static constexpr bool value = noexcept(std::declval<overloaded<F>>()(std::declval<T>()));
		};
	};

	template <typename... Fs>
	struct OneOf {
		template <typename A>
		struct IsInvocable {
			static constexpr bool value = std::disjunction_v<std::is_invocable<Fs, A>...>;
		};
	};

	template <typename T, typename Visitor>
	class VisitorWrapper {
	public:
		explicit VisitorWrapper(Visitor& v) noexcept : visitor_{v} {}
		template <typename... Ts>
		auto operator()(Ts... vs) const noexcept(noexcept(std::declval<Visitor>()(Ts{}..., T{}))) {
			return visitor_(vs..., T{});
		}

	private:
		Visitor& visitor_;
	};

	enum class KVT {
		Int64 = TAG_VARINT,
		Double = TAG_DOUBLE,
		String = TAG_STRING,
		Bool = TAG_BOOL,
		Null = TAG_NULL,
		Int = TAG_END + 1,
		Undefined,
		Composite,
		Tuple
	} value_;
	constexpr explicit KeyValueType(KVT v) noexcept : value_{v} {}

public:
	KeyValueType() = default;
	constexpr KeyValueType(Int64) noexcept : value_{KVT::Int64} {}
	constexpr KeyValueType(Double) noexcept : value_{KVT::Double} {}
	constexpr KeyValueType(String) noexcept : value_{KVT::String} {}
	constexpr KeyValueType(Bool) noexcept : value_{KVT::Bool} {}
	constexpr KeyValueType(Null) noexcept : value_{KVT::Null} {}
	constexpr KeyValueType(Int) noexcept : value_{KVT::Int} {}
	constexpr KeyValueType(Undefined) noexcept : value_{KVT::Undefined} {}
	constexpr KeyValueType(Composite) noexcept : value_{KVT::Composite} {}
	constexpr KeyValueType(Tuple) noexcept : value_{KVT::Tuple} {}
	constexpr KeyValueType(const KeyValueType& other) noexcept : value_{other.value_} {}
	constexpr KeyValueType& operator=(const KeyValueType& other) noexcept {
		value_ = other.value_;
		return *this;
	}

	template <typename... Fs>
	inline auto EvaluateOneOf(overloaded<Fs...> f) const noexcept(ForAllTypes<IsNoexcept<Fs...>::template Overloaded>) {
		static_assert(ForAnyType<OneOf<Fs...>::template IsInvocable>);
		switch (value_) {
			case KVT::Int64:
				return f(Int64{});
			case KVT::Double:
				return f(Double{});
			case KVT::String:
				return f(String{});
			case KVT::Bool:
				return f(Bool{});
			case KVT::Null:
				return f(Null{});
			case KVT::Int:
				return f(Int{});
			case KVT::Undefined:
				return f(Undefined{});
			case KVT::Composite:
				return f(Composite{});
			case KVT::Tuple:
				return f(Tuple{});
		}
		assertrx(0);
		std::abort();
	}
	template <typename... Fs>
	inline auto EvaluateOneOf(Fs... fs) const noexcept(ForAllTypes<IsNoexcept<Fs...>::template Overloaded>) {
		return EvaluateOneOf(overloaded<Fs...>{std::move(fs)...});
	}
	template <typename Visitor>
	static inline auto Visit(Visitor visitor, KeyValueType t) noexcept(ForAllTypes<IsNoexcept<Visitor>::template Overloaded>) {
		return t.EvaluateOneOf(std::move(visitor));
	}
	template <typename Visitor>
	static inline auto Visit(Visitor visitor, KeyValueType t1, KeyValueType t2) noexcept {
		switch (t2.value_) {
			case KVT::Int64:
				static_assert(ForAllTypes<IsNoexcept<VisitorWrapper<Int64, Visitor>>::template Overloaded>);
				return Visit(VisitorWrapper<Int64, Visitor>{visitor}, t1);
			case KVT::Double:
				static_assert(ForAllTypes<IsNoexcept<VisitorWrapper<Double, Visitor>>::template Overloaded>);
				return Visit(VisitorWrapper<Double, Visitor>{visitor}, t1);
			case KVT::String:
				static_assert(ForAllTypes<IsNoexcept<VisitorWrapper<String, Visitor>>::template Overloaded>);
				return Visit(VisitorWrapper<String, Visitor>{visitor}, t1);
			case KVT::Bool:
				static_assert(ForAllTypes<IsNoexcept<VisitorWrapper<Bool, Visitor>>::template Overloaded>);
				return Visit(VisitorWrapper<Bool, Visitor>{visitor}, t1);
			case KVT::Null:
				static_assert(ForAllTypes<IsNoexcept<VisitorWrapper<Null, Visitor>>::template Overloaded>);
				return Visit(VisitorWrapper<Null, Visitor>{visitor}, t1);
			case KVT::Int:
				static_assert(ForAllTypes<IsNoexcept<VisitorWrapper<Int, Visitor>>::template Overloaded>);
				return Visit(VisitorWrapper<Int, Visitor>{visitor}, t1);
			case KVT::Undefined:
				static_assert(ForAllTypes<IsNoexcept<VisitorWrapper<Undefined, Visitor>>::template Overloaded>);
				return Visit(VisitorWrapper<Undefined, Visitor>{visitor}, t1);
			case KVT::Composite:
				static_assert(ForAllTypes<IsNoexcept<VisitorWrapper<Composite, Visitor>>::template Overloaded>);
				return Visit(VisitorWrapper<Composite, Visitor>{visitor}, t1);
			case KVT::Tuple:
				static_assert(ForAllTypes<IsNoexcept<VisitorWrapper<Tuple, Visitor>>::template Overloaded>);
				return Visit(VisitorWrapper<Tuple, Visitor>{visitor}, t1);
		}
		assertrx(0);
		std::abort();
	}

	template <typename T>
	inline bool Is() const noexcept {
		static constexpr KeyValueType v{T{}};
		return v.value_ == value_;
	}
	inline bool IsSame(KeyValueType other) const noexcept { return value_ == other.value_; }
	inline static KeyValueType FromNumber(int n) {
		switch (n) {
			case static_cast<int>(KVT::Int64):
			case static_cast<int>(KVT::Double):
			case static_cast<int>(KVT::String):
			case static_cast<int>(KVT::Bool):
			case static_cast<int>(KVT::Null):
			case static_cast<int>(KVT::Int):
			case static_cast<int>(KVT::Undefined):
			case static_cast<int>(KVT::Composite):
			case static_cast<int>(KVT::Tuple):
				return KeyValueType{static_cast<KVT>(n)};
			default:
				throw Error(errParams, "Invalid int value for KeyValueType: " + std::to_string(n));
		}
	}
	inline bool IsNumeric() const noexcept {
		switch (value_) {
			case KVT::Int64:
			case KVT::Double:
			case KVT::Int:
			case KVT::Bool:
				return true;
			case KVT::String:
			case KVT::Null:
			case KVT::Undefined:
			case KVT::Composite:
			case KVT::Tuple:
				return false;
		}
		assertrx(0);
		std::abort();
	}
	inline int ToNumber() const noexcept { return static_cast<int>(value_); }
	inline std::string_view Name() const noexcept {
		using namespace std::string_view_literals;
		switch (value_) {
			case KVT::Int64:
				return "int64"sv;
			case KVT::Double:
				return "double"sv;
			case KVT::String:
				return "string"sv;
			case KVT::Bool:
				return "bool"sv;
			case KVT::Null:
				return "null"sv;
			case KVT::Int:
				return "int"sv;
			case KVT::Undefined:
				return "undefined"sv;
			case KVT::Composite:
				return "composite"sv;
			case KVT::Tuple:
				return "tuple"sv;
		}
		assertrx(0);
		std::abort();
	}
};

}  // namespace reindexer
