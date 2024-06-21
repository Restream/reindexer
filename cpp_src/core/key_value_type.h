#pragma once

#include <cstdlib>
#include <string_view>
#include <utility>
#include "core/type_consts.h"
#include "core/type_consts_helpers.h"
#include "estl/overloaded.h"
#include "tools/assertrx.h"
#include "tools/errors.h"

namespace reindexer {

class KeyValueType {
	friend class Serializer;
	friend class WrSerializer;

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
	struct Uuid {};

private:
	template <template <typename> typename T>
	static constexpr bool ForAllTypes = T<Int64>::value && T<Double>::value && T<String>::value && T<Bool>::value && T<Null>::value &&
										T<Int>::value && T<Undefined>::value && T<Composite>::value && T<Tuple>::value && T<Uuid>::value;
	template <template <typename> typename T>
	static constexpr bool ForAnyType = T<Int64>::value || T<Double>::value || T<String>::value || T<Bool>::value || T<Null>::value ||
									   T<Int>::value || T<Undefined>::value || T<Composite>::value || T<Tuple>::value || T<Uuid>::value;

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
		explicit RX_ALWAYS_INLINE VisitorWrapper(Visitor& v) noexcept : visitor_{v} {}
		template <typename... Ts>
		RX_ALWAYS_INLINE auto operator()(Ts... vs) const noexcept(noexcept(std::declval<Visitor>()(Ts{}..., T{}))) {
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
		Tuple,
		Uuid
	} value_{KVT::Undefined};
	RX_ALWAYS_INLINE constexpr explicit KeyValueType(KVT v) noexcept : value_{v} {}

	[[nodiscard]] RX_ALWAYS_INLINE static KeyValueType fromNumber(int n) {
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
			case static_cast<int>(KVT::Uuid):
				return KeyValueType{static_cast<KVT>(n)};
			default:
				throw Error(errParams, "Invalid int value for KeyValueType: " + std::to_string(n));
		}
	}
	[[nodiscard]] RX_ALWAYS_INLINE int toNumber() const noexcept { return static_cast<int>(value_); }

public:
	RX_ALWAYS_INLINE constexpr KeyValueType(Int64) noexcept : value_{KVT::Int64} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Double) noexcept : value_{KVT::Double} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(String) noexcept : value_{KVT::String} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Bool) noexcept : value_{KVT::Bool} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Null) noexcept : value_{KVT::Null} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Int) noexcept : value_{KVT::Int} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Undefined) noexcept : value_{KVT::Undefined} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Composite) noexcept : value_{KVT::Composite} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Tuple) noexcept : value_{KVT::Tuple} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Uuid) noexcept : value_{KVT::Uuid} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(const KeyValueType&) noexcept = default;
	RX_ALWAYS_INLINE constexpr KeyValueType& operator=(const KeyValueType&) noexcept = default;
	RX_ALWAYS_INLINE constexpr KeyValueType(KeyValueType&&) noexcept = default;
	RX_ALWAYS_INLINE constexpr KeyValueType& operator=(KeyValueType&&) noexcept = default;
	RX_ALWAYS_INLINE explicit KeyValueType(TagType t) {
		switch (t) {
			case TAG_VARINT:
				value_ = KVT::Int64;
				return;
			case TAG_NULL:
				value_ = KVT::Null;
				return;
			case TAG_BOOL:
				value_ = KVT::Bool;
				return;
			case TAG_STRING:
				value_ = KVT::String;
				return;
			case TAG_DOUBLE:
				value_ = KVT::Double;
				return;
			case TAG_UUID:
				value_ = KVT::Uuid;
				return;
			case TAG_ARRAY:
			case TAG_OBJECT:
			case TAG_END:
				break;
		}
		throw Error(errParams, "Invalid tag type value for KeyValueType: " + std::string{TagTypeToStr(t)});
	}

	template <typename... Fs>
	[[nodiscard]] RX_ALWAYS_INLINE auto EvaluateOneOf(overloaded<Fs...> f) const
		noexcept(ForAllTypes<IsNoexcept<Fs...>::template Overloaded>) {
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
			case KVT::Uuid:
				return f(Uuid{});
		}
		assertrx(0);
		std::abort();
	}
	template <typename... Fs>
	[[nodiscard]] RX_ALWAYS_INLINE auto EvaluateOneOf(Fs... fs) const noexcept(ForAllTypes<IsNoexcept<Fs...>::template Overloaded>) {
		return EvaluateOneOf(overloaded<Fs...>{std::move(fs)...});
	}
	template <typename Visitor>
	RX_ALWAYS_INLINE static auto Visit(Visitor visitor, KeyValueType t) noexcept(ForAllTypes<IsNoexcept<Visitor>::template Overloaded>) {
		return t.EvaluateOneOf(std::move(visitor));
	}
	template <typename Visitor>
	RX_ALWAYS_INLINE static auto Visit(Visitor visitor, KeyValueType t1, KeyValueType t2) noexcept {
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
			case KVT::Uuid:
				static_assert(ForAllTypes<IsNoexcept<VisitorWrapper<Uuid, Visitor>>::template Overloaded>);
				return Visit(VisitorWrapper<Uuid, Visitor>{visitor}, t1);
		}
		assertrx(0);
		std::abort();
	}

	template <typename T>
	[[nodiscard]] RX_ALWAYS_INLINE bool Is() const noexcept {
		static constexpr KeyValueType v{T{}};
		return v.value_ == value_;
	}
	[[nodiscard]] RX_ALWAYS_INLINE bool IsSame(KeyValueType other) const noexcept { return value_ == other.value_; }
	[[nodiscard]] RX_ALWAYS_INLINE TagType ToTagType() const noexcept {
		switch (value_) {
			case KVT::Int64:
			case KVT::Int:
				return TAG_VARINT;
			case KVT::Double:
				return TAG_DOUBLE;
			case KVT::String:
				return TAG_STRING;
			case KVT::Bool:
				return TAG_BOOL;
			case KVT::Null:
				return TAG_NULL;
			case KVT::Uuid:
				return TAG_UUID;
			case KVT::Undefined:
			case KVT::Composite:
			case KVT::Tuple:
				break;
		}
		assertrx(0);
		std::abort();
	}
	[[nodiscard]] RX_ALWAYS_INLINE bool IsNumeric() const noexcept {
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
			case KVT::Uuid:
				return false;
		}
		assertrx(0);
		std::abort();
	}
	[[nodiscard]] std::string_view Name() const noexcept {
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
			case KVT::Uuid:
				return "uuid"sv;
		}
		assertrx(0);
		std::abort();
	}

	template <typename T>
	static KeyValueType From();
};

class key_string;
class Uuid;

template <>
RX_ALWAYS_INLINE KeyValueType KeyValueType::From<bool>() {
	return KeyValueType::Bool{};
}

template <>
RX_ALWAYS_INLINE KeyValueType KeyValueType::From<int>() {
	return KeyValueType::Int{};
}

template <>
RX_ALWAYS_INLINE KeyValueType KeyValueType::From<int64_t>() {
	return KeyValueType::Int64{};
}

template <>
RX_ALWAYS_INLINE KeyValueType KeyValueType::From<double>() {
	return KeyValueType::Double{};
}

template <>
RX_ALWAYS_INLINE KeyValueType KeyValueType::From<key_string>() {
	return KeyValueType::String{};
}

template <>
RX_ALWAYS_INLINE KeyValueType KeyValueType::From<Uuid>() {
	return KeyValueType::Uuid{};
}

}  // namespace reindexer
