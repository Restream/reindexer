#pragma once

#include <cstdlib>
#include <string_view>
#include <utility>
#include "core/type_consts.h"
#include "estl/defines.h"
#include "estl/overloaded.h"
#include "tools/assertrx.h"

namespace reindexer {

struct p_string;
class Uuid;
class [[nodiscard]] KeyValueType {
public:
	// When add new type change name of function Is<> and update ForAllTypes and ForAnyType
	struct [[nodiscard]] Int64 {
		using ViewType = int64_t;
		using PayloadFieldValueType = ViewType;
	};
	struct [[nodiscard]] Double {
		using ViewType = double;
		using PayloadFieldValueType = ViewType;
	};
	struct [[nodiscard]] Float {
		using ViewType = float;
		using PayloadFieldValueType = ViewType;
	};
	struct [[nodiscard]] String {
		using ViewType = std::string_view;
		using PayloadFieldValueType = p_string;
	};
	struct [[nodiscard]] Bool {
		using ViewType = bool;
		using PayloadFieldValueType = ViewType;
	};
	struct [[nodiscard]] Null {};
	struct [[nodiscard]] Int {
		using ViewType = int32_t;
		using PayloadFieldValueType = ViewType;
	};
	struct [[nodiscard]] Undefined {};
	struct [[nodiscard]] Composite {};
	struct [[nodiscard]] Tuple {};
	struct [[nodiscard]] Uuid {
		using ViewType = reindexer::Uuid;
		using PayloadFieldValueType = ViewType;
	};
	struct [[nodiscard]] FloatVector {};

private:
	template <template <typename> typename T>
	static constexpr bool ForAllTypes =
		T<Int64>::value && T<Double>::value && T<String>::value && T<Bool>::value && T<Null>::value && T<Int>::value &&
		T<Undefined>::value && T<Composite>::value && T<Tuple>::value && T<Uuid>::value && T<FloatVector>::value && T<Float>::value;
	template <template <typename> typename T>
	static constexpr bool ForAnyType =
		T<Int64>::value || T<Double>::value || T<String>::value || T<Bool>::value || T<Null>::value || T<Int>::value ||
		T<Undefined>::value || T<Composite>::value || T<Tuple>::value || T<Uuid>::value || T<FloatVector>::value || T<Float>::value;

	template <typename F, typename... Fs>
	struct [[nodiscard]] IsNoexcept {
		template <typename T>
		struct [[nodiscard]] Overloaded {
			static constexpr bool value = noexcept(std::declval<overloaded<F, Fs...>>()(std::declval<T>()));
		};
	};
	template <typename F>
	struct [[nodiscard]] IsNoexcept<F> {
		template <typename T>
		struct [[nodiscard]] Overloaded {
			static constexpr bool value = noexcept(std::declval<overloaded<F>>()(std::declval<T>()));
		};
	};

	template <typename... Fs>
	struct [[nodiscard]] OneOf {
		template <typename A>
		struct [[nodiscard]] IsInvocable {
			static constexpr bool value = std::disjunction_v<std::is_invocable<Fs, A>...>;
		};
	};

	template <typename T, typename Visitor>
	class [[nodiscard]] VisitorWrapper {
	public:
		explicit RX_ALWAYS_INLINE VisitorWrapper(Visitor& v) noexcept : visitor_{v} {}
		template <typename... Ts>
		RX_ALWAYS_INLINE auto operator()(Ts... vs) const noexcept(noexcept(std::declval<Visitor>()(Ts{}..., T{}))) {
			return visitor_(vs..., T{});
		}

	private:
		Visitor& visitor_;
	};

	enum class [[nodiscard]] KVT : uint8_t {
		Int64 = TAG_VARINT,
		Double = TAG_DOUBLE,
		String = TAG_STRING,
		Bool = TAG_BOOL,
		Null = TAG_NULL,
		Int = TAG_END + 1,
		Undefined,
		Composite,
		Tuple,
		Uuid,
		FloatVector,
		Float
	} value_{KVT::Undefined};
	RX_ALWAYS_INLINE constexpr explicit KeyValueType(KVT v) noexcept : value_{v} {}

public:
	RX_ALWAYS_INLINE constexpr KeyValueType(Int64) noexcept : value_{KVT::Int64} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Double) noexcept : value_{KVT::Double} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Float) noexcept : value_{KVT::Float} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(String) noexcept : value_{KVT::String} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Bool) noexcept : value_{KVT::Bool} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Null) noexcept : value_{KVT::Null} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Int) noexcept : value_{KVT::Int} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Undefined) noexcept : value_{KVT::Undefined} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Composite) noexcept : value_{KVT::Composite} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Tuple) noexcept : value_{KVT::Tuple} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(Uuid) noexcept : value_{KVT::Uuid} {}
	RX_ALWAYS_INLINE constexpr KeyValueType(KeyValueType::FloatVector) noexcept : value_{KVT::FloatVector} {}
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
			case TAG_FLOAT:
				value_ = KVT::Float;
				return;
			case TAG_ARRAY:
			case TAG_OBJECT:
			case TAG_END:
				break;
		}
		throwKVTException("Invalid tag type value for KeyValueType", t);
	}

	template <typename... Fs>
	RX_ALWAYS_INLINE auto EvaluateOneOf(overloaded<Fs...> f) const noexcept(ForAllTypes<IsNoexcept<Fs...>::template Overloaded>) {
		static_assert(ForAnyType<OneOf<Fs...>::template IsInvocable>);
		switch (value_) {
			case KVT::Int64:
				return f(Int64{});
			case KVT::Double:
				return f(Double{});
			case KVT::Float:
				return f(Float{});
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
			case KVT::FloatVector:
				return f(KeyValueType::FloatVector{});
		}
		assertrx(0);
		std::abort();
	}
	template <typename... Fs>
	RX_ALWAYS_INLINE auto EvaluateOneOf(Fs... fs) const noexcept(ForAllTypes<IsNoexcept<Fs...>::template Overloaded>) {
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
			case KVT::Float:
				static_assert(ForAllTypes<IsNoexcept<VisitorWrapper<Float, Visitor>>::template Overloaded>);
				return Visit(VisitorWrapper<Float, Visitor>{visitor}, t1);
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
			case KVT::FloatVector:
				static_assert(ForAllTypes<IsNoexcept<VisitorWrapper<KeyValueType::FloatVector, Visitor>>::template Overloaded>);
				return Visit(VisitorWrapper<KeyValueType::FloatVector, Visitor>{visitor}, t1);
		}
		assertrx(0);
		std::abort();
	}

	template <typename T>
	RX_ALWAYS_INLINE bool Is() const noexcept {	 // TODO
		static constexpr KeyValueType v{T{}};
		return v.value_ == value_;
	}
	template <typename T1, typename T2, typename... Ts>
	RX_ALWAYS_INLINE bool IsOneOf() const noexcept {
		return ((Is<T1>() || Is<T2>()) || ... || Is<Ts>());
	}
	RX_ALWAYS_INLINE bool IsSame(KeyValueType other) const noexcept { return value_ == other.value_; }
	RX_ALWAYS_INLINE TagType ToTagType() const {
		switch (value_) {
			case KVT::Int64:
			case KVT::Int:
				return TAG_VARINT;
			case KVT::Double:
				return TAG_DOUBLE;
			case KVT::Float:
				return TAG_FLOAT;
			case KVT::String:
				return TAG_STRING;
			case KVT::Bool:
				return TAG_BOOL;
			case KVT::Null:
			case KVT::Undefined:
				return TAG_NULL;
			case KVT::Uuid:
				return TAG_UUID;
			case KVT::FloatVector:
				throwKVTException("Can not convert value type into CJSON tag type directly", Name());
			case KVT::Composite:
			case KVT::Tuple:
				break;
		}
		throwKVTException("Unexpected value type", Name());
	}
	RX_ALWAYS_INLINE bool IsNumeric() const noexcept {
		switch (value_) {
			case KVT::Int64:
			case KVT::Double:
			case KVT::Float:
			case KVT::Int:
			case KVT::Bool:
				return true;
			case KVT::String:
			case KVT::Null:
			case KVT::Undefined:
			case KVT::Composite:
			case KVT::Tuple:
			case KVT::Uuid:
			case KVT::FloatVector:
				return false;
		}
		assertrx(0);
		std::abort();
	}
	std::string_view Name() const noexcept;

	template <typename T>
	static KeyValueType From();

	RX_ALWAYS_INLINE static KeyValueType FromNumber(int n) {
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
			case static_cast<int>(KVT::FloatVector):
			case static_cast<int>(KVT::Float):
				return KeyValueType{static_cast<KVT>(n)};
			default:
				throwKVTException("Invalid int value for KeyValueType", n);
		}
	}
	RX_ALWAYS_INLINE int ToNumber() const noexcept { return static_cast<int>(value_); }

private:
	[[noreturn]] static void throwKVTException(std::string_view msg, std::string_view param);
	[[noreturn]] static void throwKVTException(std::string_view msg, TagType);
	[[noreturn]] static void throwKVTException(std::string_view msg, int);
};

class key_string;
class Uuid;
struct p_string;

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

template <>
RX_ALWAYS_INLINE KeyValueType KeyValueType::From<std::string_view>() {
	return KeyValueType::String{};
}

template <>
RX_ALWAYS_INLINE KeyValueType KeyValueType::From<p_string>() {
	return KeyValueType::String{};
}

}  // namespace reindexer
